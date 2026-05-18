// server/src/main.rs — Phase 6: HERD-pattern PUT via message buffer
//
// FIX: server creates one RdmaContext (QP) per client connection.
//
// Previously the server created ONE table_ctx and connected it only to
// remotes[0] (the warmup client). The 8 worker-thread QPs on the client
// each need a paired server QP to accept their RDMA READs and WRITEs.
// A single server QP connected to only the first client means the other
// 7 client QPs have no valid RC path → every RDMA op either hangs or
// returns an error → manifests as ~90% misses / stale zero reads.
//
// Solution: accept N_CLIENTS connections and for each one create a fresh
// RdmaContext (its own QP + CQ), do the RTR/RTS handshake, and keep
// all contexts alive for the duration of the benchmark.  The hash table
// buffer is shared across all server contexts via a raw pointer.

use core::rdma_context::RdmaContext;
use core::handshake::PeerInfo;
use core::table::Table;
use core::msgbuf::{MsgBuf, NUM_CORES, MSGBUF_BYTES};

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;

const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:7471";
const TABLE_CAP:      usize = 1 << 17;
const TABLE_BYTES:    usize = TABLE_CAP * 64 * 2;

const READER_THREADS: usize = 8;
// 1 warmup connection + 8 worker threads = 9 total.
const DEFAULT_N_CLIENTS: usize = READER_THREADS + 1;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    // ── Shared table buffer ────────────────────────────────────────────────
    // We allocate a single large buffer for the hash table, then register it
    // inside each per-client RdmaContext so every QP can serve READs from it.
    // The buffer is intentionally leaked (Box::into_raw) so raw pointers into
    // it remain valid for the process lifetime.
    let table_buf: *mut u8 = {
        let layout = std::alloc::Layout::from_size_align(TABLE_BYTES, 64).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "table alloc failed");
        ptr
    };

    info!("Loading 100_000 baseline keys into shared table buffer …");
    {
        let mut t = unsafe { Table::from_mr(table_buf, TABLE_CAP) };
        for i in 0u64..100_000 {
            assert!(
                t.insert(&make_key(i), &make_val(i * 2 + 1)),
                "baseline insert failed at {i}"
            );
        }
    }
    info!("Baseline loaded");

    // ── MsgBuf MR (one, shared — clients WRITE into it) ───────────────────
    // The msgbuf does not need to be registered per-client; clients WRITE
    // into it using the rkey we advertise.  One registration is enough.
    let msgbuf_ctx = Arc::new(RdmaContext::new(MSGBUF_BYTES));
    assert_eq!(
        msgbuf_ctx.buf as usize % 128, 0,
        "msgbuf base must be 128-byte aligned"
    );
    unsafe { std::ptr::write_bytes(msgbuf_ctx.buf, 0, MSGBUF_BYTES); }
    let msgbuf = Arc::new(unsafe { MsgBuf::from_mr(msgbuf_ctx.buf) });
    let msgbuf_addr = msgbuf_ctx.buf as u64;
    let msgbuf_rkey = msgbuf_ctx.rkey;
    info!(addr = msgbuf_addr, rkey = msgbuf_rkey, "MsgBuf MR ready ({MSGBUF_BYTES} bytes)");

    // ── Create one RdmaContext per client ──────────────────────────────────
    // Each context registers the SAME physical table buffer.  Because we use
    // ibv_reg_mr on the same address range, the kernel gives each a distinct
    // lkey/rkey but they all point to the same memory.
    //
    // We create them all before accepting any connections so their QPs are in
    // RESET state and ready to transition once we have the remote PeerInfo.
    let n_clients = env_usize("N_CLIENTS", DEFAULT_N_CLIENTS);
    let listen_addr = std::env::var("LISTEN_ADDR")
        .unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string());

    info!("Creating {n_clients} server-side RdmaContexts …");
    let server_ctxs: Vec<Arc<RdmaContext>> = (0..n_clients)
        .map(|i| {
            // Each context gets its own MR over the shared table buffer.
            // BUF_LEN must cover the whole table so the rkey is valid for any slot.
            let ctx = Arc::new(RdmaContext::new_with_buf(table_buf, TABLE_BYTES));
            ctx.move_to_init();
            info!(i, qpn = ctx.qpn, lid = ctx.lid, gid_index = ctx.gid_index, "Server ctx ready");
            ctx
        })
        .collect();

    // ── Handshake: accept N_CLIENTS, advertise msgbuf to each ─────────────
    // We need to advertise each server context's own QPN/GID/rkey.
    // run_server_handshake_n uses a single ctx for all — instead we do the
    // TCP exchange manually per-connection so each client gets its own QPN.
    info!("Waiting for {n_clients} client connections on {listen_addr} …");
    let remotes = accept_n_clients(
        &server_ctxs,
        &listen_addr,
        n_clients,
        msgbuf_addr,
        msgbuf_rkey,
    );
    info!("All {n_clients} clients connected");

    // ── RTR / RTS for every server QP ─────────────────────────────────────
    for (i, (ctx, remote)) in server_ctxs.iter().zip(remotes.iter()).enumerate() {
        ctx.connect_rtr(remote.qpn, remote.lid, remote.ibv_gid());
        ctx.connect_rts();
        info!(i, "Server QP {i} → RTS (paired with client qpn={})", remote.qpn);
    }
    info!("All server QPs → RTS — pollers starting …");

    // ── Per-core poller threads ────────────────────────────────────────────
    let puts_done  = Arc::new(AtomicU64::new(0));
    let stop_cores = Arc::new(AtomicBool::new(false));

    for core_id in 0..NUM_CORES {
        let msgbuf    = Arc::clone(&msgbuf);
        let puts_done = Arc::clone(&puts_done);
        let stop      = Arc::clone(&stop_cores);
        let table_ptr = table_buf as usize; // usize is Send

        std::thread::spawn(move || {
            let mut table = unsafe { Table::from_mr(table_ptr as *mut u8, TABLE_CAP) };
            info!(core_id, "Poller started");

            while !stop.load(Ordering::Relaxed) {
                let n = msgbuf.poll_core(core_id, |key, value| {
                    if !table.insert(key, value) {
                        tracing::warn!(core_id, "table full — PUT dropped");
                    }
                });
                if n > 0 {
                    puts_done.fetch_add(n as u64, Ordering::Relaxed);
                } else {
                    std::hint::spin_loop();
                }
            }
            info!(core_id, "Poller stopped");
        });
    }

    // ── Stats loop ─────────────────────────────────────────────────────────
    info!("Server ready — {} core pollers active. Ctrl-C to stop.", NUM_CORES);
    let mut last = 0u64;
    loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
        let now = puts_done.load(Ordering::Relaxed);
        info!("PUTs handled: {} total (+{} in last 5s)", now, now - last);
        last = now;
    }
}

/// Accept exactly `n` TCP connections, one per server context.
/// Each connection exchanges PeerInfo so every client gets the correct
/// server QPN for its dedicated QP.
fn accept_n_clients(
    server_ctxs: &[Arc<RdmaContext>],
    listen_addr: &str,
    n: usize,
    msgbuf_addr: u64,
    msgbuf_rkey: u32,
) -> Vec<PeerInfo> {
    use std::net::TcpListener;
    use core::handshake::exchange_with_ctx;

    let listener = TcpListener::bind(listen_addr).unwrap_or_else(|e| {
        panic!(
            "bind {listen_addr}: {e}. Another server is probably already using this port. Stop it or run with LISTEN_ADDR=0.0.0.0:<free-port> and set client SERVER_ADDR accordingly."
        )
    });
    info!("Listening on {listen_addr} for {n} connections");

    let mut remotes = Vec::with_capacity(n);
    for i in 0..n {
        let (stream, peer) = listener.accept()
            .unwrap_or_else(|e| panic!("accept #{i}: {e}"));
        info!("Accepted #{i} from {peer} → pairing with server ctx qpn={}", server_ctxs[i].qpn);
        let remote = exchange_with_ctx(&server_ctxs[i], stream, msgbuf_addr, msgbuf_rkey);
        remotes.push(remote);
    }
    remotes
}

fn make_key(n: u64) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&n.to_le_bytes());
    k
}
fn make_val(n: u64) -> [u8; 24] {
    let mut v = [0u8; 24];
    v[..8].copy_from_slice(&n.to_le_bytes());
    v
}

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
