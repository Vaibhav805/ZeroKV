// server/src/main.rs — Phase 4: concurrent writer + RDMA readers

use core::rdma_context::RdmaContext;
use core::handshake::run_server_handshake;
use core::table::{Table, write_slot};

use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;
use core::ibv_gid;

const LISTEN_ADDR: &str  = "0.0.0.0:7471";
const TABLE_CAP:   usize = 1 << 17;
const TABLE_BYTES: usize = TABLE_CAP * 64 * 2;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    // ── RDMA context — MR is the table buffer ──────────────────────────────
    let ctx = Arc::new(RdmaContext::new(TABLE_BYTES));
    info!(qpn = ctx.qpn, lid = ctx.lid,
          gid = ?unsafe { ctx.gid.raw }, "RdmaContext ready");

    // ── build table directly in MR buffer ─────────────────────────────────
    // Zero the buffer first (MR was alloc_zeroed but be explicit).
    unsafe { std::ptr::write_bytes(ctx.buf, 0, TABLE_BYTES); }

    let mut table = unsafe { Table::from_mr(ctx.buf, TABLE_CAP) };

    info!("Loading 100_000 baseline keys into MR …");
    for i in 0u64..100_000 {
        assert!(table.insert(&make_key(i), &make_val(i * 2 + 1)),
                "insert failed at {i}");
    }
    info!("Baseline loaded");

    // ── QP setup ───────────────────────────────────────────────────────────
    ctx.move_to_init();
    info!("QP → INIT");

    let addr_b = ctx.buf as u64 + (TABLE_CAP * 64) as u64;
    let remote = run_server_handshake(&ctx, LISTEN_ADDR, addr_b);
    let remote_gid = ibv_gid {
    raw: remote.gid,
};

ctx.connect_rtr(remote.qpn, remote.lid, remote_gid);

    info!("QP → RTR");
    ctx.connect_rts();
    info!("QP → RTS");

    // ── Phase 4: writer thread ─────────────────────────────────────────────
    // Shared counters for monitoring.
    let writes_done  = Arc::new(AtomicU64::new(0));
    let stop_writer  = Arc::new(AtomicBool::new(false));

    {
        let writes_done = Arc::clone(&writes_done);
        let stop        = Arc::clone(&stop_writer);
        // SAFETY: table lives in ctx.buf which is alive for the process lifetime.
        // Writer thread has exclusive write access; readers use seqlock.
        let buf     = ctx.buf as usize; // send raw addr across thread boundary
        let cap     = TABLE_CAP;

        std::thread::spawn(move || {
            let mut table = unsafe { Table::from_mr(buf as *mut u8, cap) };
            let mut i = 100_000u64; // start after baseline keys
            while !stop.load(Ordering::Relaxed) {
                // Insert a new key every iteration.
                let key = make_key(i);
                let val = make_val(i * 3 + 7); // distinct pattern from baseline
                table.insert(&key, &val);
                writes_done.fetch_add(1, Ordering::Relaxed);
                i += 1;
                // Also update some baseline keys to create concurrent write pressure.
                if i % 100 == 0 {
                    let update_key = make_key(i % 100_000);
                    let update_val = make_val(i); // updated value
                    table.insert(&update_key, &update_val);
                }
            }
            info!("Writer thread stopped after {} writes", writes_done.load(Ordering::Relaxed));
        });
    }

    // ── stats loop ─────────────────────────────────────────────────────────
    info!("Server running — writer active. Ctrl-C to stop.");
    let mut last = 0u64;
    loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
        let now = writes_done.load(Ordering::Relaxed);
        info!("Writer: {} total inserts (+{} in last 5s)", now, now - last);
        last = now;
    }
}

fn make_key(n: u64) -> [u8; 24] {
    let mut k = [0u8; 24]; k[..8].copy_from_slice(&n.to_le_bytes()); k
}
fn make_val(n: u64) -> [u8; 24] {
    let mut v = [0u8; 24]; v[..8].copy_from_slice(&n.to_le_bytes()); v
}