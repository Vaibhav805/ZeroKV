// client/src/main.rs — Phase 6: 95% GET / 5% PUT mixed workload
//
// KEY FIX: each worker thread owns its own RdmaContext (QP + CQ + MR).
// Sharing one QP across threads causes CQ races → error codes 5 / 10:
//   - Thread A posts a READ; Thread B calls poll_one() and steals the WC.
//   - Thread A's poll_one() then either hangs or gets an unrelated error WC.
// Solution: one handshake connection per thread, independent QPs throughout.

use core::rdma_context::RdmaContext;
use core::handshake::{PeerInfo, run_client_handshake};
use core::table::{h1, h2, read_slot, Slot};
use core::msgbuf::{
    build_put_payload, core_for_key,
    NUM_CORES, SLOTS_PER_CORE, MSG_BYTES,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use core::ibv_gid;

const DEFAULT_SERVER: &str = "127.0.0.1:7471";
const TABLE_CAP:      usize = 1 << 17;
const SLOT_BYTES:     u32   = 64;
const HALF_BYTES:     u64   = TABLE_CAP as u64 * SLOT_BYTES as u64;

// Each thread's MR — 4 KiB covers:
//   [0..64]    slot A GET target
//   [64..128]  slot B GET target
//   [128..256] PUT payload staging (128-byte aligned)
const BUF_LEN:            usize = 4096;
const PUT_STAGING_OFFSET: usize = 128;

// Phase 6 workload config
const READER_THREADS: usize = 8;
const OPS_PER_THREAD: usize = 100_000;
const TEST_KEYS:      u64   = 100_000;
const PUT_RATIO_PCT:  u64   = 5;

// Set to your Phase 5 GET p99 after first passing run to enable regression check.
const PHASE5_GET_P99_US: u64 = 0;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SERVER.to_string());

    // ── Warm-up: single-threaded latency check with its own QP ────────────
    info!("── Quick latency check (1 000 GETs) ──");
    {
        let ctx = RdmaContext::new(BUF_LEN);
        info!(qpn = ctx.qpn, "Warm-up RdmaContext ready");
        ctx.move_to_init();
        let remote = run_client_handshake(&ctx, &server_addr);
        let gid = ibv_gid { raw: remote.gid };
        ctx.connect_rtr(remote.qpn, remote.lid, gid);
        ctx.connect_rts();
        info!("Warm-up QP -> RTS");

        let mut lats: Vec<u64> = Vec::with_capacity(1000);
        for i in 0u64..1000 {
            let key = make_key(i % TEST_KEYS);
            let t0  = Instant::now();
            rdma_get_sync(&ctx, &remote, &key);
            lats.push(t0.elapsed().as_micros() as u64);
        }
        print_stats("GET warm-up", &lats);
    } // warm-up ctx + QP dropped here

    // ── Phase 6: concurrent mixed workload ────────────────────────────────
    info!(
        "── Phase 6: {} threads x {} ops  ({}% PUT / {}% GET) ──",
        READER_THREADS, OPS_PER_THREAD,
        PUT_RATIO_PCT, 100 - PUT_RATIO_PCT
    );

    let correct_gets  = Arc::new(AtomicU64::new(0));
    let miss_gets     = Arc::new(AtomicU64::new(0));
    let torn_gets     = Arc::new(AtomicU64::new(0));
    let mismatch_gets = Arc::new(AtomicU64::new(0));
    let put_count     = Arc::new(AtomicU64::new(0));

    let get_lats_all: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(
            READER_THREADS * OPS_PER_THREAD,
        )));
    let put_lats_all: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(
            READER_THREADS * OPS_PER_THREAD / 20,
        )));

    let server_addr = Arc::new(server_addr);
    let t_start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..READER_THREADS {
        let server_addr  = Arc::clone(&server_addr);
        let correct      = Arc::clone(&correct_gets);
        let misses_c     = Arc::clone(&miss_gets);
        let torn_c       = Arc::clone(&torn_gets);
        let mismatch_c   = Arc::clone(&mismatch_gets);
        let puts_c       = Arc::clone(&put_count);
        let get_lats_all = Arc::clone(&get_lats_all);
        let put_lats_all = Arc::clone(&put_lats_all);

        let handle = std::thread::spawn(move || {
            // ── Each thread: own RdmaContext, own QP, own MR ──────────────
            // This is the critical fix: no shared QP, no CQ races.
            let ctx = RdmaContext::new(BUF_LEN);
            ctx.move_to_init();
            let remote = run_client_handshake(&ctx, &server_addr);
            let gid = ibv_gid { raw: remote.gid };
            ctx.connect_rtr(remote.qpn, remote.lid, gid);
            ctx.connect_rts();

            let buf = ctx.buf; // this thread's own MR buffer, align(64)

            let mut local_get_lats: Vec<u64> = Vec::with_capacity(OPS_PER_THREAD);
            let mut local_put_lats: Vec<u64> = Vec::with_capacity(OPS_PER_THREAD / 20);
            let mut slot_cursors = [0usize; NUM_CORES];

            for op in 0..OPS_PER_THREAD {
                let key_n  = ((thread_id * OPS_PER_THREAD + op) as u64) % TEST_KEYS;
                let key    = make_key(key_n);
                let is_put = (key_n % 100) < PUT_RATIO_PCT;

                if is_put {
                    let value = make_val(key_n * 3 + 7);
                    let t0    = Instant::now();
                    rdma_put(&ctx, &remote, buf, &key, &value, &mut slot_cursors);
                    local_put_lats.push(t0.elapsed().as_micros() as u64);
                    puts_c.fetch_add(1, Ordering::Relaxed);
                } else {
                    let t0 = Instant::now();
                    match rdma_get_checking(&ctx, &remote, buf, &key) {
                        GetResult::Found(val) => {
                            local_get_lats.push(t0.elapsed().as_micros() as u64);
                            let val_u64 = u64::from_le_bytes(val[..8].try_into().unwrap());
                            if val_u64 == 0 {
                                mismatch_c.fetch_add(1, Ordering::Relaxed);
                            } else {
                                correct.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        GetResult::Miss => {
                            local_get_lats.push(t0.elapsed().as_micros() as u64);
                            misses_c.fetch_add(1, Ordering::Relaxed);
                        }
                        GetResult::TornRead => {
                            local_get_lats.push(t0.elapsed().as_micros() as u64);
                            torn_c.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            get_lats_all.lock().unwrap().extend_from_slice(&local_get_lats);
            put_lats_all.lock().unwrap().extend_from_slice(&local_put_lats);
            // ctx drops here — QP/CQ/MR cleaned up per thread
        });
        handles.push(handle);
    }

    for h in handles { h.join().unwrap(); }
    let elapsed = t_start.elapsed();

    // ── Results ────────────────────────────────────────────────────────────
    let total    = (READER_THREADS * OPS_PER_THREAD) as u64;
    let torn     = torn_gets.load(Ordering::Relaxed);
    let correct  = correct_gets.load(Ordering::Relaxed);
    let miss     = miss_gets.load(Ordering::Relaxed);
    let mismatch = mismatch_gets.load(Ordering::Relaxed);
    let puts     = put_count.load(Ordering::Relaxed);
    let gets     = total - puts;
    let torn_pct = if gets > 0 { torn as f64 / gets as f64 * 100.0 } else { 0.0 };
    let ops_sec  = total as f64 / elapsed.as_secs_f64();

    println!("\n====== Phase 6 Results ==============================");
    println!("  Total ops      : {total}");
    println!("  GETs issued    : {gets}");
    println!("  PUTs issued    : {puts}");
    println!("  Correct hits   : {correct}");
    println!("  Misses         : {miss}  (new keys still in-flight -- OK)");
    println!("  Torn reads     : {torn}  ({torn_pct:.4}%)");
    println!("  Mismatches     : {mismatch}  (MUST BE ZERO)");
    println!("  Throughput     : {ops_sec:.0} ops/sec");
    println!("  Elapsed        : {:.2}s", elapsed.as_secs_f64());

    let mut get_lats = get_lats_all.lock().unwrap().clone();
    let put_lats = put_lats_all.lock().unwrap().clone();
    println!();
    print_stats("GET", &get_lats);
    if !put_lats.is_empty() {
        print_stats("PUT", &put_lats);
    }
    println!("=====================================================");

    let mut fail = false;

    if mismatch > 0 {
        eprintln!("FAIL -- {} mismatched value reads (seqlock broken)", mismatch);
        fail = true;
    }
    if torn_pct > 1.0 {
        eprintln!("FAIL -- torn read rate {torn_pct:.2}% exceeds 1%");
        fail = true;
    }
    if PHASE5_GET_P99_US > 0 && !get_lats.is_empty() {
        get_lats.sort_unstable();
        let n       = get_lats.len();
        let p99     = get_lats[((99.0 / 100.0 * n as f64) as usize).min(n - 1)];
        let ceiling = (PHASE5_GET_P99_US as f64 * 1.05) as u64;
        if p99 > ceiling {
            eprintln!(
                "FAIL -- GET p99 {p99} us exceeds ceiling {ceiling} us \
                 (baseline {PHASE5_GET_P99_US} us + 5%)"
            );
            fail = true;
        } else {
            println!("PASS GET p99 {p99} us <= ceiling {ceiling} us");
        }
    }

    if fail { std::process::exit(1); }
    println!("PASS -- zero mismatches, torn rate {torn_pct:.4}%, mixed workload OK");
}

// ── PUT path ──────────────────────────────────────────────────────────────

fn rdma_put(
    ctx:          &RdmaContext,
    remote:       &PeerInfo,
    mr_buf:       *mut u8,
    key:          &[u8; 24],
    value:        &[u8; 24],
    slot_cursors: &mut [usize; NUM_CORES],
) {
    let core     = core_for_key(key);
    let slot_idx = slot_cursors[core];
    slot_cursors[core] = (slot_idx + 1) % SLOTS_PER_CORE;

    let staging = unsafe { mr_buf.add(PUT_STAGING_OFFSET) };
    let payload  = build_put_payload(key, value);
    unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), staging, 128); }

    let remote_slot_addr = remote.msgbuf_addr
        + (core * SLOTS_PER_CORE + slot_idx) as u64 * MSG_BYTES as u64;

    let wr_id = 0xEE00_0000 | ((core as u64) << 8) | (slot_idx as u64);
    unsafe {
        ctx.post_write(
            wr_id,
            PUT_STAGING_OFFSET as u32,
            128,
            remote_slot_addr,
            remote.msgbuf_rkey,
        );
    }
}

// ── GET result type ───────────────────────────────────────────────────────

enum GetResult {
    Found([u8; 24]),
    Miss,
    TornRead,
}

// ── GET path ──────────────────────────────────────────────────────────────

fn rdma_get_checking(
    ctx:    &RdmaContext,
    remote: &PeerInfo,
    mr_buf: *mut u8,
    key:    &[u8; 24],
) -> GetResult {
    for attempt in 0..4u64 {
        let ia     = h1(key) & (TABLE_CAP - 1);
        let addr_a = remote.addr + ia as u64 * SLOT_BYTES as u64;
        unsafe { ctx.post_read(0xA0 + attempt, 0, SLOT_BYTES, addr_a, remote.rkey); }
        ctx.poll_one();

        let slot_a = unsafe { &*(mr_buf as *const Slot) };
        match read_slot(slot_a) {
            Some((k, v)) if &k == key => return GetResult::Found(v),
            _ => {}
        }

        let ib     = h2(key) & (TABLE_CAP - 1);
        let addr_b = remote.addr + HALF_BYTES + ib as u64 * SLOT_BYTES as u64;
        unsafe { ctx.post_read(0xB0 + attempt, 64, SLOT_BYTES, addr_b, remote.rkey); }
        ctx.poll_one();

        let slot_b = unsafe { &*(mr_buf.add(64) as *const Slot) };
        match read_slot(slot_b) {
            Some((k, v)) if &k == key => return GetResult::Found(v),
            None => { std::hint::spin_loop(); continue; }
            _    => return GetResult::Miss,
        }
    }
    GetResult::TornRead
}

fn rdma_get_sync(ctx: &RdmaContext, remote: &PeerInfo, key: &[u8; 24]) -> Option<[u8; 24]> {
    let mr_buf = ctx.buf;
    for attempt in 0..4u64 {
        let ia     = h1(key) & (TABLE_CAP - 1);
        let addr_a = remote.addr + ia as u64 * SLOT_BYTES as u64;
        unsafe { ctx.post_read(0xC0 + attempt, 0, SLOT_BYTES, addr_a, remote.rkey); }
        ctx.poll_one();

        let slot_a = unsafe { &*(mr_buf as *const Slot) };
        if let Some((k, v)) = read_slot(slot_a) {
            if &k == key { return Some(v); }
        }

        let ib     = h2(key) & (TABLE_CAP - 1);
        let addr_b = remote.addr + HALF_BYTES + ib as u64 * SLOT_BYTES as u64;
        unsafe { ctx.post_read(0xD0 + attempt, 64, SLOT_BYTES, addr_b, remote.rkey); }
        ctx.poll_one();

        let slot_b = unsafe { &*(mr_buf.add(64) as *const Slot) };
        if let Some((k, v)) = read_slot(slot_b) {
            if &k == key { return Some(v); }
        }
    }
    None
}

// ── helpers ───────────────────────────────────────────────────────────────

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

fn print_stats(label: &str, lats: &[u64]) {
    if lats.is_empty() { info!("{label} -- no samples"); return; }
    let mut sorted = lats.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let p = |pct: f64| sorted[((pct / 100.0 * n as f64) as usize).min(n - 1)];
    info!(
        label, p50 = p(50.0), p99 = p(99.0), p999 = p(99.9),
        max = sorted[n - 1], "Latency (us)"
    );
    println!(
        "  {label:>12} latency: p50={} p99={} p999={} max={} us  (n={n})",
        p(50.0), p(99.0), p(99.9), sorted[n-1]
    );
}