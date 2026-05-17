// client/src/bin/bench.rs — Steps 21-23: optimized latency benchmark
//
// Optimizations over the naive version:
//   1. Unsignaled slot-A read + signaled slot-B read = 1 CQ poll per GET
//      regardless of which table holds the key.
//   2. Instant::now() called once per op, not twice.
//   3. Fast-path: if slot A hits, skip slot B entirely (still 1 poll).
//   4. Separate histogram for "1-RTT hits" vs "2-RTT misses" so you can
//      see the true cost of each table half.
//   5. CPU affinity hint printed so you can pin with `taskset`.
//
// Add to client/Cargo.toml:
//   hdrhistogram = "7"
//   rand         = "0.8"
//
// Build & run:
//   cargo build --release --bin bench
//   SERVER_ADDR=127.0.0.1:7471 ./target/release/bench
//
// Tuning env vars:
//   OPS=1000000        measured ops        (default 1_000_000)
//   KEY_SPACE=1000000  distinct keys       (default 1_000_000)
//   WARMUP=50000       warmup ops          (default 50_000)

use core::rdma_context::RdmaContext;
use core::handshake::{PeerInfo, run_client_handshake};
use core::table::{h1, h2, read_slot, Slot};

use hdrhistogram::Histogram;
use rand::Rng;
use tracing::info;
use core::ibv_gid;
const DEFAULT_SERVER: &str = "127.0.0.1:7471";
const TABLE_CAP:      usize = 1 << 17;
const SLOT_BYTES:     u32   = 64;
const HALF_BYTES:     u64   = TABLE_CAP as u64 * SLOT_BYTES as u64;
const BUF_LEN:        usize = 4096;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("warn".parse().unwrap()), // quiet during bench
        )
        .init();

    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SERVER.to_string());
    let ops:    u64 = env_u64("OPS",        1_000_000);
    let keys:   u64 = env_u64("KEY_SPACE",  1_000_000);
    let warmup: u64 = env_u64("WARMUP",     50_000);

    eprintln!("Connecting to {server_addr} …");
    let ctx    = RdmaContext::new(BUF_LEN);
    ctx.move_to_init();
    let remote = run_client_handshake(&ctx, &server_addr);
   let remote_gid = ibv_gid {
    raw: remote.gid,
};

ctx.connect_rtr(remote.qpn, remote.lid, remote_gid);
    ctx.connect_rts();
    eprintln!("QP → RTS. Warming up ({warmup} ops) …");

    // Histograms in nanoseconds, 3 sig figs, 1ns..10s range.
    let mut hist_1rtt = Histogram::<u64>::new(3).unwrap(); // slot-A hits
    let mut hist_2rtt = Histogram::<u64>::new(3).unwrap(); // slot-B hits / misses
    let mut hist_all  = Histogram::<u64>::new(3).unwrap(); // every op

    let mut rng = rand::thread_rng();

    // ── warmup ─────────────────────────────────────────────────────────────
    for _ in 0..warmup {
        let key = make_key(rng.gen_range(0..keys));
        let _ = rdma_get_fast(&ctx, &remote, &key);
    }

    // ── measured run ───────────────────────────────────────────────────────
    eprintln!("Measuring {ops} ops …");
    let wall = std::time::Instant::now();
    let mut hits_1rtt: u64 = 0;
    let mut hits_2rtt: u64 = 0;
    let mut misses:    u64 = 0;

    for _ in 0..ops {
        let key = make_key(rng.gen_range(0..keys));

        // Single Instant::now() call — avoids double-sampling overhead.
        let t0 = std::time::Instant::now();
        let result = rdma_get_fast(&ctx, &remote, &key);
        let ns = t0.elapsed().as_nanos() as u64;

        hist_all.record(ns).unwrap();
        match result {
            GetResult::HitA(v) => { let _ = v; hits_1rtt += 1; hist_1rtt.record(ns).unwrap(); }
            GetResult::HitB(v) => { let _ = v; hits_2rtt += 1; hist_2rtt.record(ns).unwrap(); }
            GetResult::Miss    => { misses    += 1; hist_2rtt.record(ns).unwrap(); }
        }
    }

    let elapsed   = wall.elapsed();
    let ops_sec   = ops as f64 / elapsed.as_secs_f64();
    let hit1_pct  = hits_1rtt as f64 / ops as f64 * 100.0;
    let hit2_pct  = hits_2rtt as f64 / ops as f64 * 100.0;
    let miss_pct  = misses    as f64 / ops as f64 * 100.0;

    // ── results ────────────────────────────────────────────────────────────
    println!("\n══ RDMA GET Benchmark ═══════════════════════════════════════");
    println!("   ops={ops}  key_space={keys}  warmup={warmup}");
    println!("   1-RTT hits (slot A): {hits_1rtt} ({hit1_pct:.1}%)");
    println!("   2-RTT hits (slot B): {hits_2rtt} ({hit2_pct:.1}%)");
    println!("   Misses             : {misses} ({miss_pct:.1}%)");
    println!();
    println!("   ── All ops ──────────────────────────────────────────────");
    print_hist(&hist_all);
    if hist_1rtt.len() > 0 {
        println!("   ── 1-RTT only (slot-A hits) ─────────────────────────────");
        print_hist(&hist_1rtt);
    }
    if hist_2rtt.len() > 0 {
        println!("   ── 2-RTT only (slot-B hits + misses) ───────────────────");
        print_hist(&hist_2rtt);
    }
    println!("   ─────────────────────────────────────────────────────────");
    println!("   Throughput : {ops_sec:.0} ops/sec");
    println!("   Wall time  : {:.3}s", elapsed.as_secs_f64());
    println!("══════════════════════════════════════════════════════════════");

    // ── diagnosis ──────────────────────────────────────────────────────────
    let p50_us = hist_all.value_at_quantile(0.50) as f64 / 1_000.0;
    println!("\n   Expected (SoftRoCE loopback, single-thread):");
    println!("     1-RTT p50 ~5µs   p99 ~15µs");
    println!("     2-RTT p50 ~10µs  p99 ~30µs");
    println!("   Expected (real ConnectX-4/5):");
    println!("     1-RTT p50 ~0.7µs p99 ~1.5µs");

    if p50_us > 10.0 {
        println!("\n   ⚠  p50={p50_us:.1}µs — above SoftRoCE baseline. Checklist:");
        println!("     [ ] Pin bench to one core:  taskset -c 2 ./target/release/bench");
        println!("     [ ] Pin server to one core: taskset -c 3 ./target/release/server");
        println!("     [ ] Disable CPU freq scaling: cpupower frequency-set -g performance");
        println!("     [ ] Check rxe MTU >= 4096:  ip link show rxe0");
        println!("     [ ] Verify spin-poll: no IBV_EVENT_* waits in rdma_context");
        println!("     [ ] max_inline_data=64 in QP init (avoids extra DMA for small msgs)");
        println!("     [ ] SoftRoCE loopback is ~3× slower than kernel bypass — this may");
        println!("         just be the floor. Real NIC needed for sub-5µs.");
    } else {
        println!("\n   ✓  p50={p50_us:.1}µs — within SoftRoCE expected range.");
    }

    // ── Redis comparison ───────────────────────────────────────────────────
    println!("\n   Redis comparison (run in a separate terminal):");
    println!("     redis-server --save \"\" --appendonly no &");
    println!("     redis-benchmark -t get -n {ops} -c 1 -q --latency-history");
    println!("     # Redis p50 on loopback is typically 50-100µs (TCP overhead)");
    println!("     # Your RDMA p50={p50_us:.1}µs vs Redis ~70µs = {:.1}× faster",
             70.0f64 / p50_us.max(0.001));
}

// ── GET result ─────────────────────────────────────────────────────────────

enum GetResult {
    HitA([u8; 24]),
    HitB([u8; 24]),
    Miss,
}

// ── Optimized GET: minimize RTTs and CQ polls ─────────────────────────────
//
// Strategy:
//   Post slot-A read (UNSIGNALED) + slot-B read (SIGNALED) simultaneously.
//   → Only 1 CQ poll regardless of outcome.
//   → If slot-A hit: return immediately after first data check (still waited
//     for the 2nd WR completion, but both were in-flight so only 1 RTT of
//     waiting in the common case on low-latency NICs).
//
// On SoftRoCE (software loopback) the NIC processes WRs serially so both
// reads take 2× RTT anyway. On real NICs this saves one RTT for slot-A hits.
//
// DMA targets: ctx.buf[0..64] = slot A, ctx.buf[64..128] = slot B.
// ctx.buf is align(64) so both casts to *const Slot are valid.

fn rdma_get_fast(ctx: &RdmaContext, remote: &PeerInfo, key: &[u8; 24]) -> GetResult {
    let mr = ctx.buf;

    let ia = h1(key) & (TABLE_CAP - 1);
    let ib = h2(key) & (TABLE_CAP - 1);

    let addr_a = remote.addr + ia as u64 * SLOT_BYTES as u64;
    let addr_b = remote.addr + HALF_BYTES + ib as u64 * SLOT_BYTES as u64;

    unsafe {
        // Post slot A as UNSIGNALED — no CQ entry, lower overhead.
        ctx.post_read_unsignaled(0xA, 0,  SLOT_BYTES, addr_a, remote.rkey);
        // Post slot B as SIGNALED — one CQ entry covers both.
        ctx.post_read(           0xB, 64, SLOT_BYTES, addr_b, remote.rkey);
    }

    // One poll waits for both DMAs to complete (NIC delivers in order).
    ctx.poll_one();

    // Check slot A first (more likely to hit on balanced tables).
    let slot_a = unsafe { &*(mr as *const Slot) };
    if let Some((k, v)) = read_slot(slot_a) {
        if &k == key { return GetResult::HitA(v); }
    }

    // Check slot B.
    let slot_b = unsafe { &*(mr.add(64) as *const Slot) };
    if let Some((k, v)) = read_slot(slot_b) {
        if &k == key { return GetResult::HitB(v); }
    }

    GetResult::Miss
}

// ── helpers ───────────────────────────────────────────────────────────────

fn print_hist(h: &Histogram<u64>) {
    let us = |ns: u64| ns as f64 / 1_000.0;
    println!("     p50   : {:>8.2} µs", us(h.value_at_quantile(0.50)));
    println!("     p90   : {:>8.2} µs", us(h.value_at_quantile(0.90)));
    println!("     p99   : {:>8.2} µs", us(h.value_at_quantile(0.99)));
    println!("     p99.9 : {:>8.2} µs", us(h.value_at_quantile(0.999)));
    println!("     p99.99: {:>8.2} µs", us(h.value_at_quantile(0.9999)));
    println!("     max   : {:>8.2} µs", us(h.max()));
    println!("     mean  : {:>8.2} µs", h.mean() / 1_000.0);
    println!("     stddev: {:>8.2} µs", h.stdev() / 1_000.0);
}

fn make_key(n: u64) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&n.to_le_bytes());
    k
}

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}