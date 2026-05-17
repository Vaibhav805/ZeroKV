// client/src/main.rs — Phase 4: correctness test under concurrent writes

use core::rdma_context::RdmaContext;
use core::handshake::{PeerInfo, run_client_handshake};
use core::table::{h1, h2, read_slot, Slot};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use core::ibv_gid;

const DEFAULT_SERVER: &str = "127.0.0.1:7471";
const TABLE_CAP:      usize = 1 << 17;
const SLOT_BYTES:     u32   = 64;
const HALF_BYTES:     u64   = TABLE_CAP as u64 * SLOT_BYTES as u64;

// Client MR must be large enough for two slots (128 bytes).
// We over-allocate to 4 KiB so BUF_LEN is never the problem.
const BUF_LEN: usize = 4096;

// Phase 4 config
const READER_THREADS: usize = 8;
const OPS_PER_THREAD: usize = 100_000;
const TEST_KEYS:      u64   = 100_000;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SERVER.to_string());

    // ── RDMA setup ─────────────────────────────────────────────────────────
    // RdmaContext allocates ctx.buf with align(64) and registers it as the MR.
    // ALL RDMA DMA targets must be within this buffer — we use offsets 0 and 64
    // for slot A and slot B respectively.
    let ctx = Arc::new(RdmaContext::new(BUF_LEN));
    info!(qpn = ctx.qpn, "RdmaContext ready");
    ctx.move_to_init();

    let remote = Arc::new(run_client_handshake(&ctx, &server_addr));
    let remote_gid = ibv_gid {
    raw: remote.gid,
};

ctx.connect_rtr(remote.qpn, remote.lid, remote_gid);
    ctx.connect_rts();
    info!("QP → RTS — starting Phase 4 concurrency test");

    // ── Quick latency check ────────────────────────────────────────────────
    info!("── Quick latency check (1000 reads) ──");
    let mut lats: Vec<u64> = Vec::with_capacity(1000);
    for i in 0u64..1000 {
        let key = make_key(i % TEST_KEYS);
        let t0  = Instant::now();
        rdma_get_sync(&ctx, &remote, &key);
        lats.push(t0.elapsed().as_micros() as u64);
    }
    print_stats(&lats);

    // ── Phase 4: concurrent correctness test ──────────────────────────────
    info!("── Phase 4: {} threads × {} GETs ──", READER_THREADS, OPS_PER_THREAD);

    let torn_reads   = Arc::new(AtomicU64::new(0));
    let correct_hits = Arc::new(AtomicU64::new(0));
    let misses       = Arc::new(AtomicU64::new(0));
    let mismatches   = Arc::new(AtomicU64::new(0));

    let t_start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..READER_THREADS {
        let ctx          = Arc::clone(&ctx);
        let remote       = Arc::clone(&remote);
        let torn_reads   = Arc::clone(&torn_reads);
        let correct      = Arc::clone(&correct_hits);
        let misses_c     = Arc::clone(&misses);
        let mismatches_c = Arc::clone(&mismatches);

        let handle = std::thread::spawn(move || {
            // IMPORTANT: RDMA DMA targets must be within the registered MR
            // (ctx.buf). ctx.buf is already allocated with align(64) in
            // RdmaContext::new, so offsets 0 and 64 are both 64-byte aligned.
            // We read slot A into ctx.buf[0..64] and slot B into ctx.buf[64..128].
            // Each thread uses ctx.buf — since only one outstanding RDMA op
            // exists at a time per thread (we poll before the next post), there
            // is no buffer collision within a single thread's op sequence.
            let buf = ctx.buf; // *mut u8, registered MR, align(64)

            for op in 0..OPS_PER_THREAD {
                let key_n = ((thread_id * OPS_PER_THREAD + op) as u64) % TEST_KEYS;
                let key   = make_key(key_n);

                match rdma_get_checking(&ctx, &remote, buf, &key) {
                    GetResult::Found(val) => {
                        let val_u64 = u64::from_le_bytes(val[..8].try_into().unwrap());
                        if val_u64 == 0 {
                            mismatches_c.fetch_add(1, Ordering::Relaxed);
                        } else {
                            correct.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    GetResult::Miss     => { misses_c.fetch_add(1, Ordering::Relaxed); }
                    GetResult::TornRead => { torn_reads.fetch_add(1, Ordering::Relaxed); }
                }
            }
        });
        handles.push(handle);
    }

    for h in handles { h.join().unwrap(); }
    let elapsed = t_start.elapsed();

    // ── Results ────────────────────────────────────────────────────────────
    let total    = (READER_THREADS * OPS_PER_THREAD) as u64;
    let torn     = torn_reads.load(Ordering::Relaxed);
    let correct  = correct_hits.load(Ordering::Relaxed);
    let miss     = misses.load(Ordering::Relaxed);
    let mismatch = mismatches.load(Ordering::Relaxed);
    let torn_pct = torn as f64 / total as f64 * 100.0;
    let ops_sec  = total as f64 / elapsed.as_secs_f64();

    println!("\n══ Phase 4 Results ══════════════════════════════");
    println!("  Total ops   : {total}");
    println!("  Correct hits: {correct}");
    println!("  Misses      : {miss}  (keys written after snapshot — OK)");
    println!("  Torn reads  : {torn}  ({torn_pct:.4}%)");
    println!("  Mismatches  : {mismatch}  (MUST BE ZERO)");
    println!("  Throughput  : {ops_sec:.0} ops/sec");
    println!("  Elapsed     : {:.2}s", elapsed.as_secs_f64());
    println!("═════════════════════════════════════════════════");

    if mismatch > 0 {
        eprintln!("✗ FAIL — {} torn value reads detected (seqlock broken)", mismatch);
        std::process::exit(1);
    }
    if torn_pct > 1.0 {
        eprintln!("✗ FAIL — torn read rate {torn_pct:.2}% exceeds 1% threshold");
        std::process::exit(1);
    }
    println!("✓ PASS — zero mismatches, torn rate {torn_pct:.4}%");
}

// ── GET result type ────────────────────────────────────────────────────────

enum GetResult {
    Found([u8; 24]),
    Miss,
    TornRead,
}

// ── single-slot GET with seqlock retry over RDMA ──────────────────────────
//
// `mr_buf` must be ctx.buf (the registered MR, align(64)).
// Slot A is DMA'd into mr_buf[0..64]  (local_offset 0).
// Slot B is DMA'd into mr_buf[64..128] (local_offset 64).
// Both offsets are 64-byte aligned because mr_buf itself is align(64).

fn rdma_get_checking(
    ctx:    &RdmaContext,
    remote: &PeerInfo,
    mr_buf: *mut u8,        // ctx.buf — registered MR, guaranteed align(64)
    key:    &[u8; 24],
) -> GetResult {
    for attempt in 0..4u64 {
        // ── slot A ────────────────────────────────────────────────────────
        let ia     = h1(key) & (TABLE_CAP - 1);
        let addr_a = remote.addr + ia as u64 * SLOT_BYTES as u64;
        unsafe { ctx.post_read(0xA0 + attempt, 0, SLOT_BYTES, addr_a, remote.rkey); }
        ctx.poll_one();

        // mr_buf is align(64) → casting to *const Slot is valid.
        let slot_a = unsafe { &*(mr_buf as *const Slot) };
        match read_slot(slot_a) {
            Some((k, v)) if &k == key => return GetResult::Found(v),
            None => {} // torn — fall through to slot B
            _    => {} // key mismatch — try slot B
        }

        // ── slot B ────────────────────────────────────────────────────────
        let ib     = h2(key) & (TABLE_CAP - 1);
        let addr_b = remote.addr + HALF_BYTES + ib as u64 * SLOT_BYTES as u64;
        // local_offset 64 → mr_buf+64, which is also 64-byte aligned.
        unsafe { ctx.post_read(0xB0 + attempt, 64, SLOT_BYTES, addr_b, remote.rkey); }
        ctx.poll_one();

        // mr_buf+64 is align(64) because mr_buf is align(64) and 64 % 64 == 0.
        let slot_b = unsafe { &*(mr_buf.add(64) as *const Slot) };
        match read_slot(slot_b) {
            Some((k, v)) if &k == key => return GetResult::Found(v),
            None => {
                // Both slots torn — spin and retry the whole attempt.
                std::hint::spin_loop();
                continue;
            }
            _ => return GetResult::Miss,
        }
    }

    GetResult::TornRead
}

// ── simple synchronous GET (latency benchmark) ────────────────────────────
//
// Also uses ctx.buf as the DMA target for the same alignment reasons.

fn rdma_get_sync(ctx: &RdmaContext, remote: &PeerInfo, key: &[u8; 24]) -> Option<[u8; 24]> {
    let mr_buf = ctx.buf; // align(64), registered MR

    for attempt in 0..4u64 {
        // slot A → mr_buf[0..64]
        let ia     = h1(key) & (TABLE_CAP - 1);
        let addr_a = remote.addr + ia as u64 * SLOT_BYTES as u64;
        unsafe { ctx.post_read(0xC0 + attempt, 0, SLOT_BYTES, addr_a, remote.rkey); }
        ctx.poll_one();

        let slot_a = unsafe { &*(mr_buf as *const Slot) };
        if let Some((k, v)) = read_slot(slot_a) {
            if &k == key { return Some(v); }
        }

        // slot B → mr_buf[64..128]
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

fn print_stats(lats: &[u64]) {
    let mut sorted = lats.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    let p = |pct: f64| sorted[((pct / 100.0 * n as f64) as usize).min(n - 1)];
    info!(
        p50  = p(50.0),
        p99  = p(99.0),
        p999 = p(99.9),
        max  = sorted[n - 1],
        "Latency (µs)"
    );
}