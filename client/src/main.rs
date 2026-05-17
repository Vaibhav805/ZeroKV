// client/src/main.rs
//
// Phase 1: QP setup + single RDMA READ smoke-test (Step 7).
// The client:
//   1. Creates an RdmaContext.
//   2. Runs the TCP handshake with the server.
//   3. Drives its QP through INIT → RTR → RTS.
//   4. Issues one RDMA READ WR pointing at the server's MR offset 0.
//   5. Polls the CQ.
//   6. Prints the local buffer (should be "rdma works").
//   7. Measures round-trip time with Instant::now().

use core::rdma_context::{RdmaContext};
use core::handshake::{PeerInfo, run_client_handshake};
use std::time::Instant;
use tracing::info;

/// Default: connect to localhost. Override with SERVER_ADDR env var.
const DEFAULT_SERVER: &str = "127.0.0.1:7471";
/// How many bytes to read — enough for "rdma works\0".
const READ_LEN: u32 = 16;
/// Client-side scratch buffer (1 MiB; only the first READ_LEN bytes are used).
const BUF_LEN: usize = 1 << 20;

fn main() {
    // ── logging ────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SERVER.to_string());

    // ── Step 4: create RDMA context ────────────────────────────────────────
    info!("Opening RDMA device …");
    let ctx = RdmaContext::new(BUF_LEN);
    info!(qpn = ctx.qpn, lid = ctx.lid, "RdmaContext ready");

    // ── Step 4 (QP state machine): RESET → INIT ───────────────────────────
    ctx.move_to_init();
    info!("QP moved to INIT");

    // ── Step 5: TCP handshake ──────────────────────────────────────────────
    let remote: PeerInfo = run_client_handshake(&ctx, &server_addr);
    info!(?remote, "Handshake complete");

    // ── Step 6: INIT → RTR → RTS ──────────────────────────────────────────
    ctx.connect_rtr(remote.qpn, remote.lid);
    info!("QP moved to RTR");
    ctx.connect_rts();
    info!("QP moved to RTS — client is fully connected");

    // ── Step 7: one RDMA READ at server buf[0..READ_LEN) ──────────────────
    info!("Posting RDMA READ …");
    let t0 = Instant::now();

    unsafe {
        ctx.post_read(
            /*wr_id*/        1,
            /*local_offset*/ 0,
            /*len*/          READ_LEN,
            /*remote_addr*/  remote.addr,       // server buf[0]
            /*remote_rkey*/  remote.rkey,
        );
    }

    // Spin-poll the CQ.
    let _wr_id = ctx.poll_one();
    let rtt = t0.elapsed();

    // ── Read result from local buffer ──────────────────────────────────────
    let local_slice = unsafe {
        std::slice::from_raw_parts(ctx.buf, READ_LEN as usize)
    };
    // Trim to first null byte for display.
    let end = local_slice.iter().position(|&b| b == 0).unwrap_or(READ_LEN as usize);
    let result = std::str::from_utf8(&local_slice[..end]).unwrap_or("<invalid utf8>");

    info!(result, rtt_us = rtt.as_micros(), "RDMA READ complete");

    if result == "rdma works" {
        println!("\n✓ Phase 1 PASSED: read '{result}' from server memory in {}µs", rtt.as_micros());
    } else {
        eprintln!("\n✗ Phase 1 FAILED: got '{result}', expected 'rdma works'");
        std::process::exit(1);
    }
}