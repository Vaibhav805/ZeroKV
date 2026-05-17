// server/src/main.rs
//
// Phase 1: QP setup + single RDMA READ smoke-test (Step 7).
// The server:
//   1. Creates an RdmaContext (opens device, alloc PD, register MR, create QP).
//   2. Writes "rdma works" into offset 0 of its MR buffer.
//   3. Runs the TCP handshake with the client (exchanges QPN, LID, rkey, addr).
//   4. Drives its QP through INIT → RTR → RTS.
//   5. Waits indefinitely (server CPU is idle after handshake — the client
//      does one-sided RDMA READ without involving us).
use core::rdma_context::{RdmaContext};
use core::handshake::{PeerInfo, run_server_handshake};
use tracing::info;

const LISTEN_ADDR: &str = "0.0.0.0:7471";
/// Buffer size: 1 MiB for Phase 1.  Phase 3 will replace this with the
/// hash-table allocation.
const BUF_LEN: usize = 1 << 20; // 1 MiB

fn main() {
    // ── logging ────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    // ── Step 4: create RDMA context ────────────────────────────────────────
    info!("Opening RDMA device …");
    let ctx = RdmaContext::new(BUF_LEN);
    info!(qpn = ctx.qpn, lid = ctx.lid, rkey = ctx.rkey, "RdmaContext ready");

    // ── Step 7: write sentinel into offset 0 of the MR ────────────────────
    let msg = b"rdma works";
    unsafe {
        std::ptr::copy_nonoverlapping(msg.as_ptr(), ctx.buf, msg.len());
    }
    info!("Wrote '{}' at buf[0]", std::str::from_utf8(msg).unwrap());

    // ── Step 4 (QP state machine): RESET → INIT ───────────────────────────
    ctx.move_to_init();
    info!("QP moved to INIT");

    // ── Step 5: TCP handshake ──────────────────────────────────────────────
    let remote: PeerInfo = run_server_handshake(&ctx, LISTEN_ADDR);
    info!(?remote, "Handshake complete");

    // ── Step 6: INIT → RTR → RTS ──────────────────────────────────────────
    ctx.connect_rtr(remote.qpn, remote.lid);
    info!("QP moved to RTR");
    ctx.connect_rts();
    info!("QP moved to RTS — server is fully connected");

    // ── Server idles; client will do a one-sided RDMA READ ─────────────────
    info!("Server ready. CPU is idle. Waiting for client to finish …");
    // In a real deployment we'd park the thread or spin-poll a message buffer
    // (Phase 6). For Phase 1 we just sleep forever.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}