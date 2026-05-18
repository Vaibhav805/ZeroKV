// core/src/handshake.rs
//
// TCP control channel that exchanges QPN, LID, GID, rkey, and buffer VA.
// After handshake completes the TCP socket is dropped — all further
// communication is one-sided RDMA.

use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, Write};
use serde::{Deserialize, Serialize};
use ibverbs_sys::ibv_gid;
use crate::rdma_context::RdmaContext;

/// All the information a peer needs to issue RDMA READ/WRITE ops
/// against our memory region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub qpn:         u32,
    pub lid:         u16,
    pub gid:         [u8; 16],
    pub rkey:        u32,
    pub addr:        u64,
    pub addr_b:      u64,
    pub len:         u64,
    pub msgbuf_addr: u64,
    pub msgbuf_rkey: u32,
}

impl PeerInfo {
    pub fn from_ctx(ctx: &RdmaContext) -> Self {
        let gid_bytes = unsafe { ctx.gid.raw };
        PeerInfo {
            qpn:         ctx.qpn,
            lid:         ctx.lid,
            gid:         gid_bytes,
            rkey:        ctx.rkey,
            addr:        ctx.buf as u64,
            addr_b:      0,
            len:         ctx.buf_len as u64,
            msgbuf_addr: 0,
            msgbuf_rkey: 0,
        }
    }

    /// Convert the serialised gid bytes back into ibv_gid for connect_rtr.
    pub fn ibv_gid(&self) -> ibv_gid {
        let mut g: ibv_gid = unsafe { std::mem::zeroed() };
        g.raw = self.gid;
        g
    }
}

// ── Server side ───────────────────────────────────────────────────────────

/// Server: listen, accept exactly `n_clients` connections sequentially,
/// exchange PeerInfo using the SAME ctx for all (legacy path — all clients
/// share one server QPN; use accept_n_clients in server/main.rs for
/// per-client QP pairing).
pub fn run_server_handshake_n(
    ctx:         &RdmaContext,
    listen_addr: &str,
    n_clients:   usize,
    msgbuf_addr: u64,
    msgbuf_rkey: u32,
) -> Vec<PeerInfo> {
    let listener = TcpListener::bind(listen_addr)
        .unwrap_or_else(|e| panic!("bind {listen_addr}: {e}"));
    tracing::info!(
        "Handshake: listening on {listen_addr}, expecting {n_clients} client(s)"
    );

    let mut remotes = Vec::with_capacity(n_clients);
    for i in 0..n_clients {
        let (stream, peer) = listener.accept()
            .unwrap_or_else(|e| panic!("accept #{i}: {e}"));
        tracing::info!("Handshake: accepted #{i} from {peer}");
        let remote = exchange_with_ctx(ctx, stream, msgbuf_addr, msgbuf_rkey);
        remotes.push(remote);
    }
    remotes
}

/// Server: accept exactly 1 client. Kept for Phase 1–5 compatibility.
pub fn run_server_handshake(
    ctx:         &RdmaContext,
    listen_addr: &str,
    msgbuf_addr: u64,
    msgbuf_rkey: u32,
) -> PeerInfo {
    run_server_handshake_n(ctx, listen_addr, 1, msgbuf_addr, msgbuf_rkey)
        .into_iter()
        .next()
        .unwrap()
}

// ── Client side ───────────────────────────────────────────────────────────

/// Client: connect to server, exchange PeerInfo, return remote's PeerInfo.
pub fn run_client_handshake(ctx: &RdmaContext, server_addr: &str) -> PeerInfo {
    let stream = TcpStream::connect(server_addr)
        .unwrap_or_else(|e| panic!("connect {server_addr}: {e}"));
    tracing::info!("Handshake: connected to {server_addr}");
    exchange_with_ctx(ctx, stream, 0, 0)
}

// ── Shared exchange — now pub so server/main.rs can call per-ctx ──────────

/// Send our PeerInfo (with msgbuf fields injected), receive remote's PeerInfo.
/// Made `pub` so the server can call it once per RdmaContext when pairing
/// each client connection with a dedicated server QP.
pub fn exchange_with_ctx(
    ctx:         &RdmaContext,
    mut stream:  TcpStream,
    msgbuf_addr: u64,
    msgbuf_rkey: u32,
) -> PeerInfo {
    let mut local = PeerInfo::from_ctx(ctx);
    local.msgbuf_addr = msgbuf_addr;
    local.msgbuf_rkey = msgbuf_rkey;

    let mut json = serde_json::to_string(&local).unwrap();
    json.push('\n');
    stream.write_all(json.as_bytes()).expect("handshake: write failed");

    use std::io::BufRead;
    let reader = BufReader::new(&stream);
    let mut line = String::new();
    reader.lines().next()
        .expect("handshake: no data from peer")
        .expect("handshake: IO error reading peer info")
        .clone_into(&mut line);

    let remote: PeerInfo = serde_json::from_str(&line)
        .expect("handshake: JSON parse failed");
    tracing::info!("Handshake: remote = {:?}", remote);

    remote
}
