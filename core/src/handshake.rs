// core/src/handshake.rs
//
// Step 5: TCP control channel that exchanges QPN, LID, rkey, and buffer VA.
// Both sides send their PeerInfo and receive the remote's PeerInfo.
// After this completes, the TCP socket is dropped — all further
// communication is one-sided RDMA.

use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, Write};
use serde::{Deserialize, Serialize};
use crate::rdma_context::RdmaContext;
/// All the information a peer needs to issue RDMA READ/WRITE ops
/// against our memory region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// QP number — needed to address the remote QP.
    pub qpn: u32,
    /// Local identifier (IB) or GID index (RoCE) of the port.
    pub lid: u16,
    /// Remote key — authorises RDMA access to the MR.
    pub rkey: u32,
    /// Virtual address of the beginning of the MR buffer.
    pub addr: u64,
    /// Virtual address of the second cuckoo table half (slots_b).
    /// Zero until Phase 3; set when the hash table is the MR.
    pub addr_b: u64,
    /// Length of the registered buffer in bytes.
    pub len: u64,
}

impl PeerInfo {
    pub fn from_ctx(ctx: &RdmaContext) -> Self {
        PeerInfo {
            qpn:    ctx.qpn,
            lid:    ctx.lid,
            rkey:   ctx.rkey,
            addr:   ctx.buf as u64,
            addr_b: 0,
            len:    ctx.buf_len as u64,
        }
    }
}

/// Server side: listen on `addr`, accept one connection, exchange PeerInfo,
/// return the remote's PeerInfo.
pub fn run_server_handshake(ctx: &RdmaContext, listen_addr: &str) -> PeerInfo {
    let listener = TcpListener::bind(listen_addr)
        .unwrap_or_else(|e| panic!("bind {listen_addr}: {e}"));
    tracing::info!("Handshake: listening on {listen_addr}");

    let (stream, peer) = listener.accept()
        .unwrap_or_else(|e| panic!("accept: {e}"));
    tracing::info!("Handshake: accepted connection from {peer}");

    exchange(ctx, stream)
}

/// Client side: connect to the server's `server_addr`, exchange PeerInfo,
/// return the remote's PeerInfo.
pub fn run_client_handshake(ctx: &RdmaContext, server_addr: &str) -> PeerInfo {
    let stream = TcpStream::connect(server_addr)
        .unwrap_or_else(|e| panic!("connect {server_addr}: {e}"));
    tracing::info!("Handshake: connected to {server_addr}");

    exchange(ctx, stream)
}

/// Send our PeerInfo, receive the remote's PeerInfo over `stream`.
fn exchange(ctx: &RdmaContext, mut stream: TcpStream) -> PeerInfo {
    let local = PeerInfo::from_ctx(ctx);

    // Send ours first (newline-delimited JSON, one line).
    let mut json = serde_json::to_string(&local).unwrap();
    json.push('\n');
    stream.write_all(json.as_bytes())
        .expect("handshake: write failed");

    // Read the remote's PeerInfo.
    let reader = BufReader::new(&stream);
    use std::io::BufRead;
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