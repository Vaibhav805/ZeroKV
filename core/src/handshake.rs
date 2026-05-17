// core/src/handshake.rs
//
// Step 5: TCP control channel that exchanges QPN, LID, GID, rkey, and buffer VA.
// After this completes the TCP socket is dropped — all further communication
// is one-sided RDMA.

use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, Write};
use serde::{Deserialize, Serialize};
use ibverbs_sys::ibv_gid;
use crate::rdma_context::{RdmaContext};

/// All the information a peer needs to issue RDMA READ/WRITE ops
/// against our memory region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// QP number — needed to address the remote QP.
    pub qpn: u32,
    /// Local identifier (IB). Zero on pure RoCE/SoftRoCE — use gid instead.
    pub lid: u16,
    /// GID — required for RoCE and SoftRoCE routing (fills GRH in ah_attr).
    /// Serialised as a 16-byte array (big-endian IPv6 / IB GID format).
    pub gid: [u8; 16],
    /// Remote key — authorises RDMA access to the MR.
    pub rkey: u32,
    /// Virtual address of the beginning of the MR buffer (slots_a base).
    pub addr: u64,
    /// Virtual address of the second cuckoo table half (slots_b base).
    /// Zero until Phase 3; set when the hash table is the MR.
    pub addr_b: u64,
    /// Length of the registered buffer in bytes.
    pub len: u64,
}

impl PeerInfo {
    pub fn from_ctx(ctx: &RdmaContext) -> Self {
        // ibv_gid is a union { raw: [u8;16], global: { subnet_prefix, interface_id } }.
        // Access the raw bytes via the `raw` field.
        let gid_bytes = unsafe { ctx.gid.raw };
        PeerInfo {
            qpn:    ctx.qpn,
            lid:    ctx.lid,
            gid:    gid_bytes,
            rkey:   ctx.rkey,
            addr:   ctx.buf as u64,
            addr_b: 0,
            len:    ctx.buf_len as u64,
        }
    }

    /// Convert the serialised gid bytes back into ibv_gid for connect_rtr.
    pub fn ibv_gid(&self) -> ibv_gid {
        let mut g: ibv_gid = unsafe { std::mem::zeroed() };
        unsafe { g.raw = self.gid };
        g
    }
}

/// Server side: listen on `addr`, accept one connection, exchange PeerInfo.
pub fn run_server_handshake(ctx: &RdmaContext, listen_addr: &str,addr_b: u64,) -> PeerInfo {
    let listener = TcpListener::bind(listen_addr)
        .unwrap_or_else(|e| panic!("bind {listen_addr}: {e}"));
    tracing::info!("Handshake: listening on {listen_addr}");

    let (stream, peer) = listener.accept()
        .unwrap_or_else(|e| panic!("accept: {e}"));
    tracing::info!("Handshake: accepted connection from {peer}");

    exchange(ctx, stream)
}

/// Client side: connect to server, exchange PeerInfo.
pub fn run_client_handshake(ctx: &RdmaContext, server_addr: &str) -> PeerInfo {
    let stream = TcpStream::connect(server_addr)
        .unwrap_or_else(|e| panic!("connect {server_addr}: {e}"));
    tracing::info!("Handshake: connected to {server_addr}");

    exchange(ctx, stream)
}

/// Send our PeerInfo, receive the remote's PeerInfo (newline-delimited JSON).
fn exchange(ctx: &RdmaContext, mut stream: TcpStream) -> PeerInfo {
    let local = PeerInfo::from_ctx(ctx);

    let mut json = serde_json::to_string(&local).unwrap();
    json.push('\n');
    stream.write_all(json.as_bytes()).expect("handshake: write failed");

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