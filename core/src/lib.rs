pub mod table;
pub mod rdma_context;
pub mod handshake;
pub mod msgbuf;

pub use ibverbs_sys::ibv_gid;

// Re-export table items
pub use table::*;

// Re-export msgbuf items
pub use msgbuf::{
    MsgBuf,
    MsgSlot,
    NUM_CORES,
    SLOTS_PER_CORE,
    MSG_BYTES,
    MSGBUF_BYTES,
    core_for_key,
    build_put_payload,
};