// core/src/msgbuf.rs — Phase 6 (Steps 24–26)
//
// HERD message buffer: the second MR on the server that clients RDMA-WRITE
// PUT requests into.  Server cores spin-poll their own section and call
// table.insert() when a request arrives.
//
// Layout
// ──────
//   NUM_CORES × SLOTS_PER_CORE × MSG_BYTES contiguous bytes, aligned to 128.
//
//   [core 0, slot 0][core 0, slot 1] … [core 0, slot 63]
//   [core 1, slot 0] …
//   …
//   [core 7, slot 63]
//
// Each MsgSlot is 128 bytes (two cache lines).
// Byte 0  — magic / owner byte  (0 = free, non-zero = PUT pending)
// Byte 1  — reserved / padding
// Byte 2  — padding
// Byte 3  — padding
// Byte 4  — padding
// Byte 5  — padding
// Byte 6  — padding
// Byte 7  — padding
// Bytes 8..32  — key  ([u8; 24])
// Bytes 32..56 — value ([u8; 24])
// Bytes 56..128 — padding
//
// The client writes the entire 128-byte slot atomically from its perspective
// (one RDMA WRITE).  The server polls byte 0 (magic).  When magic != 0, the
// server reads key+value, calls table.insert(), then zero-clears magic.
//
// Alignment & DMA safety
// ──────────────────────
// The whole buffer is alloc'd with align(128) and registered as a single MR
// on the server.  Each MsgSlot starts at a 128-byte-aligned offset, so both
// of its two cache lines are cache-line-aligned.  The RNIC's DMA writes the
// client payload in cache-line-sized chunks; the server polls with
// read_volatile on magic to detect completion without racing.

use std::ptr;
use std::sync::atomic::{fence, Ordering};

// ── Constants ──────────────────────────────────────────────────────────────

pub const NUM_CORES:      usize = 8;
pub const SLOTS_PER_CORE: usize = 64;
pub const MSG_BYTES:      usize = 128;

/// Total byte size of the message buffer MR.
pub const MSGBUF_BYTES: usize = NUM_CORES * SLOTS_PER_CORE * MSG_BYTES;

// Compile-time check: must be a power-of-two multiple of cache-line size.
const _: () = assert!(MSG_BYTES == 128);
const _: () = assert!(MSGBUF_BYTES == NUM_CORES * SLOTS_PER_CORE * MSG_BYTES);

// ── MsgSlot ────────────────────────────────────────────────────────────────

/// One message slot — exactly 128 bytes, aligned to 128.
///
/// The client writes this entire struct with a single RDMA WRITE.
/// Field order is significant (repr(C)):
///   [0]      magic   — non-zero when a PUT is pending
///   [1..8]   _pad0
///   [8..32]  key
///   [32..56] value
///   [56..128] _pad1
#[repr(C, align(128))]
pub struct MsgSlot {
    pub magic:  u8,
    _pad0:      [u8; 7],
    pub key:    [u8; 24],
    pub value:  [u8; 24],
    _pad1:      [u8; 72],
}

const _: () = assert!(std::mem::size_of::<MsgSlot>() == 128);
const _: () = assert!(std::mem::align_of::<MsgSlot>() == 128);

impl MsgSlot {
    pub const fn zeroed() -> Self {
        Self {
            magic:  0,
            _pad0:  [0u8; 7],
            key:    [0u8; 24],
            value:  [0u8; 24],
            _pad1:  [0u8; 72],
        }
    }
}

// ── MsgBuf ─────────────────────────────────────────────────────────────────

/// Owns (or borrows) the server-side message buffer.
///
/// Construction modes mirror `Table`:
///   - `MsgBuf::new_owned()`: allocates its own aligned buffer (tests / demos).
///   - `MsgBuf::from_mr(buf)`: wraps an externally owned MR allocation.
///
/// In both cases `base` is the pointer that was registered as an MR on the
/// RNIC; clients compute slot addresses as:
///   `base + (core_idx * SLOTS_PER_CORE + slot_idx) * MSG_BYTES`
pub struct MsgBuf {
    base:   *mut MsgSlot,
    /// Owned allocation; `None` in borrowed mode.
    _owned: Option<Vec<MsgSlot>>,
}

unsafe impl Send for MsgBuf {}
unsafe impl Sync for MsgBuf {}

impl MsgBuf {
    /// Allocate an aligned, zeroed message buffer (owned mode).
    pub fn new_owned() -> Self {
        let count = NUM_CORES * SLOTS_PER_CORE;
        let mut v: Vec<MsgSlot> = (0..count).map(|_| MsgSlot::zeroed()).collect();
        let base = v.as_mut_ptr();
        Self { base, _owned: Some(v) }
    }

    /// Wrap an externally owned MR buffer (borrowed mode).
    ///
    /// # Safety
    /// - `buf` must be non-null, aligned to 128, valid for
    ///   `MSGBUF_BYTES` bytes, and outlive this `MsgBuf`.
    /// - The buffer must already be zeroed (or the caller guarantees magic==0
    ///   for every slot before the first poll).
    pub unsafe fn from_mr(buf: *mut u8) -> Self {
        assert_eq!(buf as usize % 128, 0, "buf must be 128-byte aligned");
        Self { base: buf as *mut MsgSlot, _owned: None }
    }

    /// Pointer to the start of the MR (for registration / `PeerInfo.addr`).
    #[inline]
    pub fn base_ptr(&self) -> *mut u8 { self.base as *mut u8 }

    /// Absolute byte offset of `slot_idx` within core `core_idx`.
    ///
    /// Clients add this to `msgbuf_addr` (from handshake) to get the remote
    /// virtual address for their RDMA WRITE.
    #[inline]
    pub fn slot_offset(core_idx: usize, slot_idx: usize) -> usize {
        (core_idx * SLOTS_PER_CORE + slot_idx) * MSG_BYTES
    }

    /// Raw pointer to a specific slot (server-side use only).
    ///
    /// # Safety
    /// `core_idx < NUM_CORES`, `slot_idx < SLOTS_PER_CORE`.
    #[inline]
    unsafe fn slot_ptr(&self, core_idx: usize, slot_idx: usize) -> *mut MsgSlot {
        self.base.add(core_idx * SLOTS_PER_CORE + slot_idx)
    }

    // ── Server-side poll ────────────────────────────────────────────────────

    /// Spin-poll all slots owned by `core_idx`.
    ///
    /// Calls `on_put(key, value)` for every pending request, then clears the
    /// slot's magic byte so the client may reuse it.
    ///
    /// Returns the number of PUT requests processed in this sweep.
    ///
    /// This is meant to be called in a tight loop on a dedicated core:
    /// ```ignore
    /// loop { msgbuf.poll_core(core_id, |k, v| table.insert(k, v)); }
    /// ```
    pub fn poll_core<F>(&self, core_idx: usize, mut on_put: F) -> usize
    where
        F: FnMut(&[u8; 24], &[u8; 24]),
    {
        let mut count = 0;
        for slot_idx in 0..SLOTS_PER_CORE {
            unsafe {
                let slot = &mut *self.slot_ptr(core_idx, slot_idx);
                // Read magic with acquire semantics so the key/value bytes
                // that the RNIC DMA'd before the magic write are visible.
                let magic = ptr::read_volatile(&slot.magic);
                if magic == 0 {
                    continue;
                }
                // Acquire fence: all DMA writes for this slot are visible.
                fence(Ordering::Acquire);

                // Copy out key and value before clearing the slot.
                let key   = slot.key;
                let value = slot.value;

                // Call the user-supplied handler (e.g. table.insert).
                on_put(&key, &value);

                // Release fence: handler side-effects are visible before we
                // clear magic, so a client polling for an ACK would see them.
                fence(Ordering::Release);
                ptr::write_volatile(&mut slot.magic, 0u8);

                count += 1;
            }
        }
        count
    }
}

impl Drop for MsgBuf {
    fn drop(&mut self) {
        // Owned: Vec drops and frees the allocation.
        // Borrowed: caller owns the MR buffer — nothing to free.
    }
}

// ── Client-side helpers ────────────────────────────────────────────────────

/// Choose which server core handles a given key (Step 25).
///
/// Uses the lower bits of h1 so that the mapping is consistent with the
/// table's own hash assignment.  The client calls this to compute the
/// RDMA-WRITE target address.
#[inline]
pub fn core_for_key(key: &[u8; 24]) -> usize {
    use xxhash_rust::xxh3::xxh3_64;
    (xxh3_64(key) as usize >> 3) % NUM_CORES  // rotate 3 bits to avoid clash
                                               // with h1 low bits in table A
}

/// Build a 128-byte PUT payload ready to RDMA-WRITE into the server's
/// message buffer slot.
///
/// Layout matches `MsgSlot` exactly, so the RNIC can write the 128 bytes
/// straight into the slot with no host-side scatter.
///
/// Returns the raw bytes; the caller passes them to `ctx.post_write`.
pub fn build_put_payload(key: &[u8; 24], value: &[u8; 24]) -> [u8; 128] {
    let mut buf = [0u8; 128];
    buf[0] = 0xFF;              // magic — non-zero means "PUT pending"
    buf[8..32].copy_from_slice(key);
    buf[32..56].copy_from_slice(value);
    buf
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_layout() {
        assert_eq!(std::mem::size_of::<MsgSlot>(), 128);
        assert_eq!(std::mem::align_of::<MsgSlot>(), 128);
    }

    #[test]
    fn poll_core_basic() {
        let mb = MsgBuf::new_owned();

        // Manually write a PUT into core 2, slot 5.
        unsafe {
            let slot = &mut *mb.slot_ptr(2, 5);
            slot.key   = {let mut k=[0u8;24]; k[..8].copy_from_slice(&42u64.to_le_bytes()); k};
            slot.value = {let mut v=[0u8;24]; v[..8].copy_from_slice(&99u64.to_le_bytes()); v};
            // Write magic last — mirrors what the RNIC does.
            std::ptr::write_volatile(&mut slot.magic, 0xFF);
        }

        let mut got: Option<([u8;24],[u8;24])> = None;
        let processed = mb.poll_core(2, |k, v| {
            got = Some((*k, *v));
        });

        assert_eq!(processed, 1);
        let (k, v) = got.unwrap();
        assert_eq!(u64::from_le_bytes(k[..8].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(v[..8].try_into().unwrap()), 99);

        // Slot must be cleared after poll.
        unsafe {
            let slot = &*mb.slot_ptr(2, 5);
            assert_eq!(slot.magic, 0, "magic not cleared after poll");
        }
    }

    #[test]
    fn build_put_payload_layout() {
        let key = {let mut k=[0u8;24]; k[..8].copy_from_slice(&7u64.to_le_bytes()); k};
        let val = {let mut v=[0u8;24]; v[..8].copy_from_slice(&13u64.to_le_bytes()); v};
        let p = build_put_payload(&key, &val);
        assert_eq!(p[0], 0xFF,   "magic byte");
        assert_eq!(&p[8..32],  &key, "key region");
        assert_eq!(&p[32..56], &val, "value region");
        assert_eq!(&p[56..],   &[0u8; 72][..], "tail must be zero");
    }

    #[test]
    fn core_for_key_in_range() {
        for i in 0u64..1024 {
            let mut k = [0u8;24];
            k[..8].copy_from_slice(&i.to_le_bytes());
            assert!(core_for_key(&k) < NUM_CORES);
        }
    }

    #[test]
    fn slot_offset_no_overlap() {
        // Adjacent slots must be MSG_BYTES apart.
        let off0 = MsgBuf::slot_offset(0, 0);
        let off1 = MsgBuf::slot_offset(0, 1);
        assert_eq!(off1 - off0, MSG_BYTES);

        // First slot of core 1 must immediately follow last slot of core 0.
        let last0 = MsgBuf::slot_offset(0, SLOTS_PER_CORE - 1);
        let first1 = MsgBuf::slot_offset(1, 0);
        assert_eq!(first1 - last0, MSG_BYTES);
    }
}
#[inline]
pub fn slot_offset(core_idx: usize, slot_idx: usize) -> usize {
    (core_idx * SLOTS_PER_CORE + slot_idx) * MSG_BYTES
}
