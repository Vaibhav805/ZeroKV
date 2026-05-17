// core/src/table.rs
//
// Steps 8–11: Cuckoo hash table with seqlock write protocol.
// Two-table layout. Slots are exactly 64 bytes (one cache line).
// The server allocates this as its RDMA MR buffer.

use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::ptr;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

// ── Slot ──────────────────────────────────────────────────────────────────────

/// One cuckoo slot, exactly 64 bytes.
/// Layout:
///   [0..4)   ver_lo  (seqlock low half, u32)
///   [4..8)   ver_hi  (seqlock high half, u32)
///   [8..32)  key     ([u8;24])
///   [32..56) value   ([u8;24])
///   [56..64) _pad    (8 bytes padding to fill cache line)
#[repr(C, align(64))]
pub struct Slot {
    pub ver_lo: u32,
    pub ver_hi: u32,
    pub key:    [u8; 24],
    pub value:  [u8; 24],
    _pad:       [u8; 8],
}

// Compile-time guarantee: slot must be exactly 64 bytes.
const _: () = assert!(std::mem::size_of::<Slot>() == 64);
const _: () = assert!(std::mem::align_of::<Slot>() == 64);

impl Slot {
    pub const fn zeroed() -> Self {
        Self {
            ver_lo: 0,
            ver_hi: 0,
            key:    [0u8; 24],
            value:  [0u8; 24],
            _pad:   [0u8; 8],
        }
    }

    /// True if this slot holds a real entry.
    /// ver_lo is non-zero and even = a completed write has occurred.
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.ver_lo != 0 && self.ver_lo & 1 == 0
    }
}

// ── Hash functions ─────────────────────────────────────────────────────────

#[inline]
pub fn h1(key: &[u8; 24]) -> usize {
    xxh3_64(key) as usize
}

#[inline]
pub fn h2(key: &[u8; 24]) -> usize {
    xxh3_64_with_seed(key, 0xC0FFEE_DEADBEEF) as usize
}

// ── Seqlock write ─────────────────────────────────────────────────────────

/// Write key+value into slot with seqlock protocol (Step 11).
/// # Safety
/// Must only be called by one writer at a time for a given slot.
pub unsafe fn write_slot(slot: &mut Slot, key: &[u8; 24], val: &[u8; 24]) {
    // Increment to odd = write in progress.
    let seq_odd  = (slot.ver_lo & !1).wrapping_add(1); // always odd
    let seq_even = seq_odd.wrapping_add(1);             // always even = done
    ptr::write_volatile(&mut slot.ver_lo, seq_odd);
    fence(Ordering::Release);

    slot.key   = *key;
    slot.value = *val;

    fence(Ordering::Release);
    // Write even value to both halves — reader sees lo==hi, both even = clean.
    ptr::write_volatile(&mut slot.ver_lo, seq_even);
    ptr::write_volatile(&mut slot.ver_hi, seq_even);
}

/// Read key+value from slot using seqlock retry.
/// Returns `Some((key, value))` when a clean read succeeds,
/// `None` on repeated torn-read (should not happen under normal load).
pub fn read_slot(slot: &Slot) -> Option<([u8; 24], [u8; 24])> {
    for _ in 0..16 {
        let lo = unsafe { ptr::read_volatile(&slot.ver_lo) };
        // Odd = write in progress. Zero = never written (empty slot).
        if lo & 1 != 0 || lo == 0 { std::hint::spin_loop(); continue; }

        fence(Ordering::Acquire);
        let key   = slot.key;
        let value = slot.value;
        fence(Ordering::Acquire);

        let hi = unsafe { ptr::read_volatile(&slot.ver_hi) };
        // Clean: lo == hi, both even, both non-zero.
        if lo == hi && lo & 1 == 0 { return Some((key, value)); }
        std::hint::spin_loop();
    }
    None
}

// ── Table ─────────────────────────────────────────────────────────────────

/// Cuckoo hash table: two arrays of `cap` slots each (steps 8–10).
///
/// Supports two ownership modes:
///   - **Owned** (`Table::new`): allocates two `Vec<Slot>` internally.
///     Used in tests and any context without a pre-existing MR buffer.
///   - **Borrowed** (`Table::from_mr`): raw pointers into an externally
///     owned buffer (e.g. the RDMA MR allocation). The caller is
///     responsible for ensuring the buffer outlives the `Table`.
///
/// `cap` must be a power of two.
pub struct Table {
    slots_a: *mut Slot,
    slots_b: *mut Slot,
    pub cap: usize,
    /// `Some((vec_a, vec_b))` when the table owns its memory.
    /// `None` when the table borrows a raw MR buffer.
    _owned:  Option<(Vec<Slot>, Vec<Slot>)>,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

impl Table {
    /// Allocate an owned table (used in tests and pre-Phase-4 paths).
    pub fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "cap must be a power of two");
        let mut a: Vec<Slot> = (0..cap).map(|_| Slot::zeroed()).collect();
        let mut b: Vec<Slot> = (0..cap).map(|_| Slot::zeroed()).collect();
        let pa = a.as_mut_ptr();
        let pb = b.as_mut_ptr();
        Self { slots_a: pa, slots_b: pb, cap, _owned: Some((a, b)) }
    }

    /// Borrow a table over a raw MR buffer (Phase 3+).
    ///
    /// `buf` must point to at least `cap * 2 * 64` contiguous, aligned bytes.
    /// The first `cap` slots map to table A; the next `cap` slots to table B.
    /// The caller owns the buffer lifetime — this `Table` must not outlive it.
    ///
    /// # Safety
    /// - `buf` must be valid, non-null, and aligned to 64 bytes.
    /// - `buf` must remain live for the lifetime of this `Table`.
    /// - No other writer may concurrently mutate the buffer unless the seqlock
    ///   protocol is respected.
    pub unsafe fn from_mr(buf: *mut u8, cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "cap must be a power of two");
        let pa = buf as *mut Slot;
        let pb = (buf as *mut Slot).add(cap);
        Self { slots_a: pa, slots_b: pb, cap, _owned: None }
    }

    /// Pointer to the first byte of slots_a (for MR registration).
    pub fn slots_a_ptr(&self) -> *const u8 { self.slots_a as *const u8 }

    /// Pointer to the first byte of slots_b (for MR registration / PeerInfo).
    pub fn slots_b_ptr(&self) -> *const u8 { self.slots_b as *const u8 }

    /// Insert key→value. Returns `true` on success, `false` if the eviction
    /// chain exceeded 512 iterations (caller must rehash / resize).
    pub fn insert(&mut self, key: &[u8; 24], value: &[u8; 24]) -> bool {
        let ia = h1(key) & (self.cap - 1);
        let ib = h2(key) & (self.cap - 1);

        // Fast-path: update in place if key already exists.
        unsafe {
            let sa = &mut *self.slots_a.add(ia);
            let sb = &mut *self.slots_b.add(ib);
            if sa.is_occupied() && &sa.key == key {
                write_slot(sa, key, value);
                return true;
            }
            if sb.is_occupied() && &sb.key == key {
                write_slot(sb, key, value);
                return true;
            }
        }

        // Slow-path: eviction chain.
        let mut cur_key   = *key;
        let mut cur_value = *value;
        let mut use_a     = true;

        for _ in 0..512 {
            unsafe {
                if use_a {
                    let i = h1(&cur_key) & (self.cap - 1);
                    let s = &mut *self.slots_a.add(i);
                    if !s.is_occupied() {
                        write_slot(s, &cur_key, &cur_value);
                        return true;
                    }
                    let ek = s.key;
                    let ev = s.value;
                    write_slot(s, &cur_key, &cur_value);
                    cur_key   = ek;
                    cur_value = ev;
                    use_a     = false;
                } else {
                    let i = h2(&cur_key) & (self.cap - 1);
                    let s = &mut *self.slots_b.add(i);
                    if !s.is_occupied() {
                        write_slot(s, &cur_key, &cur_value);
                        return true;
                    }
                    let ek = s.key;
                    let ev = s.value;
                    write_slot(s, &cur_key, &cur_value);
                    cur_key   = ek;
                    cur_value = ev;
                    use_a     = true;
                }
            }
        }
        false // eviction cycle — caller must rehash
    }

    /// Look up `key`. Returns the value bytes if found.
    pub fn get(&self, key: &[u8; 24]) -> Option<[u8; 24]> {
        let ia = h1(key) & (self.cap - 1);
        let ib = h2(key) & (self.cap - 1);

        unsafe {
            if let Some((k, v)) = read_slot(&*self.slots_a.add(ia)) {
                if &k == key { return Some(v); }
            }
            if let Some((k, v)) = read_slot(&*self.slots_b.add(ib)) {
                if &k == key { return Some(v); }
            }
        }
        None
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        // Owned mode: the Vec<Slot> pair inside _owned drops here, freeing
        // the heap allocation.  The raw pointers (slots_a / slots_b) become
        // dangling at that point but are never touched again.
        //
        // Borrowed mode (_owned == None): nothing to free; the caller owns
        // the MR buffer and is responsible for its deallocation.
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn insert_and_read_50k() {
        let mut t = Table::new(1 << 17); // 131 072 slots per half
        for i in 0u64..50_000 {
            assert!(t.insert(&make_key(i), &make_val(i)), "insert failed at {i}");
        }
        for i in 0u64..50_000 {
            let v = t.get(&make_key(i)).expect("key missing");
            assert_eq!(&v[..8], &i.to_le_bytes(), "value wrong at {i}");
        }
    }

    #[test]
    fn slot_size() {
        assert_eq!(std::mem::size_of::<Slot>(), 64);
    }

    #[test]
    fn seqlock_single_threaded() {
        let mut slot = Slot::zeroed();
        let k = make_key(42);
        let v = make_val(99);
        unsafe { write_slot(&mut slot, &k, &v); }
        let (rk, rv) = read_slot(&slot).unwrap();
        assert_eq!(rk, k);
        assert_eq!(rv, v);
    }

    #[test]
    fn from_mr_roundtrip() {
        // Allocate a raw buffer mimicking the MR layout and verify from_mr
        // produces correct insert/get behaviour.
        let cap  = 1usize << 10; // 1024 slots per half
        let size = cap * 2 * std::mem::size_of::<Slot>();
        let buf  = unsafe {
            std::alloc::alloc_zeroed(
                std::alloc::Layout::from_size_align(size, 64).unwrap(),
            )
        };
        assert!(!buf.is_null());

        let mut t = unsafe { Table::from_mr(buf, cap) };
        for i in 0u64..500 {
            assert!(t.insert(&make_key(i), &make_val(i * 10)), "insert failed at {i}");
        }
        for i in 0u64..500 {
            let v = t.get(&make_key(i)).expect("key missing in from_mr table");
            assert_eq!(u64::from_le_bytes(v[..8].try_into().unwrap()), i * 10);
        }

        // Drop the table first (borrowed — nothing freed), then free the buffer.
        drop(t);
        unsafe {
            std::alloc::dealloc(
                buf,
                std::alloc::Layout::from_size_align(size, 64).unwrap(),
            );
        }
    }
}