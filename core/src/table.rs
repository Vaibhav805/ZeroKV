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

    /// True if this slot holds a real entry (non-zero key).
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.key != [0u8; 24]
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
    // Odd ver_lo = write in progress.
    let seq = slot.ver_lo.wrapping_add(1);
    ptr::write_volatile(&mut slot.ver_lo, seq);
    fence(Ordering::Release);

    slot.key   = *key;
    slot.value = *val;

    fence(Ordering::Release);
    ptr::write_volatile(&mut slot.ver_hi, seq);
}

/// Read key+value from slot using seqlock retry.
/// Returns `Some((key, value))` when a clean read succeeds,
/// `None` on repeated torn-read (should not happen under normal load).
pub fn read_slot(slot: &Slot) -> Option<([u8; 24], [u8; 24])> {
    for _ in 0..16 {
        let lo = unsafe { ptr::read_volatile(&slot.ver_lo) };
        // Odd lo means write in progress — spin.
        if lo & 1 != 0 { std::hint::spin_loop(); continue; }

        fence(Ordering::Acquire);
        let key   = slot.key;
        let value = slot.value;
        fence(Ordering::Acquire);

        let hi = unsafe { ptr::read_volatile(&slot.ver_hi) };
        if lo == hi { return Some((key, value)); }
        // Mismatch — writer was concurrent. Retry.
        std::hint::spin_loop();
    }
    None // extremely rare; caller should retry the whole GET
}

// ── Table ─────────────────────────────────────────────────────────────────

/// Cuckoo hash table: two arrays of `cap` slots each (steps 8–10).
/// `cap` must be a power of two.
pub struct Table {
    slots_a: Vec<Slot>,
    slots_b: Vec<Slot>,
    pub cap:     usize,
}

impl Table {
    pub fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "cap must be a power of two");
        let make = |n| (0..n).map(|_| Slot::zeroed()).collect::<Vec<_>>();
        Self { slots_a: make(cap), slots_b: make(cap), cap }
    }

    /// Pointer to the first byte of slots_a (used when registering the MR).
    pub fn slots_a_ptr(&self) -> *const u8 { self.slots_a.as_ptr() as *const u8 }
    /// Pointer to the first byte of slots_b.
    pub fn slots_b_ptr(&self) -> *const u8 { self.slots_b.as_ptr() as *const u8 }

    /// Insert key→value. Returns true on success, false if the eviction
    /// chain exceeded 512 iterations (caller must rehash).
    pub fn insert(&mut self, key: &[u8; 24], value: &[u8; 24]) -> bool {
        // Already present? Update in place.
        let ia = h1(key) & (self.cap - 1);
        let ib = h2(key) & (self.cap - 1);

        if self.slots_a[ia].is_occupied() && &self.slots_a[ia].key == key {
            unsafe { write_slot(&mut self.slots_a[ia], key, value); }
            return true;
        }
        if self.slots_b[ib].is_occupied() && &self.slots_b[ib].key == key {
            unsafe { write_slot(&mut self.slots_b[ib], key, value); }
            return true;
        }

        // New key — eviction chain.
        let mut cur_key   = *key;
        let mut cur_value = *value;
        let mut use_a = true; // which table to try inserting into

        for _ in 0..512 {
            if use_a {
                let i = h1(&cur_key) & (self.cap - 1);
                if !self.slots_a[i].is_occupied() {
                    unsafe { write_slot(&mut self.slots_a[i], &cur_key, &cur_value); }
                    return true;
                }
                // Evict.
                let evicted_key   = self.slots_a[i].key;
                let evicted_value = self.slots_a[i].value;
                unsafe { write_slot(&mut self.slots_a[i], &cur_key, &cur_value); }
                cur_key   = evicted_key;
                cur_value = evicted_value;
                use_a = false;
            } else {
                let i = h2(&cur_key) & (self.cap - 1);
                if !self.slots_b[i].is_occupied() {
                    unsafe { write_slot(&mut self.slots_b[i], &cur_key, &cur_value); }
                    return true;
                }
                let evicted_key   = self.slots_b[i].key;
                let evicted_value = self.slots_b[i].value;
                unsafe { write_slot(&mut self.slots_b[i], &cur_key, &cur_value); }
                cur_key   = evicted_key;
                cur_value = evicted_value;
                use_a = true;
            }
        }
        false // eviction cycle — caller must rehash
    }

    /// Look up `key`. Returns the value if found.
    pub fn get(&self, key: &[u8; 24]) -> Option<[u8; 24]> {
        let ia = h1(key) & (self.cap - 1);
        let ib = h2(key) & (self.cap - 1);

        if let Some((k, v)) = read_slot(&self.slots_a[ia]) {
            if &k == key { return Some(v); }
        }
        if let Some((k, v)) = read_slot(&self.slots_b[ib]) {
            if &k == key { return Some(v); }
        }
        None
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
        // 100K slots total, insert 50K → 50% load factor.
        let mut t = Table::new(1 << 17); // 131072 slots per half
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
}