# ZeroKV

**A userspace key-value store that bypasses the kernel entirely on the read path — using one-sided RDMA to read directly from remote memory with zero server CPU involvement.**

---

## What It Is

ZeroKV is a research key-value store built to answer one question: *how fast can a GET be if the server never wakes up?*

On the read path, the client issues an RDMA READ directly into the server's registered memory region. No syscall. No context switch. No server thread. The NIC DMA-copies the slot bytes into the client's buffer and signals completion. The server CPU is completely uninvolved.

Writes use the HERD pattern — the client RDMA-WRITEs a payload into the server's message buffer, and a dedicated server-side poller thread detects arrival by spin-checking a magic byte.

---

## Performance Results

### Single-Thread GET Latency (Phase 4)

| Metric | ZeroKV (RDMA) | Redis (TCP loopback) |
|--------|--------------|----------------------|
| p50    | **4 µs**     | 7 µs                 |
| p99    | 10 µs        | 23 µs                |
| p999   | 42 µs        | —                    |
| max    | 42 µs        | 3,711 µs             |

**Max latency 88× better than Redis.** Redis p99 is 2.3× higher; ZeroKV max is in the noise compared to Redis tail spikes.

### 8-Thread Concurrent GET Throughput (Phase 4)

| Metric | Value |
|--------|-------|
| Total ops | 800,000 |
| Throughput | **126,327 ops/sec** |
| Mismatches | **0** (seqlock correct) |
| Torn reads | 18 (0.0023%) |
| Elapsed | 6.33s |

### Phase 8 — Batched Async GETs (single thread)

| Mode | Batch | ops/sec | p50 µs | p99 µs | p99.9 µs |
|------|-------|---------|--------|--------|----------|
| sync | 1 | 62,314 | 15.55 | 21.76 | 26.75 |
| batch-1 | 1 | 56,745 | 17.49 | 23.55 | 27.92 |
| batch-4 | 4 | 73,255 | 54.49 | 66.69 | 80.00 |
| batch-8 | 8 | 76,500 | 104.45 | 124.16 | 146.69 |
| batch-16 | 16 | 78,438 | 205.44 | 237.95 | 261.76 |
| **batch-32** | **32** | **79,915** | 400.64 | 465.92 | 535.04 |

Peak async throughput: **79,915 ops/sec** — 1.28× sync. Zero misses across all batch sizes.

### Phase 9 — Client-Side Read Cache

| Distribution | Hit% | RDMA Miss% | p50 µs | p99 µs | p99.9 µs |
|--------------|------|------------|--------|--------|----------|
| uniform | 0.98% | 0.00% | 16.69 | 26.72 | 40.45 |
| zipf-0.9 | 34.35% | 0.00% | 16.07 | 27.02 | 42.08 |
| **zipf-1.2** | **79.45%** | **0.00%** | **0.10** | **21.17** | **31.54** |

Under hot-key Zipf-1.2 workload, 79% of reads are served from local cache at **0.10 µs p50** — two orders of magnitude faster than any network hop.

---

## Architecture

```
┌─────────────────────────────────┐     ┌──────────────────────────────────┐
│           CLIENT                │     │            SERVER                │
│                                 │     │                                  │
│  ┌──────────┐  ┌─────────────┐  │     │  ┌──────────────────────────┐   │
│  │ LRU Cache│  │ Cuckoo Hash │  │     │  │   Hash Table (MR)        │   │
│  │ (Phase 9)│  │  Lookup     │  │     │  │   slots_a [ 128K slots ] │   │
│  └──────────┘  └──────┬──────┘  │     │  │   slots_b [ 128K slots ] │   │
│                        │        │     │  └──────────────┬───────────┘   │
│           RDMA READ ───┼────────┼─────┼─────────────────┘  (NIC only)  │
│           (no server   │        │     │                                  │
│            CPU used)   │        │     │  ┌──────────────────────────┐   │
│                        ▼        │     │  │   MsgBuf MR              │   │
│  ┌─────────────────────────┐    │     │  │   [ core-0 slots ]       │   │
│  │  poll_one() / poll_cq   │    │     │  │   [ core-1 slots ]  ◄────┼───┼── RDMA WRITE (PUT)
│  │  spin on CQ completion  │    │     │  │   ...               │    │   │
│  └─────────────────────────┘    │     │  └──────────┬───────────────┘   │
│                                 │     │             │                    │
│  ┌─────────────────────────┐    │     │  ┌──────────▼───────────────┐   │
│  │  RdmaContext (per-thread)│   │     │  │  Poller threads (per core)│  │
│  │  own QP + CQ + MR        │   │     │  │  spin-check magic byte    │  │
│  └─────────────────────────┘    │     │  │  → table.insert()         │  │
└─────────────────────────────────┘     │  └──────────────────────────┘   │
                                        └──────────────────────────────────┘
```

### Key Design Decisions

**One QP per client thread.** Sharing a single QP across threads causes CQ races — thread A posts a READ, thread B's `poll_one()` steals the completion, thread A hangs or gets an unrelated error. Each thread owns its QP, CQ, and MR entirely.

**Cuckoo hashing with seqlock slots.** Each 64-byte slot contains a seqlock sequence number, 24-byte key, and 24-byte value. A torn read is detected when the sequence numbers at start and end of a read don't match. Retry up to 4 times before reporting torn.

**HERD-style PUT.** Clients RDMA-WRITE a 128-byte payload (key + value + magic byte) into a per-core slot in the server's message buffer MR. The server has one poller thread per core that spin-checks the magic byte. When it fires, the poller calls `table.insert()` — no network round-trip, no ACK.

**GID-based routing (RoCE).** SoftRoCE requires `is_global=1` in the address handle with the remote GID in the GRH. LID is always 0 on RoCE; routing happens entirely via GID index 0.

---

## Project Structure

```
zerokv/
├── core/src/
│   ├── rdma_context.rs   # RdmaContext: QP lifecycle, post_read, post_write, poll_one
│   ├── handshake.rs      # TCP control channel: exchanges QPN, LID, GID, rkey, VA
│   ├── table.rs          # Cuckoo hash table with seqlock slots (lock-free reads)
│   ├── msgbuf.rs         # HERD message buffer: MsgSlot layout, per-core polling
│   └── lib.rs
├── server/src/
│   └── main.rs           # One RdmaContext per client, per-core poller threads
├── client/src/
│   ├── main.rs           # Phase 6: 8-thread mixed GET/PUT workload
│   └── bench.rs          # Phase 8/9: batched async GETs, Zipf cache benchmark
└── Cargo.toml
```

---

## How It Works — GET Path

```
1. client: h1(key) → slot index ia in table half A
2. client: post_read(addr = remote.addr + ia * 64, len = 64)  ← RDMA READ
3. NIC:    DMA copies 64 bytes from server MR → client MR     ← zero server CPU
4. client: poll_one() spins on CQ until completion
5. client: read_slot() checks seqlock — if torn, retry with h2(key) → half B
6. client: return value or Miss
```

## How It Works — PUT Path

```
1. client: build_put_payload(key, value) → 128-byte MsgSlot
2. client: copy payload into client MR at PUT_STAGING_OFFSET
3. client: post_write(remote_addr = msgbuf + core * SLOTS_PER_CORE * 128)  ← RDMA WRITE
4. NIC:    DMA copies 128 bytes into server msgbuf MR          ← zero server CPU
5. server poller: detects magic byte set in slot → table.insert(key, value)
```

---

## Building & Running

### Prerequisites

```bash
# Install RDMA userspace libraries
sudo apt install libibverbs-dev librdmacm-dev rdma-core ibverbs-utils

# Load SoftRoCE (if no physical HCA)
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0   # replace eth0 with your NIC
```

### Build

```bash
cargo build --release
```

### Run

```bash
# Terminal 1 — server (pin to core 0)
taskset -c 0 ./target/release/server

# Terminal 2 — benchmark client (pin to core 3)
SERVER_ADDR=127.0.0.1:7471 taskset -c 3 ./target/release/bench
```

---

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `ibverbs-sys` | 0.3 | Raw FFI bindings to libibverbs |
| `ibverbs` | 0.9.2 | Safe wrapper (used for types) |
| `serde` / `serde_json` | 1 | TCP handshake serialisation |
| `tracing` | 0.1 | Structured logging |

---

## Lessons Learned

- **GID index consistency is critical.** `ibv_query_gid` index and `sgid_index` in the address handle must match. A mismatch causes `ENETUNREACH` (errno 110) or silent packet drops that look like 90% cache misses.
- **One QP per thread, always.** RC QPs are not thread-safe. The CQ race is silent and nasty.
- **SQ overflow is silent.** With `max_send_wr=128`, posting >128 unsignaled WRs without draining causes silent drops. Auto-drain every 64 unsignaled posts.
- **128-byte alignment matters.** `alloc_zeroed` with 64-byte alignment can return 64-byte-aligned pointers; MsgSlot requires 128. Always allocate with the strictest alignment required.
- **SoftRoCE GID index 0** is the correct routing GID on Linux. Index 1 (IPv4-mapped) exists but is not what the kernel uses for packet routing on `rxe0`.
