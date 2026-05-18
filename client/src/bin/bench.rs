// client/src/bin/bench.rs -- Phase 8: batched async GET benchmark
//
// Runs a synchronous GET baseline, then benchmarks pipelined batches where
// each logical GET posts two RDMA READ WRs and one signaled completion.

use core::handshake::{run_client_handshake, PeerInfo};
use core::ibv_gid;
use core::rdma_context::RdmaContext;
use core::table::{h1, h2, read_slot, Slot};

use hdrhistogram::Histogram;
use rand::Rng;
use std::collections::{HashMap, VecDeque};

const DEFAULT_SERVER: &str = "127.0.0.1:7471";
const TABLE_CAP: usize = 1 << 17;
const SLOT_BYTES: u32 = 64;
const HALF_BYTES: u64 = TABLE_CAP as u64 * SLOT_BYTES as u64;

const MAX_BATCH: usize = 32;
const LOCAL_STRIDE: usize = 128;
const BUF_LEN: usize = MAX_BATCH * LOCAL_STRIDE;
const BATCH_SIZES: [usize; 5] = [1, 4, 8, 16, 32];

#[derive(Clone, Copy)]
enum GetResult {
    HitA([u8; 24]),
    HitB([u8; 24]),
    Miss,
}

#[derive(Default)]
struct Counts {
    hit_a: u64,
    hit_b: u64,
    miss: u64,
}

struct BenchResult {
    label: String,
    batch: usize,
    ops: u64,
    ops_sec: f64,
    counts: Counts,
    hist: Histogram<u64>,
}

struct CacheBenchResult {
    label: String,
    ops: u64,
    cache_hits: u64,
    rdma_misses: u64,
    hist: Histogram<u64>,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("warn".parse().unwrap()),
        )
        .init();

    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SERVER.to_string());
    let ops = env_u64("OPS", 1_000_000);
    let keys = env_u64("KEY_SPACE", 100_000);
    let warmup = env_u64("WARMUP", 50_000);
    let cache_ops = env_u64("CACHE_OPS", ops.min(500_000));

    maybe_pin_cpu();
    print_hardware_diagnostics();

    eprintln!("Connecting to {server_addr} ...");
    let ctx = RdmaContext::new(BUF_LEN);
    eprintln!("Local QP={} GID_INDEX={}", ctx.qpn, ctx.gid_index);
    ctx.move_to_init();
    let remote = run_client_handshake(&ctx, &server_addr);
    ctx.connect_rtr(remote.qpn, remote.lid, ibv_gid { raw: remote.gid });
    ctx.connect_rts();

    eprintln!("QP -> RTS. Warming up ({warmup} sync ops) ...");
    let mut rng = rand::thread_rng();
    for _ in 0..warmup {
        let key = make_key(rng.gen_range(0..keys));
        let _ = rdma_get_sync_pipelined(&ctx, &remote, &key);
    }

    let sync_ops = env_u64("SYNC_OPS", ops.min(200_000));
    eprintln!("Measuring sync baseline ({sync_ops} ops) ...");
    let sync = run_sync(&ctx, &remote, sync_ops, keys, &mut rng);

    let mut results = Vec::new();
    results.push(sync);

    for batch in BATCH_SIZES {
        eprintln!("Measuring async batch={batch} ({ops} ops) ...");
        results.push(run_batch(&ctx, &remote, ops, keys, batch, &mut rng));
    }

    print_results(&results);

    eprintln!("Measuring Phase 9 cache workloads ({cache_ops} ops each) ...");
    let cache_results = run_cache_suite(&ctx, &remote, cache_ops, keys, &mut rng);
    print_cache_results(&cache_results);
}

fn run_sync(
    ctx: &RdmaContext,
    remote: &PeerInfo,
    ops: u64,
    key_space: u64,
    rng: &mut rand::rngs::ThreadRng,
) -> BenchResult {
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut counts = Counts::default();
    let wall = std::time::Instant::now();

    for _ in 0..ops {
        let key = make_key(rng.gen_range(0..key_space));
        let t0 = std::time::Instant::now();
        let result = rdma_get_sync_pipelined(ctx, remote, &key);
        hist.record(t0.elapsed().as_nanos() as u64).unwrap();
        add_count(&mut counts, result);
    }

    let elapsed = wall.elapsed().as_secs_f64();
    BenchResult {
        label: "sync".to_string(),
        batch: 1,
        ops,
        ops_sec: ops as f64 / elapsed,
        counts,
        hist,
    }
}

fn run_batch(
    ctx: &RdmaContext,
    remote: &PeerInfo,
    ops: u64,
    key_space: u64,
    batch: usize,
    rng: &mut rand::rngs::ThreadRng,
) -> BenchResult {
    assert!(batch > 0 && batch <= MAX_BATCH);

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut counts = Counts::default();
    let mut done = 0u64;
    let wall = std::time::Instant::now();

    while done < ops {
        let n = batch.min((ops - done) as usize);
        let mut batch_keys = Vec::with_capacity(n);
        for _ in 0..n {
            batch_keys.push(make_key(rng.gen_range(0..key_space)));
        }

        let t0 = std::time::Instant::now();
        let results = get_batch(ctx, remote, &batch_keys);
        let ns = t0.elapsed().as_nanos() as u64;

        for result in results {
            hist.record(ns).unwrap();
            add_count(&mut counts, result);
        }
        done += n as u64;
    }

    let elapsed = wall.elapsed().as_secs_f64();
    BenchResult {
        label: format!("batch-{batch}"),
        batch,
        ops,
        ops_sec: ops as f64 / elapsed,
        counts,
        hist,
    }
}

fn get_batch(ctx: &RdmaContext, remote: &PeerInfo, keys: &[[u8; 24]]) -> Vec<GetResult> {
    let mut remote_addrs = Vec::with_capacity(keys.len());
    for key in keys {
        let ia = h1(key) & (TABLE_CAP - 1);
        let ib = h2(key) & (TABLE_CAP - 1);
        remote_addrs.push((
            remote.addr + ia as u64 * SLOT_BYTES as u64,
            remote.addr + HALF_BYTES + ib as u64 * SLOT_BYTES as u64,
        ));
    }

    unsafe {
        ctx.post_read_pairs_batched(&remote_addrs, LOCAL_STRIDE, SLOT_BYTES, remote.rkey);
    }

    let completions = ctx.poll_n(keys.len());
    let mut results = vec![GetResult::Miss; keys.len()];

    for wr_id in completions {
        let i = wr_id as usize;
        assert!(i < keys.len(), "bad batch completion wr_id={wr_id}");
        results[i] = read_batch_result(ctx.buf, i, &keys[i]);
    }

    results
}

fn read_batch_result(mr: *mut u8, i: usize, key: &[u8; 24]) -> GetResult {
    let base = i * LOCAL_STRIDE;

    let slot_a = unsafe { &*(mr.add(base) as *const Slot) };
    if let Some((k, v)) = read_slot(slot_a) {
        if &k == key {
            return GetResult::HitA(v);
        }
    }

    let slot_b = unsafe { &*(mr.add(base + SLOT_BYTES as usize) as *const Slot) };
    if let Some((k, v)) = read_slot(slot_b) {
        if &k == key {
            return GetResult::HitB(v);
        }
    }

    GetResult::Miss
}

fn rdma_get_sync_pipelined(ctx: &RdmaContext, remote: &PeerInfo, key: &[u8; 24]) -> GetResult {
    let ia = h1(key) & (TABLE_CAP - 1);
    let ib = h2(key) & (TABLE_CAP - 1);

    let addr_a = remote.addr + ia as u64 * SLOT_BYTES as u64;
    let addr_b = remote.addr + HALF_BYTES + ib as u64 * SLOT_BYTES as u64;

    unsafe {
        ctx.post_read_unsignaled(0xA, 0, SLOT_BYTES, addr_a, remote.rkey);
        ctx.post_read(0xB, 64, SLOT_BYTES, addr_b, remote.rkey);
    }
    ctx.poll_one();

    read_batch_result(ctx.buf, 0, key)
}

fn add_count(counts: &mut Counts, result: GetResult) {
    match result {
        GetResult::HitA(v) => {
            let _ = v;
            counts.hit_a += 1;
        }
        GetResult::HitB(v) => {
            let _ = v;
            counts.hit_b += 1;
        }
        GetResult::Miss => counts.miss += 1,
    }
}

fn run_cache_suite(
    ctx: &RdmaContext,
    remote: &PeerInfo,
    ops: u64,
    key_space: u64,
    rng: &mut rand::rngs::ThreadRng,
) -> Vec<CacheBenchResult> {
    vec![
        run_cache_bench(ctx, remote, ops, key_space, Workload::Uniform, rng),
        run_cache_bench(ctx, remote, ops, key_space, Workload::Zipf(0.9), rng),
        run_cache_bench(ctx, remote, ops, key_space, Workload::Zipf(1.2), rng),
    ]
}

enum Workload {
    Uniform,
    Zipf(f64),
}

fn run_cache_bench(
    ctx: &RdmaContext,
    remote: &PeerInfo,
    ops: u64,
    key_space: u64,
    workload: Workload,
    rng: &mut rand::rngs::ThreadRng,
) -> CacheBenchResult {
    let label = match workload {
        Workload::Uniform => "uniform".to_string(),
        Workload::Zipf(alpha) => format!("zipf-{alpha:.1}"),
    };
    let sampler = match workload {
        Workload::Uniform => None,
        Workload::Zipf(alpha) => Some(ZipfSampler::new(key_space as usize, alpha)),
    };

    let mut cache = LruCache::new(1_000);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut cache_hits = 0u64;
    let mut rdma_misses = 0u64;

    for _ in 0..ops {
        let key_id = match &sampler {
            Some(zipf) => zipf.sample(rng) as u64,
            None => rng.gen_range(0..key_space),
        };
        let key = make_key(key_id);

        let t0 = std::time::Instant::now();
        if cache.get(&key).is_some() {
            cache_hits += 1;
            hist.record(t0.elapsed().as_nanos() as u64).unwrap();
            continue;
        }

        let result = rdma_get_sync_pipelined(ctx, remote, &key);
        hist.record(t0.elapsed().as_nanos() as u64).unwrap();
        match result {
            GetResult::HitA(v) | GetResult::HitB(v) => cache.insert(key, v),
            GetResult::Miss => rdma_misses += 1,
        }
    }

    CacheBenchResult {
        label,
        ops,
        cache_hits,
        rdma_misses,
        hist,
    }
}

struct LruCache {
    cap: usize,
    generation: u64,
    entries: HashMap<[u8; 24], ([u8; 24], u64)>,
    order: VecDeque<([u8; 24], u64)>,
}

impl LruCache {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            generation: 0,
            entries: HashMap::with_capacity(cap),
            order: VecDeque::with_capacity(cap * 2),
        }
    }

    fn get(&mut self, key: &[u8; 24]) -> Option<[u8; 24]> {
        let value = self.entries.get(key).map(|(value, _)| *value)?;
        self.touch(*key, value);
        Some(value)
    }

    fn insert(&mut self, key: [u8; 24], value: [u8; 24]) {
        self.touch(key, value);
        self.evict();
    }

    fn touch(&mut self, key: [u8; 24], value: [u8; 24]) {
        self.generation = self.generation.wrapping_add(1);
        self.entries.insert(key, (value, self.generation));
        self.order.push_back((key, self.generation));
    }

    fn evict(&mut self) {
        while self.entries.len() > self.cap {
            let Some((key, generation)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&key)
                .is_some_and(|(_, current)| *current == generation)
            {
                self.entries.remove(&key);
            }
        }
    }
}

struct ZipfSampler {
    cdf: Vec<f64>,
}

impl ZipfSampler {
    fn new(n: usize, alpha: f64) -> Self {
        assert!(n > 0, "zipf key space must be non-empty");
        assert!(alpha > 0.0, "zipf alpha must be positive");

        let mut cdf = Vec::with_capacity(n);
        let mut normalizer = 0.0;
        for i in 1..=n {
            normalizer += 1.0 / (i as f64).powf(alpha);
        }

        let mut cumulative = 0.0;
        for i in 1..=n {
            cumulative += (1.0 / (i as f64).powf(alpha)) / normalizer;
            cdf.push(cumulative);
        }
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }

        Self { cdf }
    }

    fn sample(&self, rng: &mut rand::rngs::ThreadRng) -> usize {
        let x = rng.gen::<f64>();
        self.cdf.partition_point(|p| *p < x)
    }
}

fn print_results(results: &[BenchResult]) {
    println!("\n====== Phase 8 Batched Async GETs ===================");
    println!(
        "{:<10} {:>5} {:>12} {:>10} {:>10} {:>10} {:>10} {:>8}",
        "mode", "batch", "ops/sec", "p50 us", "p99 us", "p99.9 us", "max us", "miss%"
    );

    for r in results {
        println!(
            "{:<10} {:>5} {:>12.0} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>7.2}%",
            r.label,
            r.batch,
            r.ops_sec,
            us(r.hist.value_at_quantile(0.50)),
            us(r.hist.value_at_quantile(0.99)),
            us(r.hist.value_at_quantile(0.999)),
            us(r.hist.max()),
            r.counts.miss as f64 / r.ops as f64 * 100.0,
        );
    }

    if let Some(sync) = results.first() {
        let peak = results
            .iter()
            .skip(1)
            .max_by(|a, b| a.ops_sec.partial_cmp(&b.ops_sec).unwrap())
            .unwrap();
        println!("-----------------------------------------------------");
        println!(
            "Peak async: batch={} {:.0} ops/sec, {:.2}x sync throughput",
            peak.batch,
            peak.ops_sec,
            peak.ops_sec / sync.ops_sec,
        );
        println!(
            "Sync p99={:.2} us, peak async p99={:.2} us",
            us(sync.hist.value_at_quantile(0.99)),
            us(peak.hist.value_at_quantile(0.99)),
        );
    }
    println!("=====================================================");
}

fn print_cache_results(results: &[CacheBenchResult]) {
    println!("\n====== Phase 9 Client-Side Read Cache ===============");
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "dist", "hit%", "rdmaMiss%", "p50 us", "p99 us", "p99.9 us", "max us"
    );

    for r in results {
        println!(
            "{:<10} {:>9.2}% {:>9.2}% {:>10.2} {:>10.2} {:>10.2} {:>10.2}",
            r.label,
            r.cache_hits as f64 / r.ops as f64 * 100.0,
            r.rdma_misses as f64 / r.ops as f64 * 100.0,
            us(r.hist.value_at_quantile(0.50)),
            us(r.hist.value_at_quantile(0.99)),
            us(r.hist.value_at_quantile(0.999)),
            us(r.hist.max()),
        );
    }

    println!("-----------------------------------------------------");
    println!("Cache size: 1000 entries. Zipf workloads model hot-key skew.");
    println!("=====================================================");
}

fn print_hardware_diagnostics() {
    println!("\n====== Phase 9 Hardware Latency Checklist ===========");
    println!("Run perf with:");
    println!("  perf stat -e cycles,instructions,cache-misses,cpu-migrations ./target/release/bench");
    println!("Pin benchmark with:");
    println!("  taskset -c <core-on-nic-numa-node> ./target/release/bench");
    println!("Or let this binary pin itself:");
    println!("  PIN_CPU=<core> ./target/release/bench");

    let nic = std::env::var("RDMA_DEV").unwrap_or_else(|_| "rxe0".to_string());
    let parent = read_trimmed(format!("/sys/class/infiniband/{nic}/parent"))
        .unwrap_or_else(|| "unknown".to_string());
    let numa = read_trimmed(format!("/sys/class/infiniband/{nic}/device/numa_node"))
        .unwrap_or_else(|| "-1".to_string());
    let cpus_allowed = current_cpus_allowed().unwrap_or_else(|| "unknown".to_string());

    println!("RDMA device: {nic}, parent netdev: {parent}, NUMA node: {numa}");
    println!("Current process allowed CPUs: {cpus_allowed}");
    if numa != "-1" && numa != "unknown" {
        if let Some(cpulist) = read_trimmed(format!("/sys/devices/system/node/node{numa}/cpulist")) {
            println!("CPUs on NIC NUMA node {numa}: {cpulist}");
        }
    } else {
        println!("NUMA node -1 means this device is virtual/unknown; pinning still helps migration spikes.");
    }
    println!("Inspect interrupt coalescing with:");
    println!("  ethtool -c {parent}");
    println!("For latency experiments, try:");
    println!("  sudo ethtool -C {parent} rx-usecs 0");
    println!("=====================================================");
}

fn maybe_pin_cpu() {
    let Some(cpu) = std::env::var("PIN_CPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    else {
        return;
    };

    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let rc = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        if rc == 0 {
            eprintln!("Pinned benchmark thread to CPU {cpu}");
        } else {
            eprintln!(
                "WARN: failed to pin benchmark thread to CPU {cpu}: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

fn current_cpus_allowed() -> Option<String> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .map(|s| s.trim().to_string())
}

fn read_trimmed(path: impl AsRef<std::path::Path>) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

fn make_key(n: u64) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&n.to_le_bytes());
    k
}

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
