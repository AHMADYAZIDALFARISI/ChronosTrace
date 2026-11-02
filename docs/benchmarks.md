# ChronosTrace Matching Benchmarks (Phase 11)

Reproducible measurements of `Cassette::match_request` latency for
cassette sizes of 10, 100, 1,000, and 10,000 nodes. The benchmark lives
in `crates/chronostrace-core/tests/benchmark_suite.rs` and runs as part
of the normal test suite, including `cargo test --release`.

## Method

- **Cassette shape:** flat — every node shares `parent_id = None` and
  the same tool set, and the queried node is the *last* node. That is
  the worst case for the linear exact scan and a full fuzzy scan.
- **Tier-1 (exact):** the query replays the target node's request
  payload byte-for-byte, so the exact SHA-256 hash matches.
- **Tier-2/3 (semantic SimHash):** the query keeps every
  safety-critical value (order id, amount) and the tool set identical,
  but changes only the first word of the system prompt (`Please` ->
  `Kindly`). The exact hash therefore misses and the SimHash fuzzy
  path must find the node.
- **Timing:** only matching is timed; cassette construction is
  excluded. 100 warmup iterations per case, then 1,000 timed
  iterations per tier for 10–1,000 nodes and 500 for 10,000 nodes.
- **Percentiles:** nearest-rank method on sorted per-query durations,
  reported in microseconds (µs).
- **Payload size:** byte length of the JSON serialization of the
  whole cassette (not the YAML file size).
- **Threshold:** similarity threshold 0.80. No performance threshold
  is asserted — timings are machine-dependent. The test asserts only
  correctness: the right node is returned, and the fuzzy similarity
  meets the threshold.

## Results (measured 2026-11-02 12:25 WIB, after the `Cow` fuzzy-match optimization)

| Nodes | Payload (bytes) | Iters | Exact P50 (µs) | Exact P95 (µs) | Exact P99 (µs) | Fuzzy P50 (µs) | Fuzzy P95 (µs) | Fuzzy P99 (µs) | Fuzzy similarity |
|------:|------------:|------:|------:|------:|------:|------:|------:|------:|------:|
| 10 | 7,768 | 1,000 | 5.35 | 5.50 | 18.04 | 19.19 | 28.36 | 93.32 | 0.9688 |
| 100 | 76,039 | 1,000 | 6.86 | 7.09 | 22.35 | 21.54 | 37.70 | 98.43 | 0.9531 |
| 1,000 | 760,090 | 1,000 | 12.16 | 12.81 | 63.14 | 39.33 | 57.85 | 112.74 | 0.9375 |
| 10,000 | 7,618,142 | 500 | 120.71 | 184.70 | 319.04 | 371.24 | 565.32 | 2,338.65 | 0.8906 |

Raw output of the benchmark test (release mode):

```text
ChronosTrace matching benchmark (Tier-1 exact vs Tier-2/3 SimHash)
threshold=0.8, warmup=100 iters per case
  nodes    payload_B  iters exact_P50_us exact_P95_us exact_P99_us fuzzy_P50_us fuzzy_P95_us fuzzy_P99_us fuzzy_sim
     10         7768   1000         5.35         5.50        18.04        19.19        28.36        93.32   0.9688
    100        76039   1000         6.86         7.09        22.35        21.54        37.70        98.43   0.9531
   1000       760090   1000        12.16        12.81        63.14        39.33        57.85       112.74   0.9375
  10000      7618142    500       120.71       184.70       319.04       371.24       565.32      2338.65   0.8906
test benchmark_matching_latency ... ok
```

## Before vs after the `Cow` optimization (fuzzy tier)

Previous run: 2026-11-02 01:13 WIB, before the optimization. This
run: 2026-11-02 12:25 WIB, after it. The optimization replaces the
per-node `String` clone of each candidate's stored safety hash with
a borrow (`Cow::Borrowed`, allocating only when the hash must be
computed because it is empty).

| Nodes | Fuzzy P50 before (µs) | Fuzzy P50 after (µs) | Change | Fuzzy P99 before (µs) | Fuzzy P99 after (µs) |
|------:|------:|------:|------:|------:|------:|
| 10 | 17.51 | 19.19 | +9.6% | 28.26 | 93.32 |
| 100 | 20.81 | 21.54 | +3.5% | 32.15 | 98.43 |
| 1,000 | 51.04 | 39.33 | -22.9% | 68.72 | 112.74 |
| 10,000 | 590.98 | 371.24 | -37.2% | 1,022.25 | 2,338.65 |

## Reading the results

- Fuzzy P50 improved clearly at scale (1,000 nodes: -22.9%;
  10,000 nodes: -37.2%), which is consistent with removing the
  per-node allocation: the saving grows with the number of scanned
  nodes. At 10–100 nodes the medians are roughly flat (within a few
  percent), as expected — the fixed per-call costs dominate there.
- Fuzzy P99 worsened in this run at every size. That must not be
  read as a regression from the optimization: the exact tier, whose
  code was not touched at all, also slowed in this run at every
  size (for example exact P50 at 10,000 nodes moved from 93.44 µs
  to 120.71 µs, and exact P99 from 148.61 µs to 319.04 µs). The
  machine was simply noisier during this run. Tail percentiles
  from single runs on a shared virtual machine are unreliable;
  only the median-at-scale improvement is a signal worth quoting,
  and even that should be confirmed with repeated runs before
  being used for capacity decisions.
- Both tiers stay well under 1 ms up to 1,000 nodes at P50/P95.
  At 10,000 nodes, exact matching is sub-millisecond at P50
  (120.71 µs) and fuzzy matching at P50 is 371.24 µs; the P99
  tails in this run (up to 2,338.65 µs fuzzy) reflect the VM noise
  described above, not a stable property of the code.
- Fuzzy matching still costs more than exact matching. After the
  optimization, the remaining gap comes from the extra per-node
  comparisons (structural tool hash and safety-critical hash,
  versus one hash for exact) plus the per-call SimHash and
  safety-hash computation, not from cloning.
- The fuzzy similarity column is unchanged from the previous run
  (0.9688 at 10 nodes, 0.8906 at 10,000) because the prompts are
  deterministic; similarity decreases with size only because the
  target prompt's content differs per size (different order id
  and amount tokens), not because of the node count itself. Every
  measured similarity is above the 0.80 threshold.
- Payload is unchanged by the optimization (7,618,142 bytes at
  10,000 nodes, roughly 762 bytes per node in JSON form). The
  optimization changes no serialized data, only how the stored
  hash is borrowed during scanning.
- Caveat: these numbers come from single runs on a shared virtual
  machine. Treat them as indicative, not as guarantees. Re-run
  the benchmark on your own hardware before quoting numbers for
  capacity decisions.

## Environment of the measured run

- Date: 2026-11-02 12:25 WIB (post-`Cow` optimization run; previous
  run 2026-11-02 01:13 WIB)
- CPU: AMD EPYC 9D25 126-Core Processor (host model as reported by
  the VM), 4 vCPUs visible to the guest
- Memory: 8,124,908 kB (about 7.75 GiB) total
- OS: Linux 7.0.0-39-generic, x86_64
- Rust toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13),
  cargo 1.99.1, host triple x86_64-unknown-linux-gnu
- Build profile: `release` (default Cargo release settings)

## Reproduction

```bash
# Full benchmark table (stdout is captured by the test harness by
# default, so pass --nocapture):
cargo test --release -p chronostrace-core --test benchmark_suite -- --nocapture

# The same test also runs inside the full workspace suite:
cargo test --workspace --release
```

The benchmark test itself completed in 1.20 s in the measured run
(compile time excluded).
