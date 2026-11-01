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

## Results (measured 2026-11-02)

| Nodes | Payload (bytes) | Iters | Exact P50 (µs) | Exact P95 (µs) | Exact P99 (µs) | Fuzzy P50 (µs) | Fuzzy P95 (µs) | Fuzzy P99 (µs) | Fuzzy similarity |
|------:|------------:|------:|------:|------:|------:|------:|------:|------:|------:|
| 10 | 7,768 | 1,000 | 5.01 | 5.14 | 8.55 | 17.51 | 17.79 | 28.26 | 0.9688 |
| 100 | 76,039 | 1,000 | 6.50 | 6.66 | 15.42 | 20.81 | 21.52 | 32.15 | 0.9531 |
| 1,000 | 760,090 | 1,000 | 9.67 | 9.96 | 21.49 | 51.04 | 60.67 | 68.72 | 0.9375 |
| 10,000 | 7,618,142 | 500 | 93.44 | 118.32 | 148.61 | 590.98 | 721.58 | 1,022.25 | 0.8906 |

Raw output of the benchmark test (release mode):

```text
ChronosTrace matching benchmark (Tier-1 exact vs Tier-2/3 SimHash)
threshold=0.8, warmup=100 iters per case
  nodes    payload_B  iters exact_P50_us exact_P95_us exact_P99_us fuzzy_P50_us fuzzy_P95_us fuzzy_P99_us fuzzy_sim
     10         7768   1000         5.01         5.14         8.55        17.51        17.79        28.26   0.9688
    100        76039   1000         6.50         6.66        15.42        20.81        21.52        32.15   0.9531
   1000       760090   1000         9.67         9.96        21.49        51.04        60.67        68.72   0.9375
  10000      7618142    500        93.44       118.32       148.61       590.98       721.58      1022.25   0.8906
test benchmark_matching_latency ... ok
```

## Reading the results

- Both tiers stay well under 1 ms up to 1,000 nodes. At 10,000
  nodes, exact matching is still sub-millisecond at P99 (148.61 µs),
  while fuzzy matching reaches 1,022.25 µs at P99 — just over 1 ms.
- Fuzzy matching costs more than exact matching and grows faster
  with cassette size. That is consistent with the implementation:
  the fuzzy path compares both the structural tool hash and the
  safety-critical hash per scanned node (exact compares one hash),
  and it clones each candidate's stored safety hash string while
  scanning. The clone-per-node behavior is a known optimization
  opportunity, not a correctness defect.
- The fuzzy similarity column decreases with size (0.9688 at 10
  nodes, 0.8906 at 10,000) because the target prompt's content
  differs per size (different order id and amount tokens), not
  because of the node count itself. Every measured similarity is
  above the 0.80 threshold.
- Payload grows linearly at roughly 762 bytes per node in JSON
  form (7,618,142 bytes at 10,000 nodes). Cassettes of this size
  are dominated by the recorded request/response payloads, not by
  the matching metadata.
- Caveat: these numbers come from a single run on a shared virtual
  machine. Treat them as indicative, not as guarantees. Re-run the
  benchmark on your own hardware before quoting numbers for
  capacity decisions.

## Environment of the measured run

- Date: 2026-11-02 01:13 WIB
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

The benchmark test itself completed in 0.95 s in the measured run
(compile time excluded).
