# PERFORMANCE_REPORT.md — Remediation Audit Cycle

## Honest scope statement
**No benchmarks were run in this cycle.** Nothing below is a benchmark result, and no performance improvement is claimed. This document records what changed that could affect performance, what was measured (test durations only), and what still needs measuring.

## Changes with performance implications (reasoning, not measurement)
- **DAG validation (CT-CORE-04):** the new depth pass is memoized O(n) after the existing O(n) shape pass. Validation cost roughly doubles in the worst case but stays linear. The 100k-node / 2,048-depth limits only reject pathological cassettes; normal trajectories are unaffected.
- **MCP fallback (CT-CORE-01):** one extra safety-hash computation per candidate examined on the structural-fallback path (cached per recorded node when the stored hash is present; recomputed only for legacy nodes with an empty stored hash). Negligible against request handling.
- **Session admission (CT-RUN-02):** one map lookup per dispatch for session-bearing requests. Negligible.
- **Directory sync (CT-CORE-05):** one extra `sync_all()` on the parent directory per save, Unix only, best-effort. Adds a small, bounded fsync cost to saves; saves are not on the hot replay path.
- **Chaos deterministic API:** one SHA-256 per decision when used. The legacy engine path is untouched and has no new cost.
- **Scrub proof API:** one extra full scrub pass (the verification re-scrub) when used. Not yet on any hot path (wiring pending), so current cost is zero.

## Timings actually observed (test suite durations, debug profile — not benchmarks)
- Final `cargo test --workspace`: all suites finished in under ~5s each; the slowest single suite was `benchmark_suite` (~5s, unchanged from baseline) and proxy lib (~2.4s). These are wall-clock test durations on this VM, not performance measurements, and are not comparable across machines.
- Baseline vs final suite durations are within noise of each other; no suite slowed materially.

## Still to measure (follow-ups, see `REMEDIATION_PLAN.md`)
- `diff_cassettes` at 1k/10k nodes (CT-DIFF-03, OPEN): the alignment scan is O(n·m) worst case and is the only plausible hot spot introduced or left by this cycle's scope.
- Save-path cost with directory sync enabled on the deployment filesystem.
- Session-cap behavior under genuine >1,024-session load (to decide whether the constant needs raising or TTL eviction after all).

## Prior benchmark data (historical, NOT this cycle's evidence)
Phase 11 fuzzy-matching benchmarks from the hardening cycle (release profile, flat cassette) are not repeated here and must not be quoted as results of this cycle.
