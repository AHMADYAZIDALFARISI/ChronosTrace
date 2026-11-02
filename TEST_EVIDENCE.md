# TEST_EVIDENCE.md — Remediation Audit Cycle

## Commands actually run (this branch, debug profile unless noted)
1. Baseline at `a05d571` (before any change): `cargo fmt --all -- --check` → exit 0; `cargo clippy --workspace --all-targets` → exit 0; `cargo test --workspace` → 84 passed, 0 failed. Full output in `BASELINE_REPORT.md`.
2. Final gate on this branch: `cargo fmt --all -- --check` → exit 0; `cargo clippy --workspace --all-targets -- -D warnings` → exit 0; `cargo test --workspace` → **91 passed, 0 failed**.

## Per-suite results (final gate, debug)
| Suite | Passed | Failed | Note |
|---|---|---|---|
| `chronostrace-chaos` lib | 2 | 0 | was 1; +1 deterministic derivation test |
| `chronostrace-cli` bin | 11 | 0 | unchanged |
| `mcp_lifecycle` integration | 5 | 0 | unchanged |
| `chronostrace-core` lib | 13 | 0 | was 10; +1 MCP safety, +1 DAG limits, +1 diff occurrence/severity |
| `benchmark_suite` | 1 | 0 | unchanged |
| `chronostrace-proxy` lib | 51 | 0 | was 49; +2 session cap tests |
| `chronostrace-scrub` lib | 8 | 0 | was 7; +1 persistence proof test |
| Other suites (doc/empty) | 0 | 0 | — |
| **Total** | **91** | **0** | 84 baseline + 7 new |

## New regression tests (all passing in the final gate)
- `mcp_structural_fallback_blocks_safety_critical_argument_change` — amount 50 → 500 misses; boolean-only change still matches structurally.
- `validate_dag_enforces_node_count_and_depth_limits` — 5-node chain fails max_nodes 3 and max_depth 2, passes defaults.
- `repeated_targets_get_occurrence_indices_and_ci_exit_codes` — occurrences 0/1; exit codes 0 (identical), 2 (response diverged), 3 (topology diverged).
- `deterministic_derivation_is_stable_and_node_specific` — same inputs same event; p=0 never injects; p=0.5 varies by node across 64 ids.
- `persistence_scrub_redacts_nested_secrets_and_proves_hash` — nested fake secrets redacted; proof hash equals SHA-256 of stored bytes.
- `session_key_rejects_overlong_keys` — 257-char key ignored.
- `dispatch_rejects_new_session_past_cap` — 1,024 sessions filled; new session gets 429 `CHRONOSTRACE_TOO_MANY_SESSIONS` without allocation; existing session unaffected.

## Failures found and fixed during the cycle (not hidden)
- Chaos refactor initially dropped the `match` opener in `inject_fault` (compile error) — fixed, gate re-run green.
- First MCP test used a numeric field as the "benign" control; it failed because the safety hash collects all numbers (discovered by reading `collect_string_literals`). The test was corrected to a boolean control, with a comment explaining why — the safety hash's near-exactness is now documented in the findings.
- Clippy `-D warnings` flagged `unnecessary_get_then_check` in the new depth pass — fixed with `contains_key`.

## Explicitly NOT run (and why)
- **TypeScript SDK** (`tsc` + vitest) and **Python SDK** (pytest): no SDK files changed on this branch, so no SDK regression is possible from this diff. SDK suites are not claimed as evidence here. They run in CI on the branch push.
- **Release-profile tests**: the payload's final gate specifies debug `cargo test --workspace`; release-profile results from the hardening cycle are historical, not this cycle's evidence, and are not repeated here.
- **1.78 toolchain build**: toolchain unavailable in this VM (see CT-BUILD-01 residual).
- **Benchmarks**: none run this cycle (see `PERFORMANCE_REPORT.md`).
- **No test was deleted or weakened.** One existing test was not modified at all; the only test-file edits are additions. `git diff --check` clean (verified at commit time).
