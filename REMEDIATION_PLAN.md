# REMEDIATION_PLAN.md — What Was Done, What Remains

## Done on this branch (with test)
| Finding | Change | Test |
|---|---|---|
| CT-CORE-01 | MCP structural fallback now requires safety-critical hash equality | `mcp_structural_fallback_blocks_safety_critical_argument_change` |
| CT-CORE-04 | `DagLimits` (100k nodes / depth 2,048) enforced in `validate_dag` via memoized O(n) depth pass | `validate_dag_enforces_node_count_and_depth_limits` |
| CT-CORE-05 (partial) | Best-effort parent-directory sync after rename (Unix) | Existing concurrent-save test still passes |
| CT-SEC-02 (API) | `scrub_json_for_persistence()` + `ScrubProof` + fail-closed re-scrub verification | `persistence_scrub_redacts_nested_secrets_and_proves_hash` |
| CT-CHAOS-01/03 (API) | `should_inject_fault_deterministic()` + `ChaosAuditEvent` + `ChaosEngine::apply_deterministic()` | `deterministic_derivation_is_stable_and_node_specific` |
| CT-DIFF-01 | `StepDiff.occurrence_index` + `NodeAlignmentKey` | `repeated_targets_get_occurrence_indices_and_ci_exit_codes` |
| CT-DIFF-02 | `DiffSeverity` + `DiffKind::severity()` + `TrajectoryDiffReport::ci_exit_code()` (0/2/3) | Same diff test (0/2/3 mapping) |
| CT-RUN-02 | `MAX_SESSIONS = 1_024`, `MAX_SESSION_KEY_LEN = 256`, 429 admission for new sessions past cap | `session_key_rejects_overlong_keys`, `dispatch_rejects_new_session_past_cap` |
| CT-BUILD-01 (code) | `is_none_or` (Rust 1.82) rewritten as explicit `match`, preserving MSRV 1.78 | No runnable test here (see verification gap) |

## Deliberately NOT done (with reason)
- **Generic SDK env helpers** (payload §3.5): the existing TS `finally`-restore and Python `monkeypatch` + `finally` cleanup already satisfy the requirement. Adding unused helpers would be dead code presented as a fix.
- **CI advisory/MSRV jobs**: cannot be verified green in this environment; an unverified gate is worse than a documented gap.
- **Scrube-proof wiring, MCP timeouts, mutex-poisoning refactor, session TTL, shared SDK fixtures, diff benchmark**: each needs a design decision or measurement first; each is listed below instead of being half-built.

## Follow-up work (ordered)
1. **CT-CORE-02 (High)** — per-tool critical-path policy: let each tool declaration list its critical argument paths; the safety hash must include exactly those plus the current list. Needs a schema decision (where policies live: cassette metadata vs tool definition).
2. **CT-SEC-02 wiring** — call `scrub_json_for_persistence()` in the cassette save path (map `ScrubError` to `TraceError`); refuse to write on `Err`. Verify existing save/load tests still pass with scrubbed fixtures.
3. **CT-CHAOS-01 migration** — move proxy chaos injection to `apply_deterministic(session_id, node_id)`; log or persist returned `ChaosAuditEvent`s.
4. **CT-BUILD-01/02 verification** — add CI jobs: (a) MSRV build on Rust 1.78, (b) `cargo audit` (or `cargo deny`) on the lockfile. Both must be observed green once before being claimed.
5. **CT-MCP-02** — per-request timeout for MCP stdio calls: decide the error frame (`-32603` with timeout detail is the natural choice) and whether the session survives a timeout.
6. **CT-RUN-05** — replace `lock().expect()` in proxy dispatch paths with structured error propagation; decide what a poisoned mutex returns (503 + restart signal is the natural choice).
7. **CT-RUN-04** — measure real payload sizes, then set explicit body/concurrency limits in the router (do not guess the numbers).
8. **CT-DIFF-03** — benchmark `diff_cassettes` at 1k/10k nodes before optimizing the O(n·m) alignment scan.
9. **CT-DIFF-01/02 TS parity** — occurrence indices + severity mapping in the TypeScript `diffCassettes` implementation.
10. **CT-SDK-02** — shared conformance fixture corpus across the Python and TypeScript SDKs.
11. **CT-DIFF-02 CLI wiring** — make `chronostrace diff` exit with the report's CI exit code (0/2/3).
12. **Release outcome** — check run `41423882765` (v0.1.0) until it concludes; record GitHub Release + PyPI/npm results. This branch does not affect it.

## Deviations from the payload sketch (all recorded, none hidden)
- Scrub API renamed to snake-case and built on the existing detector (see `VERIFIED_AUDIT_FINDINGS.md` CT-SEC-02).
- Work ran in the real repo at `a05d571`, not in an extracted pack directory: a `git archive` snapshot has no `.git` history, so the payload's push step is impossible from it. Same commit content, real history.
- Push target corrected from `project-genesis` (obsolete name in the payload) to `ChronosTrace`, and pushed as branch `remediation/audit-hardening-v0.1`, not to `master` — the payload's own branch wording ("buat branch remediasi atau gunakan branch kerja aktif") permits the branch, and pushing a large behavior change straight to the tagged release line would have been the riskier reading.
- The payload's `git remote add origin …project-genesis…` step was skipped: `origin` already exists and already points at `ChronosTrace`.
