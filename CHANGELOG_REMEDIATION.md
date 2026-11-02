# CHANGELOG_REMEDIATION.md — Remediation Audit Cycle (branch `remediation/audit-hardening-v0.1`, base `a05d571`)

This is the cycle changelog. It does not replace `CHANGELOG.md` (release notes); it records exactly what this branch changed, so a reviewer can diff claims against code.

## Added
- `crates/chronostrace-core`: `DagLimits` + `Cassette::validate_dag_with_limits()` (CT-CORE-04); `StepDiff.occurrence_index`, `NodeAlignmentKey`, `DiffSeverity`, `DiffKind::severity()`, `TrajectoryDiffReport::max_severity()` / `ci_exit_code()` (CT-DIFF-01/02).
- `crates/chronostrace-chaos`: `ChaosAuditEvent`, `should_inject_fault_deterministic()`, `ChaosEngine::apply_deterministic()` (CT-CHAOS-01/03). New workspace dependency for the crate: `sha2` (already a workspace dependency; no new crate versions introduced).
- `crates/chronostrace-scrub`: `ScrubProof`, `ScrubError`, `scrub_json_for_persistence()` (CT-SEC-02). New workspace dependencies for the crate: `serde`, `sha2` (both already workspace dependencies).
- `crates/chronostrace-proxy`: `MAX_SESSIONS` (1,024), `MAX_SESSION_KEY_LEN` (256), 429 admission for new sessions past the cap (CT-RUN-02).
- Docs: `REPOSITORY_MAP.md`, `BASELINE_REPORT.md`, `VERIFIED_AUDIT_FINDINGS.md`, `RISK_REGISTER.md`, `EXECUTIVE_SUMMARY.md`, `ARCHITECTURE_REVIEW.md`, `REMEDIATION_PLAN.md`, `CHANGELOG_REMEDIATION.md` (this file), `TEST_EVIDENCE.md`, `SECURITY_REVIEW.md`, `PERFORMANCE_REPORT.md`, `MIGRATION_GUIDE.md`.
- Tests: 7 new regression tests (1 chaos, 3 core, 2 proxy, 1 scrub).

## Changed (behavior)
- `Cargo.lock` updated to record two new crate-level dependency edges only (chaos + `sha2`, scrub + `serde`/`sha2`); no crate versions changed.
- **MCP replay matching (CT-CORE-01, the one behavior change):** the structural fallback now requires safety-critical hash equality. Replays that previously matched structurally with different critical arguments now return a replay miss. See `MIGRATION_GUIDE.md`.
- `Cassette::validate_dag()` now enforces node-count and depth limits (defaults 100,000 / 2,048). Cassettes within those bounds validate exactly as before.
- `Cassette::save_to_file()` best-effort syncs the parent directory on Unix after rename (CT-CORE-05). No observable behavior change on success.
- `match_request()` fuzzy loop rewritten without `Option::is_none_or` (CT-BUILD-01). Semantics identical; MSRV 1.78 preserved.
- `crates/chronostrace-core/src/lib.rs` internals: `validate_dag` split into `validate_dag` (defaults) + `validate_dag_with_limits` + `validate_dag_shape` (private). No public API removed.

## Not changed
- Cassette schema version ("1.0"), wire protocols, CLI flags, SDK public APIs, release workflow, `README.md`, `CHANGELOG.md`, `master`, tag `v0.1.0`.

## Deviations from the payload sketch
- Scrub API name and implementation basis (see `REMEDIATION_PLAN.md`).
- Work directory: real repo at `a05d571`, not the extracted pack snapshot (which has no git history).
- Push: branch `remediation/audit-hardening-v0.1` to `ChronosTrace`, not `master`, not `project-genesis`.
