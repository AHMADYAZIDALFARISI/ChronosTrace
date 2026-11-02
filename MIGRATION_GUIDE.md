# MIGRATION_GUIDE.md — Remediation Audit Cycle (base `a05d571` → branch `remediation/audit-hardening-v0.1`)

## Who needs to act
Anyone replaying MCP cassettes recorded before this branch, and anyone consuming diff-report JSON. Everyone else: no action.

## Behavior change 1 (the important one): MCP structural fallback is now safety-gated
- **Before:** an MCP replay request that missed the exact request hash but matched the tool schema received the recorded response, regardless of argument values.
- **After:** the same request additionally requires the safety-critical hash to match. If any critical field (amount, price, order/user/account id, currency, url, path, sql, command, … — see `SAFETY_CRITICAL_KEYS`), any string, or any number differs, the replay **misses** (`CHRONOSTRACE_REPLAY_MISS` / `ReplayMiss`) instead of returning the old response.
- **What still matches structurally:** requests differing only in booleans, nulls, or non-critical key names, with identical tool schema. Everything else needs an exact match.
- **If your replays start missing after upgrading:** that is the fix working as intended when the arguments genuinely differ critically. Regenerate the cassette (record the new arguments) or adjust the request to match the recorded arguments exactly. Do not work around it by loosening the safety hash — file an issue describing the tool and the field instead.
- **Legacy nodes:** nodes recorded before `safety_critical_hash` existed (empty stored hash) have their hash recomputed from the stored payload at match time, so old cassettes keep working under the new rule.

## Behavior change 2: DAG validation limits
- Cassettes with more than 100,000 nodes or a chain deeper than 2,048 are now rejected on load and save (`InvalidDag`). Real trajectories are far below both bounds. If you legitimately exceed them, call `validate_dag_with_limits` with your own `DagLimits` — do not fork the validator.

## Behavior change 3: session limits (proxy operators)
- New sessions past 1,024 concurrent sessions receive HTTP 429 `CHRONOSTRACE_TOO_MANY_SESSIONS`. Existing sessions are unaffected. Session keys longer than 256 characters are ignored (treated as no session). If 1,024 binds in production, raise `MAX_SESSIONS` with measured data; there is deliberately no silent eviction.

## Additive changes (no migration needed)
- `StepDiff` gains `occurrence_index` (defaults to 0 when reading old reports). Consumers that deserialize strictly with `deny_unknown_fields` will need to allow the new field; default serde consumers are unaffected.
- New APIs: `DagLimits`, `validate_dag_with_limits`, `ChaosAuditEvent`, `should_inject_fault_deterministic`, `ChaosEngine::apply_deterministic`, `ScrubProof`, `ScrubError`, `scrub_json_for_persistence`, `DiffSeverity`, `DiffKind::severity()`, `TrajectoryDiffReport::max_severity()` / `ci_exit_code()`. All are additive; no existing API was removed or re-signatured.
- The CLI `diff` subcommand does **not** yet exit with the report's CI code — that wiring is follow-up work, so CI gating on diff output must still parse the report for now.

## Not changed
- Cassette schema version ("1.0"), wire protocols, CLI flags, SDK public APIs, release workflow. Old cassettes load and validate under the new code (within the limits above).
