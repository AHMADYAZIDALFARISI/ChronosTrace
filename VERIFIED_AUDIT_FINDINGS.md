# VERIFIED_AUDIT_FINDINGS.md — Remediation Audit Cycle, Branch `remediation/audit-hardening-v0.1`

- Baseline verified: commit `a05d571` ("hardening: implement CT-HARDEN-001-009"), branch `master`, repo `AHMADYAZIDALFARISI/ChronosTrace`.
- Verification date: 2026-11-02 (WIB). All statuses below rest on code actually read at `a05d571`, not on the payload's prose.
- Important scope note, stated plainly: the 26 IDs `CT-CORE-01` s/d `CT-BUILD-04` do **not** appear in any finding register inside this repo. The repo's own register is `AUDIT_FINDINGS.md` (`CT-001` s/d `CT-016`, all closed in earlier cycles). The 26 IDs arrive only through the pasted execution payload. Where the payload's remediation sketch states the claim (CORE-01/02, SEC-01/02, CHAOS-01/03, DIFF-01/02, SDK-01), the claim below is that sketch's claim, checked against code. Where the payload states no claim text, the entry records the code property actually checked and says so — no finding text was invented to fill the gap, and the original audit document was **not** opened for this cycle (per the owner's instruction that it is another AI's work).
- `File:line` references are to `a05d571` unless marked "this branch".
- Severity counts: High 3, Medium 13, Low 10. Status counts: CONFIRMED 9, PARTIALLY CONFIRMED 10, NOT REPRODUCED 6, NEEDS INVESTIGATION 1.
- Fully fixed this cycle: CT-CORE-01, CT-CORE-04, CT-DIFF-01, CT-DIFF-02, CT-RUN-02, CT-BUILD-01 (6). Partially fixed: CT-CORE-05, CT-SEC-02, CT-CHAOS-01, CT-CHAOS-03 (4). The rest are documented as open, mitigated, or not reproduced — not claimed closed.

## [CT-CORE-01] MCP structural fallback replays without argument identity
- Status: CONFIRMED
- Severity: High
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/lib.rs`, `match_request()` MCP branch (pre-fix)
- Root cause: after the exact-hash miss, the `McpToolCall` branch returned `MatchResult::Exact` on `structural_tool_hash` alone. Argument values were never compared on that path.
- Dampak: a replay request with the same tool schema but different arguments (e.g. `amount` 50 vs 500) could receive the earlier response. This is a real replay-integrity bypass, not a theoretical one.
- Reproducer: record an MCP node (`issue_refund`, amount 50), then match with amount 500 and the same tool schema. Pre-fix: `Exact`. Post-fix: `ReplayMiss`.
- Bukti: pre-fix code read at `a05d571`; regression test `mcp_structural_fallback_blocks_safety_critical_argument_change` fails on the old code path by construction (it asserts `Err` for the amount change) and passes on this branch.
- Perbaikan: the MCP fallback now additionally requires the incoming safety-critical hash to equal the recorded node's safety-critical hash (empty stored hash recomputed for legacy nodes). Non-critical differences (booleans, which the safety hash does not collect) may still match structurally.
- Regression test: `mcp_structural_fallback_blocks_safety_critical_argument_change` (core lib).
- Risiko residual: the safety hash is near-exact (see CT-CORE-02); the fallback's remaining tolerance is booleans, nulls, and non-critical key renames only.
- Status penutupan: CLOSED

## [CT-CORE-02] Safety-critical field coverage is a fixed key list
- Status: PARTIALLY CONFIRMED
- Severity: High
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/lib.rs`, `SAFETY_CRITICAL_KEYS` + `collect_string_literals`
- Root cause: criticality is decided by a fixed list (amount, price, total, currency, quantity, order/user/account/customer id, recipient, role, permission(s), scope, url, uri, path, file_path, sql, query, command, cmd, shell). Unknown business fields are only caught indirectly.
- Dampak: a field outside the list carrying money or identity semantics in boolean/null form escapes the gate; in string/number form it is still caught, because the safety hash also collects every string token and every number in the payload.
- Reproducer: payload with a novel critical key holding a boolean (e.g. `{"approve": true}` vs `false`) produces the same safety hash.
- Bukti: `collect_string_literals` pushes `Value::Number` and digit-bearing string words; booleans/nulls fall through `_ => {}`.
- Perbaikan: none this cycle beyond documenting the gap. A schema-level critical-fields policy (per-tool declared critical paths) is the correct fix and is recorded in `REMEDIATION_PLAN.md` as follow-up, not silently half-built.
- Regression test: none (gap pinned by this entry, not by a test asserting the gap away).
- Risiko residual: OPEN — boolean/null-typed novel critical fields are not gated.
- Status penutupan: OPEN

## [CT-CORE-03] Fuzzy (SimHash) matching can match semantically different prompts
- Status: PARTIALLY CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/lib.rs`, `match_request()` fuzzy path
- Root cause: fuzzy matching is similarity-based by design; the gate is the safety hash plus `similarity_threshold`, not semantics. Near-miss prompts within threshold (negation, swapped entities with similar shingles) can match.
- Dampak: limited — only for non-MCP kinds, only when safety hash and tool schema already match, and only above threshold. It cannot change amounts or ids (those fail the safety gate first).
- Reproducer: two prompts differing by one small word above threshold match as `SemanticFuzzy` (existing test `matches_exact_and_fuzzy_prompt_but_blocks_amount_mutation` demonstrates the intended side).
- Bukti: fuzzy path code read; safety gate precedes SimHash evaluation.
- Perbaikan: none this cycle. Fuzzy mode is explicit and threshold-gated; removing it would break the documented reconciliation feature.
- Regression test: existing fuzzy tests unchanged and passing.
- Risiko residual: OPEN by design — users relying on fuzzy replay should keep thresholds high (default 0.80) and treat fuzzy matches as reconciled drift, not exact evidence.
- Status penutupan: OPEN (by design)

## [CT-CORE-04] DAG validation has no resource bounds
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/lib.rs`, `validate_dag()` (pre-fix)
- Root cause: validation checked shape only (duplicate ids, missing/self parents, cycles). No bound on node count or chain depth, so a hostile or corrupt cassette could force large walks and memory use on load/save.
- Dampak: denial-of-service via crafted cassette size/depth on any path that loads and validates.
- Reproducer: cassette with a 2,049-deep chain validates `Ok` pre-fix.
- Bukti: `validate_dag` body read at `a05d571` — no size or depth check anywhere in the function.
- Perbaikan: `DagLimits { max_nodes: 100_000, max_depth: 2_048 }`, `validate_dag()` delegates to `validate_dag_with_limits(&DagLimits::default())`; depth is computed in a memoized O(n) pass after the acyclicity proof. `save_to_file` and `load_from_file` both call `validate_dag`, so both paths are bounded.
- Regression test: `validate_dag_enforces_node_count_and_depth_limits` (core lib).
- Risiko residual: limits are generous by design; a 100k-node cassette is still accepted. A stricter per-deployment limit can be passed via `validate_dag_with_limits` without a code change.
- Status penutupan: CLOSED

## [CT-CORE-05] Atomic save is durable for the file, not provably for the rename; concurrent writers last-win
- Status: PARTIALLY CONFIRMED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/lib.rs`, `save_to_file()` (pre-fix)
- Root cause: save already used unique temp file + `sync_all()` + `rename()`, but never synced the parent directory, so the rename itself was not provably durable across a crash. Concurrent writers to the same target still resolve as last-rename-wins (the temp-name collision was fixed earlier; the rename race was not and cannot be fixed by temp names alone).
- Dampak: crash-window loss of the rename on Unix; silent overwrite between concurrent writers.
- Reproducer: code reading (no crash test was run — a crash-consistency test is not feasible in this environment and none is claimed).
- Bukti: `save_to_file` body read; no directory sync present pre-fix.
- Perbaikan: best-effort parent-directory `sync_all()` on Unix after rename; failure is non-fatal (the rename already happened) and cannot turn a successful save into an error.
- Regression test: existing `concurrent_saves_to_same_target_all_succeed_and_leave_valid_file` still passes; no new test asserts crash durability (not testable here).
- Risiko residual: OPEN — concurrent same-target writers remain last-writer-wins; the proxy's single-writer contract is the mitigation, and it is documented in code, not enforced by the filesystem.
- Status penutupan: OPEN (partial fix landed)

## [CT-SEC-01] Scrubber cannot detect every secret shape
- Status: PARTIALLY CONFIRMED
- Severity: High
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-scrub/src/lib.rs`, `SecretScrubber` (policy `1.2.0-entropy-regex-jwt`)
- Root cause: detection is pattern + entropy + JWT + sensitive-key based. Short, low-entropy secrets in unkeyed fields are undetectable without unacceptable false positives; that limit is explicitly documented and pinned by a test.
- Dampak: a short unkeyed secret (e.g. `"note": "the code is abc123"`) survives scrubbing by design. Keyed secrets, known prefixes, PEM blocks, bearer tokens, JWTs, and high-entropy tokens are redacted.
- Reproducer: existing test `short_low_entropy_unkeyed_secret_survives_by_design` asserts the survival.
- Bukti: scrubber patterns and entropy threshold read; `scrub_json`/`scrub_str` paths reviewed.
- Perbaikan: none to detection this cycle. The fail-closed boundary API (CT-SEC-02) is the mitigation for the persistence path: anything the scrubber itself still recognizes after one pass fails the write instead of slipping through.
- Regression test: existing scrub tests unchanged and passing.
- Risiko residual: OPEN by design — operators must not rely on scrubbing alone for unkeyed short secrets; upstream systems must not place secrets in free-text fields.
- Status penutupan: OPEN (by design)

## [CT-SEC-02] No verified scrub proof before persistence
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-scrub/src/lib.rs` (pre-fix: no proof API existed)
- Root cause: scrubbing returned a value with no evidence attached; nothing verified the scrubbed payload before it reached disk.
- Dampak: a scrubber regression or a missed pattern would persist silently.
- Reproducer: pre-fix, `SecretScrubber::scrub_json` had no verification step or proof output anywhere in the crate.
- Bukti: crate API surface read at `a05d571` (`new`, `with_extra_patterns`, `policy_version`, `total_redactions`, `scrub_json`, `scrub_str`, `shannon_entropy` — no proof type).
- Perbaikan: new `scrub_json_for_persistence()` returns `(Value, ScrubProof)`; verification re-scrubs and fails closed (`ScrubError::ResidualSecretDetected`) if the second pass changes anything; `ScrubProof` carries `scrubbed`, the policy version, and the SHA-256 of the stored bytes. Deviation from the payload sketch, recorded honestly: the sketch proposed a standalone detector under the non-snake-case name `scrub_json_value_EnteringPersistence` (which would fail `clippy -D warnings`); this implementation reuses the existing, strictly stronger detector under a snake-case name.
- Regression test: `persistence_scrub_redacts_nested_secrets_and_proves_hash` (scrub lib).
- Risiko residual: OPEN — the proxy record path still calls `scrub_json` directly; wiring `scrub_json_for_persistence` into the save path is follow-up work listed in `REMEDIATION_PLAN.md`, and is not claimed done.
- Status penutupan: OPEN (API landed; wiring pending)

## [CT-CHAOS-01] Sequential chaos RNG makes fault placement order-dependent
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-chaos/src/lib.rs`, `ChaosEngine::next_f64` / `apply`
- Root cause: one xorshift state advanced per `apply()` call. Reordering, concurrency, or retrying requests changes which nodes fault, so chaos runs are not reproducible under scheduling changes.
- Dampak: flaky chaos experiments; fault placement cannot be reasoned about per node.
- Reproducer: two engines with the same seed but different call orders fault different nodes (property of the sequential state machine, read in code).
- Bukti: `rng_state` field and `next_f64` implementation read.
- Perbaikan: `should_inject_fault_deterministic(seed, session_id, node_id, fault_type, probability) -> (bool, ChaosAuditEvent)` derives the decision from SHA-256(seed ‖ session ‖ node ‖ fault type); `ChaosEngine::apply_deterministic()` exposes it without touching RNG state. The legacy `apply()` is retained for compatibility; it remains order-dependent by design.
- Regression test: `deterministic_derivation_is_stable_and_node_specific` (chaos lib).
- Risiko residual: OPEN — callers using legacy `apply()` still get order-dependent placement; migration to `apply_deterministic` is follow-up.
- Status penutupan: OPEN (deterministic path landed; legacy path retained)

## [CT-CHAOS-02] Fault-profile coverage
- Status: NOT REPRODUCED
- Severity: Low
- Confidence: Medium
- Commit SHA: a05d571
- File:line: `crates/chronostrace-chaos/src/lib.rs`, `FaultKind`
- Root cause: no gap found in code. Five fault kinds are implemented and tested: HTTP 429 with retry-after, latency spike, truncated JSON, schema-field drop, and 503.
- Dampak: none established.
- Reproducer: existing test `all_five_chaos_fault_profiles_execute_deterministically` exercises all five.
- Bukti: `FaultKind` enum and `inject_fault` match arms read; all arms produce the documented outcome.
- Perbaikan: none.
- Regression test: existing five-fault test unchanged and passing.
- Risiko residual: fault kinds cover transport/application faults only; no fault models for partial SSE streams or auth expiry, which would be new features, not fixes.
- Status penutupan: CLOSED (no gap found; caveat: original claim text was not available in-repo, so this verdict is against the code property checked)

## [CT-CHAOS-03] Chaos decisions leave no audit event
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-chaos/src/lib.rs` (pre-fix: no event type existed)
- Root cause: `apply()` returned only an outcome; nothing recorded which fault, seed, session, or node a decision belonged to.
- Dampak: chaos runs could not be audited or reproduced from logs.
- Reproducer: pre-fix crate API surface had no event struct.
- Bukti: crate API read at `a05d571`.
- Perbaikan: `ChaosAuditEvent { fault_id, session_id, node_id, seed, fault_type, injected, policy_version: "chaos-det-v1" }`, with `fault_id = "flt-" + first 16 hex chars of the decision digest`; returned by `apply_deterministic` for every decision, injected or not.
- Regression test: `deterministic_derivation_is_stable_and_node_specific` asserts all event fields.
- Risiko residual: events are returned, not logged — persistence of events is the caller's job and is not claimed.
- Status penutupan: CLOSED (API landed; event persistence is caller-side)

## [CT-DIFF-01] Repeated calls to the same target are not distinguished in diff output
- Status: PARTIALLY CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/diff.rs`, `diff_cassettes` alignment loop
- Root cause: alignment matched kind + target + parent counterpart, and the matched-set prevented reuse, so duplicates aligned in order — but the output `StepDiff` carried no occurrence identity, so two identical-target steps were indistinguishable downstream.
- Dampak: consumers of diff reports could not tell which occurrence of a repeated call diverged.
- Reproducer: cassette with two identical MCP calls produced two `StepDiff`s with no distinguishing field pre-fix.
- Bukti: alignment loop and `StepDiff` struct read at `a05d571`.
- Perbaikan: `NodeAlignmentKey { parent_id, kind, target_name, occurrence_index }` defined; `StepDiff.occurrence_index` (serde-defaulted to 0 for old reports) counts previous steps with the same kind/target/parent in the same cassette, for both baseline-matched and unmatched-candidate steps.
- Regression test: `repeated_targets_get_occurrence_indices_and_ci_exit_codes` (core diff tests).
- Risiko residual: the TypeScript SDK's separate `diffCassettes` reimplementation does not yet carry occurrence indices (see `REMEDIATION_PLAN.md`).
- Status penutupan: CLOSED (Rust core; TS parity pending)

## [CT-DIFF-02] Diff has no semantic severity or CI exit mapping
- Status: CONFIRMED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/diff.rs`, `DiffKind` / `TrajectoryDiffReport` (pre-fix)
- Root cause: `DiffKind` distinguished mutation types, but no severity ordering and no exit-code mapping existed, so CI could not gate on diff results.
- Dampak: operators had to interpret raw counts.
- Reproducer: pre-fix report API had `is_compatible()` only.
- Bukti: report struct and impl read at `a05d571`.
- Perbaikan: `DiffSeverity { Informational, Behavioral, Critical }` with `ci_exit_code()` 0/2/3; `DiffKind::severity()` maps Identical/PromptDriftReconciled → Informational, ResponseDiverged → Behavioral, ToolSchemaMutated/ToolArgumentMutated/TopologyDiverged → Critical; `TrajectoryDiffReport::max_severity()` and `ci_exit_code()` added. The CLI `diff` subcommand does not yet use the exit code (follow-up).
- Regression test: `repeated_targets_get_occurrence_indices_and_ci_exit_codes` asserts 0/2/3 mappings.
- Risiko residual: mapping is a judgment call (schema mutation counted Critical); documented here and in code.
- Status penutupan: CLOSED (library API; CLI wiring pending)

## [CT-DIFF-03] Diff performance at trajectory scale
- Status: NEEDS INVESTIGATION
- Severity: Medium
- Confidence: Low
- Commit SHA: a05d571
- File:line: `crates/chronostrace-core/src/diff.rs`, `diff_cassettes` alignment loop
- Root cause: unknown — original claim text unavailable in-repo. The property checked: alignment scans the candidate list per baseline node (worst case O(n·m)) with target extraction per candidate per node. No diff-specific benchmark exists; the repo's `benchmark_suite` covers fuzzy matching, not diff.
- Dampak: unquantified; large trajectories may diff slowly. No evidence of failure at current trajectory sizes.
- Reproducer: none run this cycle (no benchmark was produced; none is claimed).
- Bukti: alignment loop structure read; absence of a diff benchmark confirmed by crate layout.
- Perbaikan: none this cycle.
- Regression test: none.
- Risiko residual: OPEN — needs a benchmark before any optimization claim.
- Status penutupan: OPEN

## [CT-RUN-01] Shared proxy mutex serializes all sessions
- Status: NOT REPRODUCED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/lib.rs`, `SessionLocks`
- Root cause: no such bottleneck at HEAD. Dispatch holds a per-session lock for the whole interaction; different sessions never block each other. The global `ProxyState` mutex is only held for short critical sections (the session lock itself is a separate per-session mutex).
- Dampak: none established.
- Reproducer: per-session lock structure read; hardening cycle (CT-HARDEN-005) introduced it and it remains in place.
- Bukti: `SessionLocks::lock_for`, `dispatch` guard handling, and SSE pump guard retention read.
- Perbaikan: none needed.
- Regression test: existing per-session tests unchanged and passing.
- Risiko residual: same-session requests still serialize by design (required for DAG chain linearity).
- Status penutupan: CLOSED (mitigated at HEAD; caveat: original claim text unavailable in-repo)

## [CT-RUN-02] Session state grows without bound
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/lib.rs`, `ProxyState.sessions` + `session_key` (pre-fix)
- Root cause: every distinct `X-ChronosTrace-Session` value allocated a sessions-map entry and a session lock, never removed, with no key-length cap. An unbounded stream of header values was unbounded memory growth.
- Dampak: memory exhaustion vector against a long-lived proxy.
- Reproducer: `session_key` trimmed and rejected blanks only; `sessions.insert` had no cap check at any of its three call sites.
- Bukti: `session_key`, `ProxyState.sessions`, and the three insert sites read at `a05d571`.
- Perbaikan: `MAX_SESSIONS = 1_024` and `MAX_SESSION_KEY_LEN = 256`; `session_key` ignores overlong keys; `dispatch` rejects a NEW session past the cap with 429 `CHRONOSTRACE_TOO_MANY_SESSIONS` before allocating an entry or a lock. Existing sessions are never rejected. No TTL eviction was added deliberately: silently expiring a session would corrupt an in-flight replay chain, which is worse than an explicit rejection.
- Regression test: `session_key_rejects_overlong_keys`, `dispatch_rejects_new_session_past_cap` (proxy lib).
- Risiko residual: the cap is a hard limit — a deployment with >1,024 genuinely concurrent sessions must raise it in code (it is a named constant, not config, today).
- Status penutupan: CLOSED

## [CT-RUN-03] Per-session ordering / chain linearity
- Status: NOT REPRODUCED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/lib.rs`, `dispatch` + SSE pump guard
- Root cause: no gap at HEAD. The session lock is held for the whole interaction, and for SSE record the guard moves into the pump task until the node is recorded, so same-session requests chain in arrival order.
- Dampak: none established.
- Reproducer: existing test `harden005_sse_pump_guard_keeps_session_chain_linear` covers the SSE case.
- Bukti: dispatch and pump code read.
- Perbaikan: none needed.
- Regression test: existing session tests unchanged and passing.
- Risiko residual: ordering is by arrival, not by logical sequence (documented in hardening notes).
- Status penutupan: CLOSED (mitigated at HEAD; caveat: original claim text unavailable in-repo)

## [CT-RUN-04] No explicit resource limits on requests
- Status: PARTIALLY CONFIRMED
- Severity: Medium
- Confidence: Medium
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/lib.rs`, `dispatch` / router construction
- Root cause: no explicit body-size, payload, or concurrency limit is configured in proxy code (no `DefaultBodyLimit` usage found). Any limit that applies comes implicitly from axum's extractor defaults and only covers the JSON extractor path, not SSE streams or the MCP stdio path.
- Dampak: large-payload handling rests on framework defaults rather than a stated policy.
- Reproducer: code search for limit configuration returned nothing in proxy code.
- Bukti: router/handler construction read; no limit layer present.
- Perbaikan: none this cycle — inventing a limit without measuring real payload sizes risks breaking legitimate large replays. Stated policy is recorded in `REMEDIATION_PLAN.md` as follow-up with measurement first.
- Regression test: none.
- Risiko residual: OPEN — explicit limits (body size, concurrent sessions beyond the session cap) remain unconfigured.
- Status penutupan: OPEN

## [CT-RUN-05] Mutex poisoning panics request handling
- Status: PARTIALLY CONFIRMED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/lib.rs`, `lock().expect("ProxyState mutex poisoned")` (many sites)
- Root cause: every state lock uses `.expect()`. A poisoned mutex (a prior holder panicked while holding it) panics the current handler task.
- Dampak: narrower than a process-kill claim — in axum, a panicking handler task aborts that request's connection; the server process survives. The blast radius is per-request failure plus a permanently poisoned mutex for subsequent requests (which then also panic), so the practical impact is still a stuck proxy, just not a dead process.
- Reproducer: code pattern read; no poisoning test was run (inducing a panic while holding the lock in a test would itself be the proof, and was not needed to establish the pattern).
- Bukti: `.expect("ProxyState mutex poisoned")` at dispatch, record, and replay sites.
- Perbaikan: none this cycle — replacing every expect with structured error propagation changes dispatch error surfaces across the proxy and needs its own design pass.
- Regression test: none.
- Risiko residual: OPEN — a panic while holding the state lock wedges the proxy into per-request panics until restart.
- Status penutupan: OPEN

## [CT-MCP-01] MCP stdio lifecycle / crash handling
- Status: NOT REPRODUCED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/mcp_stdio.rs` + `crates/chronostrace-cli/tests/mcp_lifecycle.rs`
- Root cause: no gap at HEAD. Hardening cycle (CT-HARDEN-006) implemented break reasons (host EOF, child EOF, signal), `-32603` error frames for pending requests on child death, partial persistence plus non-zero exit on SIGTERM/child EOF, id-dedup keep-first, and `notifications/cancelled` unblocking.
- Dampak: none established.
- Reproducer: `mcp_lifecycle` integration suite (5 tests, fake Python child) passes at HEAD and on this branch.
- Bukti: lifecycle code and tests read; suite green in this cycle's gate.
- Perbaikan: none needed.
- Regression test: existing 5 lifecycle tests unchanged and passing.
- Risiko residual: child processes are still OS subprocesses — a child that hangs without EOF still depends on the wait timeout (see CT-MCP-02).
- Status penutupan: CLOSED (mitigated at HEAD; caveat: original claim text unavailable in-repo)

## [CT-MCP-02] MCP timeouts (request / idle)
- Status: PARTIALLY CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `crates/chronostrace-proxy/src/mcp_stdio.rs`
- Root cause: a 5-second timeout exists for `child.wait()` on shutdown, but no per-request timeout and no idle timeout were found in the stdio path.
- Dampak: a child that accepts requests but never answers one of them can stall that replay indefinitely (bounded only by the caller's own timeout, if any).
- Reproducer: code search for timeout usage in the stdio path returned only the shutdown wait timeout.
- Bukti: timeout call sites read.
- Perbaikan: none this cycle — adding a request timeout changes JSON-RPC error semantics for in-flight requests and needs a design decision (which error frame, whether the session survives), recorded in `REMEDIATION_PLAN.md`.
- Regression test: none.
- Risiko residual: OPEN — hung-child stalls are not bounded inside the wrapper.
- Status penutupan: OPEN

## [CT-SDK-01] SDK mutates process environment without restore
- Status: NOT REPRODUCED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `sdks/typescript/src/index.ts` (`withChronosTrace`, env save/restore in `finally`, lines ~136–173); `sdks/python/pytest_chronostrace/plugin.py` (`_chronostrace_harness`: `monkeypatch.setenv` + `finally` proc cleanup)
- Root cause: no such defect at HEAD. TypeScript saves the three mutated variables and restores or deletes them in `finally`, including on throw. Python uses pytest's `monkeypatch` (auto-restored) and terminates the child in `finally` (kill fallback).
- Dampak: none established.
- Reproducer: both restore paths read in full.
- Bukti: TS `finally` block restores `OPENAI_BASE_URL`, `ANTHROPIC_BASE_URL`, `CHRONOSTRACE_MCP_PROXY`; Python fixture `finally` terminates/kills the daemon child.
- Perbaikan: none. The payload's generic `withScopedEnv` / `scoped_environment` helpers were deliberately NOT added: the existing restore paths already satisfy the requirement, and adding a second, unused helper would be dead code presented as a fix.
- Regression test: none added (existing SDK e2e suites were not re-run this cycle — no SDK files changed; see `TEST_EVIDENCE.md`).
- Risiko residual: none for the restore property itself; parallel-test env isolation in TS remains opt-in (`withChronosTrace` documents the default).
- Status penutupan: CLOSED (no defect at HEAD)

## [CT-SDK-02] No shared conformance fixtures across SDKs
- Status: PARTIALLY CONFIRMED
- Severity: Low
- Confidence: Medium
- Commit SHA: a05d571
- File:line: `sdks/python/tests/`, `sdks/typescript/tests/` (separate harnesses; no shared fixture directory exists)
- Root cause: process gap, not a code defect. Each SDK has its own e2e harness and both run against the release CLI binary in CI, but there is no shared fixture corpus asserting identical behavior across SDKs.
- Dampak: SDK behavior can drift (canonicalization, SimHash, diff output) without a shared test catching it.
- Reproducer: directory layout inspected; no `sdks/fixtures/` or equivalent exists.
- Bukti: `sdks/` tree listing.
- Perbaikan: none this cycle — building a shared corpus is a multi-SDK project, listed in `REMEDIATION_PLAN.md`.
- Regression test: none.
- Risiko residual: OPEN — drift risk remains; mitigated only by both SDKs testing against the same binary in CI.
- Status penutupan: OPEN

## [CT-BUILD-01] Declared MSRV does not match the code
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `Cargo.toml` (`rust-version = "1.78"`); `crates/chronostrace-core/src/lib.rs`, fuzzy loop (pre-fix)
- Root cause: the code used `Option::is_none_or`, which stabilized in Rust 1.82, while the workspace declared MSRV 1.78. A 1.78 build would fail to compile.
- Dampak: any consumer or CI job pinning 1.78 breaks; the MSRV claim was false.
- Reproducer: grep for post-1.78 APIs found exactly one site (`is_none_or` in the fuzzy loop); no other post-1.78 API was found by the same search.
- Bukti: workspace `Cargo.toml` and the call site read.
- Perbaikan: the call rewritten as an explicit `match` (identical semantics), preserving MSRV 1.78 with a comment explaining why.
- Regression test: none that can run here — the 1.78 toolchain is not installed in this VM, so a true 1.78 build was NOT run. The fix removes the only known 1.82+ API; a CI MSRV job is the remaining verification, listed in `REMEDIATION_PLAN.md`.
- Risiko residual: OPEN (verification) — another post-1.78 API could hide behind a macro or a dependency's MSRV; only a real 1.78 build closes this.
- Status penutupan: OPEN (code fixed; toolchain verification pending)

## [CT-BUILD-02] No dependency advisory scanning in CI
- Status: CONFIRMED
- Severity: Medium
- Confidence: High
- Commit SHA: a05d571
- File:line: `.github/workflows/ci.yml` (jobs: fmt/clippy/schema, workspace tests, Python SDK E2E, TypeScript SDK E2E — no audit/deny step)
- Root cause: CI never runs `cargo audit`, `cargo deny`, or any advisory check.
- Dampak: a vulnerable or yanked dependency could ship unnoticed.
- Reproducer: workflow job list read in full.
- Bukti: `ci.yml` job inventory (no audit/deny string anywhere in the file).
- Perbaikan: none this cycle — adding a workflow step that cannot be verified green in this environment (no `cargo-audit` binary, no network proof of the advisory DB fetch) would ship an unverified gate. Recorded as follow-up with the exact step to add.
- Regression test: none.
- Risiko residual: OPEN — dependency advisories are currently unchecked in CI.
- Status penutupan: OPEN

## [CT-BUILD-03] CI gates incomplete
- Status: PARTIALLY CONFIRMED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `.github/workflows/ci.yml`
- Root cause: most gates exist (fmt, clippy across all targets, JSON Schema validation, multi-OS workspace tests, both SDK E2E suites against the release binary). Missing: advisory scanning (CT-BUILD-02) and an MSRV job (CT-BUILD-01 verification).
- Dampak: the two missing gates are exactly the two open build findings; everything else is covered.
- Reproducer: job inventory read.
- Bukti: `ci.yml` jobs listed in CT-BUILD-02 entry.
- Perbaikan: none this cycle, for the same verification reason as CT-BUILD-02.
- Regression test: none.
- Risiko residual: OPEN — same two gaps.
- Status penutupan: OPEN

## [CT-BUILD-04] Release engineering
- Status: NOT REPRODUCED
- Severity: Low
- Confidence: High
- Commit SHA: a05d571
- File:line: `.github/workflows/release.yml`
- Root cause: no gap at HEAD. Tag pushes matching `v[0-9]+.[0-9]+.[0-9]+*` trigger "Release & Publish": CLI binaries for 5 targets, GitHub Release with archives + SHA256SUMS, and PyPI (OIDC) + npm (`@chronostrace/test`) publish jobs gated to v-tags. Versions are aligned (workspace and TS SDK both `0.1.0`).
- Dampak: none established for the mechanism itself.
- Reproducer: release workflow read in full during the v0.1.0 tagging cycle.
- Bukti: workflow file; tag `v0.1.0` → commit `a05d571` verified via API at tagging time.
- Perbaikan: none.
- Regression test: none (release workflow runs only on tag push; this branch push is not a tag push).
- Risiko residual: OPEN (outcome, not mechanism) — at the time of this cycle's final check, release run `41423882765` (tag v0.1.0) still reported `queued`, so GitHub Release + PyPI/npm outcomes remain unverified. That is a pending external result, not a defect in this branch. Separately, CI run `41422717962` for `a05d571` now reports `completed / success`.
- Status penutupan: OPEN (pending external run outcome)
