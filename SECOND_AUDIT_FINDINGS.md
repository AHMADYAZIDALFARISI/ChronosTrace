# SECOND_AUDIT_FINDINGS.md — Second Audit & Hardening Mission

Base: branch `remediation/audit-hardening-v0.1` at `3b7fa40` (the first
remediation cycle), NOT master. All verification below was executed
against that state; no finding from the first cycle was assumed still
accurate without re-checking the code. Work is uncommitted on that
branch at the time of writing; nothing was pushed by this mission.

## P0 — MCP replay safety: GAP FOUND AND CLOSED

- **Previous state (verified, not assumed):** the first cycle's fix
  gated the structural fallback on the safety-critical hash. That
  closed critical-argument replays (amount, order id, recipient)
  but still authorized replays for argument changes the safety
  hash does not see: booleans, added arguments, and key renames.
  Reproduced by failing tests before the fix.
- **Fix (`crates/chronostrace-core/src/lib.rs`,
  `Cassette::match_request`):** for `McpToolCall`, a request now
  matches only when tool identity/schema (`structural_tool_hash`)
  AND the full normalized arguments match. The structural hash is
  still required but is never sufficient on its own. Anything else
  is a `ReplayMiss` — never silently converted into a success.
- **Envelope normalization (`strip_mcp_envelope_id`):** only the
  volatile top-level JSON-RPC `"id"` on a `jsonrpc: "2.0"` message
  is stripped for matching. Business identifiers at any depth,
  including bare nested `"id"` fields, are part of the arguments.
  Recorded payloads are unchanged; normalization happens at match
  time, so both the stdio path (which already strips) and the HTTP
  path (which did not) now behave identically.
- **Behavior change (deliberate, P0-mandated):** the first cycle's
  test that allowed a boolean-only change to match structurally
  was updated to require a miss. This strengthens, not weakens,
  the guarantee; the old tolerance is superseded by this mission.
- **Tests:** `mcp_strict_argument_matching_fails_closed_on_any_change`
  (identical -> Exact; envelope-id-only change -> Exact; amount,
  order id, recipient, nested qty, added arg, boolean-only,
  missing arg -> all Miss).

## P1 — MSRV: CLAIM WAS FALSE; FORMALLY RAISED TO 1.88

- **Verified false:** `cargo +1.78 check` fails at manifest parse
  (`clap_lex 1.1.1` requires edition2024); `cargo +1.85 check`
  fails on rustc-version gates (`encoding_rs 0.8.44` and
  `icu_* 2.4.0` via reqwest -> idna require rustc >= 1.88;
  `idna_adapter 1.2.2` requires >= 1.86).
- **Fix (mission's second option):** `rust-version = "1.88"` in the
  workspace `Cargo.toml` (with an explanatory comment), README,
  and `docs/compatibility.md`. Downgrading dependencies to fit
  1.78 was rejected: it would pin older, potentially
  advisory-carrying versions for a cosmetic claim.
- **Proof:** full workspace suite passes on `cargo +1.88 test`
  (same per-suite counts as stable).
- **CI:** explicit `msrv` job added to `.github/workflows/ci.yml`
  (Rust 1.88, `cargo test --workspace --locked`). Caveat, stated
  in the job comment: this workflow triggers only on push/PR to
  master/main, so the job first runs at merge, not on a branch
  push; the 1.88 gate was verified locally before merging.
- Historical docs (`AUDIT_BASELINE.md`, `VERIFIED_AUDIT_FINDINGS.md`)
  were deliberately NOT rewritten: they record the state at their
  time. This document is the correction record.

## P1 — Scrubber hardening: 3 GAPS FOUND AND CLOSED

- **Gap 1 — plural/compound keys:** `"cookies"` (exact-match rule
  only knew `"cookie"`) and `"private_key"` with a non-PEM hex
  value (hex entropy ~4.0 < threshold 4.25) both survived.
  Fix: substring `"cookie"` rule; explicit `"private_key"` /
  `"privatekey"` rule.
- **Gap 2 — URL credentials:** URLs are explicitly excluded from
  entropy redaction and had no credential pattern, so a userinfo
  password and a query-string `api_key` survived verbatim.
  Fix: two patterns (userinfo `://user:pass@`, query
  `api_key/apikey/token/...=`) that redact only the credential
  part; host, path, and benign params survive.
- **Gap 3 — tokenization boundaries:** secrets wrapped in
  `(){}<>` kept their wrappers attached, skewing the entropy
  check. Fix: trim set extended (square brackets deliberately
  NOT trimmed, so `[REDACTED...]` markers keep passing the
  exclusion check).
- **Verified already correct (no change):** `token`,
  `access_token`, `refresh_token`, `session_token`,
  `client_secret`, `authorization`, `cookie`, PEM blocks, JWT,
  Bearer, `ghp_`/`sk-`/`AKIA` patterns, structured-text tokens,
  standalone high-entropy tokens, Unicode entropy (chars, not
  bytes).
- **Scrubbed metadata (whole-cassette gate):** both record paths
  (CLI HTTP record save, stdio record flush) marked the entire
  cassette `scrubbed=true` at save time — including when appending
  to a pre-existing cassette whose old nodes never passed through
  that run's scrubber. Fix: mark only when the cassette is fresh
  or was already marked when loaded. New nodes are always
  scrubbed; old nodes are never retroactively claimed clean.
- **Tests:** `second_audit_sensitive_keys_all_redact`,
  `second_audit_url_and_structured_text_secrets_redact`
  (both failed before the fix), and
  `second_audit_secrets_absent_from_serialized_cassette`
  (scrub -> record -> save -> read file -> assert fake secrets
  absent, markers present).
- **Residual (documented, by design):** short low-entropy secrets
  in unkeyed fields still survive (pinned by the existing
  by-design test); defense must be upstream, not in this crate.

## P2 — Safety-critical extraction: GLOBAL "id" IGNORE FIXED

- **Previous state:** `collect_string_literals` skipped every
  value under any key named `"id"` at any depth. A nested
  business identifier was invisible to the safety hash.
- **Fix:** `compute_safety_critical_hash` first strips only the
  JSON-RPC envelope id, then collects everything else. A bare
  nested `"id"` value now counts verbatim — even with no digits.
  Tokenization is now Unicode-aware: `is_numeric` replaces
  `is_ascii_digit` (non-ASCII digits count), and upper-casing
  uses `to_uppercase` (ASCII results unchanged).
- **Critical keys added:** `transaction_id`, `recipient_id`
  (mission-explicit; values without digits were previously
  invisible as structured fields).
- **Tests:** `safety_hash_counts_nested_and_unicode_business_ids`
  (nested digit id, alphabetic bare id, Unicode digits, envelope
  id invariance, transaction/recipient id).
- **Migration note:** stored safety hashes in old cassettes were
  computed under the old rule. Exact matches are unaffected;
  fuzzy matches on nodes with nested bare-`"id"` values may now
  miss (secure direction). Re-record to refresh.

## P2 — Diff engine: COVERAGE EXPANDED

- Already covered from the first cycle: duplicate targets
  (occurrence indices), reordered steps, response divergence,
  tool-argument mutation, missing nodes.
- **Added:** tool-schema mutation -> `ToolSchemaMutated` +
  Critical + exit code 3; added candidate node ->
  `TopologyDiverged` with `baseline_node_id: None`; branching
  children (one of two children missing) -> 2 identical + 1
  topology-diverged. Classifications remain deterministic
  (iteration order) and explainable (kind + counts + exit code).
- **Not changed:** the TypeScript SDK's separate `diffCassettes`
  still lacks occurrence indices/severity (carried residual).

## P2 — Runtime reliability: INSPECTED

- Session lifecycle/bounds: capped (1,024 / 256 chars / 429),
  verified in the first cycle; unchanged.
- Concurrent ordering: per-session locks held for the whole
  interaction; unchanged.
- MCP subprocess cleanup: 5s child-wait + kill, error frames,
  dropped-request counting; covered by `mcp_lifecycle` tests.
- Timeout behavior: upstream HTTP client has NO timeout
  (deliberate for long LLM generations, but unstated — now
  documented here); child wait is 5s; SSE pump is unbounded.
  No timeout was added: an arbitrary value would break
  legitimate long generations. Recommended: caller-side timeout
  as a future option, not a silent default.
- Child failure: error frames + nonzero exit; tested.
- HTTP request limits: axum's implicit 2MB JSON body limit
  applies (no explicit limit configured). Documented, not
  changed: setting the same value explicitly would be cosmetic,
  and a different value needs measured payload data first.
- Mutex poisoning: `lock().expect(...)` still panics the
  per-request task (not the process). Documented residual;
  structured propagation is the recommended next milestone.
- Concurrent writes: atomic temp+rename+dir-sync; tested.

## P3 — Engineering quality: REVIEWED

- CI: fmt/clippy/schema, 3-OS tests, Python + TS E2E exist.
  Added: MSRV job. Still missing: dependency advisory scanning
  (no `cargo audit`/`deny` step — deliberately not added
  unverified) and branch-push triggers (master/main only).
- Dependency security: no new crates. Lockfile edges unchanged
  except nothing this cycle (no new dependencies were added).
- Benchmarking: only the fuzzy benchmark suite exists; diff /
  chaos / scrub are unbenchmarked (no new benchmarks run or
  claimed this cycle).
- Release workflow: v0.1.0 run `41423882765` is still `queued`
  overall at last check, with a split outcome: both Linux musl
  CLI builds FAILED at the zigbuild compile step
  (x86_64-musl, aarch64-musl); Windows and macOS-arm64 CLI
  builds succeeded; macOS-x86_64 CLI still queued. Python wheel
  jobs: aarch64 FAILED, x86_64 FAILED, x64 succeeded, aarch64
  succeeded, x86_64 still queued. Job logs require repo admin
  rights (403 unauthenticated), so the compiler-level cause is
  NOT established here — the owner must open the failed job
  log. The wheel failures are plausibly downstream of the
  missing musl binary artifact (inference, not proof). This
  release failure is independent of this audit's code changes
  and is NOT fixed by this branch.

## Files modified

- `crates/chronostrace-core/src/lib.rs` (P0 strict MCP match,
  envelope helper, safety-id fix, Unicode tokens, 2 critical
  keys, 2 new tests, 1 updated test, MSRV comment)
- `crates/chronostrace-core/src/diff.rs` (3 new diff tests)
- `crates/chronostrace-scrub/src/lib.rs` (URL patterns, key
  rules, trim set, 2 new tests)
- `crates/chronostrace-proxy/src/lib.rs` (1 new serialization
  test)
- `crates/chronostrace-proxy/src/mcp_stdio.rs`
  (whole-cassette scrubbed gate)
- `crates/chronostrace-cli/src/main.rs`
  (whole-cassette scrubbed gate)
- `Cargo.toml` (MSRV 1.78 -> 1.88), `README.md`,
  `docs/compatibility.md` (MSRV docs)
- `.github/workflows/ci.yml` (msrv job)
- `SECOND_AUDIT_FINDINGS.md` (this document)

## Test totals (this cycle)

- Rust workspace: 99 passed / 0 failed (was 91: +8 new).
- Python SDK: 4 passed / 0 failed.
- TypeScript SDK: typecheck clean, vitest 5 passed / 0 failed.
- MSRV: full workspace suite passes on Rust 1.88; fails on
  1.78 (manifest parse) and 1.85 (rustc-version gates) with
  the evidence quoted above.
- Not run: benchmarks (none exist for the changed paths);
  release-profile suite (debug gate is the payload standard).

## Remaining risks (do not understate)

1. Boolean/null novel critical fields in NON-MCP paths: the
   safety gate still misses them on the fuzzy path (first-cycle
   residual, unchanged). MCP is now strict regardless of type.
2. Short unkeyed low-entropy secrets survive scrubbing by
   design.
3. Scrub-proof API still not wired into the save path
   (first-cycle residual).
4. No advisory scanning in CI; CI does not run on branch
   pushes.
5. Mutex-poisoning panics per-request tasks; upstream client
   has no timeout; no explicit HTTP body limit configured.
6. TS SDK diff parity (occurrence indices, severity) open.
7. v0.1.0 release partially failed (musl + wheels) — needs
   the owner's job-log inspection; unrelated to this branch.
8. Old-cassette fuzzy safety hashes (pre-fix rule) may miss
   on nested-`"id"` nodes; exact matches unaffected.

## Compatibility changes

- **MSRV 1.78 -> 1.88** (breaking for anyone building with
  1.78–1.87; required by the locked dependency set).
- **MCP replay is now strict:** any argument change (any type)
  misses. Replays that previously matched structurally with
  different non-critical arguments now miss. This is the
  mission's P0 requirement, not an accident.
- No cassette schema change, no wire-protocol change, no CLI
  flag change, no SDK API change, no new dependencies.

## Recommended next engineering milestone

1. Wire `scrub_json_for_persistence` into the save path and
   add the CI advisory + MSRV-trigger coverage (branch pushes).
2. Structured mutex-poisoning propagation + caller-configurable
   upstream timeout + measured HTTP body limit.
3. Per-tool critical-path policy for the fuzzy path (closes
   residual 1 for non-MCP kinds).
4. Diagnose the v0.1.0 musl release failure from the admin
   job log and re-run the release.
5. Only then consider merging this branch to master — after
   the owner decides on the two breaking items above (MSRV
   1.88, strict MCP replay).
