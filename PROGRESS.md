# PROGRESS.md — ChronosTrace hardening cycle

Started: 2026-11-02. Base HEAD: `c151b48`. Rule: no push, publish, or
release without explicit user approval. All work in this file is local
until the user says otherwise.

## Status

| Item | Phase | Status |
| ---- | ----- | ------ |
| Phase 0 baseline docs | 0 | Done (no implementation change) |
| CT-HARDEN-001 forward original / store scrubbed | P0 | Done, tested |
| CT-HARDEN-002 provider header allowlist | P0 | Done, tested |
| CT-HARDEN-003 SSE semantics + incremental streaming | P0 | Done, tested |
| CT-HARDEN-004 recorded HTTP status | P1 | Done, tested |
| CT-HARDEN-005 per-session concurrency | P1 | Done, tested |
| CT-HARDEN-006 MCP lifecycle | P1 | Done, tested |
| CT-HARDEN-007 transport adapter + routes | P2 | Done, tested |
| CT-HARDEN-008 unified chaos config | P2 | Done, tested |
| CT-HARDEN-009 scrubber hardening | P2 | Done, tested |

## Phase 0 — Baseline (2026-11-02)

- Read the tree at `c151b48` (proxy lib.rs, http_record.rs, mcp_stdio.rs,
  scrub, chaos, core, CLI, schema). Working tree was clean before docs.
- Created `HARDENING_BASELINE.md` (observed behavior per area),
  `HARDENING_FINDINGS.md` (9 classified issues with root causes),
  `HARDENING_PLAN.md` (binding phase order + per-item design).
- No implementation code was modified in this phase. No test results
  are claimed here; reproduction tests are recorded under each item
  as they run.

## Phase 1 — Priority 0

### CT-HARDEN-001 + CT-HARDEN-002 (2026-11-02)

- Reproduction first (all FAILED pre-fix, proxy crate):
  `harden001_body_credential_reaches_upstream_but_not_cassette`
  (upstream 401, no node),
  `harden001_secret_prompt_reaches_upstream_verbatim` (upstream echo
  arrived redacted),
  `harden002_anthropic_headers_reach_upstream_via_dispatch`
  (upstream 401, headers dropped).
- Fix: `record_interaction` now forwards the ORIGINAL request body;
  the stored node is a separate `scrub_json(original)` copy, with
  prompt/structural features derived from the scrubbed copy (replay
  already matches on scrubbed forms, so matching is unchanged).
  `forward_to_upstream` now takes an explicit header list; dispatch
  collects it via `collect_forwardable_headers` (allowlist:
  `authorization`, `x-api-key`, `anthropic-version`,
  `anthropic-beta`; outgoing content type is always
  `application/json` because the body is re-serialized JSON).
- Guard test: `harden002_non_allowlisted_headers_are_dropped`
  (`cookie`, `x-custom-secret` never forwarded).
- Result: proxy suite 24 passed (was 20), 0 failed. Existing record
  tests updated to the new `record_interaction` header-vec signature
  only; no existing test was weakened.

### CT-HARDEN-003 (2026-11-02)

- Rewrote `http_record.rs`: `SseEvent`, incremental `SseParser`,
  version-2 marker payload, v1 backcompat readers, `send_upstream_request`
  (replaces the collecting forwarder), header allowlist kept.
- Reworked `lib.rs`: `SessionLocks` + per-session async lock in
  dispatch (held for the whole interaction; moved into the SSE pump
  task until its node is recorded), streaming SSE record pump,
  SSE replay via serialized event text (v2) or data-only text (v1).
- Two behaviors found empirically, not assumed: (1) a gated upstream
  proves incrementality deterministically (first event arrives before
  the gate releases); (2) an immediately-breaking upstream body
  surfaces in reqwest as a send error (`IncompleteMessage`), so the
  proxy answers 502 and records nothing — a late break (after the
  first event) instead yields a truncated 200 prefix with no node.
  Both are now locked in by tests.
- Result: proxy suite 34 passed (was 24), 0 failed. The old SSE test
  was renamed (`records_sse_events_...`) and its assertion updated
  from v1 chunks to v2 events; no other existing test was weakened.

### CT-HARDEN-004 (2026-11-02)

- Core: additive optional `TraceNode.response_status` /
  `response_headers` (serde defaults) + `record_node_full`;
  `record_node` delegates with `None`s. Schema stays `"1.0"`;
  `schemas/ctrace-v1.schema.json` gains the optional fields and
  widens `response_payload` to any JSON value or string.
- Proxy: error responses are now recorded (status + safe headers +
  body; JSON scrubbed, other bodies as text) while the client still
  sees the verbatim response; replay renders recorded status/headers
  (old nodes default to 200), renders string payloads as text, and
  still lets injected chaos errors replace the response.
- One existing test UPDATED (not deleted): the old
  `passes_through_errors_without_recording` asserted behavior that
  004 explicitly supersedes; it now asserts passthrough + recording.
- README record-mode paragraph updated (forward-original, header
  allowlist, v2 SSE events, recorded error status).
- Result: proxy suite 37 passed (was 34), 0 failed.

### CT-HARDEN-005 (2026-11-02)

- Mechanism (`SessionLocks`, landed with 003) verified, not just
  believed: 5 concurrency tests through `dispatch` (the real handler
  path) — same-session chain, multi-session parallelism,
  sessionless chain, retry-after-429 sequence, SSE pump guard chain.
- Measurement (observed, printed by the tests, not asserted):
  same-session 4×200ms records took 812ms (serialized);
  four-session 4×200ms took 207ms (parallel). No global lock exists.
- Documented limit: the lock orders by arrival, not by logical
  sequence — concurrent same-session replays submitted out of order
  can still miss; the chain is never corrupted.
- Result: proxy suite 42 passed (was 37), 0 failed.

## CT-HARDEN-006 — MCP lifecycle (2026-11-02)

- Restructured `mcp_stdio.rs`: loop wrapped in an async block
  returning `BreakReason` (`HostEof` / `ChildEof` / `Signal`); signal
  via oneshot from a helper task (`wait_shutdown_signal`: Ctrl+C +
  SIGTERM on unix).
- `ChildEof`: `-32603` error frames for pending requests, flush,
  partial persist, `child.wait()` 5 s timeout then kill; `Err` when
  exit status non-zero or any request dropped.
- `Signal`: flush + persist partial + kill + `Err`. `HostEof`:
  unchanged. `IoError`: persist best-effort, then error.
- Tracking gated on `method` present (host) / absent (child);
  duplicate ids keep-first via `Entry::Vacant`; `cancel_seq()`
  unblocks `OrderedRecorder::drain`; `InFlightRequest.req_id` added.
- Found + fixed in testing: tokio stdin blocking-read thread blocks
  runtime shutdown while host stdin stays open (error printed but
  process alive 20 s+). CLI `mcp-wrap` arm now prints the error and
  calls `std::process::exit(1)` instead of `?`.
- Tests: unit `cancelled_sequence_unblocks_drain`; new
  `crates/chronostrace-cli/tests/mcp_lifecycle.rs` (5 tests, fake
  Python child): crash mid-flight, SIGTERM, id collision, duplicate
  ids, cancellation. `serde_yaml` added as CLI dev-dependency
  (cassettes persist as YAML, not JSON — the first test run failed
  on JSON parsing, which is how this was caught).
- Clippy fixes: `map_entry` (entry API) and pre-existing
  `too_many_arguments` on `record_node_and_advance` (`#[allow]` +
  comment; not a behavior change).
- Result: proxy suite 43 passed (was 42), CLI bin 7 passed,
  `mcp_lifecycle` 5 passed, 0 failed; fmt + clippy `-D warnings`
  clean. P1 complete; P2 (007–009) may now start.

## CT-HARDEN-007 — Transport adapter + routes (2026-11-02)

- `HTTP_ROUTES` table in proxy `lib.rs` drives one generic dispatch
  closure; per-route handlers deleted. Added POST `/v1/responses`
  (LlmCompletion) and POST `/mcp` (McpToolCall); CLI server picks
  them up via `build_router` automatically.
- `extract_prompt_text`: Anthropic content blocks (`type: "text"`
  only) + Responses `input` (string/array, `input_text`/`text`
  blocks). `extract_structural_tools` unchanged (Responses `tools`
  + MCP `params` already covered).
- README: supported route table + explicit unsupported list
  (GET/SSE GET/websockets/uploads/batch/non-POST; 404/405).
- Tests: 3 prompt unit tests + integration
  `harden007_responses_and_mcp_http_routes_record_and_replay`
  (real router, record + replay + 404/405); fake upstream gained
  `/v1/responses` + `/mcp` echo routes.
- Result: proxy suite 47 passed (was 43), CLI bin 7 passed,
  `mcp_lifecycle` 5 passed, 0 failed; fmt + clippy clean.

## CT-HARDEN-008 — Unified chaos config (2026-11-02)

- Engine already applied all profiles on both transports; the fix is
  CLI-surface only: flattened `ChaosArgs` on `replay` + `mcp-wrap`
  (`--chaos-profile` 5 values + `--chaos-probability` must pair,
  `--chaos-seed` default 42, per-profile options as `Option` with
  fail-fast mismatch errors, 429 retry-after 60 s default).
- Old flags removed (breaking change, README documents it).
- SDK migration caught by grep: Python marker kwarg + TS options
  keep public names, translate to unified flags.
- Tests: CLI parse + `build_chaos` invalid-shape matrix + HTTP
  5-profile matrix + MCP 5-profile matrix; SSE-ignore-chaos already
  covered by 003 test.
- Result: CLI bin 9 passed (was 7), proxy 49 passed (was 47),
  `mcp_lifecycle` 5 passed, 0 failed; fmt + clippy clean.

## CT-HARDEN-009 — Scrubber hardening (2026-11-02)

- Key rules: exact `token`, `*_token`, `token_*` (NOT bare
  substring — `max_tokens`/`tokens_used` survive, pinned by test);
  `passwd`/`pwd` alongside `password`.
- Patterns: explicit JWT + explicit `sk-ant-` label (ordered before
  generic `sk-`); private-key block covered by multiline/nested
  test.
- Custom patterns: `with_extra_patterns` + repeatable
  `--redact-regex` on `replay`/`mcp-wrap`/`verify` (invalid regex =
  hard error, validated eagerly); `McpStdioConfig.extra_patterns`;
  proxy gained workspace `regex` dep.
- Policy bumped to `1.2.0-entropy-regex-jwt`; README updated with
  new rules, custom flag, and documented limits (low-entropy
  unkeyed secrets survive by design; rescan stays best-effort).
- Tests: 6 new scrub tests + CLI parse + execute_cli invalid-regex
  test; `--help` verified on all three commands.
- Result: scrub 7 passed (was 1), proxy 49 passed, CLI bin 11
  passed (was 9), `mcp_lifecycle` 5 passed, 0 failed; fmt + clippy
  clean. All 9 hardening items complete.

## Final validation gate (2026-11-02, all local, no push)

- `cargo fmt --all -- --check`: OK.
- `cargo check --workspace --all-features`: OK.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: OK.
- `cargo test --workspace --all-features --release`: all suites
  pass, 0 failed (incl. proxy 49, CLI bin 11, `mcp_lifecycle` 5,
  scrub 7).
- Python SDK: fresh venv, `pytest -v` from `sdks/python` (no
  PYTHONPATH): 4 passed.
- TypeScript SDK: `npm run typecheck` OK; `npm test` (vitest)
  5 passed against the release binary — this also proves the SDK
  chaos-flag translation end to end (tests 3–4 use the old option
  names through the new CLI flags).
- JSON Schema: old-style cassette (no new fields) and new-field
  cassette (004 status + headers, text error body) both VALID
  against `schemas/ctrace-v1.schema.json`.
- `git diff --check`: clean. Final diff vs `c151b48`: 13 files
  changed (+3085/−355) + 5 new paths (`HARDENING_BASELINE.md`,
  `HARDENING_FINDINGS.md`, `HARDENING_PLAN.md`, `PROGRESS.md`,
  `crates/chronostrace-cli/tests/`). HEAD still `c151b48`.
- Cross-platform CI: runs on this push — the user approved commit
  + push on 2026-11-02 16:40 WIB. The local gate above is
  complete; the CI result (incl. windows-latest) is checked after
  the run.

## Unresolved issues

- None. CI is the only check that cannot run locally (see above);
  it runs on this push; its result is checked after the run.

## Commands reference (validation gate)

```
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --release
```

Python SDK: fresh venv under ~/workspace, `pytest -v` from
`sdks/python` with no PYTHONPATH. TypeScript SDK: `npm run typecheck`
and `npm test` from `sdks/typescript` with
`CHRONOSTRACE_BIN_PATH=<repo>/target/release/chronostrace`.
JSON Schema: validate a fixture cassette and a new-field cassette
against `schemas/ctrace-v1.schema.json` with python jsonschema.

## Unresolved issues

- None yet. Items move here only if a fix is blocked or a trade-off
  needs the user's decision; the decision and the date are recorded
  next to the item.
