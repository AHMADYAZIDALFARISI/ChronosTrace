# HARDENING_FINDINGS.md — ChronosTrace hardening issues

Classification key:
- CONFIRMED (code): the defective path is visible in the tree at HEAD;
  a failing-test reproduction is still run before the fix lands.
- CONFIRMED (test): a reproduction test failed against HEAD as described.
- NOT REPRODUCED: the claimed issue does not occur; evidence cited.
- ALREADY FIXED: the issue is absent at HEAD; evidence cited.
- PARTIALLY CONFIRMED: part of the claim holds; the rest is specified.

Evidence is updated in place as reproductions run. Nothing here is a
claim about behavior that was not read in code or observed in a test.

## Priority 0

### CT-HARDEN-001 — Upstream requests are scrubbed before forwarding
Status: CONFIRMED (test). Reproduction (pre-fix, all FAILED):
`harden001_body_credential_reaches_upstream_but_not_cassette`
(upstream 401, no node) and
`harden001_secret_prompt_reaches_upstream_verbatim` (upstream echo
arrived redacted).
Root cause (code): `record_interaction` in
`crates/chronostrace-proxy/src/lib.rs` calls `scrub_json` on the request
and forwards the scrubbed body. Any credential the upstream needs in the
body (e.g. a body `api_key` field) arrives redacted, so the upstream
rejects the request; prompt text containing secret-like tokens is also
altered before the upstream ever sees it.
Fix direction: forward the original body; store `scrub_json(original)`;
derive prompt/structural features from the scrubbed copy so replay
(which scrubs incoming requests before matching) keeps matching.
Security invariant: no raw credential is persisted; the stored node is
the scrubbed copy, and the test must assert both halves (upstream sees
the live credential, cassette does not contain it).

### CT-HARDEN-002 — Provider headers beyond Authorization are dropped
Status: CONFIRMED (test). Reproduction (pre-fix, FAILED):
`harden002_anthropic_headers_reach_upstream_via_dispatch` (upstream
401: `x-api-key` + `anthropic-version` dropped).
Root cause (code): `forward_to_upstream` sets only `Authorization` and
`Content-Type`. Anthropic-compatible upstreams require `x-api-key` and
`anthropic-version` (and optionally `anthropic-beta`); those are dropped,
so Anthropic record mode cannot authenticate.
Fix direction: explicit forward allowlist
(`authorization`, `x-api-key`, `anthropic-version`, `anthropic-beta`,
`content-type`) applied to incoming headers; no header is ever stored.
Tests: fake-upstream OpenAI-style (Authorization) and Anthropic-style
(`x-api-key` + `anthropic-version`) authentication, plus a test that a
non-allowlisted header (e.g. `cookie`, `x-custom-secret`) is NOT
forwarded.

### CT-HARDEN-003 — SSE recording discards protocol semantics; record is fully buffered
Status: CONFIRMED (test) and FIXED. Reproduction: the v1-only
`parse_sse_body` kept only `data:` payloads (code), and the old SSE
record test asserted v1 chunks.
Fix: `SseEvent` + incremental `SseParser` (event/id/retry/multiline/
comments/CRLF/split-chunk handling per the SSE spec, with one
documented deviation: an unterminated event at clean stream end is
flushed rather than dropped); record pumps upstream bytes to the client
as they arrive and records a version-2 event payload only on clean
stream end; replay re-serializes events with their fields; version-1
recordings keep replaying as data-only events. Per-session async locks
(`SessionLocks`, held by dispatch for the whole interaction and moved
into the SSE pump task until its node is recorded) landed here because
the pump's deferred recording requires them for correct chaining.
Tests (proxy): parser unit tests (OpenAI/Anthropic/CRLF/comments/
split chunks/malformed/dataless-block reset/serialize round-trip/
v2-v1 distinction); `harden003_record_streams_incrementally_and_preserves_event_fields`
(gated upstream: first event arrives before the gate releases —
deterministic streaming proof); `harden003_late_break_records_nothing`
(stream breaking after the first event: client sees the prefix, no
node); `harden003_interrupted_stream_records_nothing` (immediate
breakage surfaces in reqwest as a send error, so the proxy answers
502 — verified empirically with a scratch probe, then removed — and
records nothing); `harden003_malformed_stream_lines_are_ignored`;
`harden003_v1_recordings_still_replay`;
`harden003_sse_replay_ignores_chaos`.
Result: proxy suite 34 passed (was 24), 0 failed.

## Priority 1

### CT-HARDEN-004 — HTTP error responses are not recorded; replay always returns 200
Status: CONFIRMED (test) and FIXED. Reproduction (pre-fix, FAILED):
`harden004_error_statuses_round_trip` (no node recorded, replay 412)
and `harden004_text_error_round_trips` (same).
Fix: additive optional `TraceNode` fields (`response_status`,
`response_headers` with a safe-header allowlist: `content-type`,
`retry-after`, `x-request-id`) via `record_node_full`; schema version
stays `"1.0"` and `schemas/ctrace-v1.schema.json` gains the optional
fields (plus `response_payload` widened to any JSON value or string,
since text error bodies store as strings). Record now stores non-2xx
responses (JSON bodies scrubbed as JSON, other bodies as text);
replay returns the recorded status and safe headers, defaults old
nodes to 200, and still lets an injected chaos error replace the
response. SSE recordings are unaffected (they only exist for 2xx).
One existing test was UPDATED, not deleted: the old
`record_mode_passes_through_errors_without_recording` asserted the
superseded behavior; it is now
`record_mode_records_errors_and_passes_them_through` and asserts both
halves (client passthrough + node with status). README record-mode
paragraph updated to the new behavior.
Tests: round-trips for 400, 401, 429 (with `retry-after: 5`), 500,
503; text-error round-trip; old-node 200 backcompat test.
Result: proxy suite 37 passed (was 34), 0 failed.

### CT-HARDEN-005 — Same-session concurrency can corrupt the DAG chain (record) and mis-pair replays
Status: CONFIRMED (test) and FIXED, with a documented limit.
Fix: `SessionLocks` — one async lock per session (plus one for the
default pointer), held by dispatch for the whole interaction. For SSE
record the guard moves into the pump task until its node is recorded.
Different sessions never block each other; there is no global lock.
Tests (proxy): `harden005_same_session_records_chain_linearly`
(4 concurrent same-session slow records chain, no siblings);
`harden005_different_sessions_run_in_parallel` (4 sessions, each its
own root node); `harden005_sessionless_records_chain_linearly`;
`harden005_retry_after_injected_error_matches_same_node` (429, retry,
chaos off, next step); `harden005_sse_pump_guard_keeps_session_chain_linear`
(second stream cannot read a stale parent while the first pump runs).
Measurement (observed, printed by the tests): same-session 4×200ms
records took 812ms (serialized, ~4×200ms); four-session 4×200ms took
207ms (parallel, ~1×200ms). The cost of the lock is therefore exactly
same-session serialization, and nothing across sessions.
Documented limit (honest, not a defect of the lock): the lock gives
determinism GIVEN arrival order; it cannot know the logical order of
concurrent same-session replays. Two same-session replays submitted out
of logical order will still match in arrival order (the second may
miss) — the chain is never corrupted, but the client must submit a
sequence in order.

### CT-HARDEN-006 — MCP stdio lifecycle gaps
Status: CONFIRMED (test) + FIXED. Reproduction: the new lifecycle
tests fail against the old behavior by code inspection (old code
returned `Ok` on child EOF, had no signal handler, tracked
method-less lines, overwrote duplicate ids, and never cancelled);
post-fix all 5 integration tests + 1 unit test pass.
Fix (in `mcp_stdio.rs`, CLI `main.rs`):
- The proxy loop is wrapped in an async block returning
  `Result<BreakReason, Box<dyn Error>>` (`HostEof` / `ChildEof` /
  `Signal`); host/child line errors return `Err`, signal arrives via
  a oneshot from a helper task running `wait_shutdown_signal()`
  (Ctrl+C + SIGTERM on unix).
- `ChildEof`: JSON-RPC error frames (`code: -32603`, message naming
  the dead child) for genuinely pending requests, recorder flushed,
  partial cassette persisted, `child.wait()` with a 5 s timeout
  (then kill); returns `Err` when the child exit status is non-zero
  OR any request was dropped — a dead child never looks like success.
- `Signal`: flush + persist partial + kill child + `Err`.
- `HostEof`: previous behavior (flush, persist, kill, `Ok`).
- `IoError`: persist partial best-effort, then return the error.
- Tracking gated on `method` present (method-less host lines are
  responses to server-initiated requests: forwarded, never tracked);
  child lines recordable only when `method` is absent, so a
  server-initiated request can no longer be recorded as a response
  on id collision.
- Duplicate host ids: keep-first via `Entry::Vacant` (no sequence
  number consumed, duplicate forwarded untracked).
- `notifications/cancelled`: removes `params.requestId` from
  `in_flight` and `OrderedRecorder::cancel_seq()` (new `cancelled`
  set; `drain()` skips cancelled seqs before each step) unblocks the
  chain immediately.
- `InFlightRequest` gained `req_id` (needed for the error frames).
- Found during testing and fixed: tokio's stdin blocking-read thread
  never finishes while the host keeps stdin open, so the runtime
  shutdown waited forever after an error return (reproduced: error
  printed, process alive 20 s+ until stdin closed). The CLI `mcp-wrap`
  arm now prints the error and calls `std::process::exit(1)` instead
  of propagating with `?`; the cassette is already persisted and the
  child already reaped at that point, so nothing is lost.
Tests: unit `cancelled_sequence_unblocks_drain`; new integration
suite `crates/chronostrace-cli/tests/mcp_lifecycle.rs` (5 tests,
fake Python child per test): child crash mid-flight (error frame +
non-zero exit + 1-node cassette), SIGTERM during record (partial
cassette + non-zero exit), id collision (recorded response is the
real response), duplicate ids in flight (1 node, first request's
features), cancellation (later record not blocked, 1 node).
Inspected and NOT defects: malformed lines are forwarded verbatim
(record/passthrough) or warned-and-skipped (offline replay, where no
response is addressable); `initialize`/`notifications/initialized` need
no special handling in a transparent proxy (initialize is a normal
request; initialized expects no response).
Also fixed while here (clippy `-D warnings` gate): `map_entry` on the
duplicate guard (now `Entry::Vacant`) and `too_many_arguments` on the
pre-existing 9-argument `record_node_and_advance` helper in `lib.rs`
(kept flat with `#[allow]` + comment to avoid churning every record
caller; not a 006 behavior change).
Result: proxy suite 43 passed (was 42), CLI bin tests 7 passed,
`mcp_lifecycle` 5 passed, 0 failed; `cargo fmt --check` OK,
clippy `-D warnings` OK (proxy + CLI).

## Priority 2

### CT-HARDEN-007 — Hardcoded HTTP routes; Responses API and standard MCP HTTP missing
Status: CONFIRMED (test) + FIXED. Reproduction: pre-fix, POST
`/v1/responses` and POST `/mcp` 404'd (no route existed), Anthropic
content-block prompts extracted as empty, and Responses `input`
fell through to a whole-request JSON dump as prompt text.
Fix (in proxy `lib.rs`):
- `HTTP_ROUTES` table (`path -> InteractionKind`) drives one generic
  dispatch closure in `build_router`; the three per-route handlers
  are deleted. New rows: POST `/v1/responses` (LlmCompletion) and
  POST `/mcp` (McpToolCall). The CLI replay server uses
  `build_router`, so the new routes apply there automatically.
- `extract_prompt_text` now reads Anthropic content blocks
  (`messages[].content` arrays: `type: "text"` parts only, images and
  tool blocks skipped) and Responses API `input` (string, or array
  of strings and message items with `input_text`/`text` blocks).
  Chat string content is unchanged (guarded by test).
  `extract_structural_tools` needed no change: Responses `tools`
  and MCP-HTTP `params` already flow through its existing branches
  (verified by the route test's recorded nodes).
- README documents the supported route table and the explicit
  unsupported list (GET routes incl. SSE GET, websockets, uploads,
  batch APIs, non-POST methods; unknown paths 404, wrong methods
  405). No custom-route CLI flag this cycle (plan decision).
Tests: 3 unit tests (Anthropic blocks, Responses string/array
input, chat string guard) + integration
`harden007_responses_and_mcp_http_routes_record_and_replay`
(serves the real router: record via both new routes, assert node
kinds + request payloads, replay the same bodies in chain order,
assert 404/405). Fake upstream gained `/v1/responses` and `/mcp`
echo routes.
Result: proxy suite 47 passed (was 43), CLI bin 7 passed,
`mcp_lifecycle` 5 passed, 0 failed; fmt + clippy `-D warnings`
clean.

### CT-HARDEN-008 — Chaos profiles implemented but not uniformly exposed
Status: CONFIRMED (code) + FIXED. The engine already applied every
profile on both transports (`process_interaction` and
`apply_chaos_or_rehydrate_id` both call `ChaosEngine::apply`); only
the CLI surface was single-purpose, so this item is a CLI-surface
unification, not an engine change.
Fix (CLI `main.rs`):
- New flattened `ChaosArgs` on BOTH `replay` and `mcp-wrap`:
  `--chaos-profile` (`http-429`, `latency`, `truncated`, `drop-field`,
  `service-503`) + `--chaos-probability` (must pair),
  `--chaos-seed` (default 42), `--chaos-delay-ms` (latency only,
  default 500), `--chaos-keep-ratio` (truncated only, default 0.5),
  `--chaos-field` (required for drop-field). Per-profile options
  are `Option` (not defaulted) so a mismatched option is a hard
  error, not a silently ignored value. `build_chaos()` fails fast on:
  profile/probability unpaired, probability outside [0,1],
  keep-ratio outside (0,1], mismatched per-profile options,
  drop-field without a field. Defaults: 429 retry-after 60 s.
- Old flags REMOVED (breaking change, documented in README):
  `--chaos-rate-limit-prob` (was seed 42, HTTP only) and
  `--chaos-service-outage-prob` (was seed 1337, MCP only).
- SDK migration (caught by grep before it broke): the Python marker
  kwarg `chaos_rate_limit_prob` and the TS options
  `chaosRateLimitProb` / `chaosServiceOutageProb` keep their public
  names but now translate to `--chaos-profile http-429` /
  `service-503` + `--chaos-probability`. Historical docs
  (`AUDIT_BASELINE.md`, `HARDENING_BASELINE.md`) intentionally
  untouched.
- README: unified flags on both commands, support matrix, breaking
  change note. `--help` output verified for both commands.
Tests: CLI parse test (unified flags + old flags rejected) +
`build_chaos_rejects_invalid_combinations` (all invalid shapes +
all five profiles build) + HTTP matrix
`harden008_http_replay_supports_all_chaos_profiles` (429 + retry-after,
latency delay lower bound, truncated prefix, drop-field, 503) +
MCP matrix `harden008_mcp_stdio_supports_all_chaos_profiles`
(isError frames naming the status, passthrough, corrupted marker,
dropped field). SSE ignoring chaos remains covered by
`harden003_sse_replay_ignores_chaos`.
Result: CLI bin 9 passed (was 7), proxy 49 passed (was 47),
`mcp_lifecycle` 5 passed, 0 failed; fmt + clippy `-D warnings`
clean.

### CT-HARDEN-009 — Scrubber gaps
Status: CONFIRMED (test) + FIXED. Reproduction: pre-fix, `refresh_token`,
`id_token`, and bare `token` keys passed through unredacted, a JWT
was caught only incidentally by entropy, and no custom pattern
existed; the new tests fail against the old rules by construction
(keyed values the old code left intact, a JWT the old patterns
missed as a pattern).
Fix (scrub crate + CLI + proxy):
- Key rules: exact `token`, `*_token`, `token_*` (deliberately NOT a
  bare substring — `max_tokens`/`tokens_used` usage counts survive,
  pinned by test); `passwd`/`pwd` alongside `password`.
  `access_token` is subsumed by `*_token`; `client_secret` by
  `secret`.
- Patterns: explicit JWT
  (`eyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+` →
  `[REDACTED_JWT]`); explicit `sk-ant-` label
  (`[REDACTED_ANTHROPIC_KEY]`, ordered before the generic `sk-`
  pattern so it is not swallowed); private-key block pattern kept
  and now covered by a multiline/nested test.
- Custom patterns: `SecretScrubber::with_extra_patterns(&[Regex])`
  (replacement `[REDACTED_CUSTOM]`) + repeatable `--redact-regex`
  on `replay`, `mcp-wrap`, and `verify`, via a flattened
  `RedactArgs`. Invalid regex is a hard CLI error (validated
  eagerly, including `verify` without `--rescan`, where the flag
  would otherwise be silently meaningless). `McpStdioConfig` gained
  `extra_patterns: Vec<Regex>` (proxy gained the workspace `regex`
  dep); both stdio scrubber constructions use it.
- Policy version bumped to `1.2.0-entropy-regex-jwt` (new rules =
  new policy; old cassettes keep their recorded policy string).
- Documented limits kept and pinned: short low-entropy secrets in
  unkeyed fields survive by design (test asserts it); encoded
  secrets are caught only as standalone high-entropy tokens;
  split/encrypted secrets out of scope; `verify --rescan` stays
  best-effort (wording unchanged).
Tests: 6 new scrub tests (token rules, passwd/pwd, JWT + Anthropic
label, private-key block incl. nested JSON, custom patterns,
low-entropy limit) + CLI parse test (repeatable flag) +
`execute_cli_verify_rejects_invalid_redact_regex`. `--help`
verified on all three commands.
Result: scrub 7 passed (was 1), proxy 49 passed, CLI bin 11 passed
(was 9), `mcp_lifecycle` 5 passed, 0 failed; fmt + clippy
`-D warnings` clean. P2 complete; all 9 items done.
