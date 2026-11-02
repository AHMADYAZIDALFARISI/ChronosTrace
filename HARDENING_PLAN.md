# HARDENING_PLAN.md — ChronosTrace hardening execution plan

Order is binding: Priority 0 first, then Priority 1, then Priority 2.
No phase starts until the previous phase is implemented and tested.
Nothing is pushed, published, or released without explicit approval;
all work stays local until the user says otherwise.

## Phase 0 — Baseline (done first, no implementation changes)

- [x] Read the tree at HEAD `c151b48`.
- [x] `HARDENING_BASELINE.md`: observed behavior per area.
- [x] `HARDENING_FINDINGS.md`: classified issues with root causes.
- [x] This plan. `PROGRESS.md` is updated after every phase.

Per-issue loop (applies to every item): (1) write a failing
reproduction test, run it, record the failure; (2) implement the
minimal coherent fix; (3) the reproduction test passes plus the full
suite passes; (4) update FINDINGS with the test evidence and PROGRESS
with exact commands and results. Existing tests are never deleted or
weakened; a fix that breaks an existing test is a wrong fix until
proven otherwise.

## Phase 1 — Priority 0

### CT-HARDEN-001 — Forward original, store scrubbed
- Change `record_interaction`: forward the original request body and
  the allowlisted headers (see 002); compute the stored node from
  `scrub_json(original)`; derive prompt/structural features from the
  scrubbed copy (replay already matches on scrubbed forms, so matching
  is unchanged).
- Regression tests: (a) fake upstream requiring a body `api_key`
  returns 200 in record mode and the cassette contains no live key;
  (b) a secret inside prompt text reaches the upstream verbatim
  (echoed) but is redacted in the cassette; (c) replay of the recorded
  node still matches exactly.

### CT-HARDEN-002 — Provider header allowlist
- `FORWARDABLE_HEADERS`: `authorization`, `x-api-key`,
  `anthropic-version`, `anthropic-beta`, `content-type` (case-insensitive
  match on incoming header names; values forwarded verbatim).
  Everything else is dropped. No header is stored in any cassette.
- Regression tests: OpenAI-style upstream (Authorization required)
  and Anthropic-style upstream (`x-api-key` + `anthropic-version`
  required) both authenticate in record mode; a `cookie` /
  `x-custom-secret` header is not forwarded (upstream echoes received
  headers for assertion).

### CT-HARDEN-003 — SSE semantics + incremental streaming
- New `SseEvent { event: Option<String>, data: String, id: Option<String>,
  retry: Option<u64> }` and an incremental parser: bytes accumulate
  across chunk boundaries; events split on blank lines (CRLF
  normalized); per event, collect `data:` lines (joined with `\n`),
  `event:`, `id:`, `retry:` (parsed as u64, invalid values ignored);
  `:` comment lines and unknown fields ignored; a line without a colon
  is treated as a field name with an empty value (per SSE spec) and
  otherwise ignored.
- Record: pump the upstream byte stream; forward each byte chunk to
  the client immediately (no full buffering); feed the same bytes to
  the parser; on clean stream end, store a version-2 marker payload
  `{"_chronostrace_sse": {"version": 2, "events": [...]}}` and advance
  the session parent. On upstream stream error or client disconnect,
  forward what arrived, record NOTHING for that request, and do not
  advance the parent (a truncated stream must never replay as
  complete). A pending event without a terminating blank line at clean
  stream end IS flushed as an event (documented deviation from the
  browser rule, chosen for recording fidelity).
- Replay: version-2 events re-serialize with their fields in canonical
  order (`event:`, `id:`, `retry:`, then one `data:` line per data line)
  and terminate with stream end. Version-1 chunk payloads keep replaying
  as data-only events.
- Regression tests: OpenAI-style stream (data + `[DONE]`, multiline
  data, CRLF, comments); Anthropic-style stream (`event:` names,
  `message_stop` termination, no `[DONE]`); malformed stream (bad
  lines, unknown fields, missing blank line at end); interrupted
  upstream (error mid-stream: client sees the partial prefix, no node
  recorded, parent unadvanced); incremental delivery (client receives
  the first event before the upstream finishes — proven with a slow
  upstream that emits one event, waits, then emits the rest).

## Phase 2 — Priority 1

### CT-HARDEN-004 — Recorded HTTP status + safe headers
- `TraceNode` gains `#[serde(default)] response_status: Option<u16>`
  and `#[serde(default)] response_headers: Option<BTreeMap<String, String>>`.
  `record_node` delegates to new `record_node_full` with `None`s.
  Schema version stays `"1.0"`; the JSON schema gains the two optional
  fields. Old nodes replay as 200 with no extra headers.
- Safe response headers allowlist: `content-type`, `retry-after`,
  `x-request-id`. Nothing else is stored (never `set-cookie`,
  `authorization`, `www-authenticate`).
- Record: non-2xx responses are now recorded (status + safe headers +
  body). JSON bodies store as JSON; non-JSON bodies store as a string
  payload and replay as text with the recorded content type.
- Replay: matched nodes return their recorded status and safe headers;
  chaos still applies to JSON payloads (an injected error overrides the
  recorded status, as today).
- Regression tests: record+replay round-trips for 400, 401, 429, 500,
  503 (status, body, `retry-after` on 429); old cassette without the
  fields still replays as 200.

### CT-HARDEN-005 — Per-session concurrency
- Per-session async locks: `SessionLocks` — a std Mutex guarding a map
  from session key (or a default key) to `Arc<tokio::sync::Mutex<()>>`.
  The HTTP dispatch holds the session's lock for the whole interaction
  (record: forward+record; replay: match+advance; chaos delay happens
  after advance and outside the lock). Sessions proceed in parallel;
  only same-session requests serialize.
- Regression tests: concurrent same-session records against a slow
  upstream produce a linear parent chain in arrival order (no
  siblings); concurrent different-session records interleave freely;
  concurrent sessionless (default) requests also chain linearly;
  retry after an injected 429 matches the same node (parent preserved).
- Measurement: same workload (N requests, upstream delay D) timed for
  one session (serialized, ~N*D) vs N sessions (parallel, ~D), reported
  in PROGRESS.md. No global lock is introduced.

### CT-HARDEN-006 — MCP lifecycle
- Break-reason tracking in `run_bidirectional_child_proxy`: host EOF,
  child EOF, signal. On child EOF: synthesize JSON-RPC error frames
  (`code: -32603`, message naming the dead child) for genuinely pending
  requests, persist the partial cassette, wait for the child exit
  status, and return `Err` when it is non-zero. On signal (Ctrl+C /
  SIGTERM): persist the partial cassette and return `Err`
  (interrupted, not successful). On host EOF: current behavior
  (flush, persist, kill child, `Ok`). On loop I/O error: persist
  partial (best-effort) before propagating the error.
- Request/response distinction: only lines with `method` + `id` are
  tracked as requests; lines with `id` but no `method` are forwarded
  untracked (responses to server-initiated requests). Child lines are
  recordable only when they carry `id` without `method`.
- Duplicate ids: keep-first tracking (`entry().or_insert`).
- Cancellation: `notifications/cancelled` removes the id from
  `in_flight` and unblocks `OrderedRecorder::drain` via a cancelled-seq
  set.
- Regression tests: child crash mid-flight (pending requests get error
  frames, wrapper exits non-zero, partial cassette persisted); signal
  during record (partial cassette persisted, non-zero exit);
  server-initiated request + colliding id (no mis-record);
  cancellation unblocks the recorder chain. End-to-end wrapper tests
  use a scripted fake child process via stdio.

## Phase 3 — Priority 2

### CT-HARDEN-007 — Transport adapter + routes
- Route table in the proxy crate: `[{path, kind}]` driving one generic
  dispatch; built-in table: `/v1/chat/completions` (Llm),
  `/v1/messages` (Llm), `/v1/responses` (Llm), `/mcp/call` (Mcp),
  `/mcp` (Mcp). No custom-route flag in this cycle (kept out to avoid
  an untested configuration surface; the table is a code-level adapter
  point).
- Prompt extraction extended: Anthropic content blocks (array of
  `{type: "text", text}`) contribute their text; Responses API `input`
  (string, or array of items with `content` text) contributes text.
- README documents the supported table and the unsupported list
  explicitly: GET routes, SSE GET endpoints, websockets, file uploads,
  batch APIs, non-POST methods.
- Regression tests: `/v1/responses` and `/mcp` record+replay; Anthropic
  content-block prompt extraction; Responses `input` extraction.

### CT-HARDEN-008 — Unified chaos configuration
- `--chaos-profile` (values: `http-429`, `latency`, `truncated`,
  `drop-field`, `service-503`), `--chaos-probability`,
  `--chaos-seed` on both `replay` and `mcp-wrap`, replacing the two
  single-purpose flags (old flags are removed; this is a CLI breaking
  change, documented in README and PROGRESS). `drop-field` takes
  `--chaos-field`; `latency` takes `--chaos-delay-ms`; `truncated`
  takes `--chaos-keep-ratio`. Invalid combinations fail fast with a
  usage error.
- Support matrix (proven, not claimed): HTTP replay — all five
  profiles (integration tests via `process_interaction`); MCP stdio —
  all five mapped through JSON-RPC frames (integration tests via the
  wrapper's chaos mapping function); SSE replay — explicitly
  unsupported (test asserts SSE ignores a probability-1.0 engine).

### CT-HARDEN-009 — Scrubber hardening
- Key rules: exact `token`, `*_token`, `token_*` (NOT bare substring
  `token`, which would redact `max_tokens`/`tokens_used`); add
  `refresh_token`, `id_token` coverage via the same rule; add
  `passwd`/`pwd` handling alongside `password`.
- Patterns: explicit JWT
  (`eyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+`); explicit
  `sk-ant-` label (previously incidental via `sk-`); private-key
  block test (pattern already exists).
- Custom patterns: `SecretScrubber::with_extra_patterns` plus
  `--redact-regex <REGEX>` (repeatable, replacement
  `[REDACTED_CUSTOM]`) on `replay`, `mcp-wrap` and `verify`; an
  invalid regex is a hard CLI error, never silently ignored.
- Policy version bump to `1.2.0-entropy-regex-jwt` (new rules = new
  policy; old cassettes keep their recorded policy string).
- Limits documented in README and kept in the rescan wording:
  short low-entropy secrets in unkeyed fields, encoded secrets that
  are not standalone high-entropy tokens, and split/encrypted secrets
  are out of scope; `verify --rescan` stays best-effort.

## Validation gate (every phase)

```
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --release
# python SDK: fresh venv, pytest -v (from sdks/python, no PYTHONPATH)
# typescript SDK: npm run typecheck && npm test (CHRONOSTRACE_BIN_PATH=target/release/chronostrace)
# JSON Schema: python jsonschema validation of a fixture cassette + a new-field cassette
```

CI (GitHub Actions) is checked after any push; pushes require explicit
user approval and are out of scope for this cycle unless the user asks.
