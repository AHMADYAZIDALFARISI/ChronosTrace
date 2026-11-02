# HARDENING_BASELINE.md — ChronosTrace pre-hardening snapshot

Date: 2026-11-02. HEAD: `c151b48` (HTTP record mode + SSE record/replay).
Purpose: record what the code actually does before any hardening change, so
every finding can be judged against observed behavior, not memory.
All statements below were read from the tree at HEAD (file references are
to that tree).

## HTTP proxy (`crates/chronostrace-proxy/src/lib.rs`)

- Routes (hardcoded): POST `/v1/chat/completions`, POST `/v1/messages`,
  POST `/mcp/call`. No GET routes, no `/v1/responses`, no `/mcp`.
- Mode is chosen per server instance: `ProxyState.upstream: Option<UpstreamTarget>`.
  Replay mode = no upstream; record mode = upstream present.
- Replay (`process_interaction`): scrubs the incoming request, resolves the
  DAG parent from the session map (`X-ChronosTrace-Session`) or the shared
  default pointer, matches via `Cassette::match_request`, advances the
  parent only on a match that chaos does not turn into an injected error.
  Every matched response is returned with HTTP 200, regardless of what the
  original upstream returned.
- Record (`record_interaction`): scrubs the request FIRST, then forwards
  the scrubbed body to `{upstream}{path}`. Only `Authorization` and
  `Content-Type` are forwarded. The scrubbed request and the scrubbed
  response are stored as a node. Non-2xx upstream responses pass through
  to the client but are never recorded. Responses whose content type is
  neither JSON nor SSE are rejected with 502 and not recorded.
- Session state: `sessions: HashMap<String, Option<String>>` plus
  `current_parent_id`, all behind one `std::sync::Mutex`. The lock is held
  only for the synchronous match/advance section (replay) or for short
  scrub/parent sections (record). Upstream forwarding happens with no lock
  held, so two concurrent same-session record requests can both read the
  same parent and record siblings instead of a chain.
- Chaos: `ChaosEngine::apply` runs on JSON replay payloads only. SSE
  replays skip chaos entirely. Chaos delay sleeps after the lock is
  released; injected errors preserve the parent pointer.

## SSE (`crates/chronostrace-proxy/src/http_record.rs`)

- Record: the whole upstream body is buffered (`resp.bytes()`), then
  `parse_sse_body` splits on blank lines and keeps only `data:` payloads.
  Event names (`event:`), ids (`id:`), `retry:` values, comments and
  field order are discarded. Multi-line `data:` fields are joined with
  `\n` into one chunk string.
- Storage: response payload `{"_chronostrace_sse": {"version": 1,
  "chunks": [...]}}`. A `[DONE]` data line is kept as an ordinary chunk.
- Replay: stored chunks are emitted as `axum::response::sse::Event`
  data-only events. The client never sees the original event names, ids,
  or retry values. Nothing is streamed incrementally in record mode: the
  client receives its first byte only after the upstream body is fully
  read.
- Non-JSON, non-SSE 2xx bodies are rejected (502, not recorded).

## Headers

- Record forwarding allowlist today: `Authorization`, `Content-Type`
  only. `x-api-key`, `anthropic-version`, `anthropic-beta` and any other
  provider header are dropped. No header is ever stored in a cassette
  (only bodies are stored).

## Cassette model (`crates/chronostrace-core/src/lib.rs`, version "1.0")

- `TraceNode` has no response status or response header field. Every
  replayed node is served as 200 with a JSON body.
- `record_node` takes (parent, kind, prompt, structural, request,
  response, latency). All existing call sites use it.
- `load_from_file` backfills an empty `safety_critical_hash` and
  validates the DAG. Unknown extra fields in YAML are ignored by serde
  (so additive optional fields load cleanly in old readers only if the
  reader tolerates them; this tree's serde structs do).
- Schema: `schemas/ctrace-v1.schema.json` pins `version` to `"1.0"` and
  does not mention response status/headers.

## Prompt / structural extraction (HTTP)

- `extract_prompt_text`: `messages[].content` when `content` is a string;
  otherwise the whole request serialized. Anthropic-style content blocks
  (array of `{type, text}`) therefore yield empty prompt text, and the
  OpenAI Responses API `input` field is not handled specially.
- `extract_structural_tools`: `tools`, else `params`, else null.

## MCP stdio wrapper (`crates/chronostrace-proxy/src/mcp_stdio.rs`)

- Host lines with an `id` are tracked in `in_flight` (keyed by the
  serialized id); lines without an id are forwarded without tracking.
  No distinction is made between requests (have `method`) and responses
  (have `result`/`error`, no `method`), so a host response to a
  server-initiated request is tracked as if it were a request, and a
  server-initiated request whose id collides with a host request id can
  be recorded as that request's response.
- Child lines with an `id` are matched against `in_flight` and recorded;
  lines without an id are forwarded only. A server-initiated request is
  therefore indistinguishable from a response.
- Duplicate host request ids overwrite the earlier tracking entry.
- Record order: `OrderedRecorder` chains nodes in host request order;
  `flush` drains leftovers at shutdown. A cancelled request leaves its
  sequence number permanently unfilled, stalling the drain until flush.
- Cancellation (`notifications/cancelled`) is forwarded but never removes
  the cancelled request from `in_flight`.
- Malformed host/child lines are forwarded verbatim when they fail to
  parse (transparent). In offline replay, malformed lines are warned
  about and skipped (no response is possible without an id).
- Shutdown: the loop breaks on host-stdin EOF or child-stdout EOF, then
  flushes, marks scrubbed, persists, kills the child, and returns `Ok`.
  A child that dies (EOF) with requests still in flight therefore exits
  the wrapper with success, and the host never receives responses for
  those requests. There is no signal handler: SIGINT/SIGTERM kills the
  process before the partial cassette is persisted.
- Chaos mapping: `InjectedError` becomes a JSON-RPC `result.isError`
  frame (no `error` object, no status code transport). Latency,
  truncation and drop-field outcomes pass through as mutated payloads.

## Chaos (`crates/chronostrace-chaos/src/lib.rs`)

- Five implemented profiles: `Http429RateLimit`, `LatencySpike`,
  `TruncatedJsonPayload`, `DropSchemaField`, `ServiceUnavailable503`.
  The engine is deterministic given seed + probability (xorshift RNG).
- CLI exposure is not unified: HTTP replay exposes only the 429 profile
  (`--chaos-rate-limit-prob`, seed hardcoded 42); MCP stdio exposes only
  the 503 profile (`--chaos-service-outage-prob`, seed hardcoded 1337).
  Latency, truncation and drop-field are implemented but unreachable
  from either CLI. No CLI seed configuration exists.

## Scrubber (`crates/chronostrace-scrub/src/lib.rs`, policy `1.1.0-entropy-regex`)

- Regex patterns: `sk-…` (20+), `gh[pousr]_…` (20+), `AKIA…`,
  `bearer …` (16+), `-----BEGIN … PRIVATE KEY-----` blocks (multiline).
- Keyed redaction (case-insensitive key match): contains `api_key`,
  `apikey`, equals `authorization`/`x-api-key`/`cookie`/`set-cookie`,
  contains `secret`/`password`/`access_token`/`client_secret`.
- Entropy fallback: whitespace-separated tokens ≥ 32 chars with Shannon
  entropy ≥ 4.25 are redacted, except tokens starting with
  `[REDACTED`, `http://`, `https://`.
- Gaps (by inspection, to be confirmed by test): bare `token`,
  `refresh_token`, `id_token` keys are NOT keyed (only `access_token`
  is); no explicit JWT pattern (relies on entropy); no explicit
  `sk-ant-` label (covered incidentally by the `sk-` pattern);
  short low-entropy secrets in unkeyed fields are undetectable by
  design; base64-encoded secrets are caught only as standalone
  high-entropy tokens; no user-supplied custom patterns.
- `verify --rescan` re-runs this scrubber over stored payloads and exits
  2 on hits; it is documented as best-effort, not proof of absence.

## CLI (`crates/chronostrace-cli/src/main.rs`)

- `replay`: `--cassette`, `--bind`, `--chaos-rate-limit-prob`,
  `--mode replay|record`, `--upstream`, `--similarity-threshold`.
  Record mode saves the cassette once, on graceful shutdown
  (Ctrl+C/SIGTERM).
- `mcp-wrap`: `--mode record|replay|passthrough`,
  `--similarity-threshold`, `--simulate-latency`,
  `--chaos-service-outage-prob`, child command after `--`.
- `verify`: `--cassette`, `--rescan`.
- `diff`: `--fail-on-divergence` optional-value bool.
- No command exposes a chaos seed, a chaos profile choice, custom
  redaction patterns, or extra HTTP routes.
