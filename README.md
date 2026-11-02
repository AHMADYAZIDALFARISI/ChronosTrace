<div align="center">
  <img src="./assets/logo.png" alt="ChronosTrace Logo" width="140" height="140" />
</div>

# ChronosTrace

Deterministic VCR replay, graph-aware trajectory diffing, and chaos fault
injection for LLM and MCP workflows.

[![CI](https://github.com/AHMADYAZIDALFARISI/ChronosTrace/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/AHMADYAZIDALFARISI/ChronosTrace/actions)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)

ChronosTrace records multi-turn LLM and Model Context Protocol (MCP) tool
trajectories into content-addressed `.ctrace` cassettes and replays them
deterministically offline. It is written in Rust, with a Python `pytest`
plugin and a TypeScript SDK for Vitest.

Repository: https://github.com/AHMADYAZIDALFARISI/ChronosTrace
Default branch: `master`
Version: `0.1.0`

## Why ChronosTrace?

- **Deterministic offline replay.** Replace live LLM API calls in tests with
  recorded cassettes, so the same trajectory returns the same response.
- **Safety-first matching.** Minor prompt phrasing edits can be tolerated
  through SimHash matching, while safety-critical values must match exactly
  (see “Three-tier matching” below).
- **MCP stdio interception.** `chronostrace mcp-wrap` wraps an MCP
  JSON-RPC 2.0 stdio server for recording, replay, and chaos injection,
  normalizing volatile request IDs.
- **Seeded chaos engineering.** Inject rate limits, outages, latency,
  truncated payloads, and dropped fields to exercise retry and
  circuit-breaker logic deterministically.
- **Verified secret scrubbing.** Sensitive keys, headers, key patterns, and
  high-entropy tokens are redacted before a cassette is written, and the
  scrubber policy version and redaction count are recorded in metadata.

## Repository layout

```text
chronostrace/
├── crates/
│   ├── chronostrace-core/     # DAG state machine, SimHash, trajectory diff
│   ├── chronostrace-scrub/    # Secret scrubbing (regex + Shannon entropy)
│   ├── chronostrace-chaos/    # Seeded deterministic fault-injection engine
│   ├── chronostrace-proxy/    # HTTP replay server and MCP stdio wrapper
│   └── chronostrace-cli/      # CLI entry point and diff renderer
├── sdks/
│   ├── python/                # pytest-chronostrace plugin
│   └── typescript/            # @chronostrace/test SDK for Vitest
├── schemas/
│   └── ctrace-v1.schema.json  # JSON Schema for .ctrace cassette files
├── examples/
│   └── langgraph-refund-agent/  # Multi-turn refund agent under chaos faults
├── docs/
│   ├── architecture.md
│   ├── cassette-format.md
│   └── compatibility.md
├── AUDIT_FINDINGS.md
├── IMPLEMENTATION_PLAN.md
├── SECURITY_THREAT_MODEL.md
└── CHANGELOG.md
```

## Building the CLI

```bash
git clone https://github.com/AHMADYAZIDALFARISI/ChronosTrace.git
cd ChronosTrace
cargo build --release --package chronostrace-cli
./target/release/chronostrace --version
```

The binary is named `chronostrace` (single executable, via `[[bin]]` in
`crates/chronostrace-cli/Cargo.toml`).

## Distribution status

No package is published to PyPI, npm, or crates.io (see
[`SECURITY.md`](./SECURITY.md)). Install from source as above; the
Python and TypeScript SDKs are used from this repository (see
[SDKs](#sdks)). The package names in the manifests
(`pytest-chronostrace`, `@chronostrace/test`, `chronostrace-cli`) are
not installable from registries yet.

## CLI reference

```text
Usage: chronostrace <COMMAND>

Commands:
  replay    Start deterministic offline HTTP replay proxy for LLM & MCP calls
  mcp-wrap  Wrap an MCP stdio server for recording, offline replay, or chaos injection
  diff      Compare two .ctrace cassettes and detect trajectory regressions
  verify    Verify cassette integrity and scrub metadata flag (--rescan re-checks payloads)
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

### `replay`

```bash
chronostrace replay --cassette cassette.ctrace --bind 127.0.0.1:8787
```

Options verified from `--help`:

- `--cassette <CASSETTE>` (required)
- `--bind <BIND>` (default: `127.0.0.1:8787`)
- `--chaos-profile <PROFILE>` (`http-429`, `latency`, `truncated`,
  `drop-field`, `service-503`) with `--chaos-probability <PROB>`
  (both required together; `--chaos-seed` default `42`,
  `--chaos-delay-ms` for `latency` only, default `500`,
  `--chaos-keep-ratio` for `truncated` only, default `0.5`,
  `--chaos-field` required for `drop-field`)
- `--mode <MODE>` (`replay`, `record`; default: `replay`)
- `--upstream <BASE_URL>` (required in record mode, e.g.
  `http://127.0.0.1:8000`)
- `--similarity-threshold <THRESHOLD>` (default: `0.85`; used only when
  record mode creates a fresh cassette)

The replay server serves OpenAI-compatible paths under `/v1` and MCP HTTP
calls under `/mcp`. A request with no matching recorded node returns
HTTP `412` (`CHRONOSTRACE_REPLAY_MISS`). Only POST routes exist:

| Route | Kind |
| ----- | ---- |
| `/v1/chat/completions` | LLM completion |
| `/v1/messages` | LLM completion (Anthropic Messages) |
| `/v1/responses` | LLM completion (OpenAI Responses API) |
| `/mcp/call` | MCP tool call |
| `/mcp` | MCP tool call (Streamable HTTP) |

Explicitly unsupported: GET routes (including SSE GET streams),
websockets, file uploads, batch APIs, and non-POST methods on the
routes above (unknown paths return 404, wrong methods 405).
Parallel HTTP clients should send distinct `X-ChronosTrace-Session`
header values; each session gets its own DAG parent pointer, while
requests without the header share the default pointer.

#### Record mode

```bash
chronostrace replay --mode record --upstream http://127.0.0.1:8000 \
  --cassette cassette.ctrace
```

In record mode the same server becomes a recording proxy: requests are
forwarded to the live upstream, the client sees the upstream response
verbatim, and the scrubbed request/response pair is appended to the
cassette as a new node. The original request is forwarded; only the
scrubbed copy is stored, so a credential the upstream needs still
works while the cassette never contains it. Forwarded headers are
limited to an allowlist (`authorization`, `x-api-key`,
`anthropic-version`, `anthropic-beta`); no header is ever stored in a
cassette. JSON responses are stored as-is; SSE responses
(`text/event-stream`) are streamed to the client as they arrive and
stored as a structured event list (event/id/retry/data) under the
`_chronostrace_sse` marker, and replay re-serializes those events.
Non-2xx upstream responses are recorded with their status and safe
response headers (`content-type`, `retry-after`, `x-request-id`) and
replay reproduces that status; non-JSON error bodies are stored as
text. Recording stops on Ctrl+C / SIGTERM — that is the save point,
when the cassette is written once, atomically. Appending to an existing
cassette continues the recorded trajectory from its last node. MCP
stdio recording is available separately via `mcp-wrap --mode record`
below.

Limitations: the record client connects directly (no environment
proxies) and never follows redirects. Chaos fault injection applies to
JSON replays only, not to SSE streams.

### `mcp-wrap`

```bash
# Record
chronostrace mcp-wrap --mode record --cassette cassette.ctrace -- <child-command>

# Replay (default mode)
chronostrace mcp-wrap --mode replay --cassette cassette.ctrace
```

Options verified from `--help`:

- `--cassette <CASSETTE>` (required)
- `--mode <MODE>` (`record`, `replay`, `passthrough`; default: `replay`)
- `--similarity-threshold <THRESHOLD>` (default: `0.85`)
- `--simulate-latency`
- `--chaos-profile <PROFILE>` with `--chaos-probability <PROB>` (same
  unified flags as `replay` above; injected errors surface as MCP
  `isError: true` tool frames)
- Child command after `--`

On replay, a miss returns a JSON-RPC error with code `-32004`.

`mcp-wrap` serves one session on one stdio stream; it is not a
multi-client server. In record mode, recorded nodes are chained in the
order the host issued requests, even if the child server answers them
out of order.

### `diff`

```bash
chronostrace diff baseline.ctrace candidate.ctrace --format terminal
chronostrace diff baseline.ctrace candidate.ctrace --format markdown
chronostrace diff baseline.ctrace candidate.ctrace --format json
```

Options verified from `--help`:

- `--format <FORMAT>` (`terminal`, `markdown`, `json`; default: `terminal`)
- `--fail-on-divergence[=<true|false>]` (default: `true`)

Exit codes:

- `0`: compatible trajectory (`Identical` or `PromptDriftReconciled`
  within threshold)
- `1`: runtime or I/O error
- `2`: breaking divergence (`ToolSchemaMutated`, `ToolArgumentMutated`,
  `ResponseDiverged`, or `TopologyDiverged`)

### `verify`

```bash
chronostrace verify --cassette cassette.ctrace
chronostrace verify --cassette cassette.ctrace --rescan
```

Loads a cassette, validates its DAG structure, and prints a summary
including the cassette id, node count, similarity threshold, and the
scrub metadata flag. By default this confirms the metadata flag only;
stored payloads are not re-scanned. `verify --rescan` re-runs the
scrubber over every stored payload and reports any secret-pattern or
high-entropy hits that remain (exit code 2 when hits are found). A
clean rescan is a best-effort check, not proof that no secret is
present.

## Three-tier matching

When a request arrives during replay, ChronosTrace evaluates recorded DAG
nodes in three stages:

1. **Tier 1 — Exact canonical hash.** JSON object keys are sorted
   recursively, whole-number floats are normalized, and volatile JSON-RPC
   `id` fields are stripped before SHA-256 comparison at the active
   parent node.
2. **Tier 2 — Structural and safety-critical constraints.** The
   interaction kind, tool schema/name (`structural_tool_hash`), and all
   safety-critical values (`safety_critical_hash`, covering amounts,
   numeric literals, IDs, URLs, paths, and SQL) must match exactly. For
   `McpToolCall`, any tool argument change results in a replay miss.
3. **Tier 3 — SimHash candidate retrieval.** For `LlmCompletion`, if Tier 2
   passes, a 64-bit FNV-1a 3-word shingle SimHash is computed over the
   prompt text. If similarity meets the cassette
   `metadata.similarity_threshold`, the recorded response is replayed;
   otherwise ChronosTrace returns an explicit replay miss (HTTP `412` or
   JSON-RPC `-32004`).

See [`docs/architecture.md`](./docs/architecture.md) and
[`docs/cassette-format.md`](./docs/cassette-format.md) for details.

## Chaos fault injection

All fault profiles in `chronostrace-chaos` are driven by a seeded
Xorshift64* generator with probability clamped to `[0.0, 1.0]`:

| Fault variant | Behavior |
|---|---|
| `Http429RateLimit { retry_after_secs }` | HTTP `429` with a `Retry-After` header |
| `ServiceUnavailable503` | HTTP `503`, or an MCP `isError: true` tool response frame |
| `LatencySpike { delay_ms }` | Delay before returning the payload |
| `TruncatedJsonPayload { keep_ratio }` | Truncate JSON output at `keep_ratio`, clamped to `[0.1, 0.9]`, at a UTF-8 boundary |
| `DropSchemaField { field_name }` | Recursively remove `field_name` from the response JSON tree |

Both `replay` (HTTP) and `mcp-wrap` (stdio) expose the same chaos
surface: `--chaos-profile` (`http-429`, `latency`, `truncated`,
`drop-field`, `service-503`) plus `--chaos-probability`, with a
deterministic `--chaos-seed` (default `42`). Invalid combinations
fail fast (profile without probability, per-profile options on the
wrong profile, `drop-field` without `--chaos-field`).

Support matrix, proven by tests: all five profiles work through HTTP
replay and through MCP stdio frames (where injected errors surface
as `isError: true` tool frames). SSE streams explicitly ignore chaos
(they replay recorded events verbatim). The old single-purpose
flags (`--chaos-rate-limit-prob`, `--chaos-service-outage-prob`)
were removed; the Python/TS SDK option names (`chaos_rate_limit_prob`,
`chaosRateLimitProb`, `chaosServiceOutageProb`) still exist and now
translate to the unified flags.

## Secret scrubbing

`chronostrace-scrub` redacts, before a cassette is written:

- sensitive field names (for example API keys, authorization, cookies,
  secrets, passwords, `passwd`/`pwd`, and token keys: exact `token`,
  `*_token` such as `refresh_token`/`id_token`/`access_token`, and
  `token_*` — deliberately not a bare substring, so usage counts
  like `max_tokens` and `tokens_used` survive),
- key patterns (including `sk-` and `sk-ant-` Anthropic keys,
  `gh[pousr]_` style tokens, and JWTs),
- authorization headers and PEM blocks,
- tokens of at least 32 characters whose Shannon entropy is at least
  `4.25` bits per character (HTTP(S) URLs excluded).

Custom patterns can be added per command with repeatable
`--redact-regex <REGEX>` (on `replay`, `mcp-wrap`, and `verify`;
matches are replaced with `[REDACTED_CUSTOM]`, and an invalid regex
is a hard error).

Scrubber policy version `1.2.0-entropy-regex-jwt` and the redaction count are
recorded in cassette metadata. Cassettes are marked `scrubbed` only after
scrubbing is applied.

Documented limits (not defects): short low-entropy secrets in unkeyed
fields cannot be detected without unacceptable false positives;
encoded secrets are caught only as standalone high-entropy tokens;
split or encrypted secrets are out of scope. `verify --rescan` is a
best-effort re-check, not proof that no secret is present.

Residual risk, documented in
[`SECURITY_THREAT_MODEL.md`](./SECURITY_THREAT_MODEL.md): regex plus
entropy filtering cannot guarantee finding short, low-entropy secrets.
Use test-only credentials when recording.

## Cassette format

Cassettes are `.ctrace` files (version `1.0`) validated against
[`schemas/ctrace-v1.schema.json`](./schemas/ctrace-v1.schema.json). Each
cassette has an id, metadata (`created_at`, `similarity_threshold`,
`scrubbed`, `scrubber_policy_version`, `redaction_count`), and an ordered
list of DAG nodes linked by `parent_id`. Nodes carry the exact request
hash, prompt SimHash, structural tool hash, request and response
payloads, and recorded latency.

Writes are atomic (unique temporary file per write, sync, rename) and DAG validation
rejects duplicate nodes, missing parents, self-parents, and cycles.

## SDKs

### Python (`pytest-chronostrace`)

The plugin lives at `sdks/python/pytest_chronostrace/plugin.py`. Tests
marked with `@pytest.mark.chronostrace(<cassette>)` get a local replay
daemon started automatically, with readiness polling (not a fixed sleep),
early-exit detection, and safe child cleanup. The plugin sets, scoped to
each test:

- `OPENAI_BASE_URL` → `<daemon>/v1`
- `ANTHROPIC_BASE_URL` → `<daemon>`
- `CHRONOSTRACE_MCP_PROXY` → `<daemon>/mcp/call`

The marker also accepts `chaos_rate_limit_prob` to inject HTTP 429 faults
during the test.

### TypeScript (`@chronostrace/test`)

The SDK lives at `sdks/typescript/src/index.ts` and provides:

- `withChronosTrace(options, fn)` — spawns the replay daemon on an
  ephemeral port, waits for readiness, and tears it down (SIGTERM, then
  SIGKILL after 1 second). It mutates `process.env` only when
  `injectEnv: true` is passed.
- `McpStdioTestClient` — a JSON-RPC 2.0 stdio client over
  `chronostrace mcp-wrap`, with per-request timeouts.
- `diffCassettes(baseline, candidate)` — runs `chronostrace diff
  --format json` and returns compatibility plus the structured report.
- `CassetteBuilder` — builds `.ctrace` cassettes programmatically with
  canonical hashes and SimHashes matching the Rust core.
- `canonicalizeJson`, `sha256CanonicalJson`, `computeSimHashHex` —
  TypeScript ports aligned with the Rust implementations.

The TypeScript end-to-end suite at
`sdks/typescript/tests/chronostrace.e2e.test.ts` covers deterministic
replay, SimHash-tolerant prompt edits, HTTP 429 recovery with backoff,
MCP 503 circuit breaking, and argument-mutation detection via diff.

## Compatibility

See [`docs/compatibility.md`](./docs/compatibility.md):

- Rust MSRV: 1.88+ (Edition 2021)
- Python SDK: Python 3.10, 3.11, 3.12
- TypeScript SDK: Node.js 20.x, 22.x
- Operating systems: Linux (`x86_64`, `aarch64`), macOS (`x86_64`,
  `aarch64`), Windows (`x86_64`)

Continuous integration (`.github/workflows/ci.yml`) runs on push and pull
request to branches `master` and `main`, covering formatting, Clippy,
workspace tests, and both SDK suites.

## Development and verification

From the repository root:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --release --package chronostrace-cli
python3 -m py_compile sdks/python/pytest_chronostrace/plugin.py
```

From `sdks/typescript`, with the release binary available:

```bash
export CHRONOSTRACE_BIN_PATH="$(pwd)/../../target/release/chronostrace"
npm install
npm run typecheck
npm test
```

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the full checklist and
engineering rules.

## Further reading

- [`AUDIT_FINDINGS.md`](./AUDIT_FINDINGS.md) — findings CT-001 to CT-016
  and their implementation status
- [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md)
- [`SECURITY_THREAT_MODEL.md`](./SECURITY_THREAT_MODEL.md)
- [`CHANGELOG.md`](./CHANGELOG.md)

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](./LICENSE)).
