# Upstream Timeout Policy

Flag: `chronostrace replay --mode record --upstream <url> --upstream-timeout-secs <N>`

## Policy

| Condition | Behavior |
|---|---|
| Default `0` | No timeout (historical behavior). An unresponsive upstream can stall recording indefinitely. |
| Connect + waiting for headers | Bounded by the configured timeout on every upstream request. |
| Non-SSE response | Headers AND full body read bounded by the configured timeout. |
| SSE response | Timeout applies only until headers arrive. Once headers are received, the stream runs unbounded and is recorded when it ends cleanly. |
| Timeout during recording | `504 CHRONOSTRACE_UPSTREAM_TIMEOUT`. No cassette node is recorded; a partial body never becomes a success node. |
| Replay | Never contacts the upstream; this flag has no effect. |

## Why not a client-level total timeout

`reqwest::Client::timeout` covers the entire body read. Applied to
ChronosTrace it would kill legitimate long LLM generations and long
SSE streams. The implementation therefore keeps the client without a
total timeout and applies `tokio::time::timeout` per phase instead:
around the `send()` future (which resolves once headers arrive) and
around non-SSE body reads only.

## Error distinguishability

- Timeout: `504 CHRONOSTRACE_UPSTREAM_TIMEOUT`.
- Transport failure (connection refused, DNS failure, ...):
  `502 CHRONOSTRACE_UPSTREAM_ERROR`.
- Upstream HTTP error status (e.g. 500): recorded as a node and
  passed through verbatim, like any completed response. It is not
  synthesized by the proxy.

## Cancellation and cleanup

On timeout the timed future is dropped, cancelling the request and
freeing the connection. The session guard held for the interaction
drops with the early return, so the next request for the same
session proceeds normally. There is exactly one upstream attempt
per request: no retry, silent or otherwise.

## Configuration compatibility

Old configurations (no flag, or `--upstream-timeout-secs 0`)
behave exactly as before: `UpstreamTarget::new` is unchanged and
delegates to `with_timeout(url, 0)`. The flag applies to the HTTP
`replay --mode record` path only; the MCP stdio wrapper has no
HTTP upstream and is unaffected.

## Recommended starting value

`120` seconds, if a timeout is wanted at all. Lower values risk
false timeouts on slow model generations; the value should be set
from observed upstream latency, not guessed.
