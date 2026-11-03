# HTTP End-to-End Soak Tests

Location: `crates/chronostrace-cli/tests/soak.rs`.
Run: `cargo test -p chronostrace-cli --test soak -- --nocapture`
(`--nocapture` shows the per-scenario measurements; without it the
assertions still guard every claim.)

## What is tested, and what is not

These tests spawn the real `chronostrace` binary in record mode
(`replay --mode record --upstream <mock> --upstream-timeout-secs 2`)
and drive it over real HTTP. They are the counterpart of the
in-process dispatch tests in `chronostrace-proxy`: the dispatch
tests prove the logic, the soak tests prove the running service.

Each scenario gets a fresh proxy instance (fresh cassette, fresh
session map) and a shared deterministic mock upstream. The mock
selects its behavior from a `soak:<behavior>` marker in the
request body, because the proxy forwards to
`{upstream}{proxy-path}` — behavior cannot be selected by path.
The mock binds an ephemeral port; the proxy binds a freed port
(raced only against localhost reuse, negligible in practice).

Timeout is 2s throughout. "Long" scenarios are scaled so the test
proves the property in reasonable time (e.g. a 12-event SSE stream
at 400ms spacing lasts ~4.8s, comfortably past the 2s timeout).
Latency claims are ranges over repeated runs, never single
measurements.

## Scenarios

| Scenario | Proof |
|---|---|
| Long SSE (3 reps) | 12 events + `[DONE]`, in order, stream lasts ~4.8s past the 2s timeout; one node per completed stream. |
| Slow upstream headers (2 reps) | 5s header delay with 2s timeout → 504 `CHRONOSTRACE_UPSTREAM_TIMEOUT` in ~2.0s, no node, exactly 1 upstream hit (no retry). |
| Non-responsive upstream (2 reps) | Handler never responds → same 504 in ~2.0s, no node, no retry. |
| Stalled SSE | Headers + 2 events, 5s stall, then event + `[DONE]`: the post-stall event arrives at ~5.0s (past the timeout), the stream is not killed; a second stream dropped mid-stall frees its session (next request same session → 200) and records nothing. |
| Concurrent sessions (2 rounds x 50) | All 200, per-request latency min/max/mean per round; 100 nodes persisted. |
| Session cap pressure | 1100 distinct sessions → exactly 1024 admitted, 76 rejected with 429, 1024 upstream hits, 1024 persisted nodes. |
| Recovery | 504 (slow headers) → same session 200; 504 (never) → new session 200; only the 2 successes recorded. |

## Cleanup guarantees

`Proxy::stop()` SIGTERMs the binary (graceful shutdown persists
the cassette atomically) and asserts the process exits within
10s; otherwise it SIGKILLs and fails the test. `Drop` does the
same for failed tests, so a failing test never leaks a proxy
process. The mock upstream lives inside the test process and
dies with it. Node counts are read back from the persisted
cassette via `Cassette::load_from_file`, which also proves the
saved file is valid.

## Known finding fixed by this suite

The first run of the cap scenario admitted all 1100 sessions:
admission checked the cap under the state lock but inserted the
session entry only after the upstream roundtrip, so concurrent
new sessions all passed the check before any insert landed.
The fix (in `chronostrace-proxy/src/lib.rs`, `dispatch`) makes
check + insert atomic — a new session reserves its slot under
one lock or gets 429 — and releases the reservation when the
interaction records nothing (failed first requests never burn
slots; the SSE pump owns its reservation until the stream ends).
In-process multi-threaded regression tests lock this in
(`cap_reservation_atomic_under_concurrent_record`,
`cap_failed_sessions_do_not_burn_slots`,
`cap_broken_sse_stream_releases_reservation`).

## Residuals

- The per-session async lock map (`SessionLocks`) used to grow
  by one entry per distinct session key ever admitted. Fixed
  by opportunistic `try_remove` cleanup (see
  `docs/session-locks.md`): the locks map is now bounded by
  in-flight sessions, proven by the cap test asserting an
  empty locks map after 1024 sessions complete.
- A stalled SSE stream (headers sent, then silence) is
  deliberately never timed out after headers, per the timeout
  policy. It occupies its session (and slot) until the upstream
  closes it or the client disconnects. The pump now notices a
  client disconnect mid-stall via `tx.closed()` (biased
  `select!`, upstream branch first so a normally completed
  stream still records its node); without it, a dropped
  stalled stream leaked its pump task, connection, guard, and
  reservation indefinitely.
- These are localhost tests: they prove HTTP-level correctness
  and the cap/timeout mechanics, not WAN latency, TLS behavior,
  or sustained multi-hour stability.

## Flaky-test investigation (parallel runs)

One parallel `cargo test --workspace` run once failed a single
soak test (6 passed, 1 failed). The failing test's name was
NOT recorded: the run's output was filtered through `grep`
for summaries only, and cargo does not persist test logs, so
the original log is unavailable. That part stays unproven.

The investigation then reproduced failures deliberately by
running three soak binaries concurrently (each with 8 test
threads), which surfaced TWO distinct mechanisms, both fixed
with regression evidence:

1. **Load-induced 504s.** `soak_session_cap_holds_over_http`
   and `soak_sse_stalled_resource_impact` failed with
   `504 Gateway Timeout` on requests that should have been
   200/429: under 3x parallel load, genuinely fast requests
   were CPU-starved past the tests' shared 2s proxy timeout.
   The proxy behaved per policy (upstream exceeded the
   configured timeout); the tests' exact-count assertions
   were coupled to machine load. Fix: only timeout-behavior
   tests use 2s; cap/concurrency/resource tests run their
   proxy with a 30s timeout (`start_with_timeout`), since
   they measure admission, not timeout behavior.
2. **Fd exhaustion (EMFILE).** After fix 1, one binary failed
   in `stop()`'s cassette read with "Too many open files":
   the TEST process holds ~2x each session count in fds
   (client sockets + mock-server sockets), so two heavy
   tests at once exceeded the default 4096 fd limit. Fix:
   the two heavy tests serialize on a static `HEAVY` mutex
   within one binary. This is a harness limit, not a proxy
   bug: the proxy itself holds ~2x cap ≈ 2200 fds at full
   cap, under the same limit (deployment note: keep
   `ulimit -n` >= 4096).

Also fixed along the way: pre-fix readiness was a bare TCP
connect, which a TCP-accepting process on a reclaimed port
would pass. Readiness is now an HTTP POST to an unknown path
expecting axum 404 (no route → no dispatch → no record),
plus a child-alive check before and after the probe.
Regression test `soak_proxy_start_rejects_occupied_port`
proves a TCP-accepting dummy listener is detected, not
mistaken for ready (the old TCP check would have returned
a false proxy there).

Final evidence: standalone soak 9/9, plus three concurrent
soak binaries 9/9 each — after both fixes. A residual
sub-millisecond window remains in readiness (child exiting
exactly during the HTTP probe against a foreign HTTP server
on the same port); it is accepted, not hidden.

## Stalled-SSE resource measurements

Measured with the real binary (`soak_sse_stalled_resource_impact`,
200 stalled streams, timeout 2s):

- Proxy RSS: ~23.7 MB baseline → ~32.4 MB with 200 stalled
  streams, i.e. ~43.5 kB per stalled stream (pump task +
  reqwest response + channel + buffers + sockets). At the
  1024 cap that extrapolates to ~45 MB — memory is not the
  binding constraint.
- Fresh-session latency during the stall: ~3.8 ms — other
  sessions are unaffected by 200 stalled streams.
- Cap occupancy: 200 stalled streams hold 200 cap slots
  (830 further new sessions → 823 admitted, 7 rejected).
  This is the real exhaustion vector: stalled streams deny
  admission to new sessions, not memory.
- File descriptors: 2 sockets per stalled stream (client +
  upstream) plus pump tasks; the 4096 fd limit of this VM
  would bind around ~2000 streams, but the 1024 session cap
  binds first.
- Per-session head-of-line blocking: a stalled stream holds
  its session's guard, so a second request for the same
  session waits indefinitely (pre-existing ordering design,
  not a bug).
- RSS after dropping all 200 streams rose (~74.5 MB), which
  is NOT evidence of a pump leak: the recovery requests
  prove the guards/reservations were released, and the rise
  is confounded by allocator arena retention plus the
  826-node cassette held in memory after the 823-request
  cap probe. RSS alone cannot attribute leaks here; the
  functional proofs (recovery succeeds, cap counts exact,
  no leftover processes) are the leak evidence.

## Mitigation recommendations (NOT implemented — need approval)

The SSE no-total-timeout policy is unchanged. If stalled
streams ever become an operational problem, these are
separate, opt-in mitigations requiring their own approval:

1. **Opt-in SSE idle timeout** (e.g.
   `--sse-idle-timeout-secs`, default off): close streams
   silent for X seconds. This is a policy change and must
   not be smuggled in as a "fix".
2. **Max concurrent SSE streams** (e.g.
   `--max-sse-streams`): 429/503 for new streams beyond the
   limit, bounding tasks/connections without timing out
   legitimate streams.
3. **Per-session stream awareness**: the session lock
   already serializes same-session streams (one active SSE
   stream per session; others queue). Document this as the
   existing mitigant for single-session abuse, and its flip
   side (a stalled stream blocks its whole session).
4. **Observability**: no metrics endpoint exists today; an
   active-SSE count and RSS gauge would make the above
   limits tunable from evidence instead of guesses.
