# Final Architecture Review — remediation/audit-hardening-v0.1

Read-only review of the uncommitted work on top of `093bb82`
(SessionLocks cleanup, SSE disconnect detection, soak
harness). No code was changed in this review and no behavior
was altered. One finding (cancellation-drop reservation leak)
is reported here with a fix design, NOT implemented, per the
standing instruction that new code needs separate approval.

## 1. SessionLocks invariant verification

### Factory and clone containment — VERIFIED

- The ONLY constructor of a session lock is `lock_for`
  (`lib.rs`, `SessionLocks::impl`): `entry().or_insert_with(Arc::new(...))`
  under the map lock. No other production code creates a
  session `Arc<tokio Mutex>`.
- The ONLY production call site of `lock_for` is `dispatch`
  (one line). Test call sites are tests.
- The ONLY remover is `try_remove`. Production call sites:
  the `dispatch` tail and the four SSE pump terminal paths
  (send failure, `tx.closed()` disconnect, truncated stream,
  successful stream end). No other production code touches
  the locks map.
- Clones are therefore created only under the map lock, by
  `lock_for`, and held only by: a task owning the mutex
  guard (`lock_owned` consumes the `Arc`), a task awaiting
  `lock_owned`, or `dispatch`'s local before the guard is
  created. The `strong_count == 1` removal condition is
  exactly the set of states with no such holder, which is
  why the proof in `docs/session-locks.md` holds. There is
  no path that clones an `Arc` outside this mechanism.

### Two mutexes, one session — IMPOSSIBLE under the proof

Removal requires `strong_count == 1` checked and executed
under the same map lock that `lock_for` clones under, so
the check and the removal are atomic with respect to
cloning. A waiter holding a clone makes the count >= 2 and
defers removal to its own end, where it still uses the OLD
lock (it already holds that `Arc`). A later `lock_for`
after removal creates a fresh lock only when no task can
hold the old one. No interleaving produces two live locks
for one key. The gap between `remove_reservation` and
`try_remove` in the `dispatch` tail contains no await and
no state mutation by the finishing request, and any
interleaving task either clones (making removal fail
safely) or completes independently.

### All paths checked

| Path | Reservation | Lock entry | Verdict |
|---|---|---|---|
| Record non-SSE, node recorded | kept (`Some`) | removed at tail when idle | ok |
| Record non-SSE, timeout/502/chaos fault, no node | removed at tail | removed at tail | ok |
| Record non-SSE, upstream error status (node recorded) | kept | removed at tail | ok |
| Record SSE, success | kept (`Some`) | pump removes at stream end (after explicit guard drop) | ok |
| Record SSE, truncated/error | removed in pump | pump removes | ok |
| Record SSE, client disconnect (send fail or `tx.closed()`) | removed in pump | pump removes | ok |
| Replay hit | kept (`Some`) | removed at tail (guard dropped first) | ok |
| Replay miss, new session | removed at tail (never burns a slot) | removed at tail | ok |
| Replay miss, existing session | kept (`Some` parent) | removed at tail | ok |
| Replay chaos delay (only when chaos configured) | normal tail | normal tail | ok |
| Early 429 (cap) | never created | never created (`lock_for` runs after the cap check) | ok |
| Sessionless requests | never reserved (reservation is keyed sessions only) | `""` lock removed when idle, safe by the same proof | ok |
| MCP over HTTP (`/mcp`, `/mcp/call`) | same `dispatch` path | same cleanup | ok |
| MCP stdio (`mcp_stdio.rs`) | no shared state: zero references to sessions or `SessionLocks` | unaffected | ok |

### No entry left after a finished request — VERIFIED, with one exception

Every normal-completion path above ends with the lock entry
removed when idle, and a waiter-held entry is removed at
the waiter's end. The single exception is the
cancellation-drop path (Section 2): when the axum handler
future is dropped mid-flight, the `dispatch` tail never
runs. No test covers that path today.

## 2. FINDING: cancellation-drop reservation leak (reported, not fixed)

### Mechanism (grounded in code)

`build_router` calls `dispatch(...).await` directly in the
axum POST handler. If the client disconnects before the
handler completes, hyper drops the handler future, so
`dispatch` is dropped at its current await point and its
tail (`remove_reservation` + `try_remove`) never runs.
Strandable await points, all after the reservation insert:

- `lock_owned().await` (waiting for the session lock),
- `send_upstream_request` awaits (connect, headers, body),
- the upstream timeout wrappers,
- the replay chaos-delay sleep (chaos configurations only).

### Effect — bounded and precisely scoped

- The stranded sessions entry stays `None` and consumes
  one cap slot permanently (until process restart). The
  locks entry stays too, but only grows for keys whose
  first use disconnected mid-flight.
- NO data corruption: a later request for the same key
  finds entry `None`, proceeds as a fresh session root,
  records, and advances the entry to `Some` — the key
  self-heals on reuse. Replay behaves the same way.
- NO wrong results: stranded state can only deny future
  admission (after ~1024 such disconnects), never return
  a wrong node or break a chain.
- Sessionless requests are immune (they never reserve).

Severity: medium-low as a slow exhaustion vector; it is
NOT a correctness violation. It is nonetheless the top
hardening item before RC, because it is cheap to fix.

### Recommended fix design (NOT implemented here)

Wrap the reservation in an RAII guard: a small struct
holding `(state, session)` whose `Drop` removes the
sessions entry iff it is still `None`. `dispatch` creates
it right after inserting the reservation and forgets it
(`mem::forget` or an explicit `disarm`) on the paths
where the entry became `Some` or was already removed by
the tail. `Drop` runs on future-drop too, which closes
exactly the gap. Add a regression test that drops a
`dispatch` future mid-flight (e.g. against a slow
upstream) and asserts the sessions entry is gone and a
later request for the key succeeds. Estimated size: one
small struct + one test; no protocol or API change.

## 3. Security and compatibility verification

- **Session ordering.** Unchanged mechanism (one mutex per
  session, held for the whole interaction, SSE pump holds
  it to stream end). Cleanup only removes idle locks.
  Evidence: `harden005` ordering 5/5 + lifecycle tests.
- **Replay determinism.** Matching reads cassette nodes
  and parent pointers. Cleanup writes neither; it only
  removes `None` reservations and idle lock entries.
  Evidence: replay tests + `session_locks_replay_linear_after_cleanup`.
- **DAG parent-child integrity.** Parent pointers are
  written only in `record_node_and_advance`, untouched by
  this work. `remove_reservation` deletes only entries
  that are still `None`, so it can never delete a live
  parent pointer. Evidence: chaining tests assert
  `nodes[1].parent_id == nodes[0].node_id`.
- **Cassette consistency.** Save path untouched (atomic
  temp file + rename + dir sync; scrubbed-metadata gate
  untouched). Cleanup is in-memory only and never touches
  the file. Evidence: soak `stop()` reloads every
  cassette via `Cassette::load_from_file`.
- **Cancellation safety of the cleanup itself.**
  `remove_reservation`, guard drops, and `try_remove`
  are synchronous with no await between them in any
  terminal path, so the cleanup sequence cannot be
  interleaved mid-sequence by another task's await; any
  cross-task interleave between the two lock acquisitions
  is safe (Section 1).
- **MCP lifecycle.** HTTP MCP shares `dispatch` (same
  cleanup, covered). Stdio MCP shares no state with
  `SessionLocks` and is untouched. Evidence:
  `mcp_lifecycle` 5/5.
- **SDK compatibility.** No protocol change: no new
  headers, no changed statuses, no changed payloads.
  The only observable behavior change is a fix (a
  dropped stalled stream now frees its session instead
  of leaking it). Evidence: pytest 4/4, tsc clean,
  vitest 5/5 against a release binary built from this
  branch.

### Races NOT covered by current tests (analyzed, not all testable)

1. Cancellation-drop leak (Section 2) — real, needs the
   RAII fix + test.
2. Mutex-poisoning cascade (pre-existing, not introduced
   here): any panic while holding the state or locks
   mutex poisons it; every later `lock().expect()` panics
   and the proxy wedges until restart. No test covers
   poison recovery; the codebase deliberately uses
   `expect` (fail-fast) rather than recovery.
3. Pump-terminal vs. new-request race (same-session SSE
   ending exactly as a follow-up request arrives):
   analyzed safe (count check under the map lock), but
   no dedicated stress test forces that exact
   interleaving; sequential gated tests cover the
   orderings.
4. Soak-harness readiness sub-millisecond window (test
   only, documented in `docs/soak-test.md`).
5. Reqwest idle pooled connections to upstream are
   transport state, not sessions; they are time-bounded
   by the pool idle timeout and do not count against the
   session cap. No leak observed; noted for completeness.

## 4. SSE resource protection — analysis of the three recommendations

Policy reminder: no total timeout after SSE headers is in
force and unchanged. All three items below are opt-in or
read-only, and none is implemented here.

### (a) Opt-in SSE idle timeout

- **Benefit.** Bounds stalled-stream lifetime
  deterministically: frees cap slots, pump tasks, and
  connections without waiting for the client or upstream.
  Turns an unbounded stall into a configured worst case.
- **Compatibility risk.** Default off = zero behavior
  change. When enabled, it changes semantics for
  legitimately slow streams: a model pausing between
  tokens longer than the threshold gets cut, and the
  truncated stream records NO node (consistent with the
  existing truncated-stream rule) — the client sees a
  stream that simply ends. Operators must set the value
  above legitimate inter-token gaps (reasoning models can
  pause tens of seconds); a suggested starting guidance
  is >= 300s if enabled at all.
- **Impact on long SSE.** None for streams with regular
  events: the idle timer resets on every received chunk.
  Only silent gaps longer than the threshold are cut.
  A 3-hour stream emitting every 30s is unaffected by a
  300s idle timeout.
- **Implementation design.** CLI flag
  `--sse-idle-timeout-secs` (default 0 = off) carried on
  `ProxyState` (a proxy policy, not an upstream property).
  In the pump's biased `select!`, add a third branch: a
  `tokio::time::Sleep` recreated on every received chunk.
  On fire: same terminal cleanup as disconnect
  (remove reservation, drop guard, `try_remove`), return
  without recording. Keep it distinct from
  `--upstream-timeout-secs`, which already covers
  connect + time-to-headers. No new error code is needed
  client-side (stream ends like a disconnect); document
  the behavior and the recommended minimum.

### (b) Maximum concurrent SSE streams (429/503)

- **Benefit.** Bounds tasks, connections, and memory
  independently of the session cap, and fails fast for
  NEW streams instead of degrading admitted ones. This is
  the only recommendation that directly caps the
  slowloris-style exhaustion vector (many stalled
  streams denying admission).
- **Compatibility risk.** Default unlimited = zero
  behavior change. When set, a new stream beyond the
  limit gets 429 (retryable, consistent with the existing
  session-cap 429) — clients must already handle 429 for
  the session cap, so no new client contract appears.
  Existing streams are never killed by this limit.
  Interaction with the session cap: every stream holds
  a session, so the SSE count never exceeds the session
  cap; this is a second, tighter bound. A default equal
  to the cap would be a no-op; recommend opt-in with
  guidance (e.g. 256 for small deployments).
- **Impact on long SSE.** None for admitted streams: no
  timeout, no eviction. Only admission of additional
  streams while at the limit is affected.
- **Implementation design.** `ProxyState` counter
  (`AtomicUsize`). Increment-check must be atomic at
  stream establishment: after SSE headers are confirmed
  in `stream_sse_record`, before spawning the pump, do a
  fetch-add with rollback (if the previous value was at
  the limit, subtract back and return 429, releasing the
  reservation and guard, recording nothing). Decrement on
  every pump terminal path, plus an RAII permit guard
  with `Drop` so a panicking pump cannot leak the count.
  Count record-mode SSE only; replay SSE is fully
  materialized data, not a live stream. The same counter
  serves the observability endpoint in (c).

### (c) Observability (active streams + resource usage)

- **Benefit.** Turns limit-tuning from guessing into
  evidence: operators can see active SSE count, session
  count, lock count, and node count before an outage,
  and can set (a)/(b) from real numbers.
- **Compatibility risk.** None if it is a new,
  read-only endpoint: no existing response changes, no
  recording (the endpoint must bypass `dispatch` and the
  cassette entirely), no session or cap interaction.
  Security note: the endpoint exposes internal counts,
  so it must stay on loopback — document that the proxy
  is a local-dev tool and the endpoint is unauthenticated.
- **Impact on long SSE.** None: counters are atomic
  reads; the endpoint takes the state lock only briefly
  (or reads atomics only) and never touches a stream.
- **Implementation design.** A GET route outside
  `HTTP_ROUTES` (e.g. `/__chronostrace_health__`) in
  `build_router`, returning JSON: `sse_active`,
  `sessions`, `locks`, `nodes`, and optionally RSS
  (Linux `/proc` read with a fallback to omitting it).
  Reuse the (b) counter for `sse_active` so the two
  features share one source of truth. Keep v1 to counts
  only; per-stream stall age (needs last-activity
  timestamps) is a follow-up that pairs with (a)'s
  timer data. No new dependencies: hand-written JSON
  via `serde_json`, which is already present.

## 5. CI validation for this branch without touching master

Current workflow triggers (`ci.yml`): `push` and
`pull_request` to `master`/`main` only — five jobs
(`lint-and-audit`, `rust-test-matrix`, `sdk-python-e2e`,
`sdk-typescript-e2e`, `msrv`). There is no
`workflow_dispatch`. The branch's new work is also
uncommitted, so no CI can run on it at all until it is
committed.

Options, with consequences:

1. **Draft PR branch → master, closed unmerged
   (RECOMMENDED).** Creating a draft PR triggers the
   full GitHub matrix on the merge ref. Master is never
   changed: a draft PR that is closed without merging
   leaves master untouched (closed PRs remain visible
   in the repo's PR list, which is the only trace).
   Validates everything the local gate cannot: real
   GitHub runners and the MSRV 1.88 job. Requires
   approval to commit + create the PR (remote actions).
   Note the branch point equals current master
   (`a05d571`), so the merge ref carries no unrelated
   changes.
2. **Local gate (already green).** Run exactly the CI
   jobs' commands locally: fmt, clippy `-D warnings`,
   workspace tests, `cargo +1.88 check` (verified clean
   in this review), SDK pytest/tsc/vitest. Limitation:
   no GitHub attestation, and macOS/Windows matrix jobs
   are not covered — though the code has no
   platform-specific paths, so that residual risk is
   low, not zero.
3. **Trigger change (add the branch or
   `workflow_dispatch`).** Requires editing `ci.yml` and
   pushing — a permanent shared-CI change for a
   one-branch need. Not recommended as the first step.
4. **`act` (local Actions runner).** Needs Docker, whose
   availability on this VM is unverified, plus proxy
   complications. Not recommended.

Recommended sequence: local gate now (done) → on
approval, commit, then draft PR for the full matrix,
then close unmerged → then RC.

## 6. Remaining risks (consolidated)

- Cancellation-drop reservation leak (Section 2) — the
  top item; small RAII fix + test, not yet implemented.
- CI has never run on this branch's new work (only the
  local gate) — closes via the draft-PR option above.
- Mutex-poisoning cascade (pre-existing): a panic while
  holding a mutex wedges the proxy until restart.
- Release v0.1.0 workflow still unresolved (musl build
  failures; job logs need repo admin) — separate from
  this branch, but part of the release narrative.
- Pump-terminal race analyzed but not stress-tested
  (Section 3, item 3).
- Soak-harness readiness sub-millisecond window
  (test only).
- SSE stall still unbounded by policy (by design) until
  one of the Section 4 mitigations is approved.

## 7. Verdict: ready for Release Candidate?

**Not yet — two small steps away.** The foundation is
consistent: ordering, replay determinism, DAG integrity,
cassette consistency, MCP lifecycle, and SDK
compatibility are all verified unaffected, the
SessionLocks proof is airtight within its stated
mechanism, and the test suite is deterministic under
parallel load. But a Release Candidate should not ship
with a known, reproducible exhaustion vector (the
cancellation leak) or without ever having run the real
CI matrix on its code. After (1) the RAII reservation
guard + its regression test and (2) a draft-PR CI run
closed unmerged, this branch is RC-ready with no
further hardening identified. The three SSE mitigations
remain optional, opt-in follow-ups — they are not RC
blockers while the policy stays as it is.
