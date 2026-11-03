# Cancellation-Safe Reservation

## The gap

`dispatch` reserves a session slot (sessions entry `None`)
before doing any work, and its tail releases the reservation
when nothing was recorded. But the axum handler calls
`dispatch(...).await` directly, so if the client disconnects
mid-flight, hyper drops the handler future and the tail
never runs. Strandable await points, all after the
reservation insert:

- waiting for the session lock (`lock_owned`),
- waiting for the upstream (connect, headers, body),
- the upstream timeout wrappers,
- the replay chaos-delay sleep (chaos configurations only).

A stranded entry consumed one cap slot permanently (until
process restart). It could not corrupt data: a later request
for the same key finds entry `None`, proceeds as a fresh
root, and advances it — the key self-heals on reuse.
Sessionless requests never reserve and are immune.

## The fix: ReservationGuard (RAII)

`ReservationGuard` (in `chronostrace-proxy/src/lib.rs`,
next to `remove_reservation`) is created only for a newly
reserved session, disarmed at the start of the `dispatch`
tail (where the tail takes over cleanup; for streaming SSE
the pump owns the reservation), and otherwise removes the
entry on drop — but ONLY while it is still `None`. A
recorded node turns the entry into `Some`, which the guard
never touches, so it can neither delete another request's
reservation (a new reservation for the same key can only be
created after this entry is gone, at which point this guard
is already disarmed or has fired exactly once — `Drop`
runs exactly once) nor a live parent pointer.

The guard's `Drop` also best-effort removes the session's
lock entry via `try_remove_quiet` (same `strong_count`
condition, but never panicking). It succeeds when this
future's session-lock clones are already dropped, and
safely no-ops otherwise — in that case the entry is cleaned
by the key's next completed request, whose tail runs
`try_remove` when idle. This second cleanup was added
because the first cancellation test run showed an aborted
request also left its lock entry behind (the tail that
calls `try_remove` never runs on cancellation either).

## Why Drop is safe here (and alternatives rejected)

- **Synchronous only.** `Drop` takes the std `Mutex`.
  Every state-lock section in this codebase is brief and
  synchronous, never held across an await, and the
  dropping future holds no state lock at any await point
  — so the lock is always free promptly and never
  re-entered on the same thread. Lock order is
  state → locks-map, the same as the `dispatch` tail, so
  no new order is introduced.
- **Never panics.** A poisoned lock is skipped via
  `if let Ok` (both for the state lock and via
  `try_remove_quiet` for the map lock), because panicking
  in `Drop` during unwinding would abort the process.
- **Never awaits**, so it cannot deadlock on async
  cancellation.

Alternatives considered and rejected: a background reaper
with TTL cannot distinguish an in-flight request from an
abandoned one without ownership tracking, and TTL-based
removal was already rejected for SessionLocks; an async
cleanup task would need a cancellation signal plumbed
through every await point. The guard IS the ownership
tracking, and the current sync lock structure allows it —
no architecture change was needed.

## Regression tests (in `chronostrace-proxy` unit tests)

Each test drops a `dispatch` future mid-flight
(`JoinHandle::abort`) and proves the required properties:

- `cancel_waiting_for_session_lock_leaves_no_trace` — a
  waiter blocked on a gated SSE stream's lock is aborted.
  Note: a waiting request is by construction an EXISTING
  session (a new key's lock is fresh and uncontended), so
  it holds no reservation; the test proves the holder's
  stream still records, the session stays intact, and the
  next request chains from it (DAG intact, no lock left).
- `cancel_waiting_for_upstream_cleans_reservation` — two
  sessions wait on a gated JSON upstream; one is aborted.
  Proves the aborted reservation is gone, the cap still
  holds the other session, the other completes, and the
  aborted key works again as a fresh root.
- `cancel_during_chaos_delay_keeps_parent_pointer` —
  replay with a 5s latency fault is aborted during the
  delay. Replay advances the parent pointer BEFORE the
  delay (pre-existing semantics), so the correct
  post-abort state is a live parent pointer, which the
  guard correctly leaves alone. Proves the next
  same-session replay chains (412) and another session
  still hits (200).
- `cancel_after_reservation_before_node_records_clean_root`
  — abort after reservation, before any node. Proves the
  reservation is gone, the cap slot is freed
  (`sessions.len() == 0`), the next request records a
  root, the follow-up chains from it, and an unrelated
  session is its own root.

## What this does NOT change

Session ordering (same mutex, same hold durations),
parent-child DAG (parent pointers are written only in
`record_node_and_advance`, untouched), replay determinism
(matching reads nodes and parents, untouched), sessionless
behavior (no reservation, no guard), replay behavior
(same advance timing), and the SSE policy (no timeout
change; the pump's disconnect handling is untouched).

## Residual

- A lock entry can still linger after an abort in the
  rare case where this future's session-lock clones have
  not dropped yet when the guard fires; it is then
  cleaned by the key's next completed request. Bounded
  by distinct aborted keys, tiny per entry, reused on
  next use — not a cap or correctness issue.
- Mutex-poisoning cascade (pre-existing): a panic while
  holding a mutex wedges the proxy until restart. The
  guard's `Drop` skips poisoned locks rather than
  panicking, so it cannot make this worse.

## Residual lock entries after cancellation: lifetime analysis (2026-11-03)

Follow-up investigation, requested before any CI run.
Question: does `try_remove_quiet` failing (or cleanup
never being attempted) leave idle lock entries behind
permanently, and can repeated cancellations regrow the
map without bound?

### Complete inventory of session-lock Arc clones

For one session key, clones exist only in these places
(all created by `lock_for`, under the map lock):

1. The map's own clone (exists while the entry exists).
2. The dispatch frame's `session_mutex`, moved into
   `lock_owned()` — after the move, the dispatch frame
   holds no clone; the resulting `OwnedMutexGuard` holds
   exactly one.
3. That guard, moved into `record_interaction_inner`
   (record path) or held in the dispatch frame (replay
   path), or moved into the SSE pump task (streaming
   record only, after headers).
4. A waiter's clone: a second dispatch for the same key
   calls `lock_for` (same Arc) and awaits `lock_owned`
   on it; on acquiring, it becomes the holder.

No other production code clones a session lock
(`try_remove` callers hold no clone; `mcp_stdio.rs`
shares no state; the replay path holds its guard only
for the interaction's duration).

### When `try_remove_quiet` sees count > 1

Exactly three situations, all correct deferrals (the
entry is genuinely in use, and the user of that clone
has its own cleanup later):

- (i) a live holder guard for the key (active request
  or streaming pump);
- (ii) a live waiter clone for the key (queued request,
  which becomes the holder and cleans at its own end);
- (iii) a live pump guard for a streaming SSE session.

A fourth hypothetical case — the aborting future's own
clones not yet dropped when its guard fires — does NOT
occur for guarded dispatches, by drop order (see
below): the tail has no await (disarm + tail cleanup
are atomic w.r.t. cancellation), the guard is created
before the first await, and the session clones live in
frames that drop first.

### Drop order (the load-bearing rule)

- Record path: the session clone lives in
  `record_interaction_inner`'s frame (or, for streaming
  SSE, in the pump task, which is independent of the
  dispatch future). Dropping dispatch drops the inner
  frame first, then the dispatch frame's locals in
  reverse declaration order — `reservation_guard` was
  declared before the lock acquisition, so it drops
  after the session clones are gone.
- Replay path: the dispatch frame holds `guard`
  (declared after `reservation_guard`), so reverse
  order drops the session guard first.
- Lock-wait abort: the pending `lock_owned` subfuture
  drops before the outer frame's locals.

This is pinned by the regression test
`cancel_drop_order_inner_before_outer` (a standalone
probe: inner frame drops before the outer frame's
earlier local), which passes on the current toolchain.
If it ever fails on a new toolchain, the guard design
must be revisited.

### The gap: unguarded aborts have NO cleanup at all

`ReservationGuard` is created only for newly reserved
sessions. A follow-up request for an EXISTING session
never reserves, so it has no guard; on abort its tail
never runs and nobody attempts lock cleanup. Two
concrete cleaner-less cases, both reproduced by tests:

- Single holder abort (existing session, no waiter):
  the holder's guard drops with the frame, no
  `try_remove` is ever called, entry stays idle at
  count 1. Test
  `cancel_existing_session_holder_leaves_no_lock`
  FAILS on current code at the locks-empty assertion
  (live parent pointer correctly survives).
- Holder + waiter double abort (existing session):
  neither future has a guard, neither tail runs. The
  waiter-abort alone is fine (the holder's tail cleans
  once the waiter clone is gone), and the holder-abort
  alone with a live waiter is fine (the waiter becomes
  holder and cleans) — but when BOTH abort, no one
  remains. Test `cancel_holder_and_waiter_leaves_no_lock`
  FAILS on current code.

Answers to the five questions:

1. `try_remove_quiet` "fails" only for (i)–(iii)
   above, all correct deferrals. The real failure
   mode is different: for unguarded aborts, cleanup is
   never attempted in the first place.
2. No. A leftover clone does NOT always have a
   cleaner: the double-abort case and the single
   unguarded-holder case leave an idle entry with no
   owner and no future cleanup call.
3. Yes, possible: the clone is released during the
   aborted frame's teardown, and if no later request
   for that key ever arrives, the idle entry stays
   forever (until process restart). The next request
   for the SAME key cleans it via its tail, but a key
   that is never reused never gets cleaned.
4. Yes, measurably: 50 aborted follow-ups on 50
   distinct existing keys leave exactly 50 idle
   entries (test
   `cancel_repeated_existing_sessions_leave_no_locks`
   fails with `got 50 idle entries`). Growth is one
   entry per distinct aborted-existing key — the same
   class as the original SessionLocks growth, now
   reachable only via aborts. Guarded new-session
   aborts do NOT grow (test
   `cancel_repeated_new_sessions_leave_no_locks`
   passes: 50 aborts, zero entries).
5. Yes — see the fix design below. No TTL is needed.

### Bounded non-growth cases (not gaps)

- Sessionless requests (`""` key): no guard by design
  (nothing is reserved), so a sessionless abort can
  leave the single shared `""` entry idle — but it is
  ONE key, reused by every sessionless request, and the
  next sessionless request's tail cleans it. It cannot
  multiply.
- Pump panic: the pump's session guard drops on
  unwind but its terminal cleanup is skipped. The pump
  has no panic paths except poisoned-lock `expect`s
  (the SSE parser and record path are panic-free in
  production code), and a poisoned lock wedges the
  proxy anyway — negligible, but the sweep backstop
  below would catch it too.
- Sessions-map live pointers after abort are session
  state, not a leak: a session that recorded a node
  keeps its pointer by design (no session expiry
  exists). Only lock entries and `None` reservations
  are leaks; `None` reservations are fully covered by
  the guard (no `None` entry survived any cancel test).

### Fix design (NOT yet implemented — awaiting approval)

Primary fix — universal cleanup guard. Generalize
`ReservationGuard` so EVERY dispatch with a session
carries one (not just newly reserved sessions):

- On `Drop` (still synchronous, still never
  panicking): if this dispatch reserved AND the entry
  is still `None`, remove it (unchanged); then ALWAYS
  call `try_remove_quiet` for the key.
- Created for existing sessions and sessionless too
  (key `""`), with `reserved = false` for those; the
  sessions-entry removal stays conditional on
  `reserved`, the lock cleanup becomes unconditional.
- Declared BEFORE the session lock acquisition (as
  today) — that declaration order is load-bearing and
  must be documented as such: it guarantees the guard
  drops after the session guard (replay), after the
  inner frame (record), and after the pending lock
  future (lock-wait).
- Disarm at the tail as today; the tail already does
  both cleanups explicitly, so a disarmed guard is a
  no-op and normal completion is unchanged.
- Why it closes every case: a waiter abort's own
  guard no-ops while the holder is live (count > 1,
  correct) and cleans if it is the last clone; in the
  double-abort case the LAST guard to drop sees
  count 1 and cleans; a single unguarded-holder abort
  becomes a guarded abort. Ordering is preserved by
  the same `strong_count` proof (a guard never
  removes a lock another task holds or waits on).

Backstop (recommended, order-independent) — threshold
sweep in `lock_for`: if the map exceeds a threshold
(e.g. 2048 = 2 × MAX_SESSIONS), remove every entry
whose `strong_count` is 1, under the map lock. Safety
proof identical to `try_remove` (count 1 ⟹ no holder,
no waiter, no pump; future `lock_for` creates a fresh
lock, so two live locks per key are impossible). Cost
is amortized: after a sweep the map holds only live
entries, so the next sweep waits for 1024
above-threshold calls (the budget counts calls, not
dead entries); the sweep runs under the existing
state → map lock order, no new order. This catches
the LOCK entry of any path without a guard (pump
panic) and removes any dependence on drop order for
eventual LOCK cleanup, at the price of entries
lingering until the threshold is crossed (not
immediately). Scope: the sweep touches ONLY the
SessionLocks map; it never touches the sessions map,
so a `None` reservation left by a panicking pump is
NOT swept (see the F-02 note in Residual risks).

Rejected: Weak-in-map redesign (same guarantee as the
sweep, larger change, no benefit over guard + sweep);
any TTL (cannot prove idleness without ownership, the
same flaw already rejected twice).

### Status

Implementation is intentionally NOT changed in this
turn: only tests (5 new, 3 of them failing
reproductions) and this document. The fix above
awaits a separate approval, after which the three
reproduction tests must turn green with no other
behavior change.

UPDATE 2026-11-03: approved and implemented — see the
section below. All three reproduction tests are green
with no other behavior change.

## Universal cleanup guard + backstop sweep: implemented (2026-11-03)

Approved by the user with constraints: guard on every
dispatch, sweep threshold 2048, no behavior change to
timeout, replay, SSE policy, cap, HTTP statuses, public
API, cassette format, CI workflow, master, or release
config. No commit, push, PR, or merge (still pending a
separate decision).

### Guard: what changed in code

- `ReservationGuard` gained a `reserved: bool` field.
  `Drop` removes the sessions entry ONLY when this
  dispatch created it (`reserved`) AND it is still
  `None`; it ALWAYS runs `try_remove_quiet` for its
  key. The flag is load-bearing: without it, a
  follow-up dispatch observing another dispatch's
  still-`None` reservation could delete that
  reservation. With it, a non-reserving dispatch can
  never touch the sessions map.
- `dispatch` creates the guard for EVERY dispatch,
  unconditionally, immediately after the cap block and
  before the session lock acquisition and the first
  await. Sessionless requests get the shared `""` key
  with `reserved = false` (their guard cleans only the
  `""` lock entry on abort). The 429 early return
  happens before guard creation, so a rejected request
  carries nothing to clean.
- The tail is unchanged: disarm, `remove_reservation`
  (non-SSE), unconditional `try_remove`. For streaming
  SSE the tail disarms and the pump still owns the
  reservation and the lock until stream end.
- The declaration order is documented as an invariant
  in the guard's doc comment and at the creation site:
  guard before lock acquisition before first await, so
  on abort the session clones drop before the guard
  (pinned by `cancel_drop_order_inner_before_outer`).

### Sweep: what changed in code

- `SessionLocks` gained a `since_sweep: Mutex<usize>`
  counter (its `Default` construction is unchanged).
- `lock_for` runs the sweep only when the map exceeds
  2048 entries AND the budget is spent: the counter
  increments once per above-threshold call and the
  scan runs at most once per 1024 such calls, then
  resets. The scan itself is `retain` keeping only
  entries with `strong_count > 1`, under the map lock.
- Lock order is unchanged: the counter's mutex is only
  ever taken while the map lock is held (map ->
  counter), and the sweep holds no other lock. A
  poisoned counter lock skips the sweep (best-effort)
  but still returns the requested lock.
- The sweep is documented in code as a backstop, not a
  replacement for the guard: the guard cleans
  immediately on abort; the sweep catches the lock
  entry of paths without a guard (e.g. a panicking
  pump task) and removes any dependence on drop order
  for eventual lock cleanup. Scope: the sweep touches
  ONLY the SessionLocks map; it never touches the
  sessions map, so it cannot clean a `None` sessions
  reservation left by a panicking pump.

### Why an active lock can never be deleted

Both mechanisms use the same proof. Every clone of a
session lock comes from `lock_for`, under the map
lock. Checked under that lock, `strong_count == 1`
means the map holds the only clone: no holder guard,
no waiter clone, no pump guard exists for that key.
`try_remove`/`try_remove_quiet`/sweep all remove only
count-1 entries, so a held, waited-on, or pumped lock
(count >= 2, or >= 3 with a waiter) is never touched.
And removal cannot create two live locks for one key:
after removal, the next `lock_for` for that key
creates a fresh lock, and no task can hold the old one
(count 1 proved it). The guard additionally drops
after the session clones by declaration order, so its
own clones never inflate the count at cleanup time;
when another task's clones do inflate it, the guard
correctly no-ops and that task's own cleanup owns the
entry.

### Before / after entry counts (measured in tests)

- 50 aborted follow-ups on 50 distinct existing keys:
  BEFORE 50 idle lock entries (1 per key); AFTER 0
  entries, all 50 parent pointers intact.
- 50 aborted new-session dispatches: 0 entries both
  before and after (guard path was already clean).
- Sweep: 3072 idle entries above threshold collapse
  to 1 (the trigger key) on the budgeted sweep call;
  the 1023 preceding above-threshold calls provably do
  not sweep (map still 3072), demonstrating the
  amortization. (Constructed idle entries via direct
  `lock_for`; see the bounds correction below — this
  measures sweep mechanics, not production map sizes.)
- Lock-map bounds: see "Lock-map bounds, corrected
  (2026-11-03)" below. The earlier claim of a hard
  3073-entry bound is WITHDRAWN: it holds only if live
  entries stay ≤ 1025, which production churn can
  violate (see the reserve-abort-stall construction
  there). What the implementation does guarantee is
  split by component: idle entries are sweep-bounded;
  live entries are concurrency-bounded, not
  config-bounded.

### New tests and results (all green)

- `session_locks_sweep_removes_idle_above_threshold` —
  idle cleaned, budget gating observed (3072 before
  the budgeted call, 1 after).
- `session_locks_sweep_keeps_held_lock` — held entry
  survives (pointer identity checked), cleanup works
  after the sweep.
- `session_locks_sweep_keeps_waited_lock` — waited
  entry survives, cleanup works after the sweep.
- `session_locks_sweep_keeps_pump_lock_and_stream_completes`
  — sweep runs inside a live stream's dispatch, pump
  entry is the only one left, stream completes and
  records its node, locks empty at end.
- `session_locks_sweep_preserves_ordering_and_parent` —
  follow-up chains from the seeded node after the
  sweep, unrelated session is its own root, locks
  empty at end.
- The three former reproductions are green:
  `cancel_existing_session_holder_leaves_no_lock`,
  `cancel_holder_and_waiter_leaves_no_lock`,
  `cancel_repeated_existing_sessions_leave_no_locks`.
- Repeated runs for races: cancel suite 9/9 five
  times, session_locks suite 9/9 five times, soak
  suite 9/9 twice (second run in the full gate).

### Lock-map bounds, corrected (2026-11-03)

Correction of the earlier 3073 claim, derived from
the production code paths. The error in the old
derivation: the budget bounds sweep FREQUENCY, and
`retain` preserves every live entry — so a bound on
total size cannot follow from threshold + budget
alone. The correct guarantees split by component.

Sessions map: length ≤ 1024, always. Inserts happen
only at the cap-checked site (`dispatch`: a new key
is inserted only when length < 1024) and at the
record/replay advance sites, which write keys that
were present at the dispatch's cap check (existing
keys overwrite, no growth) or were removed after that
check (each such re-insert is offset by that removal;
concurrent re-inserts of the same removed key
overwrite each other). Sessionless traffic never
enters the sessions map. The sessions bound is solid.

Production `lock_for` paths: exactly one site,
`dispatch`. The cap gate admits a dispatch when its
key already exists in sessions (no slot consumed) or
when a slot is free (new key inserted). So keys in
sessions with live work contribute at most 1024 lock
entries, plus 1 for the shared sessionless key `""`.

Live entries are NOT config-bounded. The gap is
removed-key holders/waiters: a key can have live
lock clones while absent from sessions, via this
reserve-abort-stall cycle (all production paths):
1. Dispatch R reserves key K (entry `None`, one slot
   consumed), acquires K's lock, and waits on an
   upstream that never responds.
2. Dispatch W arrives for K while K exists, so it
   passes the gate without a slot, calls `lock_for`
   (same lock Arc), and waits on the lock.
3. R aborts: R's guard removes the `None` entry and
   its `try_remove_quiet` correctly no-ops (W's clone
   makes the count 2). K is now absent from sessions.
4. W acquires the lock and waits on the same
   unresponsive upstream. With the default timeout
   (`--upstream-timeout-secs 0` means no timeout; the
   timeout doc states an unresponsive upstream can
   stall recording indefinitely), W waits indefinitely,
   holding K's lock, consuming no sessions slot.
Repeat for distinct keys and live entries accumulate
past 1024, 2048, and 3073 alike — bounded only by
concurrent in-flight work (tasks, upstream
connections, file descriptors), not by any config.
Each such entry mirrors a live task + connection; the
map growth is a symptom of task/connection
accumulation, not an independent leak, and each entry
resolves when its holder resolves (abort, timeout,
or completion → guard/tail cleans it). In ordinary
operation without this churn, live entries stay
≤ ~1026 (sessions keys + `""` + poll-cycle
transients), because removed-key holders resolve
promptly once their upstream answers or times out.
Note the asymmetry: SSE pump keys are always in
sessions (reserved `None` or recorded `Some`), so
stalled SSE streams cannot drive this accumulation;
replay holders resolve quickly (bounded chaos delay,
no upstream wait). Only non-SSE record dispatches
with no timeout and an unresponsive upstream can.

Idle entries ARE sweep-bounded. The guard and the
tail clean idle entries immediately on every normal
dispatch path, so in production idle entries arise
essentially only from pump panic. Whenever a sweep
runs, `retain` removes every idle entry present at
that moment; sweeps run at least once per 1024
above-threshold calls (the frequency bound survives:
the counter increments only on above-threshold calls
under the map lock and resets only on a sweep, so
≥ 1024 counted calls separate consecutive sweeps,
including carried-over values). Idle therefore cannot
accumulate from normal paths at all.

Total map size = live + idle: NO universal bound.
In ordinary operation total stays ≤ ~1026 and the
map never reaches threshold. The 3073 figure is
valid only under the extra assumption live ≤ 1025,
which the churn construction violates.

Threshold 2048 in production: in ordinary operation
it is never reached (live ≤ ~1026, idle ≈ 0), so the
sweep is purely a defensive backstop that never fires.
It fires in production only in two pathological
cases: (a) live churn overshoot — the sweep then
fires on budget but correctly retains every live
entry, i.e. it removes nothing and the map stays
above threshold (futile but harmless, and frequency
still bounded); (b) mass idle from pump panics — the
sweep removes them, though the proxy is likely wedged
by poisoning anyway. Tests exceed 2048 only by direct
`lock_for` without dispatches, an artificial
construction, not a production path.

What bounds live growth in practice: the sessions
cap (for keys in sessions), `--upstream-timeout-secs`
(non-SSE stalled holders resolve via 504 → tail
cleans; SSE stalls are policy and counted in
sessions), and connection/task resources. If live
overshoot is ever observed, the remedy sits at that
layer (timeout configuration), not in SessionLocks.

Note: the code comments in `lib.rs` still carry the
old wording ("Live entries can never exceed ~1025"
and "bounded, never unbounded" in the sweep comment).
They are NOT corrected here: this turn is
documentation-only for `docs/cancellation-safety.md`
by instruction, and comment fixes belong to a future
code-touch pass.

### Residual risks

- Sweep amortization is structural (the budget
  counter), not directly test-observed under live
  load: the tests prove the budget gates the scan and
  live entries survive, but a production load profile
  was not measured. The scan covers the whole map at
  sweep time (size = live + idle at that moment) and
  runs at most once per 1024 above-threshold calls, so
  its frequency is bounded by construction even when
  its size is not; no wall-clock benchmark has been
  measured, so no microsecond figure is claimed.
- Pump panic remains a guard-less path (only via
  poisoned locks, which wedge the proxy anyway); the
  sweep now bounds its LOCK entries instead of leaving
  them forever, but a `None` sessions reservation left
  by a panicking pump is NOT swept and keeps its cap
  slot until restart (F-02). In practice this is
  negligible, since pump panic is reachable only via
  poisoned locks.
- Session cap is a LIFETIME limit, not a concurrency
  limit (F-01). The sessions map holds one entry per
  distinct session ever seen: live pointers are never
  removed — only `None` reservations are cleaned —
  because replay chaining needs them, and admission
  rejects a new key once the map holds 1024 entries.
  Consequence: after 1024 distinct sessions over the
  process lifetime (completed, live, or aborted after
  recording), every further NEW session receives 429
  ("Session limit reached; retry with an existing
  session id") until the process restarts, even if no
  sessions are currently active. Existing sessions keep
  working. This is by design for a recording proxy,
  not a leak, but operators must plan restarts (or a
  future session rotation design) around it; no
  eviction exists today. Only lock entries and `None`
  reservations were leaks, and both are now closed.

### Test coverage gaps (future work, NOT in this release scope)

Recorded 2026-11-03 from the final review. Not
implemented; the release scope stays controlled.

High priority:
1. Concurrent abort storm on many session IDs
   (current repetition tests abort sequentially).
2. Double abort in reverse order: waiter first, then
   holder (current test aborts holder first).
3. HTTP-level client disconnect while SSE is
   streaming (current abort tests are in-process; the
   soak suite has no disconnect scenario).

Next:
4. Sweep with many active SSE pumps (current pump
   test uses one pump).
5. Cancellation of a sessionless request (no test
   aborts the shared "" key).
6. Sweep and replay together (current sweep tests use
   record only; the code path is shared, but no
   replay-specific sweep test exists).
