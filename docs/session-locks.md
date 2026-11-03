# SessionLocks Lifecycle Audit

## What SessionLocks is

`SessionLocks` (in `chronostrace-proxy/src/lib.rs`) maps one
session key to one async mutex (`Arc<tokio::sync::Mutex<()>>`).
`dispatch` holds the session's lock for the whole interaction, so
same-session requests chain in arrival order even when one of
them is a streaming SSE record whose node is only written when
the stream ends. Different sessions never block each other.
Sessionless requests share the `""` key (the default pointer's
lock), which is pre-existing behavior, not something this audit
changes.

## Audit findings

1. **Creation.** `lock_for` creates an entry lazily on first use
   of a key, under the map lock. The cap check in `dispatch`
   runs before `lock_for`, so 429-rejected sessions never
   create an entry. But every admitted session — including
   sessions whose interaction then failed and whose session
   entry was released — left a lock entry forever. The map
   grew with distinct keys ever seen, unbounded.
2. **No removal existed.** There was no removal path at all,
   so the map could only grow for the process lifetime.
3. **No ordering hazard in removal, if conditioned correctly.**
   See the proof below.

## Safe removal condition (proof)

Every clone of an entry's `Arc` comes from `lock_for`, which
clones under the map lock, and is held by exactly one of:

- a task owning the mutex guard (`lock_owned` consumes the
  `Arc`, so the guard keeps it alive),
- a task between `lock_for` and `lock_owned` (it holds the
  clone while awaiting the mutex),
- `dispatch`'s local before the guard is created.

Therefore, checked under the map lock, `strong_count == 1`
(only the map's own clone) proves: no holder, no waiter, and
no in-flight SSE pump for that session. After removal, a later
`lock_for` creates a fresh lock; since no task can hold the
old one, two requests for the session can never guard on
different locks concurrently, so same-session ordering is
preserved. A waiter holding a clone makes the count >= 2, so
removal is correctly deferred to that waiter's own cleanup.
There is no TOCTOU: cloning and removal both hold the same
map lock, so the check and the removal are atomic with
respect to each other.

What this rules out, explicitly:

- **Removing while a waiter holds a clone** — blocked by the
  count. The waiter later acquires the OLD lock (it already
  holds that `Arc`), finishes, and cleans up at its own end.
- **Removing while an SSE stream is active** — the pump holds
  the guard, so the count is >= 2 and removal fails. The pump
  cleans up at stream end instead (see below).
- **TTL-based removal** — rejected. A TTL cannot prove no
  task uses the lock; the strong-count condition can, so no
  background sweeper or time-based expiry was added.

## Cleanup design (implemented)

`SessionLocks::try_remove(session) -> bool`: under the map
lock, remove the entry iff `strong_count == 1`.

Call sites, all opportunistic at interaction end (no new
runtime machinery):

- `dispatch`, after the interaction, unconditionally: for
  non-SSE the finishing guard is already gone (record drops
  it inside; replay drops it explicitly), so removal
  succeeds unless a waiter holds a clone (deferred, correct).
  For streaming SSE the pump still holds its guard, so this
  is a no-op and the pump owns cleanup.
- SSE pump terminal paths (success, upstream error, client
  disconnect): drop the guard explicitly first (ordering for
  the session is done — the node is recorded, or nothing is
  recorded), then `try_remove`.

Result: the locks map is bounded by in-flight sessions (plus
sessions with waiters), not by distinct keys ever seen. The
sessions map itself is still bounded by the 1024 cap, by
design — a live session keeps its DAG parent pointer, which
is data, not a lock entry.

## Regression tests (in `chronostrace-proxy` unit tests)

- `session_locks_try_remove_semantics` — same key shares one
  lock; held guard blocks removal; bare clone blocks removal;
  idle lock is removed; removal is idempotent; a fresh lock
  works after removal.
- `session_locks_cleanup_preserves_chaining` — two record
  requests for one session still form a parent chain despite
  cleanup between them; a new session is accepted afterwards;
  no lock entries remain for idle sessions.
- `session_locks_active_sse_keeps_protection` — during a
  gated SSE stream the entry is present and `try_remove`
  fails; at stream end the pump removes it.
- `session_locks_replay_linear_after_cleanup` — replay
  chaining survives cleanup (second same-session replay still
  misses); no lock remains.
- `cap_reservation_atomic_under_concurrent_record` (extended)
  — after 1024 concurrent sessions, the locks map is empty
  even though the sessions map holds 1024 entries.

## Residual

- Lock entries for sessions with waiters linger until the
  waiter finishes; that is correct, not a leak — the waiter
  needs the old lock.
- The sessions map (DAG parents) is intentionally kept for
  live sessions up to the 1024 cap; it is not part of this
  cleanup.
- Sessionless traffic shares one lock (`""`); removal when
  idle is safe by the same proof, but heavy sessionless
  concurrency still serializes on that one lock, as before.
