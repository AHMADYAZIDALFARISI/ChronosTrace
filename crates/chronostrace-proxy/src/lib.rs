pub mod http_record;
pub mod mcp_stdio;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chronostrace_chaos::{ChaosEngine, ChaosOutcome};
use chronostrace_core::{Cassette, InteractionKind, MatchResult};
use chronostrace_scrub::SecretScrubber;
use futures_util::{stream, StreamExt};
use http_record::{
    collect_forwardable_headers, collect_safe_response_headers, scrub_sse_event,
    send_upstream_request, serialize_sse_events, sse_chunks_from_payload, sse_events_from_payload,
    sse_marker_payload_v2, SseParser,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Live upstream for HTTP record mode. Absent in replay mode.
#[derive(Clone)]
pub struct UpstreamTarget {
    pub base_url: String,
    pub client: reqwest::Client,
    /// Upstream timeout: `None` means disabled (the historical
    /// behavior — no timeout at all). `Some(d)` bounds waiting for
    /// response headers on every request, and bounds the full body
    /// read for non-SSE responses. SSE streams are exempt from a
    /// total timeout once headers arrive (see `record_interaction`).
    /// The client itself deliberately carries NO total timeout:
    /// reqwest's client timeout would cover the whole body read and
    /// kill long SSE streams, which is exactly what this field
    /// avoids. Timeouts are applied per phase with
    /// `tokio::time::timeout` instead.
    pub timeout: Option<Duration>,
}

impl UpstreamTarget {
    /// Validate the base URL and build the forwarding client. The client
    /// connects directly (no environment proxies) and never follows
    /// redirects, so the recorded response is exactly what the upstream
    /// at this URL returned. No timeout (preserves the historical
    /// behavior); see [`UpstreamTarget::with_timeout`].
    pub fn new(base_url: &str) -> Result<Self, String> {
        Self::with_timeout(base_url, 0)
    }

    /// Like [`UpstreamTarget::new`], with an upstream timeout in
    /// seconds. `0` disables timeouts (same as `new`); a positive
    /// value enables the timeout policy documented on the field.
    pub fn with_timeout(base_url: &str, timeout_secs: u64) -> Result<Self, String> {
        let trimmed = base_url.trim_end_matches('/');
        let parsed =
            reqwest::Url::parse(trimmed).map_err(|e| format!("invalid upstream URL: {e}"))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return Err("upstream URL must use http or https".into());
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            base_url: parsed.to_string().trim_end_matches('/').to_string(),
            client,
            timeout: if timeout_secs == 0 {
                None
            } else {
                Some(Duration::from_secs(timeout_secs))
            },
        })
    }
}

/// Upstream failure response for a non-timeout transport error
/// (connection failure, DNS failure, ...). 502 + a distinct error
/// code, so callers can tell it apart from a timeout (504) and from
/// a recorded upstream HTTP error status (which is stored as a node
/// and passed through verbatim, not synthesized here).
fn upstream_error_response(message: String) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error": "CHRONOSTRACE_UPSTREAM_ERROR",
            "message": message
        })),
    )
        .into_response()
}

/// Upstream timeout response. 504 + `CHRONOSTRACE_UPSTREAM_TIMEOUT`,
/// deliberately distinct from `CHRONOSTRACE_UPSTREAM_ERROR`: a
/// timeout means "the upstream never produced a usable response in
/// time", which must never be confused with a connection failure or
/// with an HTTP error status (the latter two have their own paths).
/// No cassette node is recorded on this path.
fn upstream_timeout_response(timeout: Duration) -> Response {
    (
        StatusCode::GATEWAY_TIMEOUT,
        Json(json!({
            "error": "CHRONOSTRACE_UPSTREAM_TIMEOUT",
            "message": format!(
                "upstream did not respond within {}s",
                timeout.as_secs()
            )
        })),
    )
        .into_response()
}

/// Maximum distinct sessions a proxy keeps (CT-RUN-02). The
/// session map previously grew without bound: every distinct
/// `X-ChronosTrace-Session` value allocated an entry (and a
/// session lock) that was never removed, so an unbounded stream
/// of header values was an unbounded memory growth vector. A new
/// session past this cap is rejected with 429; existing sessions
/// keep working. No TTL eviction: sessions are cheap pointers,
/// and silently expiring one would corrupt an in-flight replay
/// chain, which is worse than an explicit rejection.
pub const MAX_SESSIONS: usize = 1_024;
/// Session keys longer than this are ignored (treated as no
/// session), so a single oversized header cannot become a
/// large map key either.
pub const MAX_SESSION_KEY_LEN: usize = 256;

pub struct ProxyState {
    pub cassette: Cassette,
    pub current_parent_id: Option<String>,
    /// Per-session DAG parents, keyed by the `X-ChronosTrace-Session`
    /// header. Requests without the header use `current_parent_id`.
    /// Without this, parallel clients sharing one proxy would advance a
    /// single global parent pointer and corrupt each other's replays.
    pub sessions: HashMap<String, Option<String>>,
    pub scrubber: SecretScrubber,
    pub chaos: Option<ChaosEngine>,
    pub upstream: Option<UpstreamTarget>,
    /// One async lock per session (plus one for the default pointer).
    /// Dispatch holds a session's lock for the whole interaction, so
    /// same-session requests chain in arrival order even when one of
    /// them is a streaming SSE record whose node is only written when
    /// the stream ends. Different sessions never block each other.
    pub session_locks: SessionLocks,
}

/// Per-session async locks, allocated lazily and shared by cloneable
/// handle. The map itself is a plain std Mutex because the critical
/// section is trivial. Entries are removed opportunistically:
/// `try_remove` at interaction end, the dispatch cleanup guard
/// on abort, and the backstop sweep in `lock_for`.
#[derive(Default)]
pub struct SessionLocks {
    map: Mutex<HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>,
    /// `lock_for` calls since the last sweep, counted only while
    /// the map is above threshold. Its own mutex is always taken
    /// while the map lock is held (map -> counter order, the only
    /// order involving it), so no lock-order inversion is possible.
    since_sweep: Mutex<usize>,
}

/// Backstop sweep threshold: when the lock map grows past this
/// many entries, `lock_for` becomes eligible to prune idle ones.
/// In ordinary operation the map stays well below threshold
/// (live entries: at most one per capped session plus the shared
/// sessionless key, plus poll-cycle transients), so the sweep is
/// a defensive backstop that normally never fires. Live entries
/// are NOT config-bounded in general: removed-key holders (a
/// waiter that acquires after the reserver's sessions entry was
/// removed, then stalls on an unresponsive upstream with no
/// timeout) can accumulate past any fixed number — see
/// docs/cancellation-safety.md "Lock-map bounds, corrected".
/// The sweep bounds idle entries only; it never deletes live
/// ones, so it cannot bring a live-overshot map back down.
const SESSION_LOCKS_SWEEP_THRESHOLD: usize = 2048;

/// Sweep budget: at most one full sweep per this many
/// above-threshold `lock_for` calls. This bounds the sweep
/// FREQUENCY (and hence its amortized cost) even in the
/// pathological case where live entries alone exceed the
/// threshold (then a sweep removes nothing, and without the
/// budget every request would rescan the map). It does NOT
/// bound total map size: total = live + idle, live is
/// concurrency-bounded (see the threshold comment), and only
/// the idle component is sweep-bounded. See
/// docs/cancellation-safety.md "Lock-map bounds, corrected".
const SESSION_LOCKS_SWEEP_BUDGET: usize = 1024;

impl SessionLocks {
    pub fn lock_for(&self, session: &Option<String>) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let key = session.clone().unwrap_or_default();
        let mut map = self.map.lock().expect("SessionLocks mutex poisoned");
        // Backstop sweep — NOT a replacement for the dispatch
        // cleanup guard (see docs/cancellation-safety.md). It
        // exists for paths without a guard (e.g. a panicking
        // pump task) and removes any dependence on drop order
        // for eventual lock cleanup. An entry with strong_count == 1
        // is held only by the map itself, which — checked under
        // the map lock, since every clone comes from this
        // function — proves no holder, no waiter, and no SSE
        // pump for that key. Removal can therefore never delete
        // an active lock, and a later lock_for creates a fresh
        // one, so two live locks for one key are impossible.
        // The budget keeps the cost amortized: the scan runs at
        // most once per SESSION_LOCKS_SWEEP_BUDGET
        // above-threshold calls, never on every request.
        if map.len() > SESSION_LOCKS_SWEEP_THRESHOLD {
            if let Ok(mut since) = self.since_sweep.lock() {
                *since += 1;
                if *since >= SESSION_LOCKS_SWEEP_BUDGET {
                    map.retain(|_, arc| std::sync::Arc::strong_count(arc) > 1);
                    *since = 0;
                }
            }
        }
        map.entry(key)
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Remove the lock entry for a session when nothing uses it.
    ///
    /// SAFETY PROOF. Every clone of an entry's `Arc` comes from
    /// `lock_for`, which clones under this same map lock, and is
    /// held by exactly one of: a task owning the mutex guard, a
    /// task awaiting `lock_owned` on it, or `dispatch`'s local
    /// before the guard is created. So, checked under the map
    /// lock, `strong_count == 1` (only the map's own clone)
    /// proves no holder, no waiter, and no in-flight SSE pump
    /// for that session. After removal, a later `lock_for`
    /// creates a fresh lock; since no task can hold the old
    /// one, two requests for the session can never guard on
    /// different locks concurrently, so same-session ordering
    /// is preserved. A waiter holding a clone makes the count
    /// at least 2, so removal is correctly deferred to that waiter's
    /// own cleanup at its end. No TTL and no background sweeper:
    /// removal happens opportunistically at interaction end,
    /// when the finishing request's own guard is already gone.
    /// Returns true when an entry was removed.
    pub fn try_remove(&self, session: &Option<String>) -> bool {
        let key = session.clone().unwrap_or_default();
        let mut map = self.map.lock().expect("SessionLocks mutex poisoned");
        match map.get(&key) {
            Some(arc) if std::sync::Arc::strong_count(arc) == 1 => {
                map.remove(&key);
                true
            }
            _ => false,
        }
    }

    /// Same as `try_remove`, but never panics: a poisoned map
    /// lock simply yields `false`. Only for `Drop` contexts
    /// (see `ReservationGuard`), where panicking during
    /// unwinding would abort the process.
    fn try_remove_quiet(&self, session: &Option<String>) -> bool {
        let key = session.clone().unwrap_or_default();
        if let Ok(mut map) = self.map.lock() {
            if let Some(arc) = map.get(&key) {
                if std::sync::Arc::strong_count(arc) == 1 {
                    map.remove(&key);
                    return true;
                }
            }
        }
        false
    }
}

/// Supported HTTP routes, driving one generic dispatch. Adding a
/// route means adding one row here, not a new handler. Only POST is
/// served: GET routes, SSE GET streams, websockets, uploads, and batch
/// APIs are explicitly unsupported (see README).
const HTTP_ROUTES: &[(&str, InteractionKind)] = &[
    ("/v1/chat/completions", InteractionKind::LlmCompletion),
    ("/v1/messages", InteractionKind::LlmCompletion),
    ("/v1/responses", InteractionKind::LlmCompletion),
    ("/mcp/call", InteractionKind::McpToolCall),
    ("/mcp", InteractionKind::McpToolCall),
];

pub fn build_router(state: Arc<Mutex<ProxyState>>) -> Router {
    let mut router = Router::new();
    for (path, kind) in HTTP_ROUTES {
        let path = *path;
        let kind = kind.clone();
        router = router.route(
            path,
            post(
                move |State(state): State<Arc<Mutex<ProxyState>>>,
                      headers: HeaderMap,
                      Json(payload): Json<Value>| async move {
                    dispatch(state, kind, path, headers, payload).await
                },
            ),
        );
    }
    router.with_state(state)
}

fn session_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-chronostrace-session")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= MAX_SESSION_KEY_LEN)
        .map(str::to_string)
}

/// Remove a session reservation made by `dispatch` when the
/// interaction recorded nothing: the entry is still `None`
/// (untouched by any success path). Success paths set the entry
/// to `Some(node_id)` via `record_node_and_advance` or the replay
/// advance branches, so they are never removed by this. Called
/// from `dispatch` for complete responses and from the SSE pump
/// for broken/dropped streams (which return before any node).
fn remove_reservation(state: &Arc<Mutex<ProxyState>>, session: &Option<String>, reserved: bool) {
    if !reserved {
        return;
    }
    if let Some(key) = session {
        let mut guard = state.lock().expect("ProxyState mutex poisoned");
        if guard.sessions.get(key) == Some(&None) {
            guard.sessions.remove(key);
        }
    }
}

/// RAII cleanup guard carried by EVERY `dispatch` (universal
/// guard), not only by dispatches that reserve a new session.
/// See docs/cancellation-safety.md for the full analysis.
///
/// TWO JOBS. (1) Reservation: `dispatch` reserves a session
/// slot (entry `None`) before doing any work, and its tail
/// releases the reservation when nothing was recorded. But if
/// the handler future is dropped mid-flight — the client
/// disconnects while waiting for the session lock, the
/// upstream, or a replay chaos delay — the tail never runs
/// and the reservation would leak a cap slot forever. The
/// guard closes that gap for the dispatch that reserved: on
/// drop it removes the entry, but ONLY while it is still
/// `None`. A recorded node turns the entry into `Some`,
/// which the guard never touches, so it can neither delete
/// another request's reservation (the `reserved` flag is what
/// prevents that: a dispatch that did not reserve never
/// removes a sessions entry, even if it observes one that is
/// `None`) nor a live parent pointer. (2) Lock entry: on drop
/// the guard ALWAYS runs `try_remove_quiet` for its key.
/// This is what closes the residual gap found 2026-11-03:
/// follow-up requests for existing sessions used to have no
/// guard at all, so aborting one left its lock entry idle
/// forever (and a holder+waiter double abort left one with
/// no cleaner at all). Now every aborting dispatch attempts
/// lock cleanup; `try_remove_quiet` succeeds exactly when
/// no other task holds or waits on the lock, so it can never
/// delete an active holder's, waiter's, or pump's lock.
///
/// DECLARATION ORDER IS LOAD-BEARING. The guard MUST be
/// declared before the session lock acquisition and before
/// the first await (see the creation site in `dispatch`).
/// On abort, the session clones drop first — the awaited
/// record frame, the replay guard held in the dispatch
/// frame (declared later, so dropped earlier in reverse
/// order), or the pending lock future — and only then does
/// this guard drop, so its `try_remove_quiet` sees the lock
/// idle. Do NOT move the creation site after the lock
/// acquisition or after an await; the regression test
/// `cancel_drop_order_inner_before_outer` pins this order.
///
/// DROP SAFETY. `Drop` is fully synchronous: it takes the std
/// `Mutex`, and every state-lock section in this codebase is
/// brief and synchronous, never held across an await, while
/// the dropping future holds no state lock at any await point
/// — so the lock is always free promptly and never re-entered
/// on the same thread. It never panics: a poisoned lock is
/// skipped via `if let Ok`, because panicking in `Drop` during
/// unwinding would abort the process. It never awaits, so it
/// cannot deadlock on async cancellation.
struct ReservationGuard {
    state: Arc<Mutex<ProxyState>>,
    key: String,
    /// True only when this dispatch created the reservation.
    /// Gates sessions-entry removal; lock cleanup runs
    /// regardless of this flag.
    reserved: bool,
    armed: bool,
}

impl ReservationGuard {
    fn new(state: Arc<Mutex<ProxyState>>, key: String, reserved: bool) -> Self {
        Self {
            state,
            key,
            reserved,
            armed: true,
        }
    }

    /// Hand cleanup to the `dispatch` tail. After this, `Drop`
    /// is inert; the tail owns removal (or, for streaming SSE,
    /// the pump does).
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Synchronous only; never panics (see the safety note
        // above). Removes the reservation only when this
        // dispatch created it AND it is still empty; always
        // attempts lock cleanup, which succeeds when this
        // future's session-lock clones are already dropped and
        // safely no-ops otherwise (a live holder's, waiter's,
        // or pump's own cleanup then owns the entry).
        if let Ok(mut guard) = self.state.lock() {
            if self.reserved && guard.sessions.get(&self.key) == Some(&None) {
                guard.sessions.remove(&self.key);
            }
            guard
                .session_locks
                .try_remove_quiet(&Some(self.key.clone()));
        }
    }
}

async fn dispatch(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    path: &str,
    headers: HeaderMap,
    payload: Value,
) -> Response {
    let session = session_key(&headers);
    // CT-RUN-02 admission, ATOMIC. The old code checked the cap
    // under the state lock but inserted the session entry much
    // later (after the session lock and, in record mode, after
    // the upstream roundtrip). On a multi-threaded runtime that
    // check-then-insert race admitted every concurrent new
    // session past the cap: the HTTP soak test admitted all
    // 1100 sessions. Now check + insert happen under one lock:
    // a new session either reserves a slot immediately or gets
    // 429 before any session lock or upstream work. A reservation
    // is released again (see below) if the interaction records
    // nothing, so failed first requests never burn slots.
    // Existing sessions are never rejected by this check.
    let mut reserved = false;
    if let Some(key) = &session {
        let mut guard = state.lock().expect("ProxyState mutex poisoned");
        if !guard.sessions.contains_key(key) {
            if guard.sessions.len() >= MAX_SESSIONS {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "error": "CHRONOSTRACE_TOO_MANY_SESSIONS",
                        "message": "Session limit reached; retry with an existing session id"
                    })),
                )
                    .into_response();
            }
            guard.sessions.insert(key.clone(), None);
            reserved = true;
        }
    }
    // Universal cleanup guard: EVERY dispatch carries one, not
    // only dispatches that reserved (see the guard's doc
    // comment). Created here, before the session lock
    // acquisition and before the first await — that order is
    // load-bearing for the Drop-time lock cleanup. Sessionless
    // requests use the shared "" key; their guard never
    // removes a sessions entry (reserved is false) but does
    // clean the "" lock entry on abort.
    let mut cleanup_guard =
        ReservationGuard::new(state.clone(), session.clone().unwrap_or_default(), reserved);
    let forwarded_headers = collect_forwardable_headers(&headers);
    let is_record = state
        .lock()
        .expect("ProxyState mutex poisoned")
        .upstream
        .is_some();
    // Hold the session's lock for the whole interaction. For SSE record
    // the guard moves into the pump task and is held until the stream's
    // node is recorded, so same-session requests chain in arrival
    // order. Different sessions never block each other.
    let session_mutex = state
        .lock()
        .expect("ProxyState mutex poisoned")
        .session_locks
        .lock_for(&session);
    let guard = session_mutex.lock_owned().await;
    let response = if is_record {
        record_interaction_inner(
            state.clone(),
            kind,
            session.clone(),
            path,
            forwarded_headers,
            payload,
            Some(guard),
            reserved,
        )
        .await
    } else {
        let r = process_interaction(state.clone(), kind, session.clone(), payload).await;
        // Replay holds its guard locally (unlike record, which
        // moves it); drop it before cleanup so try_remove below
        // can see the lock idle.
        drop(guard);
        r
    };
    // From here on the tail owns reservation cleanup, so the
    // guard must not fire: for streaming SSE the pump owns the
    // reservation, and for everything else the tail removes it
    // below. The guard only ever fires when this future is
    // dropped before reaching this point.
    cleanup_guard.disarm();
    // Release the reservation when nothing was recorded. Skipped
    // for record-mode SSE: that response is still streaming, and
    // the pump owns the reservation until the stream ends
    // (success records a node; break/drop releases it there).
    // Replay SSE responses are complete, not streaming, so they
    // are cleaned up here like any other complete response.
    let is_streaming_sse = is_record
        && response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.contains("text/event-stream"));
    if !is_streaming_sse {
        remove_reservation(&state, &session, reserved);
    }
    // Lock cleanup, unconditional and safe by the try_remove
    // proof. For non-SSE the finishing guard is already gone,
    // so this succeeds unless a waiter holds a clone — in which
    // case removal is correctly deferred to the waiter's end.
    // For streaming SSE the pump still holds its guard, so this
    // is a no-op and the pump cleans up at stream end.
    state
        .lock()
        .expect("ProxyState mutex poisoned")
        .session_locks
        .try_remove(&session);
    response
}

pub async fn process_interaction(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    session: Option<String>,
    raw_request: Value,
) -> Response {
    enum Outcome {
        Miss(String),
        Sse(String),
        Replay {
            status: u16,
            headers: HeaderMap,
            payload: Value,
            delay_ms: u64,
        },
    }

    let outcome = {
        let mut guard = state.lock().expect("ProxyState mutex poisoned");
        let scrubbed_req = guard.scrubber.scrub_json(&raw_request);

        let prompt_text = extract_prompt_text(&scrubbed_req);
        let structural_tools = extract_structural_tools(&scrubbed_req);

        // Resolve this request's DAG parent: the named session's pointer,
        // or the shared default pointer when no session header was sent.
        let parent_id: Option<String> = match &session {
            Some(key) => guard.sessions.get(key).cloned().flatten(),
            None => guard.current_parent_id.clone(),
        };

        let matched = guard.cassette.match_request(
            &parent_id,
            &kind,
            &prompt_text,
            &structural_tools,
            &scrubbed_req,
        );

        match matched {
            Ok(res) => {
                let (node_id, resp_payload, recorded_status, recorded_headers) = match res {
                    MatchResult::Exact(node) => (
                        node.node_id.clone(),
                        node.response_payload.clone(),
                        node.response_status.unwrap_or(200),
                        node.response_headers.clone().unwrap_or_default(),
                    ),
                    MatchResult::SemanticFuzzy { node, similarity } => {
                        tracing::info!(
                            node_id = %node.node_id,
                            similarity = %format!("{similarity:.3}"),
                            "ChronosTrace matched via SimHash prompt tolerance"
                        );
                        (
                            node.node_id.clone(),
                            node.response_payload.clone(),
                            node.response_status.unwrap_or(200),
                            node.response_headers.clone().unwrap_or_default(),
                        )
                    }
                };
                // SSE recordings stream back as event streams. Chaos fault
                // injection applies to JSON payloads only and is skipped
                // here so a mutated marker can never corrupt the stream.
                // Version-2 recordings re-serialize with their fields;
                // version-1 recordings (bare chunks) replay as data-only
                // events, exactly as they were stored.
                let sse_text = sse_events_from_payload(&resp_payload)
                    .map(|events| serialize_sse_events(&events))
                    .or_else(|| {
                        sse_chunks_from_payload(&resp_payload).map(|chunks| {
                            chunks
                                .iter()
                                .map(|c| format!("data: {c}\n\n"))
                                .collect::<String>()
                        })
                    });
                if let Some(text) = sse_text {
                    match &session {
                        Some(key) => {
                            guard.sessions.insert(key.clone(), Some(node_id));
                        }
                        None => {
                            guard.current_parent_id = Some(node_id);
                        }
                    }
                    Outcome::Sse(text)
                } else {
                    let chaos_res = guard
                        .chaos
                        .as_mut()
                        .map(|c| c.apply(&resp_payload))
                        .unwrap_or(ChaosOutcome::PassThrough(resp_payload));
                    // Hanya majukan pointer parent DAG apabila fault tidak menggagalkan step dengan HTTP error
                    if !matches!(chaos_res, ChaosOutcome::InjectedError { .. }) {
                        match &session {
                            Some(key) => {
                                guard.sessions.insert(key.clone(), Some(node_id));
                            }
                            None => {
                                guard.current_parent_id = Some(node_id);
                            }
                        }
                    }
                    // A recorded error status replays as that status;
                    // nodes recorded before status capture replay as
                    // 200. An injected chaos error replaces the recorded
                    // response entirely (status, body, retry-after).
                    let mut recorded_header_map = HeaderMap::new();
                    for (name, value) in &recorded_headers {
                        if let (Ok(header_name), Ok(header_value)) = (
                            header::HeaderName::from_bytes(name.as_bytes()),
                            HeaderValue::from_str(value),
                        ) {
                            recorded_header_map.insert(header_name, header_value);
                        }
                    }
                    let (status, headers, payload, delay_ms) = match chaos_res {
                        ChaosOutcome::PassThrough(payload)
                        | ChaosOutcome::MutatedPayload(payload) => {
                            (recorded_status, recorded_header_map, payload, 0)
                        }
                        ChaosOutcome::DelayThenPassThrough { delay_ms, payload } => {
                            (recorded_status, recorded_header_map, payload, delay_ms)
                        }
                        ChaosOutcome::InjectedError {
                            status_code,
                            retry_after_secs,
                            body,
                        } => {
                            let mut injected_headers = HeaderMap::new();
                            if let Some(secs) = retry_after_secs {
                                if let Ok(val) = HeaderValue::from_str(&secs.to_string()) {
                                    injected_headers.insert(header::RETRY_AFTER, val);
                                }
                            }
                            (status_code, injected_headers, body, 0)
                        }
                    };
                    Outcome::Replay {
                        status,
                        headers,
                        payload,
                        delay_ms,
                    }
                }
            }
            Err(err) => Outcome::Miss(err.to_string()),
        }
    };

    match outcome {
        Outcome::Miss(err_msg) => (
            StatusCode::PRECONDITION_FAILED,
            Json(json!({
                "error": "CHRONOSTRACE_REPLAY_MISS",
                "message": err_msg
            })),
        )
            .into_response(),
        Outcome::Sse(text) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(text))
            .unwrap_or_else(|_| (StatusCode::OK, "stream error").into_response()),
        Outcome::Replay {
            status,
            headers,
            payload,
            delay_ms,
        } => {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            if let Some(text) = payload.as_str() {
                // String payloads are text bodies (recorded non-JSON
                // error bodies), not JSON strings.
                let mut headers = headers;
                if !headers.contains_key(header::CONTENT_TYPE) {
                    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
                }
                (status_code, headers, Body::from(text.to_string())).into_response()
            } else {
                (status_code, headers, Json(payload)).into_response()
            }
        }
    }
}

/// Record-mode counterpart of [`process_interaction`]: forward the
/// ORIGINAL request to the configured upstream, store the scrubbed
/// request/response as a new cassette node, and return the upstream
/// response to the client verbatim. Error responses are passed through
/// but never recorded: replay serves recorded nodes with 200, so
/// recording an error would replay it as a success.
///
/// The original-vs-scrubbed split is deliberate. Scrubbing before
/// forwarding used to break upstreams that authenticate from the body
/// and altered prompt text before the upstream ever saw it. Matching
/// still works because replay scrubs incoming requests before matching,
/// so both sides compare the same canonical scrubbed form — while the
/// stored node never contains a raw credential.
pub async fn record_interaction(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    session: Option<String>,
    path: &str,
    forwarded_headers: Vec<(String, String)>,
    raw_request: Value,
    session_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
) -> Response {
    // Direct callers (tests) never hold a reservation; only
    // `dispatch` reserves, and it calls the inner function.
    record_interaction_inner(
        state,
        kind,
        session,
        path,
        forwarded_headers,
        raw_request,
        session_guard,
        false,
    )
    .await
}

/// Inner record path; `reserved` is `dispatch`'s atomic session
/// reservation (see `dispatch`). Kept as a flat 8-argument helper
/// like `record_node_and_advance` so the existing callers keep
/// passing exactly what they have; grouping into a struct would
/// churn every caller for no behavior change.
#[allow(clippy::too_many_arguments)]
async fn record_interaction_inner(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    session: Option<String>,
    path: &str,
    forwarded_headers: Vec<(String, String)>,
    raw_request: Value,
    session_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    reserved: bool,
) -> Response {
    let (base_url, client, timeout) = {
        let guard = state.lock().expect("ProxyState mutex poisoned");
        match &guard.upstream {
            Some(up) => (up.base_url.clone(), up.client.clone(), up.timeout),
            None => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "CHRONOSTRACE_NOT_RECORDING",
                        "message": "record mode is not configured"
                    })),
                )
                    .into_response();
            }
        }
    };

    // The stored node is a separate scrubbed copy of the original. Its
    // prompt/structural features are derived from the scrubbed copy, so
    // replay (which scrubs incoming requests before matching) compares
    // identical canonical forms. Nothing unscrubbed is ever stored.
    let (scrubbed_req, parent_id) = {
        let guard = state.lock().expect("ProxyState mutex poisoned");
        let scrubbed = guard.scrubber.scrub_json(&raw_request);
        let parent = match &session {
            Some(key) => guard.sessions.get(key).cloned().flatten(),
            None => guard.current_parent_id.clone(),
        };
        (scrubbed, parent)
    };

    let started = Instant::now();
    // UPSTREAM TIMEOUT POLICY. `reqwest::Response` is returned by
    // `send()` once HEADERS arrive, so wrapping the send future
    // bounds exactly connect + time-to-headers — never a total
    // request timeout. The SSE pump below receives the response
    // after this point and is deliberately NOT wrapped: a stream
    // that outlives the timeout keeps running. Only the non-SSE
    // body reads further down are wrapped as well.
    // On timeout the future is dropped (cancelling the request
    // and freeing the connection), the session guard held by the
    // caller drops with this early return (so the next request
    // for the session is not blocked), and NO node is recorded:
    // a timeout means no usable response ever arrived, so there
    // is nothing valid to store. There is exactly one upstream
    // attempt here — no retry, silent or otherwise.
    let upstream = match timeout {
        Some(dur) => {
            match tokio::time::timeout(
                dur,
                send_upstream_request(&client, &base_url, path, &raw_request, &forwarded_headers),
            )
            .await
            {
                Ok(Ok(resp)) => resp,
                Ok(Err(message)) => return upstream_error_response(message),
                Err(_) => return upstream_timeout_response(dur),
            }
        }
        None => {
            match send_upstream_request(&client, &base_url, path, &raw_request, &forwarded_headers)
                .await
            {
                Ok(resp) => resp,
                Err(message) => return upstream_error_response(message),
            }
        }
    };
    let status = upstream.status();
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !status.is_success() {
        // Error responses are recorded like successes (status, safe
        // headers, body) so replay reproduces the failure instead of
        // turning it into a 200. The client still sees the upstream
        // response verbatim.
        let safe_headers = collect_safe_response_headers(upstream.headers());
        // Non-SSE body read: bounded by the same timeout when one
        // is configured. A timeout here means no usable response
        // arrived, so no node is recorded (unlike a completed
        // error response, which IS recorded below).
        let body = match timeout {
            Some(dur) => match tokio::time::timeout(dur, upstream.bytes()).await {
                Ok(Ok(b)) => b,
                Ok(Err(_)) => axum::body::Bytes::new(),
                Err(_) => return upstream_timeout_response(dur),
            },
            None => upstream.bytes().await.unwrap_or_default(),
        };
        let payload = if content_type.contains("application/json") {
            match serde_json::from_slice::<Value>(&body) {
                Ok(value) => {
                    let guard = state.lock().expect("ProxyState mutex poisoned");
                    guard.scrubber.scrub_json(&value)
                }
                Err(_) => Value::String(String::from_utf8_lossy(&body).into_owned()),
            }
        } else {
            Value::String(String::from_utf8_lossy(&body).into_owned())
        };
        record_node_and_advance(
            &state,
            &session,
            &parent_id,
            kind,
            &scrubbed_req,
            payload,
            started.elapsed().as_millis() as u64,
            Some(status.as_u16()),
            Some(safe_headers),
        );
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(body))
            .unwrap_or_else(|_| (status, "upstream error").into_response());
    }

    if content_type.contains("text/event-stream") {
        // SSE exemption, deliberate: the timeout above covered
        // connect + headers only. The pump below streams until
        // the upstream closes it, however long that takes, and
        // records the node only when the stream ends cleanly.
        // Wrapping this in a total timeout would kill legitimate
        // long streams.
        return stream_sse_record(
            state,
            session,
            parent_id,
            kind,
            scrubbed_req,
            upstream,
            started,
            session_guard,
            reserved,
        )
        .await;
    }

    if content_type.contains("application/json") {
        // Non-SSE body read: bounded by the same timeout when one
        // is configured. A timeout here returns 504 with no node
        // recorded — a partial body must never become a success
        // node.
        let body = match timeout {
            Some(dur) => match tokio::time::timeout(dur, upstream.bytes()).await {
                Ok(Ok(b)) => b,
                Ok(Err(message)) => return upstream_error_response(message.to_string()),
                Err(_) => return upstream_timeout_response(dur),
            },
            None => match upstream.bytes().await {
                Ok(b) => b,
                Err(message) => return upstream_error_response(message.to_string()),
            },
        };
        let raw_json: Value = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({
                        "error": "CHRONOSTRACE_UPSTREAM_ERROR",
                        "message": "upstream claimed JSON but the body did not parse"
                    })),
                )
                    .into_response();
            }
        };
        let scrubbed_resp = {
            let guard = state.lock().expect("ProxyState mutex poisoned");
            guard.scrubber.scrub_json(&raw_json)
        };
        record_node_and_advance(
            &state,
            &session,
            &parent_id,
            kind,
            &scrubbed_req,
            scrubbed_resp,
            started.elapsed().as_millis() as u64,
            None,
            None,
        );
        // The client sees the upstream body verbatim; only the stored
        // copy is scrubbed.
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(body))
            .unwrap_or_else(|_| (status, Json(raw_json)).into_response());
    }

    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error": "CHRONOSTRACE_UPSTREAM_ERROR",
            "message": "upstream content type is neither JSON nor SSE; not recorded"
        })),
    )
        .into_response()
}

/// Stream an SSE record: pump upstream bytes to the client as they
/// arrive while parsing events incrementally, and record the node only
/// when the stream ends cleanly. A truncated stream (upstream error or
/// client disconnect) records nothing and leaves the session parent
/// untouched, because a partial stream must never replay as complete.
/// The session guard moves into the pump task, so the next same-session
/// request cannot read a stale parent while this stream is open.
#[allow(clippy::too_many_arguments)]
async fn stream_sse_record(
    state: Arc<Mutex<ProxyState>>,
    session: Option<String>,
    parent_id: Option<String>,
    kind: InteractionKind,
    scrubbed_req: Value,
    upstream: reqwest::Response,
    started: Instant,
    session_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    reserved: bool,
) -> Response {
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/event-stream")
        .to_string();
    let mut byte_stream = upstream.bytes_stream();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<axum::body::Bytes, std::io::Error>>(16);
    tokio::spawn(async move {
        // Held until the node is recorded, so same-session requests
        // chain in arrival order. Terminal paths below drop it
        // explicitly before lock cleanup: try_remove only succeeds
        // once this guard is gone (see try_remove's proof).
        let mut session_guard = session_guard;
        let mut parser = SseParser::new();
        let mut events = Vec::new();
        let mut errored = false;
        // DISCONNECT DETECTION. The old loop only noticed a
        // gone client when the next upstream chunk arrived and
        // `tx.send` failed — so a client that dropped a
        // STALLED stream (no bytes arriving) leaked the pump
        // task, its upstream connection, the session guard, and
        // the session reservation indefinitely. `tx.closed()`
        // resolves when the body receiver drops, so the pump now
        // watches it alongside the upstream stream. `biased`
        // with the upstream branch first is deliberate: when
        // both are ready (client dropped exactly as the
        // upstream ended), the stream-end branch wins and the
        // completed node is still recorded; the disconnect
        // branch only wins while the upstream is genuinely
        // pending, i.e. a true mid-stream drop. This does not
        // change the SSE timeout policy: no timeout is added,
        // client departure is merely propagated.
        loop {
            tokio::select! {
                biased;
                chunk = byte_stream.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            events.extend(parser.feed(&bytes));
                            if tx.send(Ok(bytes)).await.is_err() {
                                // Client disconnected: stop, record nothing,
                                // and release the reservation if this stream
                                // held one (dispatch skips cleanup for
                                // streaming SSE precisely so the pump owns it).
                                remove_reservation(&state, &session, reserved);
                                drop(session_guard.take());
                                state
                                    .lock()
                                    .expect("ProxyState mutex poisoned")
                                    .session_locks
                                    .try_remove(&session);
                                return;
                            }
                        }
                        Some(Err(_)) => {
                            errored = true;
                            break;
                        }
                        None => break,
                    }
                }
                _ = tx.closed() => {
                    // Client gone mid-stream (including mid-stall):
                    // same cleanup as a send failure.
                    remove_reservation(&state, &session, reserved);
                    drop(session_guard.take());
                    state
                        .lock()
                        .expect("ProxyState mutex poisoned")
                        .session_locks
                        .try_remove(&session);
                    return;
                }
            }
        }
        if errored {
            // Truncated stream: the client saw a prefix, but a partial
            // stream must never replay as complete. Record nothing and
            // leave the session parent untouched, and release the
            // reservation if this stream held one.
            remove_reservation(&state, &session, reserved);
            drop(session_guard.take());
            state
                .lock()
                .expect("ProxyState mutex poisoned")
                .session_locks
                .try_remove(&session);
            return;
        }
        if let Some(event) = parser.finish() {
            events.push(event);
        }
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let stored_events = {
            let guard = state.lock().expect("ProxyState mutex poisoned");
            events
                .iter()
                .map(|e| scrub_sse_event(&guard.scrubber, e))
                .collect::<Vec<_>>()
        };
        let payload = sse_marker_payload_v2(stored_events);
        record_node_and_advance(
            &state,
            &session,
            &parent_id,
            kind,
            &scrubbed_req,
            payload,
            elapsed_ms,
            None,
            None,
        );
        // Stream complete: release the guard first (ordering for
        // this session is done — the node is recorded), then the
        // lock entry itself, so a session that never returns
        // leaves nothing behind.
        drop(session_guard.take());
        state
            .lock()
            .expect("ProxyState mutex poisoned")
            .session_locks
            .try_remove(&session);
    });
    let body_stream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from_stream(body_stream))
        .unwrap_or_else(|_| (StatusCode::OK, "stream error").into_response())
}

/// Records one node and advances the session parent pointer. Kept as a
/// flat 9-argument helper so the record/replay dispatch paths keep
/// passing exactly what they have; grouping these into a struct would
/// churn every caller for no behavior change.
#[allow(clippy::too_many_arguments)]
fn record_node_and_advance(
    state: &Arc<Mutex<ProxyState>>,
    session: &Option<String>,
    parent_id: &Option<String>,
    kind: InteractionKind,
    scrubbed_req: &Value,
    response_payload: Value,
    elapsed_ms: u64,
    response_status: Option<u16>,
    response_headers: Option<BTreeMap<String, String>>,
) {
    let mut guard = state.lock().expect("ProxyState mutex poisoned");
    let prompt_text = extract_prompt_text(scrubbed_req);
    let structural_tools = extract_structural_tools(scrubbed_req);
    let node_id = guard.cassette.record_node_full(
        parent_id.clone(),
        kind,
        &prompt_text,
        &structural_tools,
        scrubbed_req.clone(),
        response_payload,
        elapsed_ms,
        response_status,
        response_headers,
    );
    match session {
        Some(key) => {
            guard.sessions.insert(key.clone(), Some(node_id));
        }
        None => {
            guard.current_parent_id = Some(node_id);
        }
    }
}

fn extract_prompt_text(req: &Value) -> String {
    if let Some(messages) = req.get("messages").and_then(|m| m.as_array()) {
        return messages
            .iter()
            .flat_map(|m| match m.get("content") {
                Some(Value::String(s)) => vec![s.clone()],
                // Anthropic content blocks: [{type: "text", text: ...}].
                // Non-text blocks (images, tool use) carry no prompt text
                // and are skipped.
                Some(Value::Array(blocks)) => blocks
                    .iter()
                    .filter_map(|b| {
                        if b.get("type").and_then(|t| t.as_str()) == Some("text") {
                            b.get("text").and_then(|t| t.as_str()).map(str::to_string)
                        } else {
                            None
                        }
                    })
                    .collect(),
                _ => Vec::new(),
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    // OpenAI Responses API: `input` is a string, or an array of strings
    // and message items whose content blocks use `input_text` (or
    // `text`) parts.
    if let Some(input) = req.get("input") {
        match input {
            Value::String(s) => return s.clone(),
            Value::Array(items) => {
                return items
                    .iter()
                    .flat_map(|item| match item {
                        Value::String(s) => vec![s.clone()],
                        Value::Object(_) => match item.get("content") {
                            Some(Value::String(s)) => vec![s.clone()],
                            Some(Value::Array(blocks)) => blocks
                                .iter()
                                .filter_map(|b| {
                                    let t = b.get("type").and_then(|t| t.as_str());
                                    if t == Some("input_text") || t == Some("text") {
                                        b.get("text").and_then(|t| t.as_str()).map(str::to_string)
                                    } else {
                                        None
                                    }
                                })
                                .collect(),
                            _ => Vec::new(),
                        },
                        _ => Vec::new(),
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
            }
            _ => {}
        }
    }
    req.to_string()
}

fn extract_structural_tools(req: &Value) -> Value {
    if let Some(tools) = req.get("tools") {
        return tools.clone();
    }
    if let Some(params) = req.get("params") {
        return params.clone();
    }
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Path;
    use axum::response::sse::{Event, Sse};
    use chronostrace_chaos::{ChaosProfile, FaultKind};
    use mcp_stdio::extract_mcp_matching_features;

    fn sample_proxy_state(chaos: Option<ChaosEngine>) -> Arc<Mutex<ProxyState>> {
        let mut cassette = Cassette::new("proxy_unit_test", 0.80);
        let tools = json!([{"name": "verify_order"}]);
        let prompt = "You are a helpful financial refund agent. Verify order ORD-900 for $50.";
        let req = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": prompt}],
            "tools": tools
        });
        let resp = json!({"choices": [{"message": {"content": "Verified ORD-900"}}]});
        cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            prompt,
            &tools,
            req,
            resp,
            15,
        );

        Arc::new(Mutex::new(ProxyState {
            cassette,
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos,
            upstream: None,
            session_locks: SessionLocks::default(),
        }))
    }

    async fn response_json(resp: Response) -> (StatusCode, HeaderMap, Value) {
        let (parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (
            parts.status,
            parts.headers,
            serde_json::from_slice(&bytes).unwrap(),
        )
    }

    async fn response_text(resp: Response) -> (StatusCode, HeaderMap, String) {
        let (parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    #[tokio::test]
    async fn proxy_returns_200_on_exact_and_fuzzy_match() {
        let state = sample_proxy_state(None);
        let req = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": "You are a helpful financial refund assistant! Verify order ORD-900 for $50."}],
            "tools": [{"name": "verify_order"}]
        });

        let (status, _, body) = response_json(
            process_interaction(state.clone(), InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["choices"][0]["message"]["content"], "Verified ORD-900");
        assert!(state.lock().unwrap().current_parent_id.is_some());
    }

    #[tokio::test]
    async fn proxy_returns_412_on_safety_critical_amount_mutation() {
        let state = sample_proxy_state(None);
        let mutated_req = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $500."}],
            "tools": [{"name": "verify_order"}]
        });

        let (status, _, body) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, mutated_req).await,
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["error"], "CHRONOSTRACE_REPLAY_MISS");
    }

    #[tokio::test]
    async fn proxy_injects_http_429_with_retry_after_header_and_preserves_parent_id() {
        let chaos = ChaosEngine::new(ChaosProfile {
            name: "rate_limit_test".into(),
            seed: 7,
            probability: 1.0,
            fault: FaultKind::Http429RateLimit {
                retry_after_secs: 3,
            },
        });
        let state = sample_proxy_state(Some(chaos));
        let req = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
            "tools": [{"name": "verify_order"}]
        });

        let (status, headers, _) = response_json(
            process_interaction(state.clone(), InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            headers.get(header::RETRY_AFTER).unwrap().to_str().unwrap(),
            "3"
        );
        // Pointer parent tidak boleh maju jika request gagal karena HTTP 429 sehingga retry berikutnya tetap cocok
        assert!(state.lock().unwrap().current_parent_id.is_none());
    }

    // CT-HARDEN-008: every chaos profile manifests through HTTP replay
    // (429 is also covered by the dedicated test above; this is the
    // full support matrix in one place).
    #[tokio::test]
    async fn harden008_http_replay_supports_all_chaos_profiles() {
        let matching_req = || {
            json!({
                "model": "gpt-4o",
                "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
                "tools": [{"name": "verify_order"}]
            })
        };
        let recorded_resp = json!({"choices": [{"message": {"content": "Verified ORD-900"}}]});

        // http-429: injected error replaces the response entirely.
        let state = sample_proxy_state(Some(ChaosEngine::new(ChaosProfile {
            name: "m429".into(),
            seed: 11,
            probability: 1.0,
            fault: FaultKind::Http429RateLimit {
                retry_after_secs: 60,
            },
        })));
        let (status, headers, _) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, matching_req()).await,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            headers.get(header::RETRY_AFTER).unwrap().to_str().unwrap(),
            "60"
        );

        // latency: the recorded payload passes through after the delay.
        let state = sample_proxy_state(Some(ChaosEngine::new(ChaosProfile {
            name: "mlat".into(),
            seed: 11,
            probability: 1.0,
            fault: FaultKind::LatencySpike { delay_ms: 200 },
        })));
        let started = Instant::now();
        let (status, _, body) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, matching_req()).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, recorded_resp);
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "latency fault must actually delay the replay"
        );

        // truncated: the payload is replaced by a corrupted prefix.
        let state = sample_proxy_state(Some(ChaosEngine::new(ChaosProfile {
            name: "mtrunc".into(),
            seed: 11,
            probability: 1.0,
            fault: FaultKind::TruncatedJsonPayload { keep_ratio: 0.5 },
        })));
        let (status, _, body) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, matching_req()).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let raw = serde_json::to_string(&recorded_resp).unwrap();
        let corrupted = body["_chronostrace_corrupted_raw"].as_str().unwrap();
        assert!(raw.starts_with(corrupted));
        assert!(corrupted.len() < raw.len());

        // drop-field: the named field is gone at every depth.
        let state = sample_proxy_state(Some(ChaosEngine::new(ChaosProfile {
            name: "mdrop".into(),
            seed: 11,
            probability: 1.0,
            fault: FaultKind::DropSchemaField {
                field_name: "content".to_string(),
            },
        })));
        let (status, _, body) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, matching_req()).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["choices"][0]["message"].get("content").is_none());

        // service-503: injected error, no retry-after.
        let state = sample_proxy_state(Some(ChaosEngine::new(ChaosProfile {
            name: "m503".into(),
            seed: 11,
            probability: 1.0,
            fault: FaultKind::ServiceUnavailable503,
        })));
        let (status, headers, _) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, matching_req()).await,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(headers.get(header::RETRY_AFTER).is_none());
    }

    #[test]
    fn session_key_parses_header_and_ignores_blank() {
        let mut headers = HeaderMap::new();
        assert!(session_key(&headers).is_none());
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_static("  session-a  "),
        );
        assert_eq!(session_key(&headers).as_deref(), Some("session-a"));
        headers.insert("x-chronostrace-session", HeaderValue::from_static("   "));
        assert!(session_key(&headers).is_none());
    }

    // CT-RUN-02: overlong session keys are ignored, and a new
    // session past the cap is rejected with 429 before it can
    // allocate a session entry or lock. Existing sessions pass.
    #[test]
    fn session_key_rejects_overlong_keys() {
        let mut headers = HeaderMap::new();
        let long_key = "s".repeat(MAX_SESSION_KEY_LEN + 1);
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_str(&long_key).unwrap(),
        );
        assert!(session_key(&headers).is_none());
    }

    #[tokio::test]
    async fn dispatch_rejects_new_session_past_cap() {
        let state = sample_proxy_state(None);
        {
            let mut guard = state.lock().unwrap();
            for i in 0..MAX_SESSIONS {
                guard.sessions.insert(format!("s{i}"), None);
            }
        }
        // A brand-new session past the cap: 429, no allocation.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_static("brand-new-session"),
        );
        let (status, _, body) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/chat/completions",
                headers,
                json!({"model": "gpt-4o", "messages": []}),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["error"], "CHRONOSTRACE_TOO_MANY_SESSIONS");
        assert_eq!(state.lock().unwrap().sessions.len(), MAX_SESSIONS);
        // An existing session is unaffected by the cap.
        let mut headers = HeaderMap::new();
        headers.insert("x-chronostrace-session", HeaderValue::from_static("s0"));
        let (status, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/chat/completions",
                headers,
                json!({"model": "gpt-4o", "messages": []}),
            )
            .await,
        )
        .await;
        assert_ne!(status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn proxy_sessions_have_independent_dag_parents() {
        let state = sample_proxy_state(None);
        let node_id = state.lock().unwrap().cassette.nodes[0].node_id.clone();
        let req = || {
            json!({
                "model": "gpt-4o",
                "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
                "tools": [{"name": "verify_order"}]
            })
        };

        // Session A matches the root node and advances only its own pointer.
        let (status, _, _) = response_json(
            process_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                Some("session-a".to_string()),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Session B replays the same trajectory independently: its parent
        // is still None, so it matches the root node again.
        let (status, _, _) = response_json(
            process_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                Some("session-b".to_string()),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Session A replaying the same request again now misses: its parent
        // advanced past the root node, and no child was recorded.
        let (status, _, _) = response_json(
            process_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                Some("session-a".to_string()),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);

        let guard = state.lock().unwrap();
        assert_eq!(
            guard.sessions.get("session-a").cloned().flatten(),
            Some(node_id.clone())
        );
        assert_eq!(
            guard.sessions.get("session-b").cloned().flatten(),
            Some(node_id)
        );
        // The default pointer was never touched by session traffic.
        assert!(guard.current_parent_id.is_none());
    }

    #[tokio::test]
    async fn proxy_http_mcp_prompts_get_param_change_misses() {
        // The HTTP handler derives structural tools from the full params,
        // so unlike the old stdio extractor it never had the method-only
        // hole. This test locks that behavior in.
        let mut cassette = Cassette::new("http_mcp_prompts", 0.80);
        let req_a = json!({"jsonrpc": "2.0", "method": "prompts/get", "params": {"name": "greeting", "arguments": {}}});
        let resp = json!({"result": "hello"});
        cassette.record_node(
            None,
            InteractionKind::McpToolCall,
            &req_a.to_string(),
            &req_a["params"],
            req_a.clone(),
            resp,
            5,
        );
        let state = Arc::new(Mutex::new(ProxyState {
            cassette,
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: None,
            upstream: None,
            session_locks: SessionLocks::default(),
        }));
        let (status, _, _) = response_json(
            process_interaction(
                state.clone(),
                InteractionKind::McpToolCall,
                None,
                req_a.clone(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let req_b = json!({"jsonrpc": "2.0", "method": "prompts/get", "params": {"name": "farewell", "arguments": {}}});
        let (status, _, _) = response_json(
            process_interaction(state, InteractionKind::McpToolCall, None, req_b).await,
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    }

    /// Fake upstream server for record-mode tests: a JSON echo endpoint
    /// that requires a bearer token, an SSE endpoint, and a failing
    /// endpoint. Returns its base URL; the server task lives until the
    /// test runtime shuts down.
    async fn fake_upstream() -> String {
        let app = Router::new()
            .route(
                "/v1/chat/completions",
                post(|headers: HeaderMap, Json(body): Json<Value>| async move {
                    if headers
                        .get(header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        != Some("Bearer test-key")
                    {
                        return (
                            StatusCode::UNAUTHORIZED,
                            Json(json!({"error": "unauthorized"})),
                        );
                    }
                    (StatusCode::OK, Json(json!({"echo": body})))
                }),
            )
            .route(
                "/v1/stream",
                post(|Json(_body): Json<Value>| async {
                    let events = vec!["{\"delta\":\"hi\"}", "{\"delta\":\"there\"}", "[DONE]"];
                    let stream = stream::iter(
                        events
                            .into_iter()
                            .map(|c| Ok::<_, std::convert::Infallible>(Event::default().data(c))),
                    );
                    Sse::new(stream)
                }),
            )
            .route(
                "/v1/fail",
                post(|Json(_body): Json<Value>| async {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "boom"})),
                    )
                }),
            )
            .route(
                "/v1/body-key",
                post(|Json(body): Json<Value>| async move {
                    if body.get("api_key").and_then(|v| v.as_str()) != Some("live-key-123") {
                        return (
                            StatusCode::UNAUTHORIZED,
                            Json(json!({"error": "body api_key missing or wrong"})),
                        );
                    }
                    (StatusCode::OK, Json(json!({"echo": body})))
                }),
            )
            .route(
                "/v1/anthropic",
                post(|headers: HeaderMap, Json(_body): Json<Value>| async move {
                    let key_ok =
                        headers.get("x-api-key").and_then(|v| v.to_str().ok()) == Some("ant-key-1");
                    let version_ok = headers
                        .get("anthropic-version")
                        .and_then(|v| v.to_str().ok())
                        == Some("2023-06-01");
                    if key_ok && version_ok {
                        (StatusCode::OK, Json(json!({"ok": true})))
                    } else {
                        (
                            StatusCode::UNAUTHORIZED,
                            Json(json!({"error": "anthropic headers missing"})),
                        )
                    }
                }),
            )
            .route(
                "/v1/broken-stream",
                post(|Json(_body): Json<Value>| async move {
                    let chunks: Vec<Result<axum::body::Bytes, std::io::Error>> = vec![
                        Ok(axum::body::Bytes::from("data: {\"delta\":\"first\"}\n\n")),
                        Err(std::io::Error::other("boom")),
                    ];
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(stream::iter(chunks)),
                    )
                }),
            )
            .route(
                "/v1/messy-stream",
                post(|Json(_body): Json<Value>| async move {
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from("garbage line\nretry: not-a-number\nid: bad\0id\ndata: ok\n\n"),
                    )
                }),
            )
            .route(
                "/v1/err/:code",
                post(
                    |Path(code): Path<u16>, Json(_body): Json<Value>| async move {
                        let status =
                            StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                        let body = json!({"error": format!("upstream error {code}")});
                        if status == StatusCode::TOO_MANY_REQUESTS {
                            (status, [(header::RETRY_AFTER, "5")], Json(body)).into_response()
                        } else {
                            (status, Json(body)).into_response()
                        }
                    },
                ),
            )
            .route(
                "/v1/err-text",
                post(|Json(_body): Json<Value>| async move {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CONTENT_TYPE, "text/plain")],
                        Body::from("boom text"),
                    )
                }),
            )
            .route(
                "/v1/slow",
                post(|Json(body): Json<Value>| async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    (StatusCode::OK, Json(json!({"echo": body})))
                }),
            )
            .route(
                "/v1/echo-headers",
                post(|headers: HeaderMap, Json(_body): Json<Value>| async move {
                    let mut echoed = serde_json::Map::new();
                    for (name, value) in headers.iter() {
                        if name.as_str() == "host" || name.as_str() == "content-length" {
                            continue;
                        }
                        if let Ok(v) = value.to_str() {
                            echoed.insert(name.as_str().to_string(), Value::String(v.to_string()));
                        }
                    }
                    (StatusCode::OK, Json(Value::Object(echoed)))
                }),
            )
            .route(
                "/v1/responses",
                post(|Json(body): Json<Value>| async move {
                    (StatusCode::OK, Json(json!({"echo": body})))
                }),
            )
            .route(
                "/mcp",
                post(|Json(body): Json<Value>| async move {
                    (StatusCode::OK, Json(json!({"echo": body})))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// Gated SSE upstream for the incremental-streaming proof: it emits
    /// one Anthropic-style event, then waits for the test to release the
    /// gate before emitting the rest. If the proxy buffered the whole
    /// upstream body, the test could never observe the first event
    /// before releasing the gate.
    async fn gated_sse_upstream() -> (String, Arc<tokio::sync::Notify>) {
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_for_route = notify.clone();
        let notify_for_broken = notify.clone();
        let app = Router::new()
            .route(
                "/v1/gated-broken",
                post(move |Json(_body): Json<Value>| {
                    let notify = notify_for_broken.clone();
                    async move {
                        // Raw body stream (not axum Sse) so the error
                        // item reaches the wire exactly as a body error.
                        let stream = stream::unfold((0u8, notify), |(step, notify)| async move {
                            match step {
                                0 => Some((
                                    Ok::<_, std::io::Error>(axum::body::Bytes::from(
                                        "data: {\"delta\":\"first\"}\n\n",
                                    )),
                                    (1, notify),
                                )),
                                1 => {
                                    notify.notified().await;
                                    Some((
                                        Err::<_, std::io::Error>(std::io::Error::other("boom")),
                                        (2, notify),
                                    ))
                                }
                                _ => None,
                            }
                        });
                        (
                            StatusCode::OK,
                            [(header::CONTENT_TYPE, "text/event-stream")],
                            Body::from_stream(stream),
                        )
                    }
                }),
            )
            .route(
                "/v1/gated-stream",
                post(move |Json(_body): Json<Value>| {
                    let notify = notify_for_route.clone();
                    async move {
                        let stream = stream::unfold((0u8, notify), |(step, notify)| async move {
                            match step {
                                0 => Some((
                                    Ok::<_, std::convert::Infallible>(
                                        Event::default()
                                            .event("content_block_delta")
                                            .data("{\"delta\":\"first\"}"),
                                    ),
                                    (1, notify),
                                )),
                                1 => {
                                    notify.notified().await;
                                    Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default()
                                                .event("message_stop")
                                                .data("{\"type\":\"message_stop\"}"),
                                        ),
                                        (2, notify),
                                    ))
                                }
                                2 => Some((
                                    Ok::<_, std::convert::Infallible>(
                                        Event::default().data("[DONE]"),
                                    ),
                                    (3, notify),
                                )),
                                _ => None,
                            }
                        });
                        Sse::new(stream)
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), notify)
    }

    fn bearer_headers() -> Vec<(String, String)> {
        vec![("authorization".to_string(), "Bearer test-key".to_string())]
    }

    fn record_state(base_url: String) -> Arc<Mutex<ProxyState>> {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        Arc::new(Mutex::new(ProxyState {
            cassette: Cassette::new("record_test", 0.80),
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: None,
            upstream: Some(UpstreamTarget {
                base_url,
                client,
                timeout: None,
            }),
            session_locks: SessionLocks::default(),
        }))
    }

    #[tokio::test]
    async fn record_mode_forwards_json_and_records_node() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let (status, _, body) = response_json(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/chat/completions",
                bearer_headers(),
                req,
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["echo"]["model"], "gpt-4o");
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 1);
        assert!(guard.current_parent_id.is_some());
    }

    #[tokio::test]
    async fn record_mode_records_errors_and_passes_them_through() {
        let base = fake_upstream().await;
        let state = record_state(base);
        // Missing bearer token: the upstream 401 passes through AND is recorded.
        // (CT-HARDEN-004 changed this on purpose: unrecorded errors used
        // to replay as 200. The client still sees the verbatim response.)
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let (status, _, body) = response_json(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/chat/completions",
                vec![],
                req,
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"], "unauthorized");
        // Upstream 500: same treatment.
        let (status, _, _) = response_json(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/fail",
                bearer_headers(),
                json!({}),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 2);
        assert_eq!(guard.cassette.nodes[0].response_status, Some(401));
        assert_eq!(guard.cassette.nodes[1].response_status, Some(500));
    }

    // CT-HARDEN-001 reproduction: the upstream needs a body api_key to
    // authenticate. Scrubbing before forwarding redacts it, so the
    // upstream rejects the request and nothing is recorded.
    #[tokio::test]
    async fn harden001_body_credential_reaches_upstream_but_not_cassette() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "api_key": "live-key-123", "messages": [{"role": "user", "content": "hi"}]});
        let (status, _, body) = response_json(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/body-key",
                vec![],
                req,
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["echo"]["model"], "gpt-4o");
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 1);
        let stored = guard.cassette.nodes[0].request_payload.to_string();
        assert!(
            !stored.contains("live-key-123"),
            "cassette must never persist the live credential: {stored}"
        );
    }

    // CT-HARDEN-001 reproduction: a secret inside prompt text must reach
    // the upstream verbatim (the upstream echoes it) while the stored
    // node holds only the redacted form.
    #[tokio::test]
    async fn harden001_secret_prompt_reaches_upstream_verbatim() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let secret = "sk-proj-abcdefghijklmnopqrstuvwx";
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": format!("my key is {secret}")}]});
        let (status, _, body) = response_json(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/chat/completions",
                bearer_headers(),
                req,
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let echoed = body["echo"].to_string();
        assert!(
            echoed.contains(secret),
            "upstream must receive the original prompt, got: {echoed}"
        );
        let guard = state.lock().unwrap();
        let stored = guard.cassette.nodes[0].request_payload.to_string();
        assert!(
            !stored.contains(secret),
            "cassette leaked the secret: {stored}"
        );
    }

    // CT-HARDEN-002 reproduction (handler path): an Anthropic-style
    // upstream requires x-api-key + anthropic-version. The current
    // forwarder drops both, so dispatch must fail with 401 today.
    #[tokio::test]
    async fn harden002_anthropic_headers_reach_upstream_via_dispatch() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("ant-key-1"));
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        let (status, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/anthropic",
                headers,
                json!({"model": "claude-test", "messages": [{"role": "user", "content": "hi"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 1);
    }

    // CT-HARDEN-002 guard: headers outside the allowlist must never be
    // forwarded, even when the client sends them.
    #[tokio::test]
    async fn harden002_non_allowlisted_headers_are_dropped() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_static("Bearer test-key"));
        headers.insert("cookie", HeaderValue::from_static("session=secret"));
        headers.insert(
            "x-custom-secret",
            HeaderValue::from_static("should-not-arrive"),
        );
        let (status, _, body) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/echo-headers",
                headers,
                json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["authorization"], "Bearer test-key");
        assert!(body.get("cookie").is_none());
        assert!(body.get("x-custom-secret").is_none());
    }

    #[tokio::test]
    async fn record_mode_records_sse_events_and_replays_as_event_stream() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let (status, headers, text) = response_text(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/stream",
                bearer_headers(),
                req.clone(),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(headers
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/event-stream"));
        assert!(text.contains("data: {\"delta\":\"hi\"}"));
        assert!(text.contains("data: [DONE]"));
        // The stored node carries the version-2 SSE marker, not the
        // raw stream, with the events preserved structurally.
        {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 1);
            let events =
                sse_events_from_payload(&guard.cassette.nodes[0].response_payload).unwrap();
            let datas: Vec<&str> = events.iter().map(|e| e.data.as_str()).collect();
            assert_eq!(
                datas,
                vec!["{\"delta\":\"hi\"}", "{\"delta\":\"there\"}", "[DONE]"]
            );
        }
        // Replay the same request (now in replay mode) streams the events
        // back instead of returning JSON.
        {
            let mut guard = state.lock().unwrap();
            guard.upstream = None;
            guard.current_parent_id = None;
        }
        let (status, headers, text) = response_text(
            process_interaction(state.clone(), InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(headers
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/event-stream"));
        assert!(text.contains("data: {\"delta\":\"there\"}"));
        assert!(text.contains("data: [DONE]"));
    }

    // CT-HARDEN-003: the first event must arrive while the upstream is
    // still gated (incremental streaming, not full buffering), and the
    // recorded node must preserve event names structurally.
    #[tokio::test]
    async fn harden003_record_streams_incrementally_and_preserves_event_fields() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let resp = record_interaction(
            state.clone(),
            InteractionKind::LlmCompletion,
            None,
            "/v1/gated-stream",
            vec![],
            req,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (_, body) = resp.into_parts();
        let mut data_stream = body.into_data_stream();
        // Accumulate until the first event is fully visible. The
        // upstream cannot emit anything else until the gate releases,
        // so everything seen here is the first event, however chunked.
        let mut first = String::new();
        while !(first.contains("content_block_delta") && first.contains("first")) {
            let chunk = data_stream.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        // Gate still closed at this point: a buffering proxy could not
        // have delivered anything yet.
        gate.notify_one();
        let mut rest = String::new();
        while let Some(chunk) = data_stream.next().await {
            rest.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        assert!(rest.contains("message_stop"));
        assert!(rest.contains("[DONE]"));
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 1);
        let events = sse_events_from_payload(&guard.cassette.nodes[0].response_payload).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.as_deref(), Some("content_block_delta"));
        assert_eq!(events[0].data, "{\"delta\":\"first\"}");
        assert_eq!(events[1].event.as_deref(), Some("message_stop"));
        assert_eq!(events[2].data, "[DONE]");
    }

    // CT-HARDEN-003: an interrupted stream must not be recorded.
    // Immediate breakage surfaces in reqwest as a send() error
    // (hyper reports IncompleteMessage), so the proxy answers 502
    // rather than a truncated 200 — and records nothing.
    #[tokio::test]
    async fn harden003_interrupted_stream_records_nothing() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let resp = record_interaction(
            state.clone(),
            InteractionKind::LlmCompletion,
            None,
            "/v1/broken-stream",
            vec![],
            req,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let guard = state.lock().unwrap();
        assert!(guard.cassette.nodes.is_empty());
        assert!(guard.current_parent_id.is_none());
    }

    // CT-HARDEN-003: a stream that breaks AFTER the first event must
    // still not be recorded, and the client must see the prefix it
    // already received (200 with a truncated body), not an error.
    #[tokio::test]
    async fn harden003_late_break_records_nothing() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let resp = record_interaction(
            state.clone(),
            InteractionKind::LlmCompletion,
            None,
            "/v1/gated-broken",
            vec![],
            req,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (_, body) = resp.into_parts();
        let mut data_stream = body.into_data_stream();
        let mut first = String::new();
        while !first.contains("first") {
            let chunk = data_stream.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        gate.notify_one();
        let mut rest = String::new();
        while let Some(chunk) = data_stream.next().await {
            rest.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        assert!(!rest.contains("second"));
        let guard = state.lock().unwrap();
        assert!(guard.cassette.nodes.is_empty());
        assert!(guard.current_parent_id.is_none());
    }

    // CT-HARDEN-003: malformed SSE lines never become events.
    #[tokio::test]
    async fn harden003_malformed_stream_lines_are_ignored() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let resp = record_interaction(
            state.clone(),
            InteractionKind::LlmCompletion,
            None,
            "/v1/messy-stream",
            vec![],
            req,
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (_, _, _) = response_text(resp).await;
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 1);
        let events = sse_events_from_payload(&guard.cassette.nodes[0].response_payload).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "ok");
        assert_eq!(events[0].event, None);
        assert_eq!(events[0].retry, None);
    }

    // CT-HARDEN-003: version-1 recordings (bare chunks) still replay as
    // data-only events.
    #[tokio::test]
    async fn harden003_v1_recordings_still_replay() {
        let mut cassette = Cassette::new("v1_replay", 0.80);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let payload =
            json!({"_chronostrace_sse": {"version": 1, "chunks": ["{\"a\":1}", "[DONE]"]}});
        cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            "hi",
            &Value::Null,
            req.clone(),
            payload,
            5,
        );
        let state = Arc::new(Mutex::new(ProxyState {
            cassette,
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: None,
            upstream: None,
            session_locks: SessionLocks::default(),
        }));
        let (status, headers, text) = response_text(
            process_interaction(state, InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(headers
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/event-stream"));
        assert!(text.contains("data: {\"a\":1}"));
        assert!(text.contains("data: [DONE]"));
    }

    // CT-HARDEN-003/008: SSE replay ignores chaos even at probability
    // 1.0 — a mutated marker must never corrupt the stream.
    #[tokio::test]
    async fn harden003_sse_replay_ignores_chaos() {
        let chaos = ChaosEngine::new(ChaosProfile {
            name: "chaos-429".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::Http429RateLimit {
                retry_after_secs: 2,
            },
        });
        let state = sample_proxy_state(Some(chaos));
        let req = json!({"model": "gpt-4o", "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}], "tools": [{"name": "verify_order"}]});
        let payload = json!({"_chronostrace_sse": {"version": 2, "events": [{"event": "delta", "data": "hello"}]}});
        {
            let mut guard = state.lock().unwrap();
            guard.cassette.nodes.clear();
            guard.cassette.record_node(
                None,
                InteractionKind::LlmCompletion,
                "You are a helpful financial refund agent. Verify order ORD-900 for $50.",
                &json!([{"name": "verify_order"}]),
                req.clone(),
                payload,
                5,
            );
        }
        let (status, _, text) = response_text(
            process_interaction(state, InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(text.contains("event: delta"));
        assert!(text.contains("data: hello"));
        assert!(!text.contains("rate_limit"));
    }

    // CT-HARDEN-004: recorded error statuses must replay as the same
    // status, not as 200.
    #[tokio::test]
    async fn harden004_error_statuses_round_trip() {
        let base = fake_upstream().await;
        for code in [400u16, 401, 429, 500, 503] {
            let state = record_state(base.clone());
            let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
            let path = format!("/v1/err/{code}");
            let (status, _, body) = response_json(
                record_interaction(
                    state.clone(),
                    InteractionKind::LlmCompletion,
                    None,
                    &path,
                    vec![],
                    req.clone(),
                    None,
                )
                .await,
            )
            .await;
            assert_eq!(status.as_u16(), code, "record client status");
            assert_eq!(body["error"], format!("upstream error {code}"));
            {
                let guard = state.lock().unwrap();
                assert_eq!(guard.cassette.nodes.len(), 1);
                assert_eq!(guard.cassette.nodes[0].response_status, Some(code));
                if code == 429 {
                    assert_eq!(
                        guard.cassette.nodes[0]
                            .response_headers
                            .as_ref()
                            .unwrap()
                            .get("retry-after")
                            .map(String::as_str),
                        Some("5")
                    );
                }
            }
            {
                let mut guard = state.lock().unwrap();
                guard.upstream = None;
                guard.current_parent_id = None;
            }
            let (status, headers, _) = response_json(
                process_interaction(state.clone(), InteractionKind::LlmCompletion, None, req).await,
            )
            .await;
            assert_eq!(status.as_u16(), code, "replay status");
            if code == 429 {
                assert_eq!(
                    headers
                        .get(header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok()),
                    Some("5")
                );
            }
        }
    }

    // CT-HARDEN-004: a non-JSON error body replays as text with its
    // recorded content type.
    #[tokio::test]
    async fn harden004_text_error_round_trips() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let (status, _, text) = response_text(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/err-text",
                vec![],
                req.clone(),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(text, "boom text");
        {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 1);
            assert_eq!(guard.cassette.nodes[0].response_status, Some(500));
            assert_eq!(
                guard.cassette.nodes[0].response_payload,
                Value::String("boom text".to_string())
            );
        }
        {
            let mut guard = state.lock().unwrap();
            guard.upstream = None;
            guard.current_parent_id = None;
        }
        let (status, headers, text) = response_text(
            process_interaction(state.clone(), InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(headers
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/plain"));
        assert_eq!(text, "boom text");
    }

    // CT-HARDEN-004: nodes recorded before status capture (no status
    // fields) keep replaying as 200.
    #[tokio::test]
    async fn harden004_old_nodes_replay_as_200() {
        let mut cassette = Cassette::new("old_node", 0.80);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            "hi",
            &Value::Null,
            req.clone(),
            json!({"ok": true}),
            5,
        );
        let state = Arc::new(Mutex::new(ProxyState {
            cassette,
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: None,
            upstream: None,
            session_locks: SessionLocks::default(),
        }));
        let (status, _, body) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, req).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ok"], true);
    }

    // CT-HARDEN-005: concurrent same-session records must chain
    // linearly in arrival order, never as siblings. The assertion is
    // structural (chain shape), not about which request won the race.
    #[tokio::test]
    async fn harden005_same_session_records_chain_linearly() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let started = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let state = state.clone();
            let mut headers = HeaderMap::new();
            headers.insert("x-chronostrace-session", HeaderValue::from_static("s1"));
            let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": format!("req-{i}")}]});
            tasks.push(tokio::spawn(async move {
                dispatch(
                    state,
                    InteractionKind::LlmCompletion,
                    "/v1/slow",
                    headers,
                    req,
                )
                .await
            }));
        }
        for task in tasks {
            let resp = task.await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
        eprintln!(
            "harden005 same-session 4x200ms records took {:?} (serialized expectation ~800ms)",
            started.elapsed()
        );
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 4);
        assert!(guard.cassette.nodes[0].parent_id.is_none());
        for i in 1..4 {
            assert_eq!(
                guard.cassette.nodes[i].parent_id,
                Some(guard.cassette.nodes[i - 1].node_id.clone()),
                "node {i} must chain from node {}",
                i - 1
            );
        }
        let mut contents: Vec<String> = guard
            .cassette
            .nodes
            .iter()
            .map(|n| {
                n.request_payload["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        contents.sort();
        assert_eq!(contents, vec!["req-0", "req-1", "req-2", "req-3"]);
    }

    // CT-HARDEN-005: different sessions never block each other — four
    // slow records across four sessions finish in roughly one slow
    // round trip, and each session gets its own root node.
    #[tokio::test]
    async fn harden005_different_sessions_run_in_parallel() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let started = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let state = state.clone();
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-chronostrace-session",
                HeaderValue::from_str(&format!("sess-{i}")).unwrap(),
            );
            let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": format!("req-{i}")}]});
            tasks.push(tokio::spawn(async move {
                dispatch(
                    state,
                    InteractionKind::LlmCompletion,
                    "/v1/slow",
                    headers,
                    req,
                )
                .await
            }));
        }
        for task in tasks {
            let resp = task.await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
        eprintln!(
            "harden005 four-session 4x200ms records took {:?} (parallel expectation ~200ms)",
            started.elapsed()
        );
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 4);
        assert!(guard.cassette.nodes.iter().all(|n| n.parent_id.is_none()));
        assert_eq!(guard.sessions.len(), 4);
    }

    // CT-HARDEN-005: sessionless (default pointer) concurrent records
    // chain linearly too.
    #[tokio::test]
    async fn harden005_sessionless_records_chain_linearly() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let mut tasks = Vec::new();
        for i in 0..4 {
            let state = state.clone();
            let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": format!("req-{i}")}]});
            tasks.push(tokio::spawn(async move {
                dispatch(
                    state,
                    InteractionKind::LlmCompletion,
                    "/v1/slow",
                    HeaderMap::new(),
                    req,
                )
                .await
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().status(), StatusCode::OK);
        }
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 4);
        assert!(guard.cassette.nodes[0].parent_id.is_none());
        for i in 1..4 {
            assert_eq!(
                guard.cassette.nodes[i].parent_id,
                Some(guard.cassette.nodes[i - 1].node_id.clone())
            );
        }
    }

    // CT-HARDEN-005: retry after an injected 429 matches the same node
    // (parent preserved), then succeeds once chaos is off, and the
    // next step matches the following node.
    #[tokio::test]
    async fn harden005_retry_after_injected_error_matches_same_node() {
        let chaos = ChaosEngine::new(ChaosProfile {
            name: "chaos-429".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::Http429RateLimit {
                retry_after_secs: 2,
            },
        });
        let mut cassette = Cassette::new("retry_chain", 0.80);
        let req1 =
            json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "step one"}]});
        let req2 =
            json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "step two"}]});
        let prompt1 = extract_prompt_text(&req1);
        let prompt2 = extract_prompt_text(&req2);
        let node1 = cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            &prompt1,
            &extract_structural_tools(&req1),
            req1.clone(),
            json!({"step": 1}),
            5,
        );
        cassette.record_node(
            Some(node1.clone()),
            InteractionKind::LlmCompletion,
            &prompt2,
            &extract_structural_tools(&req2),
            req2.clone(),
            json!({"step": 2}),
            5,
        );
        let state = Arc::new(Mutex::new(ProxyState {
            cassette,
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: Some(chaos),
            upstream: None,
            session_locks: SessionLocks::default(),
        }));
        // Attempt 1 and retry: 429 both times, parent stays None.
        for _ in 0..2 {
            let (status, _, _) = response_json(
                process_interaction(
                    state.clone(),
                    InteractionKind::LlmCompletion,
                    None,
                    req1.clone(),
                )
                .await,
            )
            .await;
            assert_eq!(status.as_u16(), 429);
            assert!(state.lock().unwrap().current_parent_id.is_none());
        }
        // Chaos off: the same request succeeds and advances to node1,
        // then step two matches node2.
        state.lock().unwrap().chaos = None;
        let (status, _, body) = response_json(
            process_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                req1.clone(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["step"], 1);
        assert_eq!(state.lock().unwrap().current_parent_id, Some(node1.clone()));
        let (status, _, body) = response_json(
            process_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                req2.clone(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["step"], 2);
    }

    // CT-HARDEN-005: the SSE pump holds its session guard until its
    // node is recorded, so a second same-session stream cannot read a
    // stale parent and record a sibling.
    #[tokio::test]
    async fn harden005_sse_pump_guard_keeps_session_chain_linear() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_static("sse-session"),
        );
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        // First stream: dispatch returns immediately (pump spawned),
        // then its first event arrives while gated.
        let resp1 = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-stream",
            headers.clone(),
            req.clone(),
        )
        .await;
        assert_eq!(resp1.status(), StatusCode::OK);
        let (_, body1) = resp1.into_parts();
        let mut stream1 = body1.into_data_stream();
        let mut first = String::new();
        while !first.contains("first") {
            let chunk = stream1.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        // Second stream in the same session: its dispatch blocks on the
        // session lock until the first pump finishes, so it cannot read
        // a stale parent.
        let state2 = state.clone();
        let headers2 = headers.clone();
        let req2 = req.clone();
        let task2 = tokio::spawn(async move {
            dispatch(
                state2,
                InteractionKind::LlmCompletion,
                "/v1/gated-stream",
                headers2,
                req2,
            )
            .await
        });
        // Release the first stream; it records node 1 and frees the lock.
        gate.notify_one();
        let mut rest1 = String::new();
        while let Some(chunk) = stream1.next().await {
            rest1.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        assert!(rest1.contains("[DONE]"));
        // Now the second stream proceeds; release its gate too.
        let resp2 = task2.await.unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let (_, body2) = resp2.into_parts();
        let mut stream2 = body2.into_data_stream();
        let mut first2 = String::new();
        while !first2.contains("first") {
            let chunk = stream2.next().await.expect("stream must yield");
            first2.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        gate.notify_one();
        while let Some(chunk) = stream2.next().await {
            let _ = chunk.unwrap();
        }
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 2);
        assert!(guard.cassette.nodes[0].parent_id.is_none());
        assert_eq!(
            guard.cassette.nodes[1].parent_id,
            Some(guard.cassette.nodes[0].node_id.clone())
        );
    }

    #[test]
    fn mcp_feature_extractor_handles_tools_call_and_sampling() {
        let tool_req = json!({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": "verify_order", "arguments": {"order_id": "ORD-900"}}
        });
        let (prompt, structural) = extract_mcp_matching_features(&tool_req);
        assert!(prompt.contains("tools/call"));
        assert_eq!(structural["method"], "tools/call");
        assert_eq!(structural["params"]["name"], "verify_order");
    }

    // CT-HARDEN-007 unit: Anthropic content blocks yield their text
    // parts; non-text blocks are skipped.
    #[test]
    fn extract_prompt_text_reads_anthropic_content_blocks() {
        let req = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "hello blocks"},
                    {"type": "image", "source": {"type": "base64", "data": "xx"}}
                ]
            }]
        });
        assert_eq!(extract_prompt_text(&req), "hello blocks");
    }

    // CT-HARDEN-007 unit: Responses API `input`, string and array.
    #[test]
    fn extract_prompt_text_reads_responses_input() {
        let string_input = json!({"model": "gpt-5", "input": "summarize this"});
        assert_eq!(extract_prompt_text(&string_input), "summarize this");
        let array_input = json!({"model": "gpt-5", "input": [
            {"role": "user", "content": [{"type": "input_text", "text": "first part"}]},
            "trailing",
            {"role": "user", "content": [{"type": "output_text", "text": "ignored"}]}
        ]});
        assert_eq!(extract_prompt_text(&array_input), "first part\ntrailing");
    }

    // CT-HARDEN-007 guard: plain chat string content is unchanged.
    #[test]
    fn extract_prompt_text_keeps_chat_string_content() {
        let req = json!({"messages": [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "hi"}
        ]});
        assert_eq!(extract_prompt_text(&req), "sys\nhi");
    }

    // CT-HARDEN-007: the new routes serve through build_router in both
    // record and replay mode; an unknown path 404s and GET on a known
    // path 405s. Before this fix both POSTs 404'd (no route existed).
    #[tokio::test]
    async fn harden007_responses_and_mcp_http_routes_record_and_replay() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let app = build_router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let url = format!("http://{addr}");

        let responses_req = json!({"model": "gpt-5", "input": "hello responses"});
        let resp_body: Value = client
            .post(format!("{url}/v1/responses"))
            .json(&responses_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp_body["echo"]["input"], "hello responses");
        let mcp_req = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                             "params": {"name": "verify_order"}});
        let mcp_body: Value = client
            .post(format!("{url}/mcp"))
            .json(&mcp_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(mcp_body["echo"]["params"]["name"], "verify_order");

        let recorded = state.lock().unwrap().cassette.clone();
        assert_eq!(recorded.nodes.len(), 2);
        assert_eq!(recorded.nodes[0].kind, InteractionKind::LlmCompletion);
        assert_eq!(recorded.nodes[1].kind, InteractionKind::McpToolCall);
        assert_eq!(
            recorded.nodes[0].request_payload["input"],
            "hello responses"
        );

        // Replay the same bodies through the same router from the
        // recorded cassette (no upstream): both return the recorded
        // echo in chain order.
        let replay_state = Arc::new(Mutex::new(ProxyState {
            cassette: recorded,
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: None,
            upstream: None,
            session_locks: SessionLocks::default(),
        }));
        let replay_app = build_router(replay_state);
        let replay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replay_addr = replay_listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(replay_listener, replay_app).await.unwrap();
        });
        let replay_url = format!("http://{replay_addr}");
        let replayed: Value = client
            .post(format!("{replay_url}/v1/responses"))
            .json(&responses_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(replayed["echo"]["input"], "hello responses");
        let replayed_mcp: Value = client
            .post(format!("{replay_url}/mcp"))
            .json(&mcp_req)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(replayed_mcp["echo"]["params"]["name"], "verify_order");

        let unknown = client
            .post(format!("{replay_url}/v1/unknown"))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        let wrong_method = client
            .get(format!("{replay_url}/v1/responses"))
            .send()
            .await
            .unwrap();
        assert_eq!(wrong_method.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    // SECOND AUDIT P1: known test secrets must not appear in
    // serialized cassette output after the scrub pipeline.
    #[test]
    fn second_audit_secrets_absent_from_serialized_cassette() {
        let scrubber = SecretScrubber::new();
        let raw_req = json!({
            "model": "gpt-4o",
            "messages": [{"content": "hi"}],
            "api_key": "sk-ant-api03-FAKEFAKEFAKEFAKE1234",
            "headers": {"authorization": "Bearer abcdefghijklmnop"},
        });
        let raw_resp = json!({"token": "tok_value_123", "text": "ok"});
        let mut cassette = Cassette::new("secret_serialization", 0.80);
        let tools = json!([{"name": "noop"}]);
        cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            "hi",
            &tools,
            scrubber.scrub_json(&raw_req),
            scrubber.scrub_json(&raw_resp),
            1,
        );
        let dir =
            std::env::temp_dir().join(format!("chronostrace-secret-out-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("out.ctrace");
        cassette.save_to_file(&target).unwrap();
        let text = std::fs::read_to_string(&target).unwrap();
        for leaked in ["FAKEFAKEFAKEFAKE", "abcdefghijklmnop", "tok_value_123"] {
            assert!(
                !text.contains(leaked),
                "secret leaked into serialized cassette: {leaked}"
            );
        }
        assert!(text.contains("REDACTED"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // LOAD TEST (dispatch level, in-process): 200 distinct
    // sessions replaying concurrently must all complete, and a
    // burst past the session cap must admit exactly MAX_SESSIONS
    // and reject the rest with 429 — deterministically, regardless
    // of task scheduling order. Scope, stated plainly: this
    // exercises the dispatch + session-lock + cap path, NOT the
    // HTTP socket layer or a real upstream. A full HTTP soak test
    // against the running binary is still future work.
    #[tokio::test]
    async fn load_concurrent_sessions_and_cap_pressure() {
        let state = sample_proxy_state(None);
        let req = || {
            json!({
                "model": "gpt-4o",
                "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
                "tools": json!([{"name": "verify_order"}])
            })
        };
        let burst = |state: Arc<Mutex<ProxyState>>, prefix: &str, count: usize| {
            let mut handles = Vec::new();
            for i in 0..count {
                let st = state.clone();
                let session = format!("{prefix}-{i}");
                let payload = req();
                handles.push(tokio::spawn(async move {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        "x-chronostrace-session",
                        HeaderValue::from_str(&session).unwrap(),
                    );
                    let (status, _, _) = response_json(
                        dispatch(
                            st,
                            InteractionKind::LlmCompletion,
                            "/v1/chat/completions",
                            headers,
                            payload,
                        )
                        .await,
                    )
                    .await;
                    status
                }));
            }
            handles
        };
        let start = std::time::Instant::now();
        let mut ok = 0;
        for h in burst(state.clone(), "load", 200) {
            if h.await.unwrap() == StatusCode::OK {
                ok += 1;
            }
        }
        let burst_elapsed = start.elapsed();
        assert_eq!(ok, 200, "all 200 concurrent session replays must succeed");
        assert!(
            burst_elapsed.as_secs() < 60,
            "200-dispatch burst took too long (hang?): {burst_elapsed:?}"
        );
        eprintln!("load: 200 concurrent session dispatches OK in {burst_elapsed:?}");
        // Cap pressure: 1100 more distinct sessions on top of the
        // 200 already admitted. Exactly MAX_SESSIONS may exist in
        // total; every further new session must get 429.
        let mut ok2 = 0;
        let mut rejected = 0;
        for h in burst(state.clone(), "pressure", 1100) {
            match h.await.unwrap() {
                StatusCode::OK => ok2 += 1,
                StatusCode::TOO_MANY_REQUESTS => rejected += 1,
                other => panic!("unexpected status under cap pressure: {other}"),
            }
        }
        assert_eq!(ok2, MAX_SESSIONS - 200);
        assert_eq!(rejected, 1100 - (MAX_SESSIONS - 200));
        assert_eq!(state.lock().unwrap().sessions.len(), MAX_SESSIONS);
        eprintln!("load: cap pressure admitted {ok2}, rejected {rejected}, map holds MAX_SESSIONS");
    }

    // UPSTREAM TIMEOUT (see docs/upstream-timeout.md). Mock upstream
    // with controllable behaviors; all tests go through dispatch()
    // except the replay test, which must call process_interaction()
    // directly — dispatch() routes to record whenever an upstream
    // is configured, so it cannot express "replay with a timeout
    // configured but never contacted".
    async fn timeout_upstream() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = Arc::new(AtomicUsize::new(0));
        let count = |hits: Arc<AtomicUsize>| {
            move || {
                hits.fetch_add(1, Ordering::SeqCst);
            }
        };
        let app = Router::new()
            .route(
                "/slow-headers",
                post({
                    let hits = hits.clone();
                    move |Json(_body): Json<Value>| {
                        let hits = hits.clone();
                        async move {
                            count(hits)();
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            Json(json!({"ok": true}))
                        }
                    }
                }),
            )
            .route(
                "/delayed-sse-headers",
                post({
                    let hits = hits.clone();
                    move |Json(_body): Json<Value>| {
                        let hits = hits.clone();
                        async move {
                            count(hits)();
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            let stream = stream::unfold(0u8, |step| async move {
                                match step {
                                    0 => Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("{\"delta\":\"late\"}"),
                                        ),
                                        1,
                                    )),
                                    1 => Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("[DONE]"),
                                        ),
                                        2,
                                    )),
                                    _ => None,
                                }
                            });
                            Sse::new(stream)
                        }
                    }
                }),
            )
            .route(
                "/hang-body",
                post({
                    let hits = hits.clone();
                    move |Json(_body): Json<Value>| {
                        let hits = hits.clone();
                        async move {
                            count(hits)();
                            let first = stream::once(async {
                                Ok::<_, std::io::Error>(axum::body::Bytes::from("{\"partial\":"))
                            });
                            let never: futures_util::stream::Pending<
                                Result<axum::body::Bytes, std::io::Error>,
                            > = stream::pending();
                            (
                                StatusCode::OK,
                                [(header::CONTENT_TYPE, "application/json")],
                                Body::from_stream(first.chain(never)),
                            )
                        }
                    }
                }),
            )
            .route(
                "/slow-sse",
                post({
                    let hits = hits.clone();
                    move |Json(_body): Json<Value>| {
                        let hits = hits.clone();
                        async move {
                            count(hits)();
                            let stream = stream::unfold(0u8, |step| async move {
                                if step < 5 {
                                    tokio::time::sleep(Duration::from_millis(600)).await;
                                    Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default()
                                                .data(format!("{{\"delta\":\"e{step}\"}}")),
                                        ),
                                        step + 1,
                                    ))
                                } else if step == 5 {
                                    Some((
                                        Ok::<_, std::convert::Infallible>(
                                            Event::default().data("[DONE]"),
                                        ),
                                        6,
                                    ))
                                } else {
                                    None
                                }
                            });
                            Sse::new(stream)
                        }
                    }
                }),
            )
            .route(
                "/fast",
                post({
                    let hits = hits.clone();
                    move |Json(_body): Json<Value>| {
                        let hits = hits.clone();
                        async move {
                            count(hits)();
                            Json(json!({"choices": [{"message": {"content": "fast ok"}}]}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), hits)
    }

    fn timeout_record_state(base_url: String, timeout_secs: u64) -> Arc<Mutex<ProxyState>> {
        Arc::new(Mutex::new(ProxyState {
            cassette: Cassette::new("timeout_test", 0.80),
            current_parent_id: None,
            sessions: HashMap::new(),
            scrubber: SecretScrubber::new(),
            chaos: None,
            upstream: Some(UpstreamTarget::with_timeout(&base_url, timeout_secs).unwrap()),
            session_locks: SessionLocks::default(),
        }))
    }

    fn timeout_req() -> Value {
        json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]})
    }

    async fn timeout_dispatch(
        state: Arc<Mutex<ProxyState>>,
        path: &str,
        session: Option<&str>,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut headers = HeaderMap::new();
        if let Some(key) = session {
            headers.insert(
                "x-chronostrace-session",
                HeaderValue::from_str(key).unwrap(),
            );
        }
        response_json(
            dispatch(
                state,
                InteractionKind::LlmCompletion,
                path,
                headers,
                timeout_req(),
            )
            .await,
        )
        .await
    }

    // Mandatory test 1: default 0 preserves previous behavior.
    #[tokio::test]
    async fn timeout_default_zero_preserves_behavior() {
        assert!(UpstreamTarget::new("http://127.0.0.1:9")
            .unwrap()
            .timeout
            .is_none());
        assert!(UpstreamTarget::with_timeout("http://127.0.0.1:9", 0)
            .unwrap()
            .timeout
            .is_none());
        assert_eq!(
            UpstreamTarget::with_timeout("http://127.0.0.1:9", 120)
                .unwrap()
                .timeout,
            Some(Duration::from_secs(120))
        );
        let (base, hits) = timeout_upstream().await;
        let state = timeout_record_state(base, 0);
        let (status, _, _) = timeout_dispatch(state.clone(), "/slow-headers", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 1);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // Mandatory test 2: slow upstream before headers -> timeout.
    #[tokio::test]
    async fn timeout_slow_headers_yields_504_and_no_node() {
        let (base, hits) = timeout_upstream().await;
        let state = timeout_record_state(base, 1);
        let (status, _, body) = timeout_dispatch(state.clone(), "/slow-headers", None).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body["error"], "CHRONOSTRACE_UPSTREAM_TIMEOUT");
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 0);
        assert!(state.lock().unwrap().current_parent_id.is_none());
        // Exactly one upstream attempt: no silent retry.
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // Mandatory test 3: non-SSE hanging body -> timeout.
    #[tokio::test]
    async fn timeout_hanging_body_yields_504_and_no_node() {
        let (base, hits) = timeout_upstream().await;
        let state = timeout_record_state(base, 1);
        let (status, _, body) = timeout_dispatch(state.clone(), "/hang-body", None).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body["error"], "CHRONOSTRACE_UPSTREAM_TIMEOUT");
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 0);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // Mandatory test 4: an SSE stream longer than the timeout keeps
    // running once headers arrived. The 120s figure from the policy
    // is scaled down (1s timeout, ~3s stream) so the test proves
    // the same property in reasonable time.
    #[tokio::test]
    async fn timeout_sse_long_stream_exempt_after_headers() {
        let (base, hits) = timeout_upstream().await;
        let state = timeout_record_state(base, 1);
        let resp = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/slow-sse",
            HeaderMap::new(),
            timeout_req(),
        )
        .await;
        let (status, _, text) = response_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        for i in 0..5 {
            assert!(
                text.contains(&format!("e{i}")),
                "missing SSE event e{i}: {text}"
            );
        }
        assert!(text.contains("[DONE]"));
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 1);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // SSE headers are still bounded: a delayed SSE handshake times out.
    #[tokio::test]
    async fn timeout_sse_delayed_headers_still_times_out() {
        let (base, hits) = timeout_upstream().await;
        let state = timeout_record_state(base, 1);
        let (status, _, body) = timeout_dispatch(state.clone(), "/delayed-sse-headers", None).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(body["error"], "CHRONOSTRACE_UPSTREAM_TIMEOUT");
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 0);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // Mandatory tests 5 (no success node, asserted above) + 6: the
    // next request for the same session still works after a timeout,
    // proving the session guard was released (a leaked guard would
    // hang this dispatch, so it is itself bounded).
    #[tokio::test]
    async fn timeout_next_request_same_session_still_works() {
        let (base, hits) = timeout_upstream().await;
        let state = timeout_record_state(base, 1);
        let (status, _, _) =
            timeout_dispatch(state.clone(), "/slow-headers", Some("after-timeout")).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 0);
        let second = tokio::time::timeout(
            Duration::from_secs(10),
            timeout_dispatch(state.clone(), "/fast", Some("after-timeout")),
        )
        .await
        .expect("second request must not hang on a leaked session lock");
        assert_eq!(second.0, StatusCode::OK);
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 1);
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    // Mandatory test 7: replay never contacts the upstream,
    // regardless of the configured timeout.
    #[tokio::test]
    async fn timeout_replay_unaffected_by_upstream_timeout() {
        let (base, hits) = timeout_upstream().await;
        let state = sample_proxy_state(None);
        state.lock().unwrap().upstream = Some(UpstreamTarget::with_timeout(&base, 1).unwrap());
        let sample_req = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
            "tools": json!([{"name": "verify_order"}])
        });
        let (status, _, body) = response_json(
            process_interaction(state, InteractionKind::LlmCompletion, None, sample_req).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.to_string().contains("Verified ORD-900"));
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    // CT-RUN-02 atomicity: the HTTP soak test admitted all 1100
    // concurrent sessions because admission checked the cap under
    // one lock but inserted the entry after the upstream
    // roundtrip. These tests run on a multi-threaded runtime so
    // the race window is real (the default current-thread
    // runtime serializes the tasks and hides it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cap_reservation_atomic_under_concurrent_record() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let mut handles = Vec::new();
        for i in 0..1100 {
            let st = state.clone();
            let session = format!("caprec-{i}");
            handles.push(tokio::spawn(async move {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-chronostrace-session",
                    HeaderValue::from_str(&session).unwrap(),
                );
                // fake_upstream requires the bearer header, else 401.
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&bearer_headers()[0].1).unwrap(),
                );
                let (status, _, _) = response_json(
                    dispatch(
                        st,
                        InteractionKind::LlmCompletion,
                        "/v1/chat/completions",
                        headers,
                        json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
                    )
                    .await,
                )
                .await;
                status
            }));
        }
        let mut ok = 0;
        let mut rejected = 0;
        for h in handles {
            match h.await.unwrap() {
                StatusCode::OK => ok += 1,
                StatusCode::TOO_MANY_REQUESTS => rejected += 1,
                other => panic!("unexpected status: {other}"),
            }
        }
        assert_eq!(ok, MAX_SESSIONS);
        assert_eq!(rejected, 1100 - MAX_SESSIONS);
        let guard = state.lock().unwrap();
        assert_eq!(guard.sessions.len(), MAX_SESSIONS);
        assert_eq!(guard.cassette.nodes.len(), MAX_SESSIONS);
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "1024 live sessions but zero idle locks: cleanup bounds the locks map to in-flight sessions"
        );
    }

    // Failed first requests must not burn slots: every
    // reservation is released when nothing is recorded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cap_failed_sessions_do_not_burn_slots() {
        let state = timeout_record_state("http://127.0.0.1:9".to_string(), 0);
        let mut handles = Vec::new();
        for i in 0..200 {
            let st = state.clone();
            let session = format!("capfail-{i}");
            handles.push(tokio::spawn(async move {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-chronostrace-session",
                    HeaderValue::from_str(&session).unwrap(),
                );
                headers.insert(
                    "authorization",
                    HeaderValue::from_str(&bearer_headers()[0].1).unwrap(),
                );
                let (status, _, _) = response_json(
                    dispatch(
                        st,
                        InteractionKind::LlmCompletion,
                        "/v1/chat/completions",
                        headers,
                        json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
                    )
                    .await,
                )
                .await;
                status
            }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap(), StatusCode::BAD_GATEWAY);
        }
        assert!(
            state.lock().unwrap().sessions.is_empty(),
            "failed sessions must release their reservations"
        );
        assert!(state.lock().unwrap().cassette.nodes.is_empty());
        // And a fresh session still gets a slot afterwards.
        let base = fake_upstream().await;
        state.lock().unwrap().upstream = Some(UpstreamTarget::new(&base).unwrap());
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_static("capfail-after"),
        );
        headers.insert(
            "authorization",
            HeaderValue::from_str(&bearer_headers()[0].1).unwrap(),
        );
        let (status, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/chat/completions",
                headers,
                json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // A broken SSE stream releases its reservation when the pump
    // gives up (dispatch skips cleanup for streaming SSE, so the
    // pump must own it).
    #[tokio::test]
    async fn cap_broken_sse_stream_releases_reservation() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_static("broken-0"),
        );
        let resp = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-broken",
            headers,
            req,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (_, body) = resp.into_parts();
        let mut data_stream = body.into_data_stream();
        let mut first = String::new();
        while !first.contains("first") {
            let chunk = data_stream.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        gate.notify_one();
        while let Some(chunk) = data_stream.next().await {
            let _ = chunk.unwrap();
        }
        let guard = state.lock().unwrap();
        assert!(guard.cassette.nodes.is_empty());
        assert!(
            !guard.sessions.contains_key("broken-0"),
            "broken stream must release its reservation"
        );
    }

    // SESSION LOCKS LIFECYCLE (see docs/session-locks.md).
    #[tokio::test]
    async fn session_locks_try_remove_semantics() {
        let locks = SessionLocks::default();
        let session = Some("life-0".to_string());
        let a1 = locks.lock_for(&session);
        let a2 = locks.lock_for(&session);
        assert!(std::sync::Arc::ptr_eq(&a1, &a2), "same key shares one lock");
        // A held guard blocks removal.
        let guard = a1.clone().lock_owned().await;
        assert!(!locks.try_remove(&session), "held lock must not be removed");
        drop(guard);
        // A bare clone (task between lock_for and lock_owned)
        // blocks removal too.
        let held = a1.clone();
        drop(a1);
        drop(a2);
        assert!(
            !locks.try_remove(&session),
            "outstanding clone must block removal"
        );
        drop(held);
        assert!(locks.try_remove(&session), "idle lock is removed");
        assert!(!locks.try_remove(&session), "second removal is a no-op");
        // A fresh lock works after removal.
        let a3 = locks.lock_for(&session);
        let g = a3.lock_owned().await;
        drop(g);
        assert!(locks.try_remove(&session));
    }

    // Cleanup must not break same-session chaining: two record
    // requests for one session still form a parent chain, and a
    // new session is accepted afterwards, with no lock entries
    // left behind for idle sessions.
    #[tokio::test]
    async fn session_locks_cleanup_preserves_chaining() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let dispatch_for = |state: Arc<Mutex<ProxyState>>, session: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-chronostrace-session",
                HeaderValue::from_str(session).unwrap(),
            );
            headers.insert(
                "authorization",
                HeaderValue::from_str(&bearer_headers()[0].1).unwrap(),
            );
            async move {
                response_json(
                    dispatch(
                        state,
                        InteractionKind::LlmCompletion,
                        "/v1/chat/completions",
                        headers,
                        req(),
                    )
                    .await,
                )
                .await
                .0
            }
        };
        assert_eq!(dispatch_for(state.clone(), "chain-0").await, StatusCode::OK);
        assert_eq!(dispatch_for(state.clone(), "chain-0").await, StatusCode::OK);
        {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 2);
            assert_eq!(
                guard.cassette.nodes[1].parent_id.as_deref(),
                Some(guard.cassette.nodes[0].node_id.as_str()),
                "second request chains after the first despite lock cleanup"
            );
            assert_eq!(
                guard.sessions.get("chain-0").cloned().flatten().as_deref(),
                Some(guard.cassette.nodes[1].node_id.as_str())
            );
            assert!(
                guard.session_locks.map.lock().unwrap().is_empty(),
                "idle session locks are cleaned, even for live sessions"
            );
        }
        assert_eq!(dispatch_for(state.clone(), "chain-1").await, StatusCode::OK);
        assert!(
            state
                .lock()
                .unwrap()
                .session_locks
                .map
                .lock()
                .unwrap()
                .is_empty(),
            "new session after cleanup leaves no lock behind either"
        );
    }

    // An active SSE stream keeps its lock: try_remove fails
    // mid-stream, and the pump cleans up at stream end.
    #[tokio::test]
    async fn session_locks_active_sse_keeps_protection() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_static("sse-life"),
        );
        let resp = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-stream",
            headers,
            req,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (_, body) = resp.into_parts();
        let mut data_stream = body.into_data_stream();
        let mut first = String::new();
        while !(first.contains("content_block_delta") && first.contains("first")) {
            let chunk = data_stream.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        // Stream active: the pump holds the guard, so removal
        // must fail and the entry must still be the live lock.
        assert!(
            state
                .lock()
                .unwrap()
                .session_locks
                .map
                .lock()
                .unwrap()
                .contains_key("sse-life"),
            "active stream keeps its lock entry"
        );
        assert!(
            !state
                .lock()
                .unwrap()
                .session_locks
                .try_remove(&Some("sse-life".to_string())),
            "active stream lock must not be removed"
        );
        gate.notify_one();
        while let Some(chunk) = data_stream.next().await {
            let _ = chunk.unwrap();
        }
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 1);
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "pump cleans the lock at stream end"
        );
    }

    // Replay chaining survives cleanup: the second same-session
    // replay still misses (parent advanced), and no lock remains.
    #[tokio::test]
    async fn session_locks_replay_linear_after_cleanup() {
        let state = sample_proxy_state(None);
        let req = || {
            json!({
                "model": "gpt-4o",
                "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
                "tools": json!([{"name": "verify_order"}])
            })
        };
        let dispatch_replay = |state: Arc<Mutex<ProxyState>>| {
            let mut headers = HeaderMap::new();
            headers.insert("x-chronostrace-session", HeaderValue::from_static("rep-0"));
            async move {
                response_json(
                    dispatch(
                        state,
                        InteractionKind::LlmCompletion,
                        "/v1/chat/completions",
                        headers,
                        req(),
                    )
                    .await,
                )
                .await
                .0
            }
        };
        assert_eq!(dispatch_replay(state.clone()).await, StatusCode::OK);
        assert_eq!(
            dispatch_replay(state.clone()).await,
            StatusCode::PRECONDITION_FAILED,
            "second replay misses: parent advanced despite lock cleanup"
        );
        assert!(
            state
                .lock()
                .unwrap()
                .session_locks
                .map
                .lock()
                .unwrap()
                .is_empty(),
            "replay leaves no lock behind"
        );
    }

    // CANCELLATION-SAFE RESERVATION (see docs/cancellation-safety.md).
    // Every test drops a `dispatch` future mid-flight and proves
    // the reservation is cleaned without disturbing other work.
    async fn gated_json_upstream() -> (
        String,
        Arc<tokio::sync::Notify>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let notify = Arc::new(tokio::sync::Notify::new());
        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let notify_for_route = notify.clone();
        let received_for_route = received.clone();
        let app = Router::new().route(
            "/v1/gated-json",
            post(move |Json(body): Json<Value>| {
                let notify = notify_for_route.clone();
                let received = received_for_route.clone();
                async move {
                    received.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    notify.notified().await;
                    (StatusCode::OK, Json(json!({"echo": body})))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), notify, received)
    }

    fn session_headers(session: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-chronostrace-session",
            HeaderValue::from_str(session).unwrap(),
        );
        headers
    }

    async fn wait_until_session(
        state: &Arc<Mutex<ProxyState>>,
        key: &str,
        want: Option<Option<String>>,
        what: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let got = state.lock().unwrap().sessions.get(key).cloned();
            // want: None = entry absent; Some(None) = reservation;
            // Some(Some(_)) = live parent pointer.
            let matched = matches!(
                (&want, &got),
                (None, None) | (Some(None), Some(None)) | (Some(Some(_)), Some(Some(_)))
            );
            if matched {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // 1. Abort while waiting for the session lock. A waiting
    // request is by construction an EXISTING session (a new
    // key's lock is fresh and uncontended), so it holds no
    // reservation — what must hold is that the abort leaves
    // the holder untouched and the lock usable afterwards.
    #[tokio::test]
    async fn cancel_waiting_for_session_lock_leaves_no_trace() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        let sse_req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        // Holder: gated SSE stream for session "cancel-lock".
        let resp = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-stream",
            session_headers("cancel-lock"),
            sse_req(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (_, body) = resp.into_parts();
        let mut data_stream = body.into_data_stream();
        let mut first = String::new();
        while !(first.contains("content_block_delta") && first.contains("first")) {
            let chunk = data_stream.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        // Waiter: same session, blocked on the lock the pump holds.
        let waiter = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/chat/completions",
            session_headers("cancel-lock"),
            json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}),
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiter.is_finished(), "waiter must be blocked on the lock");
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        // Holder unaffected: release + drain to a recorded node.
        gate.notify_one();
        while let Some(chunk) = data_stream.next().await {
            let _ = chunk.unwrap();
        }
        let node0 = {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 1, "holder records its node");
            let node0 = guard.cassette.nodes[0].node_id.clone();
            assert_eq!(
                guard
                    .sessions
                    .get("cancel-lock")
                    .cloned()
                    .flatten()
                    .as_deref(),
                Some(node0.as_str()),
                "holder session intact after waiter abort"
            );
            node0
        };
        // Next request for the same session chains from it.
        let resp2 = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-stream",
            session_headers("cancel-lock"),
            sse_req(),
        )
        .await;
        assert_eq!(resp2.status(), StatusCode::OK);
        let (_, body2) = resp2.into_parts();
        let mut stream2 = body2.into_data_stream();
        let mut first2 = String::new();
        while !(first2.contains("content_block_delta") && first2.contains("first")) {
            let chunk = stream2.next().await.expect("stream must yield");
            first2.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        gate.notify_one();
        while let Some(chunk) = stream2.next().await {
            let _ = chunk.unwrap();
        }
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 2);
        assert_eq!(
            guard.cassette.nodes[1].parent_id.as_deref(),
            Some(node0.as_str()),
            "DAG chain intact after lock-wait abort"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind"
        );
    }

    // 2. Abort while waiting for the upstream. The aborted
    // reservation is cleaned, the cap slot frees, and a
    // concurrent request for another session completes.
    #[tokio::test]
    async fn cancel_waiting_for_upstream_cleans_reservation() {
        let (base, gate, received) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let a = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers("cancel-up-a"),
            req(),
        ));
        let b = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers("cancel-up-b"),
            req(),
        ));
        // Both handlers entered and are waiting on the gate.
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            assert!(Instant::now() < deadline, "upstream never received both");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        a.abort();
        assert!(a.await.unwrap_err().is_cancelled());
        {
            let guard = state.lock().unwrap();
            assert!(
                !guard.sessions.contains_key("cancel-up-a"),
                "aborted reservation cleaned"
            );
            assert_eq!(
                guard.sessions.get("cancel-up-b"),
                Some(&None),
                "other session still waiting, undisturbed"
            );
        }
        gate.notify_one();
        let (status_b, _, _) = response_json(b.await.unwrap()).await;
        assert_eq!(status_b, StatusCode::OK, "other session completes");
        // Aborted key works again, as a fresh root.
        gate.notify_one();
        let (status_a2, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers("cancel-up-a"),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status_a2, StatusCode::OK, "cap slot freed for the key");
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 2);
        assert!(
            guard.cassette.nodes.iter().all(|n| n.parent_id.is_none()),
            "independent sessions are independent roots"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind"
        );
    }

    // 3. Abort during a replay chaos delay. Replay advances the
    // parent pointer BEFORE the delay sleep (pre-existing
    // semantics), so the correct post-abort state is a live
    // parent pointer — which the guard must NOT delete. The
    // test proves the guard leaves `Some` entries alone, the
    // DAG chains, and other sessions are unaffected.
    #[tokio::test]
    async fn cancel_during_chaos_delay_keeps_parent_pointer() {
        let state = sample_proxy_state(Some(ChaosEngine::new(ChaosProfile {
            name: "cdelay".into(),
            seed: 7,
            probability: 1.0,
            fault: FaultKind::LatencySpike { delay_ms: 5000 },
        })));
        let req = || {
            json!({
                "model": "gpt-4o",
                "messages": [{"role": "system", "content": "You are a helpful financial refund agent. Verify order ORD-900 for $50."}],
                "tools": json!([{"name": "verify_order"}])
            })
        };
        let handle = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/chat/completions",
            session_headers("cancel-chaos"),
            req(),
        ));
        // Entry advanced to Some: we are inside the delay sleep.
        wait_until_session(
            &state,
            "cancel-chaos",
            Some(Some(String::new())),
            "parent advance before chaos delay",
        )
        .await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        let node_id = {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 1, "replay records nothing new");
            guard
                .sessions
                .get("cancel-chaos")
                .cloned()
                .flatten()
                .expect("parent pointer must survive the abort")
        };
        assert_eq!(node_id, state.lock().unwrap().cassette.nodes[0].node_id);
        // Next request for the same session chains (parent
        // advanced, no child) and another session still hits.
        let (status_same, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/chat/completions",
                session_headers("cancel-chaos"),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(
            status_same,
            StatusCode::PRECONDITION_FAILED,
            "DAG chaining intact after chaos-delay abort"
        );
        let (status_other, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/chat/completions",
                session_headers("cancel-chaos-other"),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status_other, StatusCode::OK, "other session unaffected");
        assert!(
            state
                .lock()
                .unwrap()
                .session_locks
                .map
                .lock()
                .unwrap()
                .is_empty(),
            "no lock left behind"
        );
    }

    // 4. Abort after reservation, before any node. The
    // reservation is cleaned, the cap slot frees, the next
    // request records a root, and the follow-up chains.
    #[tokio::test]
    async fn cancel_after_reservation_before_node_records_clean_root() {
        let (base, gate, _) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let handle = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers("cancel-res"),
            req(),
        ));
        // Reservation exists, no node yet (gate still closed).
        wait_until_session(&state, "cancel-res", Some(None), "reservation").await;
        assert_eq!(state.lock().unwrap().cassette.nodes.len(), 0);
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        {
            let guard = state.lock().unwrap();
            assert!(
                !guard.sessions.contains_key("cancel-res"),
                "reservation cleaned"
            );
            assert_eq!(guard.sessions.len(), 0, "cap slot freed");
            assert!(
                guard.session_locks.map.lock().unwrap().is_empty(),
                "no lock left behind"
            );
        }
        // Next request for the key records a root.
        gate.notify_one();
        let (status1, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers("cancel-res"),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status1, StatusCode::OK);
        let node0 = {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 1);
            assert!(
                guard.cassette.nodes[0].parent_id.is_none(),
                "first node after abort is a root"
            );
            guard.cassette.nodes[0].node_id.clone()
        };
        // Follow-up chains from it.
        gate.notify_one();
        let (status2, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers("cancel-res"),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status2, StatusCode::OK);
        // An unrelated session still works.
        gate.notify_one();
        let (status3, _, _) = response_json(
            dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers("cancel-res-other"),
                req(),
            )
            .await,
        )
        .await;
        assert_eq!(status3, StatusCode::OK);
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 3);
        assert_eq!(
            guard.cassette.nodes[1].parent_id.as_deref(),
            Some(node0.as_str()),
            "DAG chain intact after abort"
        );
        assert!(
            guard.cassette.nodes[2].parent_id.is_none(),
            "unrelated session is its own root"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind"
        );
    }

    // 5. Drop-order probe: pins the language assumption the
    // cancellation design relies on. When an async future is
    // dropped mid-await, the awaited subfuture must drop
    // BEFORE the outer frame's earlier locals, so a cleanup
    // guard declared before the session lock always sees the
    // session clones already released. If this ever fails on
    // a new toolchain, the guard design must be revisited.
    struct DropLog {
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        name: &'static str,
    }
    impl Drop for DropLog {
        fn drop(&mut self) {
            self.log.lock().unwrap().push(self.name);
        }
    }
    #[tokio::test]
    async fn cancel_drop_order_inner_before_outer() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log_inner = log.clone();
        let inner = async move {
            let _inner = DropLog {
                log: log_inner,
                name: "inner",
            };
            std::future::pending::<()>().await;
        };
        let log_outer = log.clone();
        let outer = async move {
            let _outer = DropLog {
                log: log_outer,
                name: "outer",
            };
            inner.await;
        };
        let handle = tokio::spawn(outer);
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        assert_eq!(
            *log.lock().unwrap(),
            vec!["inner", "outer"],
            "awaited subfuture must drop before the outer frame's earlier locals"
        );
    }

    // 6. Regression (failed before the universal guard):
    // abort of an EXISTING session's holder. Only new
    // sessions used to get a guard; a follow-up request for
    // an existing session had none, its tail never ran on
    // abort, and nothing removed its lock entry. The live
    // parent pointer must survive (it is session state, not
    // a leak), but the lock entry must not linger.
    #[tokio::test]
    async fn cancel_existing_session_holder_leaves_no_lock() {
        let (base, gate, received) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let key = "cancel-exist-holder";
        // Seed: one completed record, so the session EXISTS.
        let seed = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers(key),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "seed never reached upstream");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        gate.notify_one();
        let (status_seed, _, _) = response_json(seed.await.unwrap()).await;
        assert_eq!(status_seed, StatusCode::OK);
        let node0 = state.lock().unwrap().cassette.nodes[0].node_id.clone();
        // Follow-up for the existing session, aborted mid-flight.
        let handle = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers(key),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            assert!(
                Instant::now() < deadline,
                "follow-up never reached upstream"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        let guard = state.lock().unwrap();
        assert_eq!(
            guard.cassette.nodes.len(),
            1,
            "aborted request records nothing"
        );
        assert_eq!(
            guard.sessions.get(key).cloned().flatten().as_deref(),
            Some(node0.as_str()),
            "live parent pointer must survive the abort"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind after unguarded holder abort"
        );
    }

    // 7. Regression (failed before the universal guard):
    // holder AND waiter for the same existing session both
    // abort. Neither future used to have a guard and neither
    // tail ran, so no one ever attempted lock cleanup for
    // the key.
    #[tokio::test]
    async fn cancel_holder_and_waiter_leaves_no_lock() {
        let (base, gate, received) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let key = "cancel-pair";
        let seed = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers(key),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "seed never reached upstream");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        gate.notify_one();
        let (status_seed, _, _) = response_json(seed.await.unwrap()).await;
        assert_eq!(status_seed, StatusCode::OK);
        let node0 = state.lock().unwrap().cassette.nodes[0].node_id.clone();
        // Holder: existing session, waiting on the gate, lock held.
        let holder = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers(key),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            assert!(Instant::now() < deadline, "holder never reached upstream");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Waiter: blocked on the session lock, never reaches upstream.
        let waiter = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers(key),
            req(),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            received.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "waiter must be blocked on the lock, not on the upstream"
        );
        holder.abort();
        assert!(holder.await.unwrap_err().is_cancelled());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let guard = state.lock().unwrap();
        assert_eq!(
            guard.sessions.get(key).cloned().flatten().as_deref(),
            Some(node0.as_str()),
            "live parent pointer must survive the double abort"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind after holder+waiter double abort"
        );
    }

    // 8. Repeated aborts of NEW sessions: the guard path.
    // Must stay clean (this one passes on current code).
    #[tokio::test]
    async fn cancel_repeated_new_sessions_leave_no_locks() {
        let (base, _gate, received) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        for i in 0..50 {
            let key = format!("cancel-new-{i}");
            let before = received.load(std::sync::atomic::Ordering::SeqCst);
            let handle = tokio::spawn(dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers(&key),
                req(),
            ));
            let deadline = Instant::now() + Duration::from_secs(5);
            while received.load(std::sync::atomic::Ordering::SeqCst) <= before {
                assert!(Instant::now() < deadline, "dispatch never reached upstream");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            handle.abort();
            assert!(handle.await.unwrap_err().is_cancelled());
        }
        let guard = state.lock().unwrap();
        assert_eq!(guard.sessions.len(), 0, "no reservation left behind");
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind after 50 new-session aborts"
        );
    }

    // 9. Regression (failed before the universal guard):
    // repeated aborts of EXISTING sessions with distinct
    // keys. Each aborted follow-up used to leave one idle
    // lock entry, so the map grew by one per distinct key.
    #[tokio::test]
    async fn cancel_repeated_existing_sessions_leave_no_locks() {
        let (base, gate, received) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        for i in 0..50 {
            let key = format!("cancel-exist-{i}");
            // Seed the session with one completed record.
            let before = received.load(std::sync::atomic::Ordering::SeqCst);
            let seed = tokio::spawn(dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers(&key),
                req(),
            ));
            let deadline = Instant::now() + Duration::from_secs(5);
            while received.load(std::sync::atomic::Ordering::SeqCst) <= before {
                assert!(Instant::now() < deadline, "seed never reached upstream");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            gate.notify_one();
            let (status_seed, _, _) = response_json(seed.await.unwrap()).await;
            assert_eq!(status_seed, StatusCode::OK);
            // Follow-up for the existing session, aborted mid-flight.
            let before = received.load(std::sync::atomic::Ordering::SeqCst);
            let handle = tokio::spawn(dispatch(
                state.clone(),
                InteractionKind::LlmCompletion,
                "/v1/gated-json",
                session_headers(&key),
                req(),
            ));
            let deadline = Instant::now() + Duration::from_secs(5);
            while received.load(std::sync::atomic::Ordering::SeqCst) <= before {
                assert!(
                    Instant::now() < deadline,
                    "follow-up never reached upstream"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            handle.abort();
            assert!(handle.await.unwrap_err().is_cancelled());
        }
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 50, "seeds all recorded");
        assert_eq!(guard.sessions.len(), 50, "live parent pointers intact");
        let locks = guard.session_locks.map.lock().unwrap().len();
        assert_eq!(
            locks, 0,
            "no lock left behind after 50 existing-session aborts, got {locks} idle entries"
        );
    }

    // Backstop sweep tests. Each lock_for call below the
    // threshold never sweeps; above it, the budget allows one
    // full sweep per 1024 calls. Filling 2049 idle entries
    // leaves the map at 2049 with the budget unspent, so 1023
    // further above-threshold calls still do not sweep, and
    // the 1024th does.
    fn fill_idle_locks(locks: &SessionLocks, n: usize) {
        for i in 0..n {
            let _ = locks.lock_for(&Some(format!("idle-{i}")));
        }
    }

    #[test]
    fn session_locks_sweep_removes_idle_above_threshold() {
        let locks = SessionLocks::default();
        fill_idle_locks(&locks, 2049);
        assert_eq!(
            locks.map.lock().unwrap().len(),
            2049,
            "no sweep below the budget"
        );
        for i in 0..1023 {
            let _ = locks.lock_for(&Some(format!("budget-{i}")));
        }
        assert_eq!(
            locks.map.lock().unwrap().len(),
            3072,
            "budget gates the sweep: no sweep in the first 1023 above-threshold calls"
        );
        let _ = locks.lock_for(&Some("trigger".to_string()));
        let map = locks.map.lock().unwrap();
        assert_eq!(map.len(), 1, "sweep removes every idle entry");
        assert!(map.contains_key("trigger"));
    }

    #[tokio::test]
    async fn session_locks_sweep_keeps_held_lock() {
        let locks = SessionLocks::default();
        fill_idle_locks(&locks, 2049);
        let held = locks.lock_for(&Some("held".to_string()));
        let _guard = held.clone().lock_owned().await;
        for i in 0..1023 {
            let _ = locks.lock_for(&Some(format!("budget-{i}")));
        }
        let map = locks.map.lock().unwrap();
        assert_eq!(map.len(), 2, "only the held entry and the trigger survive");
        assert!(map.contains_key("held"));
        assert!(std::sync::Arc::ptr_eq(map.get("held").unwrap(), &held));
        drop(map);
        drop(_guard);
        drop(held);
        assert!(
            locks.try_remove(&Some("held".to_string())),
            "cleanup still works for the held entry after the sweep"
        );
    }

    #[tokio::test]
    async fn session_locks_sweep_keeps_waited_lock() {
        let locks = SessionLocks::default();
        fill_idle_locks(&locks, 2049);
        let held = locks.lock_for(&Some("held".to_string()));
        let _guard = held.clone().lock_owned().await;
        // Bare clone: a task between lock_for and lock_owned.
        // (This lock_for call itself is above threshold, so it
        // consumes one budget unit: 2 spent before the loop.)
        let _waiter = locks.lock_for(&Some("held".to_string()));
        for i in 0..1022 {
            let _ = locks.lock_for(&Some(format!("budget-{i}")));
        }
        let map = locks.map.lock().unwrap();
        assert_eq!(
            map.len(),
            2,
            "only the waited entry and the trigger survive"
        );
        assert!(map.contains_key("held"));
        drop(map);
        drop(_guard);
        drop(_waiter);
        drop(held);
        assert!(
            locks.try_remove(&Some("held".to_string())),
            "cleanup still works for the waited entry after the sweep"
        );
    }

    #[tokio::test]
    async fn session_locks_sweep_keeps_pump_lock_and_stream_completes() {
        let (base, gate) = gated_sse_upstream().await;
        let state = record_state(base);
        fill_idle_locks(&state.lock().unwrap().session_locks, 2049);
        {
            let guard = state.lock().unwrap();
            for i in 0..1023 {
                let _ = guard.session_locks.lock_for(&Some(format!("budget-{i}")));
            }
        }
        // The stream's lock_for is the 1024th above-threshold
        // call: the sweep runs inside dispatch and must spare
        // the pump's lock while removing all 3072 idle entries.
        let sse_req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let resp = dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-stream",
            session_headers("sweep-pump"),
            sse_req(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        {
            let guard = state.lock().unwrap();
            let map = guard.session_locks.map.lock().unwrap();
            assert_eq!(map.len(), 1, "sweep leaves only the pump's entry");
            assert!(map.contains_key("sweep-pump"));
        }
        let (_, body) = resp.into_parts();
        let mut data_stream = body.into_data_stream();
        let mut first = String::new();
        while !(first.contains("content_block_delta") && first.contains("first")) {
            let chunk = data_stream.next().await.expect("stream must yield");
            first.push_str(&String::from_utf8_lossy(&chunk.unwrap()));
        }
        gate.notify_one();
        while let Some(chunk) = data_stream.next().await {
            let _ = chunk.unwrap();
        }
        let guard = state.lock().unwrap();
        assert_eq!(
            guard.cassette.nodes.len(),
            1,
            "pump records its node after surviving the sweep"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "pump terminal cleans up after the sweep"
        );
    }

    #[tokio::test]
    async fn session_locks_sweep_preserves_ordering_and_parent() {
        let (base, gate, received) = gated_json_upstream().await;
        let state = record_state(base);
        let req = || json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        // Seed session "sweep-order" with one node.
        let seed = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers("sweep-order"),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "seed never reached upstream");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        gate.notify_one();
        let (status_seed, _, _) = response_json(seed.await.unwrap()).await;
        assert_eq!(status_seed, StatusCode::OK);
        let node0 = state.lock().unwrap().cassette.nodes[0].node_id.clone();
        // Fill idle entries past the threshold, then spend the
        // budget so the follow-up's lock_for runs the sweep.
        // (The seed's tail already removed its own lock entry,
        // so the sweep has no live entry to spare here; what
        // it must not touch is the sessions parent pointer.)
        fill_idle_locks(&state.lock().unwrap().session_locks, 2049);
        {
            let guard = state.lock().unwrap();
            for i in 0..1023 {
                let _ = guard.session_locks.lock_for(&Some(format!("budget-{i}")));
            }
        }
        let follow = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers("sweep-order"),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            assert!(
                Instant::now() < deadline,
                "follow-up never reached upstream"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        gate.notify_one();
        let (status_follow, _, _) = response_json(follow.await.unwrap()).await;
        assert_eq!(status_follow, StatusCode::OK);
        // An unrelated session still records its own root.
        let other = tokio::spawn(dispatch(
            state.clone(),
            InteractionKind::LlmCompletion,
            "/v1/gated-json",
            session_headers("sweep-other"),
            req(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.load(std::sync::atomic::Ordering::SeqCst) < 3 {
            assert!(
                Instant::now() < deadline,
                "other session never reached upstream"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        gate.notify_one();
        let (status_other, _, _) = response_json(other.await.unwrap()).await;
        assert_eq!(status_other, StatusCode::OK);
        let guard = state.lock().unwrap();
        assert_eq!(guard.cassette.nodes.len(), 3);
        assert_eq!(
            guard.cassette.nodes[1].parent_id.as_deref(),
            Some(node0.as_str()),
            "follow-up chains from the seeded node after the sweep"
        );
        assert!(
            guard.cassette.nodes[2].parent_id.is_none(),
            "unrelated session is its own root after the sweep"
        );
        assert!(
            guard.session_locks.map.lock().unwrap().is_empty(),
            "no lock left behind after the sweep"
        );
    }
}
