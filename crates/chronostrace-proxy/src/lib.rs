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
}

impl UpstreamTarget {
    /// Validate the base URL and build the forwarding client. The client
    /// connects directly (no environment proxies) and never follows
    /// redirects, so the recorded response is exactly what the upstream
    /// at this URL returned.
    pub fn new(base_url: &str) -> Result<Self, String> {
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
        })
    }
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
/// handle. The map itself is a plain std Mutex because entries are
/// only created, never removed, and the critical section is trivial.
#[derive(Default)]
pub struct SessionLocks {
    map: Mutex<HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>,
}

impl SessionLocks {
    pub fn lock_for(&self, session: &Option<String>) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let key = session.clone().unwrap_or_default();
        let mut map = self.map.lock().expect("SessionLocks mutex poisoned");
        map.entry(key)
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
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

async fn dispatch(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    path: &str,
    headers: HeaderMap,
    payload: Value,
) -> Response {
    let session = session_key(&headers);
    // CT-RUN-02 admission: reject a NEW session past the cap
    // before it allocates a session entry or a session lock.
    // Existing sessions are never rejected by this check.
    if let Some(key) = &session {
        let at_cap = {
            let guard = state.lock().expect("ProxyState mutex poisoned");
            !guard.sessions.contains_key(key) && guard.sessions.len() >= MAX_SESSIONS
        };
        if at_cap {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "CHRONOSTRACE_TOO_MANY_SESSIONS",
                    "message": "Session limit reached; retry with an existing session id"
                })),
            )
                .into_response();
        }
    }
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
    if is_record {
        record_interaction(
            state,
            kind,
            session,
            path,
            forwarded_headers,
            payload,
            Some(guard),
        )
        .await
    } else {
        process_interaction(state, kind, session, payload).await
    }
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
    let (base_url, client) = {
        let guard = state.lock().expect("ProxyState mutex poisoned");
        match &guard.upstream {
            Some(up) => (up.base_url.clone(), up.client.clone()),
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
    let upstream =
        match send_upstream_request(&client, &base_url, path, &raw_request, &forwarded_headers)
            .await
        {
            Ok(resp) => resp,
            Err(message) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({
                        "error": "CHRONOSTRACE_UPSTREAM_ERROR",
                        "message": message
                    })),
                )
                    .into_response();
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
        let body = upstream.bytes().await.unwrap_or_default();
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
        return stream_sse_record(
            state,
            session,
            parent_id,
            kind,
            scrubbed_req,
            upstream,
            started,
            session_guard,
        )
        .await;
    }

    if content_type.contains("application/json") {
        let body = match upstream.bytes().await {
            Ok(b) => b,
            Err(message) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({
                        "error": "CHRONOSTRACE_UPSTREAM_ERROR",
                        "message": message.to_string()
                    })),
                )
                    .into_response();
            }
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
        // chain in arrival order.
        let _session_guard = session_guard;
        let mut parser = SseParser::new();
        let mut events = Vec::new();
        let mut errored = false;
        while let Some(chunk) = byte_stream.next().await {
            match chunk {
                Ok(bytes) => {
                    events.extend(parser.feed(&bytes));
                    if tx.send(Ok(bytes)).await.is_err() {
                        // Client disconnected: stop, record nothing.
                        return;
                    }
                }
                Err(_) => {
                    errored = true;
                    break;
                }
            }
        }
        if errored {
            // Truncated stream: the client saw a prefix, but a partial
            // stream must never replay as complete. Record nothing and
            // leave the session parent untouched.
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
            upstream: Some(UpstreamTarget { base_url, client }),
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
}
