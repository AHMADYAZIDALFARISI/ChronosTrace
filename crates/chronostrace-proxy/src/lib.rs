pub mod http_record;
pub mod mcp_stdio;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::post,
    Json, Router,
};
use chronostrace_chaos::{ChaosEngine, ChaosOutcome};
use chronostrace_core::{Cassette, InteractionKind, MatchResult};
use chronostrace_scrub::SecretScrubber;
use futures_util::stream;
use http_record::{
    forward_to_upstream, parse_sse_body, scrub_sse_chunk, sse_chunks_from_payload,
    sse_marker_payload,
};
use serde_json::{json, Value};
use std::collections::HashMap;
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
}

pub fn build_router(state: Arc<Mutex<ProxyState>>) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/messages", post(handle_messages))
        .route("/mcp/call", post(handle_mcp_replay))
        .with_state(state)
}

fn session_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-chronostrace-session")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

async fn handle_chat_completions(
    State(state): State<Arc<Mutex<ProxyState>>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    dispatch(
        state,
        InteractionKind::LlmCompletion,
        "/v1/chat/completions",
        headers,
        payload,
    )
    .await
}

async fn handle_messages(
    State(state): State<Arc<Mutex<ProxyState>>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    dispatch(
        state,
        InteractionKind::LlmCompletion,
        "/v1/messages",
        headers,
        payload,
    )
    .await
}

async fn handle_mcp_replay(
    State(state): State<Arc<Mutex<ProxyState>>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    dispatch(
        state,
        InteractionKind::McpToolCall,
        "/mcp/call",
        headers,
        payload,
    )
    .await
}

async fn dispatch(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    path: &str,
    headers: HeaderMap,
    payload: Value,
) -> Response {
    let session = session_key(&headers);
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let is_record = state
        .lock()
        .expect("ProxyState mutex poisoned")
        .upstream
        .is_some();
    if is_record {
        record_interaction(state, kind, session, path, auth, payload).await
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
        Sse(Vec<String>),
        Chaos(ChaosOutcome),
    }

    let mut headers = HeaderMap::new();
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
                let (node_id, resp_payload) = match res {
                    MatchResult::Exact(node) => {
                        (node.node_id.clone(), node.response_payload.clone())
                    }
                    MatchResult::SemanticFuzzy { node, similarity } => {
                        tracing::info!(
                            node_id = %node.node_id,
                            similarity = %format!("{similarity:.3}"),
                            "ChronosTrace matched via SimHash prompt tolerance"
                        );
                        (node.node_id.clone(), node.response_payload.clone())
                    }
                };
                // SSE recordings stream back as event streams. Chaos fault
                // injection applies to JSON payloads only and is skipped
                // here so a mutated marker can never corrupt the stream.
                if let Some(chunks) = sse_chunks_from_payload(&resp_payload) {
                    match &session {
                        Some(key) => {
                            guard.sessions.insert(key.clone(), Some(node_id));
                        }
                        None => {
                            guard.current_parent_id = Some(node_id);
                        }
                    }
                    Outcome::Sse(chunks)
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
                    Outcome::Chaos(chaos_res)
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
        Outcome::Sse(chunks) => {
            let stream = stream::iter(
                chunks
                    .into_iter()
                    .map(|c| Ok::<_, std::convert::Infallible>(Event::default().data(c))),
            );
            Sse::new(stream).into_response()
        }
        Outcome::Chaos(chaos_step) => match chaos_step {
            ChaosOutcome::PassThrough(body) | ChaosOutcome::MutatedPayload(body) => {
                (StatusCode::OK, headers, Json(body)).into_response()
            }
            ChaosOutcome::DelayThenPassThrough { delay_ms, payload } => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                (StatusCode::OK, headers, Json(payload)).into_response()
            }
            ChaosOutcome::InjectedError {
                status_code,
                retry_after_secs,
                body,
            } => {
                if let Some(secs) = retry_after_secs {
                    if let Ok(val) = HeaderValue::from_str(&secs.to_string()) {
                        headers.insert(header::RETRY_AFTER, val);
                    }
                }
                (
                    StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    headers,
                    Json(body),
                )
                    .into_response()
            }
        },
    }
}

/// Record-mode counterpart of [`process_interaction`]: forward the
/// scrubbed request to the configured upstream, store the scrubbed
/// response as a new cassette node, and return the upstream response to
/// the client verbatim. Error responses are passed through but never
/// recorded: replay serves recorded nodes with 200, so recording an
/// error would replay it as a success.
pub async fn record_interaction(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    session: Option<String>,
    path: &str,
    auth: Option<String>,
    raw_request: Value,
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

    // Scrub before forwarding: replay scrubs incoming requests before
    // matching, so the recorded request must be the scrubbed form for
    // exact matching to work later. Nothing unscrubbed is ever stored.
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
    let forwarded =
        forward_to_upstream(&client, &base_url, path, &scrubbed_req, auth.as_deref()).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let up = match forwarded {
        Ok(up) => up,
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
    if !(200..300).contains(&up.status) {
        let status = StatusCode::from_u16(up.status).unwrap_or(StatusCode::BAD_GATEWAY);
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, up.content_type)
            .body(Body::from(up.body))
            .unwrap_or_else(|_| (status, "upstream error").into_response());
    }

    if up.content_type.contains("text/event-stream") {
        let chunks = parse_sse_body(&up.body);
        let stored_chunks = {
            let guard = state.lock().expect("ProxyState mutex poisoned");
            chunks
                .iter()
                .map(|c| scrub_sse_chunk(&guard.scrubber, c))
                .collect::<Vec<_>>()
        };
        let payload = sse_marker_payload(stored_chunks);
        record_node_and_advance(
            &state,
            &session,
            &parent_id,
            kind,
            &scrubbed_req,
            payload,
            elapsed_ms,
        );
        let stream = stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<_, std::convert::Infallible>(Event::default().data(c))),
        );
        return Sse::new(stream).into_response();
    }

    if up.content_type.contains("application/json") {
        let raw_json: Value = match serde_json::from_slice(&up.body) {
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
            elapsed_ms,
        );
        // The client sees the upstream body verbatim; only the stored
        // copy is scrubbed.
        let status = StatusCode::from_u16(up.status).unwrap_or(StatusCode::OK);
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, up.content_type)
            .body(Body::from(up.body))
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

fn record_node_and_advance(
    state: &Arc<Mutex<ProxyState>>,
    session: &Option<String>,
    parent_id: &Option<String>,
    kind: InteractionKind,
    scrubbed_req: &Value,
    response_payload: Value,
    elapsed_ms: u64,
) {
    let mut guard = state.lock().expect("ProxyState mutex poisoned");
    let prompt_text = extract_prompt_text(scrubbed_req);
    let structural_tools = extract_structural_tools(scrubbed_req);
    let node_id = guard.cassette.record_node(
        parent_id.clone(),
        kind,
        &prompt_text,
        &structural_tools,
        scrubbed_req.clone(),
        response_payload,
        elapsed_ms,
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
            .filter_map(|m| m.get("content").and_then(|c| c.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
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
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
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
                Some("Bearer test-key".to_string()),
                req,
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
    async fn record_mode_passes_through_errors_without_recording() {
        let base = fake_upstream().await;
        let state = record_state(base);
        // Missing bearer token: the upstream 401 passes through, no node.
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
        let (status, _, body) = response_json(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/chat/completions",
                None,
                req,
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
                Some("Bearer test-key".to_string()),
                json!({}),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(state.lock().unwrap().cassette.nodes.is_empty());
    }

    #[tokio::test]
    async fn record_mode_records_sse_chunks_and_replays_as_event_stream() {
        let base = fake_upstream().await;
        let state = record_state(base);
        let req = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "stream": true});
        let (status, headers, text) = response_text(
            record_interaction(
                state.clone(),
                InteractionKind::LlmCompletion,
                None,
                "/v1/stream",
                Some("Bearer test-key".to_string()),
                req.clone(),
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
        // The stored node carries the SSE marker, not the raw stream.
        {
            let guard = state.lock().unwrap();
            assert_eq!(guard.cassette.nodes.len(), 1);
            let chunks =
                sse_chunks_from_payload(&guard.cassette.nodes[0].response_payload).unwrap();
            assert_eq!(
                chunks,
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
}
