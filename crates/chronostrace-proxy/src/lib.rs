pub mod mcp_stdio;

use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use chronostrace_chaos::{ChaosEngine, ChaosOutcome};
use chronostrace_core::{Cassette, InteractionKind, MatchResult};
use chronostrace_scrub::SecretScrubber;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct ProxyState {
    pub cassette: Cassette,
    pub current_parent_id: Option<String>,
    pub scrubber: SecretScrubber,
    pub chaos: Option<ChaosEngine>,
}

pub fn build_router(state: Arc<Mutex<ProxyState>>) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(handle_llm_replay))
        .route("/v1/messages", post(handle_llm_replay))
        .route("/mcp/call", post(handle_mcp_replay))
        .with_state(state)
}

async fn handle_llm_replay(
    State(state): State<Arc<Mutex<ProxyState>>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    process_interaction(state, InteractionKind::LlmCompletion, payload).await
}

async fn handle_mcp_replay(
    State(state): State<Arc<Mutex<ProxyState>>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    process_interaction(state, InteractionKind::McpToolCall, payload).await
}

pub async fn process_interaction(
    state: Arc<Mutex<ProxyState>>,
    kind: InteractionKind,
    raw_request: Value,
) -> (StatusCode, HeaderMap, Json<Value>) {
    let mut headers = HeaderMap::new();
    let (match_outcome, chaos_step) = {
        let mut guard = state.lock().expect("ProxyState mutex poisoned");
        let scrubbed_req = guard.scrubber.scrub_json(&raw_request);

        let prompt_text = extract_prompt_text(&scrubbed_req);
        let structural_tools = extract_structural_tools(&scrubbed_req);

        let matched = guard.cassette.match_request(
            &guard.current_parent_id,
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
                let chaos_res = guard
                    .chaos
                    .as_mut()
                    .map(|c| c.apply(&resp_payload))
                    .unwrap_or(ChaosOutcome::PassThrough(resp_payload));
                // Hanya majukan pointer parent DAG apabila fault tidak menggagalkan step dengan HTTP error
                if !matches!(chaos_res, ChaosOutcome::InjectedError { .. }) {
                    guard.current_parent_id = Some(node_id);
                }
                (Ok(()), Some(chaos_res))
            }
            Err(err) => (Err(err.to_string()), None),
        }
    };

    if let Err(err_msg) = match_outcome {
        return (
            StatusCode::PRECONDITION_FAILED,
            headers,
            Json(json!({
                "error": "CHRONOSTRACE_REPLAY_MISS",
                "message": err_msg
            })),
        );
    }

    match chaos_step.unwrap() {
        ChaosOutcome::PassThrough(body) | ChaosOutcome::MutatedPayload(body) => {
            (StatusCode::OK, headers, Json(body))
        }
        ChaosOutcome::DelayThenPassThrough { delay_ms, payload } => {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            (StatusCode::OK, headers, Json(payload))
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
            scrubber: SecretScrubber::new(),
            chaos,
        }))
    }

    #[tokio::test]
    async fn proxy_returns_200_on_exact_and_fuzzy_match() {
        let state = sample_proxy_state(None);
        let req = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": "You are a helpful financial refund assistant! Verify order ORD-900 for $50."}],
            "tools": [{"name": "verify_order"}]
        });

        let (status, _, Json(body)) =
            process_interaction(state.clone(), InteractionKind::LlmCompletion, req).await;
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

        let (status, _, Json(body)) =
            process_interaction(state, InteractionKind::LlmCompletion, mutated_req).await;
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

        let (status, headers, _) =
            process_interaction(state.clone(), InteractionKind::LlmCompletion, req).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            headers.get(header::RETRY_AFTER).unwrap().to_str().unwrap(),
            "3"
        );
        // Pointer parent tidak boleh maju jika request gagal karena HTTP 429 sehingga retry berikutnya tetap cocok
        assert!(state.lock().unwrap().current_parent_id.is_none());
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
