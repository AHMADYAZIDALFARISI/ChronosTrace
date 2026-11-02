use chronostrace_chaos::{ChaosEngine, ChaosOutcome};
use chronostrace_core::diff::strip_jsonrpc_id;
use chronostrace_core::{Cassette, InteractionKind, MatchResult, TraceError};
use chronostrace_scrub::SecretScrubber;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpWrapMode {
    Record,
    Replay,
    PassthroughOnMiss,
}

pub struct McpStdioConfig {
    pub mode: McpWrapMode,
    pub cassette_path: PathBuf,
    pub similarity_threshold: f64,
    pub simulate_recorded_latency: bool,
    pub chaos: Option<ChaosEngine>,
    pub child_command: Vec<String>,
}

struct InFlightRequest {
    scrubbed_request_no_id: Value,
    prompt_text: String,
    structural_tools: Value,
    started_at: Instant,
    seq: u64,
}

/// A recorded MCP interaction waiting for its turn in request order.
struct PendingRecord {
    prompt_text: String,
    structural_tools: Value,
    scrubbed_request_no_id: Value,
    scrubbed_response_no_id: Value,
    elapsed_ms: u64,
}

/// Chains recorded MCP nodes in the order the host issued requests, not
/// the order child responses arrived. A response that arrives early waits
/// in `pending` until every earlier request has been recorded; without
/// this, concurrent requests to an async child server would link parents
/// by response arrival order instead of request causality.
#[derive(Default)]
struct OrderedRecorder {
    next_seq: u64,
    next_to_record: u64,
    pending: HashMap<u64, PendingRecord>,
}

impl OrderedRecorder {
    fn assign_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    fn push(
        &mut self,
        seq: u64,
        record: PendingRecord,
        cassette: &mut Cassette,
        current_parent_id: &mut Option<String>,
    ) {
        self.pending.insert(seq, record);
        self.drain(cassette, current_parent_id);
    }

    fn drain(&mut self, cassette: &mut Cassette, current_parent_id: &mut Option<String>) {
        while let Some(record) = self.pending.remove(&self.next_to_record) {
            let new_node_id = cassette.record_node(
                current_parent_id.clone(),
                InteractionKind::McpToolCall,
                &record.prompt_text,
                &record.structural_tools,
                record.scrubbed_request_no_id,
                record.scrubbed_response_no_id,
                record.elapsed_ms,
            );
            *current_parent_id = Some(new_node_id);
            self.next_to_record += 1;
        }
    }

    fn flush(&mut self, cassette: &mut Cassette, current_parent_id: &mut Option<String>) {
        let mut seqs: Vec<u64> = self.pending.keys().copied().collect();
        seqs.sort_unstable();
        for seq in seqs {
            if let Some(record) = self.pending.remove(&seq) {
                let new_node_id = cassette.record_node(
                    current_parent_id.clone(),
                    InteractionKind::McpToolCall,
                    &record.prompt_text,
                    &record.structural_tools,
                    record.scrubbed_request_no_id,
                    record.scrubbed_response_no_id,
                    record.elapsed_ms,
                );
                *current_parent_id = Some(new_node_id);
            }
        }
        self.next_to_record = self.next_seq;
    }
}

pub async fn run_mcp_stdio_wrapper(
    mut config: McpStdioConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    match config.mode {
        McpWrapMode::Replay => run_offline_replay(&mut config).await,
        McpWrapMode::Record | McpWrapMode::PassthroughOnMiss => {
            run_bidirectional_child_proxy(&mut config).await
        }
    }
}

async fn run_offline_replay(config: &mut McpStdioConfig) -> Result<(), Box<dyn std::error::Error>> {
    let cassette = Cassette::load_from_file(&config.cassette_path)?;
    let scrubber = SecretScrubber::new();
    let mut current_parent_id: Option<String> = None;

    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();

    while let Some(line) = reader.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let raw_msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "Ignoring malformed JSON-RPC line on stdin");
                continue;
            }
        };

        let Some(req_id) = raw_msg.get("id").cloned() else {
            continue;
        };

        let scrubbed_no_id = scrubber.scrub_json(&strip_jsonrpc_id(&raw_msg));
        let (prompt_text, structural_tools) = extract_mcp_matching_features(&scrubbed_no_id);

        match cassette.match_request(
            &current_parent_id,
            &InteractionKind::McpToolCall,
            &prompt_text,
            &structural_tools,
            &scrubbed_no_id,
        ) {
            Ok(match_res) => {
                let node = match match_res {
                    MatchResult::Exact(n) => n,
                    MatchResult::SemanticFuzzy { node, .. } => node,
                };
                current_parent_id = Some(node.node_id.clone());

                if config.simulate_recorded_latency && node.recorded_latency_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(node.recorded_latency_ms)).await;
                }

                let response_frame =
                    apply_chaos_or_rehydrate_id(&mut config.chaos, &req_id, &node.response_payload)
                        .await;

                write_jsonrpc_line(&mut stdout, &response_frame).await?;
            }
            Err(err) => {
                let err_frame = json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {
                        "code": -32004,
                        "message": format!("ChronosTrace Replay Miss: {err}"),
                        "data": { "parent_node_id": current_parent_id }
                    }
                });
                write_jsonrpc_line(&mut stdout, &err_frame).await?;
            }
        }
    }

    Ok(())
}

async fn run_bidirectional_child_proxy(
    config: &mut McpStdioConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if config.child_command.is_empty() {
        return Err(
            "Record/Passthrough mode requires a child MCP server command after `--`".into(),
        );
    }

    let mut cassette = if config.cassette_path.exists() {
        Cassette::load_from_file(&config.cassette_path)?
    } else {
        let stem = config
            .cassette_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("mcp_session");
        Cassette::new(stem, config.similarity_threshold)
    };

    let exe = &config.child_command[0];
    let args = &config.child_command[1..];

    let mut child = Command::new(exe)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let mut child_stdin = child.stdin.take().expect("Failed to open child stdin");
    let child_stdout = child.stdout.take().expect("Failed to open child stdout");

    let mut host_stdin_lines = BufReader::new(tokio::io::stdin()).lines();
    let mut child_stdout_lines = BufReader::new(child_stdout).lines();
    let mut host_stdout = tokio::io::stdout();

    let scrubber = SecretScrubber::new();
    let mut current_parent_id: Option<String> = None;
    let mut in_flight: HashMap<String, InFlightRequest> = HashMap::new();
    let mut recorder = OrderedRecorder::default();

    loop {
        tokio::select! {
            host_line = host_stdin_lines.next_line() => {
                match host_line? {
                    Some(line) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if let Ok(raw_req) = serde_json::from_str::<Value>(trimmed) {
                            if let Some(req_id) = raw_req.get("id").cloned() {
                                let scrubbed_no_id = scrubber.scrub_json(&strip_jsonrpc_id(&raw_req));
                                let (prompt_text, structural_tools) =
                                    extract_mcp_matching_features(&scrubbed_no_id);

                                if config.mode == McpWrapMode::PassthroughOnMiss {
                                    if let Ok(matched) = cassette.match_request(
                                        &current_parent_id,
                                        &InteractionKind::McpToolCall,
                                        &prompt_text,
                                        &structural_tools,
                                        &scrubbed_no_id,
                                    ) {
                                        let node = match matched {
                                            MatchResult::Exact(n) => n,
                                            MatchResult::SemanticFuzzy { node, .. } => node,
                                        };
                                        current_parent_id = Some(node.node_id.clone());
                                        let response_frame = apply_chaos_or_rehydrate_id(
                                            &mut config.chaos,
                                            &req_id,
                                            &node.response_payload,
                                        ).await;
                                        write_jsonrpc_line(&mut host_stdout, &response_frame).await?;
                                        continue;
                                    }
                                }

                                let id_key = serde_json::to_string(&req_id)?;
                                in_flight.insert(
                                    id_key,
                                    InFlightRequest {
                                        scrubbed_request_no_id: scrubbed_no_id,
                                        prompt_text,
                                        structural_tools,
                                        started_at: Instant::now(),
                                        seq: recorder.assign_seq(),
                                    },
                                );
                            }
                        }

                        child_stdin.write_all(line.as_bytes()).await?;
                        child_stdin.write_all(b"\n").await?;
                        child_stdin.flush().await?;
                    }
                    None => break,
                }
            }
            child_line = child_stdout_lines.next_line() => {
                match child_line? {
                    Some(line) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            if let Ok(raw_resp) = serde_json::from_str::<Value>(trimmed) {
                                if let Some(resp_id) = raw_resp.get("id") {
                                    let id_key = serde_json::to_string(resp_id)?;
                                    if let Some(tracked) = in_flight.remove(&id_key) {
                                        let elapsed_ms = tracked.started_at.elapsed().as_millis() as u64;
                                        let scrubbed_resp_no_id =
                                            scrubber.scrub_json(&strip_jsonrpc_id(&raw_resp));

                                        recorder.push(
                                            tracked.seq,
                                            PendingRecord {
                                                prompt_text: tracked.prompt_text,
                                                structural_tools: tracked.structural_tools,
                                                scrubbed_request_no_id: tracked.scrubbed_request_no_id,
                                                scrubbed_response_no_id: scrubbed_resp_no_id,
                                                elapsed_ms,
                                            },
                                            &mut cassette,
                                            &mut current_parent_id,
                                        );
                                    }
                                }
                            }
                        }
                        host_stdout.write_all(line.as_bytes()).await?;
                        host_stdout.write_all(b"\n").await?;
                        host_stdout.flush().await?;
                    }
                    None => break,
                }
            }
        }
    }

    // Record any buffered responses in request order, even if an earlier
    // response never arrived.
    recorder.flush(&mut cassette, &mut current_parent_id);

    cassette.mark_scrubbed(scrubber.policy_version(), scrubber.total_redactions());
    persist_cassette(&cassette, &config.cassette_path)?;
    let _ = child.kill().await;
    Ok(())
}

fn persist_cassette(cassette: &Cassette, path: &Path) -> Result<(), TraceError> {
    cassette.save_to_file(path)?;
    Ok(())
}

pub fn extract_mcp_matching_features(scrubbed_no_id: &Value) -> (String, Value) {
    let method = scrubbed_no_id
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or("");
    let params = scrubbed_no_id.get("params").cloned().unwrap_or(Value::Null);

    // Every method, including prompts/get and sampling/createMessage, carries
    // its full params in the structural payload. The MCP matcher accepts a
    // node on a structural-hash hit alone (after the exact-hash tier), so a
    // method-only structural payload would let two calls with different
    // parameters match each other and serve the wrong recorded response.
    // tools/call already behaved this way; this makes the other methods
    // consistent with it.
    let prompt_text = if method == "sampling/createMessage" || method == "prompts/get" {
        params.to_string()
    } else {
        format!("{method}:{params}")
    };
    (
        prompt_text,
        json!({
            "method": method,
            "params": params
        }),
    )
}

async fn apply_chaos_or_rehydrate_id(
    chaos: &mut Option<ChaosEngine>,
    req_id: &Value,
    recorded_response_no_id: &Value,
) -> Value {
    let outcome = chaos
        .as_mut()
        .map(|c| c.apply(recorded_response_no_id))
        .unwrap_or_else(|| ChaosOutcome::PassThrough(recorded_response_no_id.clone()));

    match outcome {
        ChaosOutcome::PassThrough(payload) | ChaosOutcome::MutatedPayload(payload) => {
            inject_jsonrpc_id(payload, req_id)
        }
        ChaosOutcome::DelayThenPassThrough { delay_ms, payload } => {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            inject_jsonrpc_id(payload, req_id)
        }
        ChaosOutcome::InjectedError {
            status_code, body, ..
        } => {
            json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "isError": true,
                    "content": [
                        {
                            "type": "text",
                            "text": format!("[ChronosTrace Chaos Fault HTTP {}]: {}", status_code, body)
                        }
                    ]
                }
            })
        }
    }
}

fn inject_jsonrpc_id(mut payload: Value, req_id: &Value) -> Value {
    if let Value::Object(ref mut map) = payload {
        map.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        map.insert("id".to_string(), req_id.clone());
    }
    payload
}

async fn write_jsonrpc_line(
    stdout: &mut tokio::io::Stdout,
    frame: &Value,
) -> Result<(), std::io::Error> {
    let mut serialized = serde_json::to_vec(frame)?;
    serialized.push(b'\n');
    stdout.write_all(&serialized).await?;
    stdout.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mcp_req(method: &str, params: Value) -> Value {
        json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params })
    }

    #[test]
    fn prompts_get_structural_includes_full_params() {
        let a = extract_mcp_matching_features(&strip_jsonrpc_id(&mcp_req(
            "prompts/get",
            json!({"name": "greeting", "arguments": {"style": "formal"}}),
        )));
        let b = extract_mcp_matching_features(&strip_jsonrpc_id(&mcp_req(
            "prompts/get",
            json!({"name": "farewell", "arguments": {"style": "formal"}}),
        )));
        assert_ne!(a.1, b.1);
        assert_eq!(a.1["params"]["name"], "greeting");
        // Identical params must still produce an identical structural payload.
        let a2 = extract_mcp_matching_features(&strip_jsonrpc_id(&mcp_req(
            "prompts/get",
            json!({"name": "greeting", "arguments": {"style": "formal"}}),
        )));
        assert_eq!(a.1, a2.1);
    }

    #[test]
    fn sampling_create_message_structural_includes_full_params() {
        let a = extract_mcp_matching_features(&strip_jsonrpc_id(&mcp_req(
            "sampling/createMessage",
            json!({"messages": [{"role": "user", "content": "hello"}]}),
        )));
        let b = extract_mcp_matching_features(&strip_jsonrpc_id(&mcp_req(
            "sampling/createMessage",
            json!({"messages": [{"role": "user", "content": "goodbye"}]}),
        )));
        assert_ne!(a.1, b.1);
    }

    #[test]
    fn prompts_get_param_change_is_a_replay_miss_not_a_match() {
        let mut cassette = Cassette::new("prompts_get_regression", 0.80);
        let req_a = strip_jsonrpc_id(&mcp_req(
            "prompts/get",
            json!({"name": "greeting", "arguments": {}}),
        ));
        let (prompt_a, structural_a) = extract_mcp_matching_features(&req_a);
        cassette.record_node(
            None,
            InteractionKind::McpToolCall,
            &prompt_a,
            &structural_a,
            req_a.clone(),
            json!({"result": "hello"}),
            5,
        );

        // Replaying the identical call still matches exactly.
        let (prompt_a2, structural_a2) = extract_mcp_matching_features(&req_a);
        assert!(matches!(
            cassette.match_request(
                &None,
                &InteractionKind::McpToolCall,
                &prompt_a2,
                &structural_a2,
                &req_a
            ),
            Ok(MatchResult::Exact(_))
        ));

        // Replaying with a different prompt name at the same DAG position
        // must miss. Before the fix, the method-only structural payload made
        // this return the recorded "greeting" response as an Exact match.
        let req_b = strip_jsonrpc_id(&mcp_req(
            "prompts/get",
            json!({"name": "farewell", "arguments": {}}),
        ));
        let (prompt_b, structural_b) = extract_mcp_matching_features(&req_b);
        assert!(cassette
            .match_request(
                &None,
                &InteractionKind::McpToolCall,
                &prompt_b,
                &structural_b,
                &req_b
            )
            .is_err());
    }

    fn pending_record(name: &str) -> PendingRecord {
        PendingRecord {
            prompt_text: format!("tools/call:{name}"),
            structural_tools: json!({"method": "tools/call", "params": {"name": name}}),
            scrubbed_request_no_id: json!({"method": "tools/call", "params": {"name": name}}),
            scrubbed_response_no_id: json!({"result": name}),
            elapsed_ms: 1,
        }
    }

    #[test]
    fn ordered_recorder_chains_in_request_order_despite_response_order() {
        let mut cassette = Cassette::new("order_recorder", 0.80);
        let mut parent: Option<String> = None;
        let mut recorder = OrderedRecorder::default();
        let s0 = recorder.assign_seq();
        let s1 = recorder.assign_seq();
        // The second response arrives first: nothing is recorded yet.
        recorder.push(s1, pending_record("second"), &mut cassette, &mut parent);
        assert!(cassette.nodes.is_empty());
        assert!(parent.is_none());
        // The first response arrives: both drain in request order.
        recorder.push(s0, pending_record("first"), &mut cassette, &mut parent);
        assert_eq!(cassette.nodes.len(), 2);
        assert!(cassette.nodes[0].parent_id.is_none());
        assert_eq!(
            cassette.nodes[1].parent_id,
            Some(cassette.nodes[0].node_id.clone())
        );
        assert_eq!(parent, Some(cassette.nodes[1].node_id.clone()));
        assert_eq!(cassette.nodes[0].response_payload["result"], "first");
        assert_eq!(cassette.nodes[1].response_payload["result"], "second");
    }

    #[test]
    fn ordered_recorder_flush_drains_leftovers() {
        let mut cassette = Cassette::new("order_flush", 0.80);
        let mut parent: Option<String> = None;
        let mut recorder = OrderedRecorder::default();
        let _s0 = recorder.assign_seq();
        let s1 = recorder.assign_seq();
        recorder.push(s1, pending_record("second"), &mut cassette, &mut parent);
        assert!(cassette.nodes.is_empty());
        recorder.flush(&mut cassette, &mut parent);
        assert_eq!(cassette.nodes.len(), 1);
        assert_eq!(cassette.nodes[0].response_payload["result"], "second");
    }
}
