use crate::simhash::simhash_similarity;
use crate::{
    compute_safety_critical_hash, sha256_canonical_json, Cassette, InteractionKind, TraceNode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffKind {
    Identical,
    PromptDriftReconciled,
    ToolSchemaMutated,
    ToolArgumentMutated,
    ResponseDiverged,
    TopologyDiverged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDelta {
    pub path: String,
    pub baseline_value: Value,
    pub candidate_value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDiff {
    pub step_index: usize,
    pub baseline_node_id: Option<String>,
    pub candidate_node_id: Option<String>,
    pub interaction_kind: InteractionKind,
    pub target_name: String,
    pub kind: DiffKind,
    pub prompt_similarity: f64,
    pub baseline_latency_ms: u64,
    pub candidate_latency_ms: u64,
    pub argument_deltas: Vec<FieldDelta>,
    pub response_deltas: Vec<FieldDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrajectoryDiffReport {
    pub baseline_id: String,
    pub candidate_id: String,
    pub total_baseline_steps: usize,
    pub total_candidate_steps: usize,
    pub identical_count: usize,
    pub reconciled_drift_count: usize,
    pub mutated_args_count: usize,
    pub response_diverged_count: usize,
    pub topology_diverged_count: usize,
    pub baseline_total_latency_ms: u64,
    pub candidate_total_latency_ms: u64,
    pub steps: Vec<StepDiff>,
}

impl TrajectoryDiffReport {
    pub fn is_compatible(&self) -> bool {
        self.mutated_args_count == 0
            && self.response_diverged_count == 0
            && self.topology_diverged_count == 0
    }
}

pub fn diff_cassettes(baseline: &Cassette, candidate: &Cassette) -> TrajectoryDiffReport {
    let threshold = baseline
        .metadata
        .similarity_threshold
        .max(candidate.metadata.similarity_threshold);

    let mut matched_cand_indices = HashSet::new();
    let mut steps = Vec::new();

    let mut identical_count = 0;
    let mut reconciled_drift_count = 0;
    let mut mutated_args_count = 0;
    let mut response_diverged_count = 0;
    let mut topology_diverged_count = 0;

    let baseline_total_latency_ms: u64 = baseline.nodes.iter().map(|n| n.recorded_latency_ms).sum();
    let candidate_total_latency_ms: u64 =
        candidate.nodes.iter().map(|n| n.recorded_latency_ms).sum();

    for (b_idx, base) in baseline.nodes.iter().enumerate() {
        let base_target = extract_target_name(&base.request_payload);

        let aligned_cand_idx = if candidate.nodes.get(b_idx).is_some_and(|c| {
            !matched_cand_indices.contains(&b_idx)
                && c.kind == base.kind
                && extract_target_name(&c.request_payload) == base_target
        }) {
            Some(b_idx)
        } else {
            candidate.nodes.iter().enumerate().find_map(|(c_idx, c)| {
                if !matched_cand_indices.contains(&c_idx)
                    && c.kind == base.kind
                    && extract_target_name(&c.request_payload) == base_target
                {
                    Some(c_idx)
                } else {
                    None
                }
            })
        };

        let fallback_idx = aligned_cand_idx.or_else(|| {
            if b_idx < candidate.nodes.len() && !matched_cand_indices.contains(&b_idx) {
                Some(b_idx)
            } else {
                None
            }
        });

        if let Some(c_idx) = fallback_idx {
            matched_cand_indices.insert(c_idx);
            let cand = &candidate.nodes[c_idx];
            let step = compare_node_pair(steps.len(), base, cand, threshold);
            match step.kind {
                DiffKind::Identical => identical_count += 1,
                DiffKind::PromptDriftReconciled => reconciled_drift_count += 1,
                DiffKind::ToolSchemaMutated | DiffKind::ToolArgumentMutated => {
                    mutated_args_count += 1
                }
                DiffKind::ResponseDiverged => response_diverged_count += 1,
                DiffKind::TopologyDiverged => topology_diverged_count += 1,
            }
            steps.push(step);
        } else {
            topology_diverged_count += 1;
            steps.push(StepDiff {
                step_index: steps.len(),
                baseline_node_id: Some(base.node_id.clone()),
                candidate_node_id: None,
                interaction_kind: base.kind.clone(),
                target_name: base_target,
                kind: DiffKind::TopologyDiverged,
                prompt_similarity: 0.0,
                baseline_latency_ms: base.recorded_latency_ms,
                candidate_latency_ms: 0,
                argument_deltas: vec![],
                response_deltas: vec![],
            });
        }
    }

    for (c_idx, cand) in candidate.nodes.iter().enumerate() {
        if !matched_cand_indices.contains(&c_idx) {
            topology_diverged_count += 1;
            steps.push(StepDiff {
                step_index: steps.len(),
                baseline_node_id: None,
                candidate_node_id: Some(cand.node_id.clone()),
                interaction_kind: cand.kind.clone(),
                target_name: extract_target_name(&cand.request_payload),
                kind: DiffKind::TopologyDiverged,
                prompt_similarity: 0.0,
                baseline_latency_ms: 0,
                candidate_latency_ms: cand.recorded_latency_ms,
                argument_deltas: vec![],
                response_deltas: vec![],
            });
        }
    }

    TrajectoryDiffReport {
        baseline_id: baseline.cassette_id.clone(),
        candidate_id: candidate.cassette_id.clone(),
        total_baseline_steps: baseline.nodes.len(),
        total_candidate_steps: candidate.nodes.len(),
        identical_count,
        reconciled_drift_count,
        mutated_args_count,
        response_diverged_count,
        topology_diverged_count,
        baseline_total_latency_ms,
        candidate_total_latency_ms,
        steps,
    }
}

fn compare_node_pair(
    step_index: usize,
    base: &TraceNode,
    cand: &TraceNode,
    threshold: f64,
) -> StepDiff {
    let base_target = extract_target_name(&base.request_payload);
    let cand_target = extract_target_name(&cand.request_payload);

    let base_sim = u64::from_str_radix(&base.prompt_simhash, 16).unwrap_or(0);
    let cand_sim = u64::from_str_radix(&cand.prompt_simhash, 16).unwrap_or(0);
    let prompt_similarity = simhash_similarity(base_sim, cand_sim);

    let base_resp_hash = sha256_canonical_json(&base.response_payload);
    let cand_resp_hash = sha256_canonical_json(&cand.response_payload);

    let base_safety = compute_safety_critical_hash(&base.request_payload);
    let cand_safety = compute_safety_critical_hash(&cand.request_payload);

    let mut argument_deltas = Vec::new();
    diff_json_values(
        "request",
        &base.request_payload,
        &cand.request_payload,
        &mut argument_deltas,
    );

    let mut response_deltas = Vec::new();
    diff_json_values(
        "response",
        &base.response_payload,
        &cand.response_payload,
        &mut response_deltas,
    );

    let kind = if base.kind != cand.kind || base_target != cand_target {
        DiffKind::TopologyDiverged
    } else if base.exact_request_hash == cand.exact_request_hash && base_resp_hash == cand_resp_hash
    {
        DiffKind::Identical
    } else if base.structural_tool_hash != cand.structural_tool_hash {
        if base.kind == InteractionKind::LlmCompletion {
            DiffKind::ToolSchemaMutated
        } else {
            DiffKind::ToolArgumentMutated
        }
    } else if base_safety != cand_safety {
        DiffKind::ToolArgumentMutated
    } else if base_resp_hash != cand_resp_hash {
        DiffKind::ResponseDiverged
    } else if prompt_similarity >= threshold {
        DiffKind::PromptDriftReconciled
    } else {
        DiffKind::TopologyDiverged
    };

    let display_target = if base_target == cand_target {
        base_target
    } else {
        format!("{base_target} -> {cand_target}")
    };

    StepDiff {
        step_index,
        baseline_node_id: Some(base.node_id.clone()),
        candidate_node_id: Some(cand.node_id.clone()),
        interaction_kind: cand.kind.clone(),
        target_name: display_target,
        kind,
        prompt_similarity,
        baseline_latency_ms: base.recorded_latency_ms,
        candidate_latency_ms: cand.recorded_latency_ms,
        argument_deltas,
        response_deltas,
    }
}

pub fn extract_target_name(payload: &Value) -> String {
    if let Some(method) = payload.get("method").and_then(|v| v.as_str()) {
        if method == "tools/call" {
            if let Some(tool_name) = payload
                .get("params")
                .and_then(|p| p.get("name"))
                .and_then(|n| n.as_str())
            {
                return format!("mcp:tools/call({tool_name})");
            }
        }
        return format!("mcp:{method}");
    }
    if let Some(model) = payload.get("model").and_then(|v| v.as_str()) {
        return format!("llm:{model}");
    }
    "unknown_target".to_string()
}

pub fn diff_json_values(prefix: &str, base: &Value, cand: &Value, out: &mut Vec<FieldDelta>) {
    if base == cand {
        return;
    }
    match (base, cand) {
        (Value::Object(b_map), Value::Object(c_map)) => {
            let keys: BTreeSet<&String> = b_map.keys().chain(c_map.keys()).collect();
            for key in keys {
                if key == "id" && prefix == "request" {
                    continue;
                }
                let next_path = format!("{prefix}.{key}");
                match (b_map.get(key), c_map.get(key)) {
                    (Some(bv), Some(cv)) => diff_json_values(&next_path, bv, cv, out),
                    (Some(bv), None) => out.push(FieldDelta {
                        path: next_path,
                        baseline_value: bv.clone(),
                        candidate_value: Value::Null,
                    }),
                    (None, Some(cv)) => out.push(FieldDelta {
                        path: next_path,
                        baseline_value: Value::Null,
                        candidate_value: cv.clone(),
                    }),
                    (None, None) => unreachable!(),
                }
            }
        }
        (Value::Array(b_arr), Value::Array(c_arr)) => {
            let max_i = b_arr.len().max(c_arr.len());
            for i in 0..max_i {
                let next_path = format!("{prefix}[{i}]");
                match (b_arr.get(i), c_arr.get(i)) {
                    (Some(bv), Some(cv)) => diff_json_values(&next_path, bv, cv, out),
                    (Some(bv), None) => out.push(FieldDelta {
                        path: next_path,
                        baseline_value: bv.clone(),
                        candidate_value: Value::Null,
                    }),
                    (None, Some(cv)) => out.push(FieldDelta {
                        path: next_path,
                        baseline_value: Value::Null,
                        candidate_value: cv.clone(),
                    }),
                    (None, None) => unreachable!(),
                }
            }
        }
        _ => out.push(FieldDelta {
            path: prefix.to_string(),
            baseline_value: base.clone(),
            candidate_value: cand.clone(),
        }),
    }
}

pub fn strip_jsonrpc_id(payload: &Value) -> Value {
    if let Value::Object(map) = payload {
        let mut cloned = map.clone();
        cloned.remove("id");
        Value::Object(cloned)
    } else {
        payload.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detects_reconciled_prompt_drift_and_tool_arg_mutations() {
        let mut baseline = Cassette::new("refund_baseline", 0.80);
        let mut candidate = Cassette::new("refund_candidate", 0.80);

        let p1_base = "You are a helpful financial refund agent. Verify order_id before refunding.";
        // Single-word edge change: the genuinely minor drift this assertion is about.
        // (The previous pair also swapped "before" for "prior to", an insertion that
        // shifts every later 3-word shingle and scored 0.640625 under this scheme.)
        let p1_cand = "We are a helpful financial refund agent. Verify order_id before refunding.";
        let tool_schema = json!([{"name": "verify_order"}]);
        let llm_resp =
            json!({"tool_calls": [{"name": "verify_order", "arguments": {"order_id": "ORD-900"}}]});

        let b_n1 = baseline.record_node(
            None,
            InteractionKind::LlmCompletion,
            p1_base,
            &tool_schema,
            json!({"model": "gpt-4o", "messages": [{"content": p1_base}]}),
            llm_resp.clone(),
            420,
        );
        let c_n1 = candidate.record_node(
            None,
            InteractionKind::LlmCompletion,
            p1_cand,
            &tool_schema,
            json!({"model": "gpt-4o", "messages": [{"content": p1_cand}]}),
            llm_resp,
            390,
        );

        let mcp_req_base = strip_jsonrpc_id(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": "issue_refund", "arguments": { "order_id": "ORD-900", "amount": 50.0 } }
        }));
        let mcp_req_cand = strip_jsonrpc_id(&json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": { "name": "issue_refund", "arguments": { "order_id": "ORD-900", "amount": 500.0 } }
        }));
        let mcp_resp = json!({"result": {"status": "ok"}});
        // Extract params first: record_node takes &params and the owned request
        // in the same call, so borrowing params out of the moved value is E0505.
        let mcp_params_base = mcp_req_base["params"].clone();
        let mcp_params_cand = mcp_req_cand["params"].clone();

        baseline.record_node(
            Some(b_n1),
            InteractionKind::McpToolCall,
            "tools/call",
            &mcp_params_base,
            mcp_req_base,
            mcp_resp.clone(),
            18,
        );
        candidate.record_node(
            Some(c_n1),
            InteractionKind::McpToolCall,
            "tools/call",
            &mcp_params_cand,
            mcp_req_cand,
            mcp_resp,
            21,
        );

        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.steps[0].kind, DiffKind::PromptDriftReconciled);
        assert_eq!(report.steps[1].kind, DiffKind::ToolArgumentMutated);
        assert!(!report.is_compatible());
    }
}
