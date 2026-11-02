use crate::simhash::simhash_similarity;
use crate::{
    compute_safety_critical_hash, sha256_canonical_json, Cassette, InteractionKind, TraceNode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};

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
    /// Occurrence index of this step among steps with the same
    /// kind, target, and parent in its own cassette (CT-DIFF-01).
    /// 0 for the first occurrence. Defaults to 0 when reading
    /// reports produced before this field existed.
    #[serde(default)]
    pub occurrence_index: usize,
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

    /// Worst severity across steps (CT-DIFF-02).
    pub fn max_severity(&self) -> DiffSeverity {
        self.steps
            .iter()
            .map(|s| s.kind.severity())
            .max()
            .unwrap_or(DiffSeverity::Informational)
    }

    /// CI exit code for the report (CT-DIFF-02): 0 clean,
    /// 2 behavioral divergence, 3 critical divergence.
    pub fn ci_exit_code(&self) -> i32 {
        self.max_severity().ci_exit_code()
    }
}

/// Semantic severity of one diff step (CT-DIFF-02). Ordered so
/// `max()` yields the worst step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DiffSeverity {
    Informational,
    Behavioral,
    Critical,
}

impl DiffSeverity {
    pub fn ci_exit_code(&self) -> i32 {
        match self {
            DiffSeverity::Informational => 0,
            DiffSeverity::Behavioral => 2,
            DiffSeverity::Critical => 3,
        }
    }
}

impl DiffKind {
    pub fn severity(&self) -> DiffSeverity {
        match self {
            DiffKind::Identical | DiffKind::PromptDriftReconciled => DiffSeverity::Informational,
            DiffKind::ResponseDiverged => DiffSeverity::Behavioral,
            DiffKind::ToolSchemaMutated
            | DiffKind::ToolArgumentMutated
            | DiffKind::TopologyDiverged => DiffSeverity::Critical,
        }
    }
}

/// Stable identity for one aligned step (CT-DIFF-01). The
/// occurrence index counts previous steps with the same kind,
/// target, and parent inside the same cassette, so repeated calls
/// to the same target are distinguishable instead of collapsing
/// onto the first match.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeAlignmentKey {
    pub parent_id: Option<String>,
    pub kind: String,
    pub target_name: String,
    pub occurrence_index: usize,
}

pub fn diff_cassettes(baseline: &Cassette, candidate: &Cassette) -> TrajectoryDiffReport {
    let threshold = baseline
        .metadata
        .similarity_threshold
        .max(candidate.metadata.similarity_threshold);

    let mut matched_cand_indices = HashSet::new();
    // Baseline node id -> its position, so parent lookups are O(1).
    let baseline_pos: HashMap<&str, usize> = baseline
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.node_id.as_str(), i))
        .collect();
    // Baseline position -> matched candidate position.
    let mut base_to_cand: HashMap<usize, usize> = HashMap::new();
    let mut steps = Vec::new();

    let mut identical_count = 0;
    let mut reconciled_drift_count = 0;
    let mut mutated_args_count = 0;
    let mut response_diverged_count = 0;
    let mut topology_diverged_count = 0;

    let baseline_total_latency_ms: u64 = baseline.nodes.iter().map(|n| n.recorded_latency_ms).sum();
    let candidate_total_latency_ms: u64 =
        candidate.nodes.iter().map(|n| n.recorded_latency_ms).sum();

    // Occurrence counts per (kind, target, parent) within each
    // cassette (CT-DIFF-01): repeated calls to the same target
    // get distinct, stable indices instead of sharing one.
    let mut baseline_occurrences: HashMap<(String, String, Option<String>), usize> = HashMap::new();
    let mut candidate_occurrences: HashMap<(String, String, Option<String>), usize> =
        HashMap::new();
    for (b_idx, base) in baseline.nodes.iter().enumerate() {
        let base_target = extract_target_name(&base.request_payload);
        let base_occ_key = (
            format!("{:?}", base.kind),
            base_target.clone(),
            base.parent_id.clone(),
        );
        let base_occurrence = *baseline_occurrences.get(&base_occ_key).unwrap_or(&0);
        baseline_occurrences.insert(base_occ_key, base_occurrence + 1);

        // Graph-aware alignment: a candidate matches only when it has the
        // same kind and target AND its parent is the matched counterpart of
        // the baseline node's parent (or both are roots). Array position is
        // never consulted. Without the parent check, two nodes with the same
        // kind and target at different DAG positions could be paired and
        // reported Identical even though the topology changed.
        let aligned_cand_idx = candidate.nodes.iter().enumerate().find_map(|(c_idx, c)| {
            if matched_cand_indices.contains(&c_idx)
                || c.kind != base.kind
                || extract_target_name(&c.request_payload) != base_target
            {
                return None;
            }
            let parent_ok = match (&base.parent_id, &c.parent_id) {
                (None, None) => true,
                (Some(bp), Some(cp)) => baseline_pos
                    .get(bp.as_str())
                    .and_then(|p_idx| base_to_cand.get(p_idx))
                    .and_then(|c_mapped| candidate.nodes.get(*c_mapped))
                    .is_some_and(|n| n.node_id == *cp),
                _ => false,
            };
            if parent_ok {
                Some(c_idx)
            } else {
                None
            }
        });

        if let Some(c_idx) = aligned_cand_idx {
            matched_cand_indices.insert(c_idx);
            base_to_cand.insert(b_idx, c_idx);
            let cand = &candidate.nodes[c_idx];
            let step = compare_node_pair(steps.len(), base_occurrence, base, cand, threshold);
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
                occurrence_index: base_occurrence,
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
            let cand_target = extract_target_name(&cand.request_payload);
            let cand_occ_key = (
                format!("{:?}", cand.kind),
                cand_target.clone(),
                cand.parent_id.clone(),
            );
            let cand_occurrence = *candidate_occurrences.get(&cand_occ_key).unwrap_or(&0);
            candidate_occurrences.insert(cand_occ_key, cand_occurrence + 1);
            topology_diverged_count += 1;
            steps.push(StepDiff {
                step_index: steps.len(),
                occurrence_index: cand_occurrence,
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
    occurrence_index: usize,
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
        occurrence_index,
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

    fn mcp_call(name: &str, args: Value) -> (Value, Value) {
        let params = json!({ "name": name, "arguments": args });
        let req = json!({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": params
        });
        (params, strip_jsonrpc_id(&req))
    }

    #[test]
    fn same_target_different_parents_is_topology_diverged_not_identical() {
        // Baseline chain: root -> verify -> charge -> verify.
        // Candidate: root -> verify -> charge, but the second verify is a
        // sibling of the first (parent = root), not a child of charge.
        // Same kind and target at the same array index must not pair them.
        let mut baseline = Cassette::new("chain_baseline", 0.80);
        let mut candidate = Cassette::new("chain_candidate", 0.80);

        let llm_req = json!({"model": "gpt-4o", "messages": [{"content": "root"}]});
        let llm_resp = json!({"ok": true});
        let schema = json!([{"name": "verify_order"}]);
        let (v_params, v_req) = mcp_call("verify_order", json!({ "order_id": "ORD-900" }));
        let (c_params, c_req) =
            mcp_call("charge", json!({ "order_id": "ORD-900", "amount": 50.0 }));

        let b_r = baseline.record_node(
            None,
            InteractionKind::LlmCompletion,
            "root",
            &schema,
            llm_req.clone(),
            llm_resp.clone(),
            1,
        );
        let b_v1 = baseline.record_node(
            Some(b_r.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &v_params,
            v_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        let b_ch = baseline.record_node(
            Some(b_v1.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &c_params,
            c_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        baseline.record_node(
            Some(b_ch.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &v_params,
            v_req.clone(),
            json!({"result": "ok"}),
            1,
        );

        let c_r = candidate.record_node(
            None,
            InteractionKind::LlmCompletion,
            "root",
            &schema,
            llm_req.clone(),
            llm_resp.clone(),
            1,
        );
        let c_v1 = candidate.record_node(
            Some(c_r.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &v_params,
            v_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        let c_ch = candidate.record_node(
            Some(c_v1.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &c_params,
            c_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        // Second verify as a sibling of the first, not a child of charge.
        candidate.record_node(
            Some(c_r.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &v_params,
            v_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        let _ = c_ch;

        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.steps.len(), 5);
        assert_eq!(report.steps[0].kind, DiffKind::Identical);
        assert_eq!(report.steps[1].kind, DiffKind::Identical);
        assert_eq!(report.steps[2].kind, DiffKind::Identical);
        // The second verify must not pair with its same-index counterpart:
        // its parent differs (charge vs root).
        assert_eq!(report.steps[3].kind, DiffKind::TopologyDiverged);
        assert!(report.steps[3].candidate_node_id.is_none());
        assert_eq!(report.steps[4].kind, DiffKind::TopologyDiverged);
        assert!(report.steps[4].baseline_node_id.is_none());
        assert_eq!(report.topology_diverged_count, 2);
        assert!(!report.is_compatible());
    }

    #[test]
    fn reordered_steps_are_topology_diverged_not_mispaired() {
        let mut baseline = Cassette::new("order_baseline", 0.80);
        let mut candidate = Cassette::new("order_candidate", 0.80);

        let llm_req = json!({"model": "gpt-4o", "messages": [{"content": "root"}]});
        let schema = json!([{"name": "verify_order"}]);
        let (v_params, v_req) = mcp_call("verify_order", json!({ "order_id": "ORD-900" }));
        let (c_params, c_req) =
            mcp_call("charge", json!({ "order_id": "ORD-900", "amount": 50.0 }));

        let b_r = baseline.record_node(
            None,
            InteractionKind::LlmCompletion,
            "root",
            &schema,
            llm_req.clone(),
            json!({"ok": true}),
            1,
        );
        let b_a = baseline.record_node(
            Some(b_r.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &v_params,
            v_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        baseline.record_node(
            Some(b_a.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &c_params,
            c_req.clone(),
            json!({"result": "ok"}),
            1,
        );

        let c_r = candidate.record_node(
            None,
            InteractionKind::LlmCompletion,
            "root",
            &schema,
            llm_req.clone(),
            json!({"ok": true}),
            1,
        );
        let c_b = candidate.record_node(
            Some(c_r.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &c_params,
            c_req.clone(),
            json!({"result": "ok"}),
            1,
        );
        candidate.record_node(
            Some(c_b.clone()),
            InteractionKind::McpToolCall,
            "tools/call",
            &v_params,
            v_req.clone(),
            json!({"result": "ok"}),
            1,
        );

        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.steps[0].kind, DiffKind::Identical);
        // Both reordered steps are unmatched, not paired with each other.
        assert_eq!(report.steps[1].kind, DiffKind::TopologyDiverged);
        assert!(report.steps[1].candidate_node_id.is_none());
        assert_eq!(report.steps[2].kind, DiffKind::TopologyDiverged);
        assert!(report.steps[2].candidate_node_id.is_none());
        assert_eq!(report.topology_diverged_count, 4);
        assert!(!report.is_compatible());
    }

    // CT-DIFF-01/02: repeated calls to the same target carry
    // distinct occurrence indices, and the report maps its worst
    // step to a CI exit code.
    #[test]
    fn repeated_targets_get_occurrence_indices_and_ci_exit_codes() {
        fn build_pair() -> (Cassette, Cassette) {
            let mut baseline = Cassette::new("occ_base", 0.80);
            let mut candidate = Cassette::new("occ_cand", 0.80);
            let tools = json!([{"name": "charge"}]);
            let req = json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {"name": "charge", "arguments": {"amount": 10}}
            });
            let resp = json!({"charged": true});
            for _ in 0..2 {
                baseline.record_node(
                    None,
                    InteractionKind::McpToolCall,
                    "charge",
                    &tools,
                    req.clone(),
                    resp.clone(),
                    5,
                );
                candidate.record_node(
                    None,
                    InteractionKind::McpToolCall,
                    "charge",
                    &tools,
                    req.clone(),
                    resp.clone(),
                    5,
                );
            }
            (baseline, candidate)
        }

        let (baseline, candidate) = build_pair();
        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.steps.len(), 2);
        assert_eq!(report.steps[0].occurrence_index, 0);
        assert_eq!(report.steps[1].occurrence_index, 1);
        assert_eq!(report.steps[0].kind, DiffKind::Identical);
        assert_eq!(report.steps[1].kind, DiffKind::Identical);
        assert_eq!(report.max_severity(), DiffSeverity::Informational);
        assert_eq!(report.ci_exit_code(), 0);

        // Same args, different response on the second call:
        // behavioral divergence, exit code 2.
        let (baseline, mut candidate) = build_pair();
        candidate.nodes[1].response_payload = json!({"charged": false});
        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.steps[1].kind, DiffKind::ResponseDiverged);
        assert_eq!(report.max_severity(), DiffSeverity::Behavioral);
        assert_eq!(report.ci_exit_code(), 2);

        // Missing second call: topology divergence, exit code 3.
        let (baseline, mut candidate) = build_pair();
        candidate.nodes.pop();
        let report = diff_cassettes(&baseline, &candidate);
        assert!(report
            .steps
            .iter()
            .any(|s| s.kind == DiffKind::TopologyDiverged));
        assert_eq!(report.max_severity(), DiffSeverity::Critical);
        assert_eq!(report.ci_exit_code(), 3);
    }

    // SECOND AUDIT P2: tool-schema mutation, added nodes, and
    // branching children each classify deterministically.
    #[test]
    fn diff_tool_schema_mutation_is_critical() {
        let mut baseline = Cassette::new("schema_base", 0.80);
        let mut candidate = Cassette::new("schema_cand", 0.80);
        let tools_v1 = json!([{"name": "verify_order"}]);
        let tools_v2 = json!([{"name": "verify_order", "description": "v2"}]);
        let resp = json!({"ok": true});
        baseline.record_node(
            None,
            InteractionKind::LlmCompletion,
            "verify",
            &tools_v1,
            json!({"model": "gpt-4o", "messages": [{"content": "verify"}], "tools": tools_v1}),
            resp.clone(),
            10,
        );
        candidate.record_node(
            None,
            InteractionKind::LlmCompletion,
            "verify",
            &tools_v2,
            json!({"model": "gpt-4o", "messages": [{"content": "verify"}], "tools": tools_v2}),
            resp,
            10,
        );
        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.steps[0].kind, DiffKind::ToolSchemaMutated);
        assert_eq!(report.steps[0].kind.severity(), DiffSeverity::Critical);
        assert_eq!(report.mutated_args_count, 1);
        assert_eq!(report.ci_exit_code(), 3);
    }

    #[test]
    fn diff_added_candidate_node_is_topology_diverged() {
        let mut baseline = Cassette::new("add_base", 0.80);
        let mut candidate = Cassette::new("add_cand", 0.80);
        let tools = json!([{"name": "noop"}]);
        let req = json!({"tools": tools});
        let resp = json!({"ok": true});
        baseline.record_node(
            None,
            InteractionKind::McpToolCall,
            "one",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        candidate.record_node(
            None,
            InteractionKind::McpToolCall,
            "one",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        candidate.record_node(
            None,
            InteractionKind::McpToolCall,
            "two",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.total_candidate_steps, 2);
        let added = report
            .steps
            .iter()
            .find(|s| s.baseline_node_id.is_none())
            .expect("added node must appear as its own step");
        assert_eq!(added.kind, DiffKind::TopologyDiverged);
        assert_eq!(report.topology_diverged_count, 1);
        assert!(!report.is_compatible());
    }

    #[test]
    fn diff_branching_children_missing_child_diverges() {
        let mut baseline = Cassette::new("branch_base", 0.80);
        let mut candidate = Cassette::new("branch_cand", 0.80);
        let tools = json!([{"name": "noop"}]);
        let req = json!({"tools": tools});
        let resp = json!({"ok": true});
        let b_root = baseline.record_node(
            None,
            InteractionKind::McpToolCall,
            "root",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        baseline.record_node(
            Some(b_root.clone()),
            InteractionKind::McpToolCall,
            "left",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        baseline.record_node(
            Some(b_root),
            InteractionKind::McpToolCall,
            "right",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        let c_root = candidate.record_node(
            None,
            InteractionKind::McpToolCall,
            "root",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        candidate.record_node(
            Some(c_root),
            InteractionKind::McpToolCall,
            "left",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        let report = diff_cassettes(&baseline, &candidate);
        assert_eq!(report.identical_count, 2);
        assert_eq!(report.topology_diverged_count, 1);
        assert!(report
            .steps
            .iter()
            .any(|s| s.kind == DiffKind::TopologyDiverged
                && s.baseline_node_id.is_some()
                && s.candidate_node_id.is_none()));
    }
}
