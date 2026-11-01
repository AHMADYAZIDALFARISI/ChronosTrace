use crate::simhash::{compute_simhash, simhash_similarity};
use crate::{sha256_canonical_json, Cassette, InteractionKind, TraceNode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffKind {
    Identical,
    PromptDriftReconciled,
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
    let max_len = baseline.nodes.len().max(candidate.nodes.len());
    let mut steps = Vec::with_capacity(max_len);

     let mut identical_count = 0;
     let mut reconciled_drift_count = 0;
     let mut mutated_args_count = 0;
     let mut response_diverged_count = 0;
     let mut topology_diverged_count = 0;

     let baseline_total_latency_ms: u64 = baseline.nodes.iter().map(|n| n.recorded_latency_ms).sum();
     let candidate_total_latency_ms: u64 = candidate.nodes.iter().map(|n| n.recorded_latency_ms).sum();

     for idx in 0..max_len {
         let base_opt = baseline.nodes.get(idx);
         let cand_opt = candidate.nodes.get(idx);

          match (base_opt, cand_opt) {
              (Some(base), Some(cand)) => {
                  let step = compare_node_pair(idx, base, cand);
                  match step.kind {
                      DiffKind::Identical => identical_count += 1,
                      DiffKind::PromptDriftReconciled => reconciled_drift_count += 1,
                      DiffKind::ToolArgumentMutated => mutated_args_count += 1,
                      DiffKind::ResponseDiverged => response_diverged_count += 1,
                      DiffKind::TopologyDiverged => topology_diverged_count += 1,
                  }
                  steps.push(step);
              }
              (Some(base), None) => {
                  topology_diverged_count += 1;
                  steps.push(StepDiff {
                      step_index: idx,
                      baseline_node_id: Some(base.node_id.clone()),
                      candidate_node_id: None,





                       interaction_kind: base.kind.clone(),
                       target_name: extract_target_name(&base.request_payload),
                       kind: DiffKind::TopologyDiverged,
                       prompt_similarity: 0.0,
                       baseline_latency_ms: base.recorded_latency_ms,
                       candidate_latency_ms: 0,
                       argument_deltas: vec![],
                       response_deltas: vec![],
                   });
               }
               (None, Some(cand)) => {
                   topology_diverged_count += 1;
                   steps.push(StepDiff {
                       step_index: idx,
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
               (None, None) => unreachable!(),
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

fn compare_node_pair(step_index: usize, base: &TraceNode, cand: &TraceNode) -> StepDiff {
    let base_target = extract_target_name(&base.request_payload);
    let cand_target = extract_target_name(&cand.request_payload);

     let base_sim = u64::from_str_radix(&base.prompt_simhash, 16).unwrap_or(0);
     let cand_sim = u64::from_str_radix(&cand.prompt_simhash, 16).unwrap_or(0);
     let prompt_similarity = simhash_similarity(base_sim, cand_sim);

     let base_resp_hash = sha256_canonical_json(&base.response_payload);
     let cand_resp_hash = sha256_canonical_json(&cand.response_payload);

     let mut argument_deltas = Vec::new();
     diff_json_values("request", &base.request_payload, &cand.request_payload, &mut argument_deltas);

     let mut response_deltas = Vec::new();
     diff_json_values("response", &base.response_payload, &cand.response_payload, &mut response_deltas);

     let kind = if base.kind != cand.kind || base_target != cand_target {
         DiffKind::TopologyDiverged
     } else if base.exact_request_hash == cand.exact_request_hash && base_resp_hash == cand_resp_hash {
         DiffKind::Identical
     } else if base.structural_tool_hash == cand.structural_tool_hash && base_resp_hash == cand_resp_hash {
         DiffKind::PromptDriftReconciled
     } else if base.structural_tool_hash != cand.structural_tool_hash {
         DiffKind::ToolArgumentMutated
     } else {
         DiffKind::ResponseDiverged
     };

     StepDiff {
         step_index,
         baseline_node_id: Some(base.node_id.clone()),
         candidate_node_id: Some(cand.node_id.clone()),





          interaction_kind: cand.kind.clone(),
          target_name: if base_target == cand_target { base_target } else { format!("{base_target} -> {cand_target}")
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
            if let Some(tool_name) = payload.get("params").and_then(|p| p.get("name")).and_then(|n| n.as_str()) {
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
                    (Some(bv), None) => out.push(FieldDelta { path: next_path, baseline_value: bv.clone(), candidate
                    (None, Some(cv)) => out.push(FieldDelta { path: next_path, baseline_value: Value::Null, candidat
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
                    (Some(bv), None) => out.push(FieldDelta { path: next_path, baseline_value: bv.clone(), candidate
                    (None, Some(cv)) => out.push(FieldDelta { path: next_path, baseline_value: Value::Null, candidat
                    (None, None) => unreachable!(),
                }
            }
        }
        _ => out.push(FieldDelta { path: prefix.to_string(), baseline_value: base.clone(), candidate_value: cand.clo
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
