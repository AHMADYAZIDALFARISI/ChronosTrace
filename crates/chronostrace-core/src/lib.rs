pub mod diff;
pub mod simhash;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use simhash::{compute_simhash, simhash_similarity};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const SAFETY_CRITICAL_KEYS: &[&str] = &[
    "amount",
    "price",
    "total",
    "currency",
    "quantity",
    "order_id",
    "user_id",
    "account_id",
    "customer_id",
    "recipient",
    "role",
    "permission",
    "permissions",
    "scope",
    "url",
    "uri",
    "path",
    "file_path",
    "sql",
    "query",
    "command",
    "cmd",
    "shell",
];

#[derive(Error, Debug)]
pub enum TraceError {
    #[error("I/O error accessing cassette: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML serialization error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid DAG structure in cassette: {0}")]
    InvalidDag(String),
    #[error("Replay miss: no compatible DAG node found for parent={parent_id:?}, kind={kind:?} (best SimHash similarity={best_score:.3}, required>={threshold:.3})")]
    ReplayMiss {
        parent_id: Option<String>,
        kind: InteractionKind,
        best_score: f64,
        threshold: f64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InteractionKind {
    LlmCompletion,
    McpToolCall,
    HttpToolCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassetteMetadata {
    pub created_at: String,
    pub similarity_threshold: f64,
    pub scrubbed: bool,
    #[serde(default = "default_policy_version")]
    pub scrubber_policy_version: String,
    #[serde(default)]
    pub redaction_count: usize,
}

fn default_policy_version() -> String {
    "unverified".to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceNode {
    pub node_id: String,
    pub parent_id: Option<String>,
    pub kind: InteractionKind,
    pub exact_request_hash: String,
    pub prompt_simhash: String,
    pub structural_tool_hash: String,
    #[serde(default)]
    pub safety_critical_hash: String,
    pub request_payload: Value,
    pub response_payload: Value,
    pub recorded_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cassette {
    pub version: String,
    pub cassette_id: String,
    pub metadata: CassetteMetadata,
    pub nodes: Vec<TraceNode>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MatchResult<'a> {
    Exact(&'a TraceNode),
    SemanticFuzzy {
        node: &'a TraceNode,
        similarity: f64,
    },
}

impl Cassette {
    pub fn new(cassette_id: impl Into<String>, similarity_threshold: f64) -> Self {
        let clamped_threshold = similarity_threshold.clamp(0.5, 1.0);
        Self {
            version: "1.0".to_string(),
            cassette_id: cassette_id.into(),
            metadata: CassetteMetadata {
                created_at: current_utc_iso8601(),
                similarity_threshold: clamped_threshold,
                scrubbed: false,
                scrubber_policy_version: "none".to_string(),
                redaction_count: 0,
            },
            nodes: Vec::new(),
        }
    }

    pub fn mark_scrubbed(&mut self, policy_version: impl Into<String>, redaction_count: usize) {
        self.metadata.scrubbed = true;
        self.metadata.scrubber_policy_version = policy_version.into();
        self.metadata.redaction_count += redaction_count;
    }

    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, TraceError> {
        let raw = fs::read_to_string(path)?;
        let mut cassette: Self = serde_yaml::from_str(&raw)?;
        for node in &mut cassette.nodes {
            if node.safety_critical_hash.is_empty() {
                node.safety_critical_hash = compute_safety_critical_hash(&node.request_payload);
            }
        }
        cassette.validate_dag()?;
        Ok(cassette)
    }

    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), TraceError> {
        self.validate_dag()?;
        let target_path = path.as_ref();
        if let Some(parent) = target_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let yaml = serde_yaml::to_string(self)?;
        let tmp_path = target_path.with_extension(format!("ctrace.{}.tmp", std::process::id()));
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(yaml.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, target_path)?;
        Ok(())
    }

    pub fn validate_dag(&self) -> Result<(), TraceError> {
        let mut seen_ids = HashSet::new();
        for node in &self.nodes {
            if !seen_ids.insert(node.node_id.as_str()) {
                return Err(TraceError::InvalidDag(format!(
                    "Duplicate node_id detected: {}",
                    node.node_id
                )));
            }
        }
        for node in &self.nodes {
            if let Some(ref pid) = node.parent_id {
                if pid == &node.node_id || !seen_ids.contains(pid.as_str()) {
                    return Err(TraceError::InvalidDag(format!(
                        "Node {} references missing or self parent_id {}",
                        node.node_id, pid
                    )));
                }
            }
        }
        // General cycle detection: each node has at most one parent, so any
        // cycle is a repeated node within a single ancestor-chain walk. Nodes
        // whose chain is proven acyclic are marked done, keeping the pass O(n).
        let parent_of: HashMap<&str, Option<&str>> = self
            .nodes
            .iter()
            .map(|n| (n.node_id.as_str(), n.parent_id.as_deref()))
            .collect();
        let mut done: HashSet<&str> = HashSet::new();
        for node in &self.nodes {
            let mut path: HashSet<&str> = HashSet::new();
            let mut current = Some(node.node_id.as_str());
            while let Some(id) = current {
                if done.contains(id) {
                    break;
                }
                if !path.insert(id) {
                    return Err(TraceError::InvalidDag(format!(
                        "Cycle detected in parent chain at node {}",
                        id
                    )));
                }
                current = parent_of.get(id).copied().flatten();
            }
            for id in path {
                done.insert(id);
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_node(
        &mut self,
        parent_id: Option<String>,
        kind: InteractionKind,
        prompt_text: &str,
        structural_tool_payload: &Value,
        request_payload: Value,
        response_payload: Value,
        recorded_latency_ms: u64,
    ) -> String {
        let exact_request_hash = sha256_canonical_json(&request_payload);
        let structural_tool_hash = sha256_canonical_json(structural_tool_payload);
        let safety_critical_hash = compute_safety_critical_hash(&request_payload);
        let simhash_u64 = compute_simhash(prompt_text);
        let prompt_simhash = format!("{simhash_u64:016x}");

        let node_id = format!(
            "node_{:04}_{}",
            self.nodes.len() + 1,
            &exact_request_hash[..8]
        );

        self.nodes.push(TraceNode {
            node_id: node_id.clone(),
            parent_id,
            kind,
            exact_request_hash,
            prompt_simhash,
            structural_tool_hash,
            safety_critical_hash,
            request_payload,
            response_payload,
            recorded_latency_ms,
        });

        node_id
    }

    pub fn match_request<'a>(
        &'a self,
        parent_id: &Option<String>,
        kind: &InteractionKind,
        prompt_text: &str,
        structural_tool_payload: &Value,
        request_payload: &Value,
    ) -> Result<MatchResult<'a>, TraceError> {
        let req_hash = sha256_canonical_json(request_payload);

        if let Some(exact_node) = self.nodes.iter().find(|n| {
            &n.parent_id == parent_id && &n.kind == kind && n.exact_request_hash == req_hash
        }) {
            return Ok(MatchResult::Exact(exact_node));
        }

        if *kind == InteractionKind::McpToolCall {
            let tool_hash = sha256_canonical_json(structural_tool_payload);
            if let Some(mcp_node) = self.nodes.iter().find(|n| {
                &n.parent_id == parent_id && &n.kind == kind && n.structural_tool_hash == tool_hash
            }) {
                return Ok(MatchResult::Exact(mcp_node));
            }
            return Err(TraceError::ReplayMiss {
                parent_id: parent_id.clone(),
                kind: kind.clone(),
                best_score: 0.0,
                threshold: self.metadata.similarity_threshold,
            });
        }

        let tool_hash = sha256_canonical_json(structural_tool_payload);
        let safety_hash = compute_safety_critical_hash(request_payload);
        let incoming_simhash = compute_simhash(prompt_text);
        let mut best_candidate: Option<(&TraceNode, f64)> = None;

        for node in &self.nodes {
            let node_safety: Cow<'_, str> = if node.safety_critical_hash.is_empty() {
                Cow::Owned(compute_safety_critical_hash(&node.request_payload))
            } else {
                Cow::Borrowed(node.safety_critical_hash.as_str())
            };

            if &node.parent_id == parent_id
                && &node.kind == kind
                && node.structural_tool_hash == tool_hash
                && node_safety.as_ref() == safety_hash.as_str()
            {
                let recorded_simhash = u64::from_str_radix(&node.prompt_simhash, 16).unwrap_or(0);
                let score = simhash_similarity(incoming_simhash, recorded_simhash);
                if best_candidate.is_none_or(|(_, best)| score > best) {
                    best_candidate = Some((node, score));
                }
            }
        }

        if let Some((node, score)) = best_candidate {
            if score >= self.metadata.similarity_threshold {
                return Ok(MatchResult::SemanticFuzzy {
                    node,
                    similarity: score,
                });
            }
            return Err(TraceError::ReplayMiss {
                parent_id: parent_id.clone(),
                kind: kind.clone(),
                best_score: score,
                threshold: self.metadata.similarity_threshold,
            });
        }

        Err(TraceError::ReplayMiss {
            parent_id: parent_id.clone(),
            kind: kind.clone(),
            best_score: 0.0,
            threshold: self.metadata.similarity_threshold,
        })
    }
}

pub fn compute_safety_critical_hash(payload: &Value) -> String {
    let mut extracted = BTreeMap::new();
    collect_safety_critical_fields("", payload, &mut extracted);
    let numeric_tokens = extract_numeric_and_id_tokens(payload);
    let envelope = json!({
        "critical_fields": extracted,
        "numeric_and_id_tokens": numeric_tokens,
    });
    sha256_canonical_json(&envelope)
}

fn collect_safety_critical_fields(prefix: &str, value: &Value, out: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let lower = k.to_ascii_lowercase();
                let path = if prefix.is_empty() {
                    lower.clone()
                } else {
                    format!("{prefix}.{lower}")
                };
                if SAFETY_CRITICAL_KEYS.contains(&lower.as_str()) {
                    out.insert(path.clone(), canonicalize_json(v));
                }
                collect_safety_critical_fields(&path, v, out);
            }
        }
        Value::Array(arr) => {
            for (idx, item) in arr.iter().enumerate() {
                collect_safety_critical_fields(&format!("{prefix}[{idx}]"), item, out);
            }
        }
        _ => {}
    }
}

fn extract_numeric_and_id_tokens(value: &Value) -> Vec<String> {
    let mut tokens = Vec::new();
    collect_string_literals(value, &mut tokens);
    tokens
}

fn collect_string_literals(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            for word in s.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '.') {
                let trimmed = word.trim_matches('.');
                if !trimmed.is_empty() && trimmed.chars().any(|c| c.is_ascii_digit()) {
                    out.push(trimmed.to_ascii_uppercase());
                }
            }
        }
        Value::Number(n) => out.push(n.to_string()),
        Value::Array(arr) => {
            for v in arr {
                collect_string_literals(v, out);
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                if k != "id" {
                    collect_string_literals(v, out);
                }
            }
        }
        _ => {}
    }
}

pub fn sha256_canonical_json(value: &Value) -> String {
    let canonical = canonicalize_json(value);
    let serialized = serde_json::to_vec(&canonical).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    hex::encode(hasher.finalize())
}

pub fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.clone(), canonicalize_json(v));
            }
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(canonicalize_json).collect()),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f >= (i64::MIN as f64) && f <= (i64::MAX as f64) {
                    return Value::Number((f as i64).into());
                }
            }
            Value::Number(n.clone())
        }
        other => other.clone(),
    }
}

pub fn current_utc_iso8601() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let minute = (rem % 3600) / 60;
    let second = rem % 60;

    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact_and_fuzzy_prompt_but_blocks_amount_mutation() {
        let mut cassette = Cassette::new("test_cassette", 0.80);
        let tools = json!([{"name": "issue_refund"}]);
        let p_base = "You are a helpful refund agent for AcmeShop. Verify order ORD-900 for $50.";
        let req_base = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": p_base}],
            "tools": tools
        });
        let resp = json!({"choices": [{"message": {"content": "OK"}}]});

        cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            p_base,
            &tools,
            req_base.clone(),
            resp,
            100,
        );

        let res_exact = cassette
            .match_request(
                &None,
                &InteractionKind::LlmCompletion,
                p_base,
                &tools,
                &req_base,
            )
            .expect("Exact match must succeed");
        assert!(matches!(res_exact, MatchResult::Exact(_)));

        // Single-word edge change with identical safety tokens ($50, ORD-900).
        // (The previous "agent"->"assistant" middle-word pair scored 0.78125 under
        // 3-word shingles, below the 0.80 threshold this test asserts.)
        let p_fuzzy = "We are a helpful refund agent for AcmeShop. Verify order ORD-900 for $50.";
        let req_fuzzy = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": p_fuzzy}],
            "tools": tools
        });
        let res_fuzzy = cassette
            .match_request(
                &None,
                &InteractionKind::LlmCompletion,
                p_fuzzy,
                &tools,
                &req_fuzzy,
            )
            .expect("Fuzzy match with identical safety tokens must succeed");
        assert!(matches!(res_fuzzy, MatchResult::SemanticFuzzy { .. }));

        let p_adversarial =
            "You are a helpful refund agent for AcmeShop. Verify order ORD-900 for $500.";
        let req_adversarial = json!({
            "model": "gpt-4o",
            "messages": [{"role": "system", "content": p_adversarial}],
            "tools": tools
        });
        let res_adv = cassette.match_request(
            &None,
            &InteractionKind::LlmCompletion,
            p_adversarial,
            &tools,
            &req_adversarial,
        );
        assert!(
            res_adv.is_err(),
            "Amount mutation ($50 vs $500) must fail replay!"
        );
    }

    #[test]
    fn canonical_json_normalizes_whole_floats_and_key_order() {
        let a = json!({"b": 50.0, "a": 1});
        let b = json!({"a": 1, "b": 50});
        assert_eq!(sha256_canonical_json(&a), sha256_canonical_json(&b));
    }

    #[test]
    fn validate_dag_rejects_parent_cycles() {
        let mut cassette = Cassette::new("cycle_cassette", 0.80);
        let tools = json!([{"name": "noop"}]);
        let req = json!({"tools": tools});
        let resp = json!({"ok": true});
        let root = cassette.record_node(
            None,
            InteractionKind::McpToolCall,
            "root",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        let child = cassette.record_node(
            Some(root),
            InteractionKind::McpToolCall,
            "child",
            &tools,
            req,
            resp,
            1,
        );
        // Close a 2-cycle: the root's parent becomes its own child.
        cassette.nodes[0].parent_id = Some(child);
        let err = cassette
            .validate_dag()
            .expect_err("2-cycle must be rejected");
        assert!(matches!(err, TraceError::InvalidDag(_)));
    }

    #[test]
    fn validate_dag_accepts_branching_chains() {
        let mut cassette = Cassette::new("dag_cassette", 0.80);
        let tools = json!([{"name": "noop"}]);
        let req = json!({"tools": tools});
        let resp = json!({"ok": true});
        let root = cassette.record_node(
            None,
            InteractionKind::McpToolCall,
            "root",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        cassette.record_node(
            Some(root.clone()),
            InteractionKind::McpToolCall,
            "left",
            &tools,
            req.clone(),
            resp.clone(),
            1,
        );
        cassette.record_node(
            Some(root),
            InteractionKind::McpToolCall,
            "right",
            &tools,
            req,
            resp,
            1,
        );
        assert!(cassette.validate_dag().is_ok());
    }
}
