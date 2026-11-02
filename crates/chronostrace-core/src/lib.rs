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
use std::sync::atomic::{AtomicU64, Ordering};
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
    "transaction_id",
    "recipient",
    "recipient_id",
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
    /// HTTP status the upstream returned when this node was recorded.
    /// Absent for nodes recorded before status capture (and for MCP
    /// nodes, which have no HTTP status); replay defaults those to 200.
    #[serde(default)]
    pub response_status: Option<u16>,
    /// Safe upstream response headers captured with the node
    /// (content-type, retry-after, x-request-id only — never cookies
    /// or authorization material). Absent means "no headers recorded".
    #[serde(default)]
    pub response_headers: Option<BTreeMap<String, String>>,
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

/// Resource bounds for DAG validation (CT-CORE-04). Validation
/// previously checked shape (duplicates, missing/self parents,
/// cycles) but placed no bound on size or depth, so a hostile or
/// corrupt cassette could force unbounded walks and memory use.
/// Defaults are generous for real trajectories and only reject
/// pathological graphs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DagLimits {
    pub max_nodes: usize,
    pub max_depth: usize,
}

impl Default for DagLimits {
    fn default() -> Self {
        Self {
            max_nodes: 100_000,
            max_depth: 2_048,
        }
    }
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
        // The temporary name must be unique per write, not just per
        // process: two concurrent writes from the same process to the
        // same target previously shared one PID-only temp path, so they
        // overwrote each other's bytes and the last rename won. A
        // process-wide atomic counter closes that collision on every
        // platform, including Windows.
        static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path =
            target_path.with_extension(format!("ctrace.{}.{}.tmp", std::process::id(), unique));
        let write_result = (|| -> Result<(), TraceError> {
            let mut file = File::create(&tmp_path)?;
            file.write_all(yaml.as_bytes())?;
            file.sync_all()?;
            fs::rename(&tmp_path, target_path)?;
            // CT-CORE-05: file sync alone does not durably record the
            // rename itself. Best-effort directory sync on Unix closes
            // that gap; failure here is non-fatal (the rename already
            // happened) and must not turn a successful save into an
            // error. Concurrent writers to the same target remain
            // last-writer-wins at the rename — documented, not fixed,
            // because cross-process file locking would change the
            // single-writer contract the proxy already enforces.
            #[cfg(unix)]
            {
                if let Some(parent) = target_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        if let Ok(dir) = File::open(parent) {
                            let _ = dir.sync_all();
                        }
                    }
                }
            }
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        write_result
    }

    pub fn validate_dag(&self) -> Result<(), TraceError> {
        self.validate_dag_with_limits(&DagLimits::default())
    }

    /// Shape validation plus resource bounds (CT-CORE-04).
    pub fn validate_dag_with_limits(&self, limits: &DagLimits) -> Result<(), TraceError> {
        if self.nodes.len() > limits.max_nodes {
            return Err(TraceError::InvalidDag(format!(
                "Node count {} exceeds limit {}",
                self.nodes.len(),
                limits.max_nodes
            )));
        }
        self.validate_dag_shape()?;
        // Depth pass: safe now that the shape pass proved acyclicity.
        // Depth is memoized per node, so the pass stays O(n).
        let parent_of: HashMap<&str, Option<&str>> = self
            .nodes
            .iter()
            .map(|n| (n.node_id.as_str(), n.parent_id.as_deref()))
            .collect();
        let mut depths: HashMap<&str, usize> = HashMap::new();
        for node in &self.nodes {
            let mut chain: Vec<&str> = Vec::new();
            let mut current = Some(node.node_id.as_str());
            while let Some(id) = current {
                if let Some(&d) = depths.get(id) {
                    let mut depth = d;
                    for id_in_chain in chain.iter().rev() {
                        depth += 1;
                        depths.insert(id_in_chain, depth);
                    }
                    break;
                }
                chain.push(id);
                current = parent_of.get(id).copied().flatten();
            }
            if !depths.contains_key(node.node_id.as_str()) {
                // Chain reached a root without hitting a memoized node:
                // depths grow from the root (depth 1) outward.
                let mut depth = 0;
                for id_in_chain in chain.iter().rev() {
                    depth += 1;
                    depths.insert(id_in_chain, depth);
                }
            }
            if depths[node.node_id.as_str()] > limits.max_depth {
                return Err(TraceError::InvalidDag(format!(
                    "Node {} depth {} exceeds limit {}",
                    node.node_id,
                    depths[node.node_id.as_str()],
                    limits.max_depth
                )));
            }
        }
        Ok(())
    }

    fn validate_dag_shape(&self) -> Result<(), TraceError> {
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
        self.record_node_full(
            parent_id,
            kind,
            prompt_text,
            structural_tool_payload,
            request_payload,
            response_payload,
            recorded_latency_ms,
            None,
            None,
        )
    }

    /// Full node recording, including the HTTP status and safe
    /// response headers for HTTP nodes. MCP nodes and legacy callers
    /// pass `None` for both and replay as 200 with no extra headers.
    #[allow(clippy::too_many_arguments)]
    pub fn record_node_full(
        &mut self,
        parent_id: Option<String>,
        kind: InteractionKind,
        prompt_text: &str,
        structural_tool_payload: &Value,
        request_payload: Value,
        response_payload: Value,
        recorded_latency_ms: u64,
        response_status: Option<u16>,
        response_headers: Option<BTreeMap<String, String>>,
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
            response_status,
            response_headers,
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
            // SECOND AUDIT P0: strict MCP matching. The structural
            // fallback from the previous cycle still authorized
            // replays for argument changes the safety hash does not
            // see (booleans, added arguments, key renames). Now an
            // MCP request matches ONLY when tool identity/schema AND
            // the full arguments match. The structural hash is still
            // required (tool identity + schema), but it is never
            // sufficient on its own: the normalized argument hash
            // must also match. Anything else is a replay miss —
            // never silently converted into a successful replay.
            // Normalization strips ONLY the volatile JSON-RPC
            // envelope id (top-level "id" on a jsonrpc 2.0 message);
            // business identifiers at any depth, including bare
            // nested "id" fields, are part of the arguments.
            let tool_hash = sha256_canonical_json(structural_tool_payload);
            let incoming_args_hash = sha256_canonical_json(&strip_mcp_envelope_id(request_payload));
            if let Some(mcp_node) = self.nodes.iter().find(|n| {
                &n.parent_id == parent_id
                    && &n.kind == kind
                    && n.structural_tool_hash == tool_hash
                    && sha256_canonical_json(&strip_mcp_envelope_id(&n.request_payload))
                        == incoming_args_hash
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
                // CT-BUILD-01: Option::is_none_or stabilized in Rust
                // 1.82. Written out explicitly (retained as-is
                // after the second-audit MSRV raise to 1.88, which
                // was driven by dependency floors, not this code).
                let is_better = match best_candidate {
                    None => true,
                    Some((_, best)) => score > best,
                };
                if is_better {
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

/// Strip ONLY the volatile JSON-RPC envelope id (SECOND AUDIT
/// P0/P2). A top-level `"id"` is removed if and only if the
/// payload is a JSON-RPC 2.0 message. Any other top-level `"id"`,
/// and every nested `"id"` at any depth, is a business identifier
/// and is preserved. This is what separates the envelope (which
/// legitimately changes per request) from identifiers (which must
/// never be silently ignored).
pub fn strip_mcp_envelope_id(payload: &Value) -> Value {
    if let Value::Object(map) = payload {
        let is_rpc = map.get("jsonrpc").and_then(|v| v.as_str()) == Some("2.0");
        if is_rpc {
            let mut cloned = map.clone();
            cloned.remove("id");
            return Value::Object(cloned);
        }
    }
    payload.clone()
}

pub fn compute_safety_critical_hash(payload: &Value) -> String {
    // SECOND AUDIT P2: normalize the envelope id first, then
    // collect everything else — including nested business ids.
    let normalized = strip_mcp_envelope_id(payload);
    let mut extracted = BTreeMap::new();
    collect_safety_critical_fields("", &normalized, &mut extracted);
    let numeric_tokens = extract_numeric_and_id_tokens(&normalized);
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
                // SECOND AUDIT P2: Unicode-aware digit test.
                // `is_ascii_digit` silently dropped identifiers
                // written with non-ASCII digits (e.g. Arabic-Indic
                // digits); `is_numeric` counts Unicode decimal
                // digits as well. Upper-casing is likewise
                // Unicode-aware (ASCII results unchanged).
                if !trimmed.is_empty() && trimmed.chars().any(|c| c.is_numeric()) {
                    out.push(trimmed.to_uppercase());
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
                // SECOND AUDIT P2: a bare "id" key below the
                // envelope is a business identifier. Its value
                // counts verbatim — even with no digits — instead
                // of being dropped by the old `k != "id"` filter,
                // which globally ignored every field named "id".
                if k == "id" {
                    match v {
                        Value::String(s) => out.push(s.clone()),
                        Value::Number(n) => out.push(n.to_string()),
                        other => out.push(serde_json::to_string(other).unwrap_or_default()),
                    }
                } else {
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

    #[test]
    fn concurrent_saves_to_same_target_all_succeed_and_leave_valid_file() {
        let dir = std::env::temp_dir().join(format!(
            "chronostrace-concurrent-save-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("shared.ctrace");

        let mut handles = Vec::new();
        for t in 0..8 {
            let target = target.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..10 {
                    let mut cassette = Cassette::new(format!("writer-{t}"), 0.80);
                    let tools = json!([{"name": "noop"}]);
                    cassette.record_node(
                        None,
                        InteractionKind::McpToolCall,
                        "root",
                        &tools,
                        json!({"tools": tools, "iter": i}),
                        json!({"ok": true}),
                        1,
                    );
                    cassette.save_to_file(&target).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // The surviving file must be one complete, valid cassette, and no
        // temporary file may be left behind.
        let loaded = Cassette::load_from_file(&target).unwrap();
        assert_eq!(loaded.nodes.len(), 1);
        assert!(loaded.validate_dag().is_ok());
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "tmp"))
            .collect();
        assert!(leftovers.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // CT-CORE-01: before the fix, the MCP structural fallback
    // returned Exact for any arguments sharing the tool schema, so
    // amount=50 could replay the response recorded for amount=500
    // (and vice versa). The fallback now requires the
    // safety-critical hash to match.
    #[test]
    fn mcp_structural_fallback_blocks_safety_critical_argument_change() {
        let mut cassette = Cassette::new("mcp_safety_cassette", 0.80);
        let tools = json!([{"name": "issue_refund"}]);
        let req_base = json!({
            "tool": "issue_refund",
            "arguments": {"amount": 50, "currency": "USD", "dry_run": true}
        });
        let resp = json!({"refunded": true});
        cassette.record_node(
            None,
            InteractionKind::McpToolCall,
            "refund",
            &tools,
            req_base.clone(),
            resp,
            1,
        );

        // Critical change: amount 50 -> 500 must MISS, not replay.
        let req_changed = json!({
            "tool": "issue_refund",
            "arguments": {"amount": 500, "currency": "USD", "dry_run": true}
        });
        let res = cassette.match_request(
            &None,
            &InteractionKind::McpToolCall,
            "refund",
            &tools,
            &req_changed,
        );
        assert!(res.is_err(), "critical amount change must not replay");

        // SECOND AUDIT P0 SUPERSEDES the previous cycle's
        // tolerance here: a boolean-only change is still an
        // argument change, and MCP replay is now strict — it
        // must MISS, not match structurally.
        let req_benign = json!({
            "tool": "issue_refund",
            "arguments": {"amount": 50, "currency": "USD", "dry_run": false}
        });
        let res = cassette.match_request(
            &None,
            &InteractionKind::McpToolCall,
            "refund",
            &tools,
            &req_benign,
        );
        assert!(
            res.is_err(),
            "second-audit P0: even a boolean-only MCP argument change must fail closed"
        );
    }

    // CT-CORE-04: DAG validation now enforces resource bounds.
    #[test]
    fn validate_dag_enforces_node_count_and_depth_limits() {
        let mut cassette = Cassette::new("limits_cassette", 0.80);
        let tools = json!([{"name": "noop"}]);
        let req = json!({"tools": tools});
        let resp = json!({"ok": true});
        let mut parent = None;
        for i in 0..5 {
            parent = Some(cassette.record_node(
                parent,
                InteractionKind::McpToolCall,
                "step",
                &tools,
                req.clone(),
                resp.clone(),
                1,
            ));
            let _ = i;
        }
        assert!(cassette.validate_dag().is_ok());
        let err = cassette
            .validate_dag_with_limits(&DagLimits {
                max_nodes: 3,
                max_depth: 2_048,
            })
            .expect_err("5 nodes must exceed a max_nodes of 3");
        assert!(matches!(err, TraceError::InvalidDag(_)));
        // The chain is 5 deep, so a max_depth of 2 must also fail.
        let err = cassette
            .validate_dag_with_limits(&DagLimits {
                max_nodes: 100_000,
                max_depth: 2,
            })
            .expect_err("depth 5 must exceed a max_depth of 2");
        assert!(matches!(err, TraceError::InvalidDag(_)));
    }

    // SECOND AUDIT P0: MCP replay is strict. Any argument change —
    // critical or not — must miss. Only the volatile JSON-RPC
    // envelope id may differ. (Pre-fix, the structural fallback
    // still matched boolean-only and added-argument changes.)
    #[test]
    fn mcp_strict_argument_matching_fails_closed_on_any_change() {
        fn mcp_req(
            id: u64,
            amount: u64,
            order_id: &str,
            recipient: &str,
            qty: u64,
            extra: bool,
            dry_run: bool,
        ) -> Value {
            let mut args = json!({
                "amount": amount,
                "order_id": order_id,
                "recipient": recipient,
                "items": [{"sku": "A", "qty": qty}],
                "dry_run": dry_run,
            });
            if extra {
                args.as_object_mut()
                    .unwrap()
                    .insert("note".to_string(), json!("x"));
            }
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": "charge", "arguments": args},
            })
        }
        let tools = json!([{"name": "charge"}]);
        let mut cassette = Cassette::new("mcp_strict", 0.80);
        cassette.record_node(
            None,
            InteractionKind::McpToolCall,
            "charge",
            &tools,
            mcp_req(7, 50, "ORD-1", "user_42", 1, false, true),
            json!({"charged": true}),
            1,
        );
        let attempt = |req: Value| {
            cassette.match_request(&None, &InteractionKind::McpToolCall, "charge", &tools, &req)
        };
        // Control: identical everything -> Exact.
        assert!(matches!(
            attempt(mcp_req(7, 50, "ORD-1", "user_42", 1, false, true)),
            Ok(MatchResult::Exact(_))
        ));
        // Volatile envelope id only -> still Exact.
        assert!(matches!(
            attempt(mcp_req(99, 50, "ORD-1", "user_42", 1, false, true)),
            Ok(MatchResult::Exact(_))
        ));
        // Every argument change below must MISS.
        for (label, req) in [
            (
                "amount",
                mcp_req(7, 500, "ORD-1", "user_42", 1, false, true),
            ),
            (
                "order_id",
                mcp_req(7, 50, "ORD-2", "user_42", 1, false, true),
            ),
            (
                "recipient",
                mcp_req(7, 50, "ORD-1", "user_43", 1, false, true),
            ),
            (
                "nested qty",
                mcp_req(7, 50, "ORD-1", "user_42", 2, false, true),
            ),
            (
                "added arg",
                mcp_req(7, 50, "ORD-1", "user_42", 1, true, true),
            ),
            (
                "boolean only",
                mcp_req(7, 50, "ORD-1", "user_42", 1, false, false),
            ),
        ] {
            assert!(
                attempt(req).is_err(),
                "MCP argument change ({label}) must fail closed"
            );
        }
        // Missing argument: recipient dropped -> Miss.
        let mut missing = mcp_req(7, 50, "ORD-1", "user_42", 1, false, true);
        missing["params"]["arguments"]
            .as_object_mut()
            .unwrap()
            .remove("recipient");
        assert!(
            attempt(missing).is_err(),
            "MCP missing argument must fail closed"
        );
    }

    // SECOND AUDIT P2: business identifiers count toward the
    // safety hash; only the JSON-RPC envelope id is volatile.
    #[test]
    fn safety_hash_counts_nested_and_unicode_business_ids() {
        // Bare nested "id" (business identifier), digit-bearing.
        let a = json!({"params": {"id": "ORD-900"}});
        let b = json!({"params": {"id": "ORD-901"}});
        assert_ne!(
            compute_safety_critical_hash(&a),
            compute_safety_critical_hash(&b),
            "nested business id change must change the safety hash"
        );
        // Bare nested "id" with no ASCII digits at all.
        let a = json!({"params": {"id": "alpha"}});
        let b = json!({"params": {"id": "beta"}});
        assert_ne!(
            compute_safety_critical_hash(&a),
            compute_safety_critical_hash(&b),
            "alphabetic business id change must change the safety hash"
        );
        // Unicode digits count as digits.
        let a = json!({"note": "order ٩٠٠"});
        let b = json!({"note": "order ٩٠١"});
        assert_ne!(
            compute_safety_critical_hash(&a),
            compute_safety_critical_hash(&b),
            "Unicode-digit change must change the safety hash"
        );
        // But the volatile JSON-RPC envelope id must NOT change it.
        let a = json!({"jsonrpc": "2.0", "id": 7, "method": "tools/call"});
        let b = json!({"jsonrpc": "2.0", "id": 99, "method": "tools/call"});
        assert_eq!(
            compute_safety_critical_hash(&a),
            compute_safety_critical_hash(&b),
            "envelope id change must not change the safety hash"
        );
        // Explicit critical keys: transaction_id and recipient_id.
        let a = json!({"transaction_id": "txn_abc", "recipient_id": "user_abc"});
        let b = json!({"transaction_id": "txn_abd", "recipient_id": "user_abc"});
        assert_ne!(
            compute_safety_critical_hash(&a),
            compute_safety_critical_hash(&b),
            "transaction_id change must change the safety hash"
        );
        let c = json!({"transaction_id": "txn_abc", "recipient_id": "user_abd"});
        assert_ne!(
            compute_safety_critical_hash(&a),
            compute_safety_critical_hash(&c),
            "recipient_id change must change the safety hash"
        );
    }
}
