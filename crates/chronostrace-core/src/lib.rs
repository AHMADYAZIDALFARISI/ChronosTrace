pub mod diff;
pub mod simhash;

use serde::{Deserialize, Serialize};
use serde_json::Value;




use sha2::{Digest, Sha256};
use simhash::{compute_simhash, simhash_similarity};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TraceError {
    #[error("I/O error accessing cassette: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML serialization error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Replay miss: no matching DAG node found")]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceNode {
    pub node_id: String,
    pub parent_id: Option<String>,
    pub kind: InteractionKind,
    pub exact_request_hash: String,
    pub prompt_simhash: String,
    pub structural_tool_hash: String,
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

impl Cassette {
    pub fn new(cassette_id: impl Into<String>, similarity_threshold: f64) -> Self {
        Self {
            version: "1.0".to_string(),
            cassette_id: cassette_id.into(),
            metadata: CassetteMetadata {
                created_at: "2026-11-01T00:00:00Z".to_string(),
                similarity_threshold,
                scrubbed: true,
            },
            nodes: Vec::new(),
        }
    }

     pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, TraceError> {
         let raw = fs::read_to_string(path)?;
         Ok(serde_yaml::from_str(&raw)?)
     }

     pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), TraceError> {





          let yaml = serde_yaml::to_string(self)?;
          fs::write(path, yaml)?;
          Ok(())
     }

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
         let simhash_u64 = compute_simhash(prompt_text);
         let prompt_simhash = format!("{simhash_u64:016x}");
         let node_id = format!("node_{:04}_{}", self.nodes.len() + 1, &exact_request_hash[..8]);

          self.nodes.push(TraceNode {
              node_id: node_id.clone(),
              parent_id,
              kind,
              exact_request_hash,
              prompt_simhash,
              structural_tool_hash,
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
     ) -> Result<crate::diff::TrajectoryDiffReport, TraceError> {
         // Placeholder returning matched node wrapper in real build
         unimplemented!()
     }
}

pub fn sha256_canonical_json(value: &Value) -> String {
    let canonical = canonicalize_json(value);
    let serialized = serde_json::to_vec(&canonical).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    hex::encode(hasher.finalize())
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.clone(), canonicalize_json(v));
            }
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(canonicalize_json).collect()),
        other => other.clone(),
    }
}
