use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

pub struct SecretScrubber {
    patterns: Vec<(Regex, &'static str)>,
    entropy_threshold: f64,
}

impl Default for SecretScrubber {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretScrubber {
    pub fn new() -> Self {
        let patterns = vec![
            (Regex::new(r"sk-[a-zA-Z0-9_\-]{20,}").unwrap(), "[REDACTED_LLM_KEY]"),
            (Regex::new(r"gh[pousr]_[a-zA-Z0-9]{30,}").unwrap(), "[REDACTED_GITHUB_TOKEN]"),
            (Regex::new(r"AKIA[0-9A-Z]{16}").unwrap(), "[REDACTED_AWS_KEY_ID]"),
        ];
        Self { patterns, entropy_threshold: 4.35 }
    }

     pub fn scrub_json(&self, value: &Value) -> Value {
         match value {
             Value::Object(map) => {
                 let mut cleaned = serde_json::Map::new();
                 for (k, v) in map {
                     let lower_k = k.to_ascii_lowercase();
                     if lower_k.contains("api_key") || lower_k.contains("secret") || lower_k.contains("password") {
                         cleaned.insert(k.clone(), Value::String("[REDACTED_SENSITIVE_FIELD]".into()));
                     } else {
                         cleaned.insert(k.clone(), self.scrub_json(v));
                     }
                 }
                 Value::Object(cleaned)
             }
             Value::Array(arr) => Value::Array(arr.iter().map(|v| self.scrub_json(v)).collect()),
             Value::String(s) => Value::String(self.scrub_str(s)),
             other => other.clone(),
         }
     }

     pub fn scrub_str(&self, input: &str) -> String {
         let mut output = input.to_string();
         for (regex, replacement) in &self.patterns {
             output = regex.replace_all(&output, *replacement).into_owned();
         }
         output
     }
}
