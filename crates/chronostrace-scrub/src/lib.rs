use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const SCRUBBER_POLICY_VERSION: &str = "1.1.0-entropy-regex";

pub struct SecretScrubber {
    patterns: Vec<(Regex, &'static str)>,
    entropy_threshold: f64,
    redaction_counter: AtomicUsize,
}

impl Default for SecretScrubber {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretScrubber {
    pub fn new() -> Self {
        let patterns = vec![
            (
                Regex::new(r"sk-[a-zA-Z0-9_\-]{20,}").unwrap(),
                "[REDACTED_LLM_KEY]",
            ),
            (
                Regex::new(r"gh[pousr]_[a-zA-Z0-9]{20,}").unwrap(),
                "[REDACTED_GITHUB_TOKEN]",
            ),
            (
                Regex::new(r"AKIA[0-9A-Z]{16}").unwrap(),
                "[REDACTED_AWS_KEY_ID]",
            ),
            (
                Regex::new(r"(?i)bearer\s+[a-zA-Z0-9_\-\.=]{16,}").unwrap(),
                "Bearer [REDACTED_TOKEN]",
            ),
            (
                Regex::new(
                    r"-----BEGIN [A-Z ]+ PRIVATE KEY-----[\s\S]*?-----END [A-Z ]+ PRIVATE KEY-----",
                )
                .unwrap(),
                "[REDACTED_PRIVATE_KEY]",
            ),
        ];
        Self {
            patterns,
            entropy_threshold: 4.25,
            redaction_counter: AtomicUsize::new(0),
        }
    }

    pub fn policy_version(&self) -> &'static str {
        SCRUBBER_POLICY_VERSION
    }

    pub fn total_redactions(&self) -> usize {
        self.redaction_counter.load(Ordering::Relaxed)
    }

    pub fn scrub_json(&self, value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut cleaned = serde_json::Map::new();
                for (k, v) in map {
                    let lower_k = k.to_ascii_lowercase();
                    if lower_k.contains("api_key")
                        || lower_k.contains("apikey")
                        || lower_k == "authorization"
                        || lower_k == "x-api-key"
                        || lower_k == "cookie"
                        || lower_k == "set-cookie"
                        || lower_k.contains("secret")
                        || lower_k.contains("password")
                        || lower_k.contains("access_token")
                        || lower_k.contains("client_secret")
                    {
                        self.redaction_counter.fetch_add(1, Ordering::Relaxed);
                        cleaned.insert(
                            k.clone(),
                            Value::String("[REDACTED_SENSITIVE_FIELD]".into()),
                        );
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
            if regex.is_match(&output) {
                self.redaction_counter.fetch_add(1, Ordering::Relaxed);
                output = regex.replace_all(&output, *replacement).into_owned();
            }
        }

        output
            .split_whitespace()
            .map(|token| {
                let clean =
                    token.trim_matches(|c: char| c == '"' || c == '\'' || c == ',' || c == ';');
                if clean.len() >= 32
                    && !clean.starts_with("[REDACTED")
                    && !clean.starts_with("http://")
                    && !clean.starts_with("https://")
                    && shannon_entropy(clean) >= self.entropy_threshold
                {
                    self.redaction_counter.fetch_add(1, Ordering::Relaxed);
                    "[REDACTED_HIGH_ENTROPY_SECRET]"
                } else {
                    token
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

pub fn shannon_entropy(s: &str) -> f64 {
    if s.is_empty() {
        return 0.0;
    }
    let mut counts = HashMap::new();
    let mut total = 0.0;
    for ch in s.chars() {
        *counts.entry(ch).or_insert(0usize) += 1;
        total += 1.0;
    }
    counts.values().fold(0.0, |acc, &count| {
        let p = (count as f64) / total;
        acc - p * p.log2()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scrubs_known_patterns_headers_and_high_entropy_secrets() {
        let scrubber = SecretScrubber::new();
        let payload = json!({
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            "nested": {
                "prompt": "My key is sk-proj-1234567890abcdefghijklmnop and token ghp_1234567890abcdefghijklmnopqrstuv",
                "high_entropy": "aB3$kL9!mN2@pQ5^rT8&vW1*xY4(zC7)dF0_gH6"
            }
        });

        let scrubbed = scrubber.scrub_json(&payload);
        let serialized = scrubbed.to_string();
        assert!(!serialized.contains("sk-proj-"));
        assert!(!serialized.contains("ghp_12345"));
        assert!(!serialized.contains("aB3$kL9!"));
        assert!(scrubber.total_redactions() >= 4);
    }
}
