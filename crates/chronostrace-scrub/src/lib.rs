use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const SCRUBBER_POLICY_VERSION: &str = "1.2.0-entropy-regex-jwt";

/// Proof that a payload passed through the scrubber before
/// persistence (CT-SEC-02). The flag alone is not evidence; the
/// policy version plus the hash of the stored bytes is what makes
/// the claim checkable after the fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrubProof {
    pub scrubbed: bool,
    pub policy_version: String,
    pub payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScrubError {
    /// The scrubber's own second pass still changed the payload,
    /// i.e. the first pass left something recognizable behind.
    ResidualSecretDetected(String),
    SerializationError(String),
}

impl std::fmt::Display for ScrubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScrubError::ResidualSecretDetected(path) => write!(
                f,
                "Scrubbing verification failed: residual sensitive pattern detected at {path}"
            ),
            ScrubError::SerializationError(msg) => {
                write!(f, "Serialization failed during scrub verification: {msg}")
            }
        }
    }
}

impl std::error::Error for ScrubError {}

/// Fail-closed scrub for the persistence boundary (CT-SEC-01/02).
/// Scrubs with the existing `SecretScrubber` (pattern, JWT, and
/// entropy detection — deliberately reused, not reimplemented),
/// then verifies by re-scrubbing: if the second pass changes
/// anything, the first pass left a recognizable secret behind and
/// the call FAILS instead of returning a payload to store. Callers
/// must not persist the input when this returns `Err`.
///
/// Deviation from the audit payload's sketch, recorded honestly:
/// the sketch proposed a new standalone detector under the name
/// `scrub_json_value_EnteringPersistence` (non-snake-case, which
/// would trip `clippy -D warnings`). This implementation keeps the
/// behavior — scrub, verify, proof — under a snake-case name and
/// on top of the existing detector, which is strictly stronger
/// (entropy + JWT + custom patterns).
pub fn scrub_json_for_persistence(input: &Value) -> Result<(Value, ScrubProof), ScrubError> {
    let scrubber = SecretScrubber::new();
    let scrubbed = scrubber.scrub_json(input);
    let rescrubbed = scrubber.scrub_json(&scrubbed);
    if rescrubbed != scrubbed {
        return Err(ScrubError::ResidualSecretDetected(
            "re-scrub changed the payload".to_string(),
        ));
    }
    let serialized =
        serde_json::to_vec(&scrubbed).map_err(|e| ScrubError::SerializationError(e.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    let digest = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok((
        scrubbed,
        ScrubProof {
            scrubbed: true,
            policy_version: SCRUBBER_POLICY_VERSION.to_string(),
            payload_sha256: digest,
        },
    ))
}

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
            // Anthropic keys first: the generic sk- pattern below would
            // otherwise swallow them under the LLM label.
            (
                Regex::new(r"sk-ant-[a-zA-Z0-9_\-]{20,}").unwrap(),
                "[REDACTED_ANTHROPIC_KEY]",
            ),
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
            // Explicit JWT shape (header.payload.signature, base64url).
            // Previously a JWT was caught only incidentally by the
            // entropy fallback when it appeared as a standalone token.
            (
                Regex::new(r"eyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+").unwrap(),
                "[REDACTED_JWT]",
            ),
        ];
        Self {
            patterns,
            entropy_threshold: 4.25,
            redaction_counter: AtomicUsize::new(0),
        }
    }

    /// Append caller-supplied redaction patterns (replacement
    /// `[REDACTED_CUSTOM]`). Applied after the built-in patterns.
    pub fn with_extra_patterns(mut self, patterns: &[Regex]) -> Self {
        for pattern in patterns {
            self.patterns.push((pattern.clone(), "[REDACTED_CUSTOM]"));
        }
        self
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
                    // Token keys: exact `token`, `*_token` (refresh_token,
                    // id_token, access_token), `token_*`. Deliberately NOT
                    // a bare substring match: `max_tokens` and
                    // `tokens_used` are usage counts, not secrets, and
                    // must survive scrubbing.
                    let is_token_key = lower_k == "token"
                        || lower_k.ends_with("_token")
                        || lower_k.starts_with("token_");
                    if lower_k.contains("api_key")
                        || lower_k.contains("apikey")
                        || lower_k == "authorization"
                        || lower_k == "x-api-key"
                        || lower_k == "cookie"
                        || lower_k == "set-cookie"
                        || lower_k.contains("secret")
                        || lower_k.contains("password")
                        || lower_k.contains("passwd")
                        || lower_k.contains("pwd")
                        || is_token_key
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

    // CT-HARDEN-009: token-key rules. Exact `token`, `*_token`, and
    // `token_*` redact; usage counts must NOT.
    #[test]
    fn token_key_rules_redact_tokens_but_not_usage_counts() {
        let scrubber = SecretScrubber::new();
        let payload = json!({
            "token": "tok-secret-value",
            "refresh_token": "refresh-secret-value",
            "id_token": "id-secret-value",
            "my_token": "my-secret-value",
            "token_value": "token-secret-value",
            "access_token": "access-secret-value",
            "max_tokens": 4096,
            "tokens_used": 123,
            "usage": {"prompt_tokens": 10, "completion_tokens": 20}
        });
        let scrubbed = scrubber.scrub_json(&payload);
        for key in [
            "token",
            "refresh_token",
            "id_token",
            "my_token",
            "token_value",
            "access_token",
        ] {
            assert_eq!(
                scrubbed[key], "[REDACTED_SENSITIVE_FIELD]",
                "{key} must be redacted"
            );
        }
        assert_eq!(scrubbed["max_tokens"], 4096);
        assert_eq!(scrubbed["tokens_used"], 123);
        assert_eq!(scrubbed["usage"]["prompt_tokens"], 10);
        assert_eq!(scrubbed["usage"]["completion_tokens"], 20);
    }

    // CT-HARDEN-009: passwd/pwd keys redact alongside password.
    #[test]
    fn passwd_and_pwd_keys_redact() {
        let scrubber = SecretScrubber::new();
        let payload = json!({
            "password": "pw-secret",
            "passwd": "passwd-secret",
            "pwd": "pwd-secret",
            "db_pwd_hash": "hash-secret"
        });
        let scrubbed = scrubber.scrub_json(&payload);
        for key in ["password", "passwd", "pwd", "db_pwd_hash"] {
            assert_eq!(scrubbed[key], "[REDACTED_SENSITIVE_FIELD]");
        }
    }

    // CT-HARDEN-009: explicit JWT shape (jwt.io public example token,
    // not a real credential) and explicit Anthropic key label.
    #[test]
    fn jwt_pattern_and_anthropic_label() {
        let scrubber = SecretScrubber::new();
        let jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
        let scrubbed = scrubber.scrub_str(&format!("auth {jwt} end"));
        assert!(!scrubbed.contains("eyJhbGciOi"));
        assert!(scrubbed.contains("[REDACTED_JWT]"));
        let scrubbed = scrubber.scrub_str("key sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAA end");
        assert!(scrubbed.contains("[REDACTED_ANTHROPIC_KEY]"));
        assert!(!scrubbed.contains("sk-ant-api03"));
        // A generic sk- key still gets the LLM label, not the Anthropic one.
        let scrubbed = scrubber.scrub_str("key sk-AAAAAAAAAAAAAAAAAAAAAAAA end");
        assert!(scrubbed.contains("[REDACTED_LLM_KEY]"));
    }

    // CT-HARDEN-009: private-key blocks (multiline values) redact.
    #[test]
    fn private_key_block_redacts() {
        let scrubber = SecretScrubber::new();
        let block =
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEFAKEKEYDATA\n-----END RSA PRIVATE KEY-----";
        let scrubbed = scrubber.scrub_str(&format!("cert:\n{block}\ndone"));
        assert!(scrubbed.contains("[REDACTED_PRIVATE_KEY]"));
        assert!(!scrubbed.contains("MIIEFAKEKEYDATA"));
        // Nested JSON: the block inside an object field redacts too.
        let payload = json!({"nested": {"key_material": block}});
        let scrubbed = scrubber.scrub_json(&payload);
        assert!(!scrubbed.to_string().contains("MIIEFAKEKEYDATA"));
    }

    // CT-HARDEN-009: caller-supplied custom patterns.
    #[test]
    fn custom_patterns_redact_with_custom_label() {
        let scrubber =
            SecretScrubber::new().with_extra_patterns(&[Regex::new(r"INTERNAL-[0-9]+").unwrap()]);
        let scrubbed = scrubber.scrub_str("ref INTERNAL-4242 and plain text");
        assert!(scrubbed.contains("[REDACTED_CUSTOM]"));
        assert!(!scrubbed.contains("INTERNAL-4242"));
        assert!(scrubbed.contains("plain text"));
    }

    // CT-HARDEN-009 documented limits, pinned as behavior: a short
    // low-entropy secret in an unkeyed field is NOT detectable without
    // unacceptable false positives, so it survives.
    #[test]
    fn short_low_entropy_unkeyed_secret_survives_by_design() {
        let scrubber = SecretScrubber::new();
        let payload = json!({"note": "the code is abc123"});
        let scrubbed = scrubber.scrub_json(&payload);
        assert_eq!(scrubbed["note"], "the code is abc123");
    }

    // CT-SEC-01/02: the fail-closed persistence boundary redacts
    // nested secrets and proves the stored bytes.
    #[test]
    fn persistence_scrub_redacts_nested_secrets_and_proves_hash() {
        let payload = json!({
            "auth": {"api_key": "sk-ant-api03-FAKEFAKEFAKEFAKE1234"},
            "nested": {"token": "ghp_FAKEFAKEFAKEFAKEFAKEFAKE1234"},
            "note": "plain text survives",
        });
        let (scrubbed, proof) = scrub_json_for_persistence(&payload).expect("must scrub clean");
        let rendered = scrubbed.to_string();
        assert!(!rendered.contains("FAKEFAKEFAKEFAKE"));
        assert!(rendered.contains("plain text survives"));
        assert!(proof.scrubbed);
        assert_eq!(proof.policy_version, SCRUBBER_POLICY_VERSION);
        let mut hasher = Sha256::new();
        hasher.update(serde_json::to_vec(&scrubbed).unwrap());
        let expected = hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(proof.payload_sha256, expected);
    }
}
