use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Audit event for one fault decision (CT-CHAOS-03). Carries no
/// payload, only identities, so it can be logged without leaking
/// recorded data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChaosAuditEvent {
    pub fault_id: String,
    pub session_id: String,
    pub node_id: String,
    pub seed: u64,
    pub fault_type: String,
    pub injected: bool,
    pub policy_version: String,
}

/// Deterministic fault decision (CT-CHAOS-01). The sequential
/// xorshift RNG in `ChaosEngine::apply` depends on call order, so
/// concurrent or reordered scheduling changes which nodes fault.
/// This derivation depends only on (seed, session, node, fault
/// type): the same inputs always give the same decision, in any
/// order, on any run. The first 8 digest bytes are the sample;
/// `fault_id` is the first 16 hex chars of the same digest.
pub fn should_inject_fault_deterministic(
    seed: u64,
    session_id: &str,
    node_id: &str,
    fault_type: &str,
    probability: f64,
) -> (bool, ChaosAuditEvent) {
    let clamped = probability.clamp(0.0, 1.0);
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    hasher.update(b":");
    hasher.update(session_id.as_bytes());
    hasher.update(b":");
    hasher.update(node_id.as_bytes());
    hasher.update(b":");
    hasher.update(fault_type.as_bytes());
    let digest = hasher.finalize();

    let mut raw_u64 = [0u8; 8];
    raw_u64.copy_from_slice(&digest[0..8]);
    let sample = (u64::from_le_bytes(raw_u64) as f64) / (u64::MAX as f64);
    let injected = clamped > 0.0 && sample < clamped;

    let fault_id = format!(
        "flt-{}",
        digest[0..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    (
        injected,
        ChaosAuditEvent {
            fault_id,
            session_id: session_id.to_string(),
            node_id: node_id.to_string(),
            seed,
            fault_type: fault_type.to_string(),
            injected,
            policy_version: "chaos-det-v1".to_string(),
        },
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultKind {
    Http429RateLimit { retry_after_secs: u64 },
    LatencySpike { delay_ms: u64 },
    TruncatedJsonPayload { keep_ratio: f32 },
    DropSchemaField { field_name: String },
    ServiceUnavailable503,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosProfile {
    pub name: String,
    pub seed: u64,
    pub probability: f64,
    pub fault: FaultKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChaosOutcome {
    PassThrough(Value),
    DelayThenPassThrough {
        delay_ms: u64,
        payload: Value,
    },
    InjectedError {
        status_code: u16,
        retry_after_secs: Option<u64>,
        body: Value,
    },
    MutatedPayload(Value),
}

pub struct ChaosEngine {
    profile: ChaosProfile,
    rng_state: u64,
}

impl ChaosEngine {
    pub fn new(mut profile: ChaosProfile) -> Self {
        profile.probability = profile.probability.clamp(0.0, 1.0);
        let initial_seed = if profile.seed == 0 {
            0xDEAD_BEEF_CAFE_BABEu64
        } else {
            profile.seed
        };
        Self {
            profile,
            rng_state: initial_seed,
        }
    }

    fn next_f64(&mut self) -> f64 {
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        let out = x.wrapping_mul(0x2545_F491_4F6C_DD1Du64);
        (out >> 11) as f64 / ((1u64 << 53) as f64)
    }

    pub fn apply(&mut self, original_response: &Value) -> ChaosOutcome {
        if self.profile.probability <= 0.0 || self.next_f64() > self.profile.probability {
            return ChaosOutcome::PassThrough(original_response.clone());
        }
        self.inject_fault(original_response)
    }

    /// Order-independent fault decision (CT-CHAOS-01/03). Unlike
    /// `apply`, this never touches the sequential RNG state, so
    /// concurrent scheduling cannot change which nodes fault. The
    /// returned event is the audit record for the decision.
    pub fn apply_deterministic(
        &self,
        session_id: &str,
        node_id: &str,
        original_response: &Value,
    ) -> (ChaosOutcome, ChaosAuditEvent) {
        let (injected, event) = should_inject_fault_deterministic(
            self.profile.seed,
            session_id,
            node_id,
            self.fault_type_name(),
            self.profile.probability,
        );
        if !injected {
            return (ChaosOutcome::PassThrough(original_response.clone()), event);
        }
        (self.inject_fault(original_response), event)
    }

    fn fault_type_name(&self) -> &'static str {
        match &self.profile.fault {
            FaultKind::Http429RateLimit { .. } => "http-429",
            FaultKind::LatencySpike { .. } => "latency",
            FaultKind::TruncatedJsonPayload { .. } => "truncated",
            FaultKind::DropSchemaField { .. } => "drop-field",
            FaultKind::ServiceUnavailable503 => "service-503",
        }
    }

    fn inject_fault(&self, original_response: &Value) -> ChaosOutcome {
        match &self.profile.fault {
            FaultKind::Http429RateLimit { retry_after_secs } => ChaosOutcome::InjectedError {
                status_code: 429,
                retry_after_secs: Some(*retry_after_secs),
                body: json!({
                    "error": {
                        "type": "rate_limit_error",
                        "message": "ChronosTrace Chaos: Rate limit exceeded.",
                        "retry_after": retry_after_secs
                    }
                }),
            },
            FaultKind::LatencySpike { delay_ms } => ChaosOutcome::DelayThenPassThrough {
                delay_ms: *delay_ms,
                payload: original_response.clone(),
            },
            FaultKind::TruncatedJsonPayload { keep_ratio } => {
                let raw = serde_json::to_string(original_response).unwrap_or_default();
                let clamped = keep_ratio.clamp(0.1, 0.9);
                let cut_bytes = ((raw.len() as f32) * clamped) as usize;
                let safe_cut = raw
                    .char_indices()
                    .map(|(i, _)| i)
                    .take_while(|&i| i <= cut_bytes)
                    .last()
                    .unwrap_or(0);
                ChaosOutcome::MutatedPayload(json!({
                    "_chronostrace_corrupted_raw": &raw[..safe_cut]
                }))
            }
            FaultKind::DropSchemaField { field_name } => {
                let mut cloned = original_response.clone();
                drop_field_recursive(&mut cloned, field_name);
                ChaosOutcome::MutatedPayload(cloned)
            }
            FaultKind::ServiceUnavailable503 => ChaosOutcome::InjectedError {
                status_code: 503,
                retry_after_secs: None,
                body: json!({
                    "error": {
                        "type": "service_unavailable",
                        "message": "ChronosTrace Chaos: Upstream MCP tool server unreachable."
                    }
                }),
            },
        }
    }
}

fn drop_field_recursive(value: &mut Value, field_name: &str) {
    match value {
        Value::Object(map) => {
            map.remove(field_name);
            for v in map.values_mut() {
                drop_field_recursive(v, field_name);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                drop_field_recursive(v, field_name);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_five_chaos_fault_profiles_execute_deterministically() {
        let sample = json!({"status": "ok", "refundable_amount": 50});

        let mut c429 = ChaosEngine::new(ChaosProfile {
            name: "t1".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::Http429RateLimit {
                retry_after_secs: 2,
            },
        });
        assert!(matches!(
            c429.apply(&sample),
            ChaosOutcome::InjectedError {
                status_code: 429,
                retry_after_secs: Some(2),
                ..
            }
        ));

        let mut c503 = ChaosEngine::new(ChaosProfile {
            name: "t2".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::ServiceUnavailable503,
        });
        assert!(matches!(
            c503.apply(&sample),
            ChaosOutcome::InjectedError {
                status_code: 503,
                ..
            }
        ));

        let mut clat = ChaosEngine::new(ChaosProfile {
            name: "t3".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::LatencySpike { delay_ms: 25 },
        });
        assert!(matches!(
            clat.apply(&sample),
            ChaosOutcome::DelayThenPassThrough { delay_ms: 25, .. }
        ));

        let mut cdrop = ChaosEngine::new(ChaosProfile {
            name: "t4".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::DropSchemaField {
                field_name: "refundable_amount".into(),
            },
        });
        if let ChaosOutcome::MutatedPayload(val) = cdrop.apply(&sample) {
            assert!(val.get("refundable_amount").is_none());
        } else {
            panic!("Expected MutatedPayload");
        }

        // The fifth fault (the test name promises all five): truncation must
        // yield a strict, valid-UTF-8 prefix of the serialized original.
        let mut ctrunc = ChaosEngine::new(ChaosProfile {
            name: "t5".into(),
            seed: 1,
            probability: 1.0,
            fault: FaultKind::TruncatedJsonPayload { keep_ratio: 0.5 },
        });
        let raw = serde_json::to_string(&sample).unwrap();
        if let ChaosOutcome::MutatedPayload(val) = ctrunc.apply(&sample) {
            let corrupted = val
                .get("_chronostrace_corrupted_raw")
                .and_then(|v| v.as_str())
                .expect("truncated payload must carry _chronostrace_corrupted_raw");
            assert!(raw.starts_with(corrupted));
            assert!(corrupted.len() < raw.len());
        } else {
            panic!("Expected MutatedPayload");
        }
    }

    // CT-CHAOS-01/03: the deterministic derivation is order-independent
    // and carries a complete audit event.
    #[test]
    fn deterministic_derivation_is_stable_and_node_specific() {
        let sample = json!({"status": "ok"});
        let engine = ChaosEngine::new(ChaosProfile {
            name: "det".into(),
            seed: 42,
            probability: 1.0,
            fault: FaultKind::ServiceUnavailable503,
        });
        // Probability 1.0 always injects; repeated calls agree exactly.
        let (out1, ev1) = engine.apply_deterministic("s1", "node_0001", &sample);
        let (out2, ev2) = engine.apply_deterministic("s1", "node_0001", &sample);
        assert_eq!(ev1, ev2);
        assert!(ev1.injected);
        assert!(ev1.fault_id.starts_with("flt-"));
        assert_eq!(ev1.policy_version, "chaos-det-v1");
        assert_eq!(ev1.session_id, "s1");
        assert_eq!(ev1.node_id, "node_0001");
        assert_eq!(ev1.fault_type, "service-503");
        assert!(matches!(
            out1,
            ChaosOutcome::InjectedError {
                status_code: 503,
                ..
            }
        ));
        assert_eq!(std::mem::discriminant(&out1), std::mem::discriminant(&out2));

        // Probability 0.0 never injects, regardless of inputs.
        let engine0 = ChaosEngine::new(ChaosProfile {
            name: "det0".into(),
            seed: 42,
            probability: 0.0,
            fault: FaultKind::ServiceUnavailable503,
        });
        let (_, ev0) = engine0.apply_deterministic("s1", "node_0001", &sample);
        assert!(!ev0.injected);

        // At p=0.5 across many node ids, decisions must vary by node:
        // a derivation that ignored node_id would inject (or pass)
        // uniformly, which is exactly the order-dependence bug.
        let engine_half = ChaosEngine::new(ChaosProfile {
            name: "det-half".into(),
            seed: 7,
            probability: 0.5,
            fault: FaultKind::ServiceUnavailable503,
        });
        let decisions: Vec<bool> = (0..64)
            .map(|i| {
                let (_, ev) =
                    engine_half.apply_deterministic("s1", &format!("node_{i:04}"), &sample);
                ev.injected
            })
            .collect();
        assert!(
            decisions.contains(&true) && decisions.contains(&false),
            "deterministic decisions must vary by node_id at p=0.5"
        );
    }
}
