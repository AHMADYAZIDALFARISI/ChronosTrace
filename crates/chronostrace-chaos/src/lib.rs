use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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
}
