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

pub enum ChaosOutcome {
    PassThrough(Value),
    DelayThenPassThrough { delay_ms: u64, payload: Value },
    InjectedError { status_code: u16, body: Value },
    MutatedPayload(Value),
}

pub struct ChaosEngine {
    profile: ChaosProfile,
    rng_state: u64,
}

impl ChaosEngine {
    pub fn new(profile: ChaosProfile) -> Self {
        Self { rng_state: if profile.seed == 0 { 0xDEAD_BEEF } else { profile.seed }, profile }
    }

     fn next_f64(&mut self) -> f64 {
         let mut x = self.rng_state;
         x ^= x >> 12;
         x ^= x << 25;
         x ^= x >> 27;
         self.rng_state = x;
         ((x.wrapping_mul(0x2545_F491_4F6C_DD1Du64) >> 11) as f64) / ((1u64 << 53) as f64)
     }

     pub fn apply(&mut self, original_response: &Value) -> ChaosOutcome {
         if self.next_f64() > self.profile.probability {
             return ChaosOutcome::PassThrough(original_response.clone());
         }
         match &self.profile.fault {
             FaultKind::Http429RateLimit { .. } => ChaosOutcome::InjectedError {
                 status_code: 429,
                 body: json!({ "error": "rate_limit_exceeded" }),
             },
             _ => ChaosOutcome::PassThrough(original_response.clone()),
         }
     }
}
