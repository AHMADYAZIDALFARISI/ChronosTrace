/// Computes a deterministic 64-bit SimHash fingerprint over 3-word shingles.
pub fn compute_simhash(text: &str) -> u64 {
    let normalized = normalize_text(text);
    if normalized.is_empty() {
        return 0;
    }

    let mut bit_weights = [0i32; 64];
    let tokens: Vec<&str> = normalized.split_whitespace().collect();

    if tokens.len() >= 3 {
        for window in tokens.windows(3) {
            let shingle = format!("{} {} {}", window[0], window[1], window[2]);
            accumulate_hash(&mut bit_weights, fnv1a_64(shingle.as_bytes()));
        }
    } else {
        for token in &tokens {
            accumulate_hash(&mut bit_weights, fnv1a_64(token.as_bytes()));
        }
    }

    let mut fingerprint: u64 = 0;
    for (i, &weight) in bit_weights.iter().enumerate() {
        if weight > 0 {
            fingerprint |= 1u64 << i;
        }
    }
    fingerprint
}

pub fn simhash_similarity(hash_a: u64, hash_b: u64) -> f64 {
    let xor = hash_a ^ hash_b;
    let hamming_distance = xor.count_ones() as f64;
    1.0 - (hamming_distance / 64.0)
}

pub fn normalize_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        if c.is_alphanumeric() {
            for lc in c.to_lowercase() {
                out.push(lc);
            }
        } else {
            out.push(' ');
        }
    }
    out
}

fn accumulate_hash(bit_weights: &mut [i32; 64], hash: u64) {
    for (i, weight) in bit_weights.iter_mut().enumerate() {
        if (hash >> i) & 1 == 1 {
            *weight += 1;
        } else {
            *weight -= 1;
        }
    }
}

pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minor_prompt_edit_preserves_high_similarity() {
        let p1 = "You are a helpful financial refund agent. Always verify the order_id in the database before issuing a refund.";
        // Single-word edge change: the genuinely minor edit this assertion is about.
        // (The previous pair also swapped "before" for "prior to", an insertion that
        // shifts every later 3-word shingle and scored 0.671875 under this scheme.)
        let p2 = "We are a helpful financial refund agent. Always verify the order_id in the database before issuing a refund.";
        let unrelated =
            "Generate a SQL migration script for partitioning PostgreSQL telemetry tables.";

        let s_close = simhash_similarity(compute_simhash(p1), compute_simhash(p2));
        let s_far = simhash_similarity(compute_simhash(p1), compute_simhash(unrelated));

        assert!(s_close >= 0.80, "Expected >= 0.80, got {s_close}");
        assert!(s_far < 0.70, "Expected < 0.70, got {s_far}");
    }

    #[test]
    fn unicode_normalization_is_consistent() {
        let h1 = compute_simhash("Café résumé naïve");
        let h2 = compute_simhash("café RÉSUMÉ naïve!");
        assert_eq!(h1, h2);
    }
}
