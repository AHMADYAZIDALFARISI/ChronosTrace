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

fn normalize_text(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c.is_whitespace() {
                c.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect()
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

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
         hash ^= byte as u64;
         hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
