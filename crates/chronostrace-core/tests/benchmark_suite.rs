//! Reproducible matching benchmarks (Phase 11).
//!
//! This integration test measures `Cassette::match_request` latency for
//! cassette sizes of 10, 100, 1,000, and 10,000 nodes. It runs as part of
//! the normal test suite (including `cargo test --release`) and prints a
//! summary table to stdout; run it with `-- --nocapture` to see the table.
//!
//! Design (read this before quoting any number):
//!
//! * The cassette is flat: every node shares `parent_id = None` and the
//!   same tool set, and the queried node is the *last* node. That is the
//!   worst case for the linear exact scan and a full fuzzy scan.
//! * Tier-1 (exact) queries replay the target node's request payload
//!   byte-for-byte, so the exact hash matches.
//! * Tier-2/3 (semantic) queries keep every safety-critical value
//!   (order id, amount) and the tool set identical, but change only the
//!   first word of the system prompt (`Please` -> `Kindly`). The exact
//!   hash therefore misses and the SimHash fuzzy path must find the node.
//! * Only matching latency is timed. Cassette construction is excluded.
//! * No performance threshold is asserted: timings are machine-dependent.
//!   The test asserts only correctness (the right node is returned, and
//!   the fuzzy similarity meets the 0.80 threshold).
//! * Percentiles use the nearest-rank method on sorted per-query
//!   durations. Payload size is the byte length of the JSON serialization
//!   of the whole cassette (not the YAML file size).

use chronostrace_core::{Cassette, InteractionKind, MatchResult};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const THRESHOLD: f64 = 0.80;
const WARMUP_ITERS: usize = 100;

/// A recorded prompt for node `i`. All numbers and ids are kept identical
/// in the edited (fuzzy) query; only the first word changes.
fn prompt_for(i: usize) -> String {
    format!(
        "Please process refund request number {i} for order ORD-{i:05} in the amount of {} dollars \
         with standard verification and confirmation",
        50 + i
    )
}

fn edited_prompt_for(i: usize) -> String {
    prompt_for(i).replacen("Please", "Kindly", 1)
}

fn tools_payload() -> Value {
    json!([{"name": "verify_order"}, {"name": "issue_refund"}])
}

fn request_payload(i: usize, prompt: &str) -> Value {
    json!({
        "model": "test-model",
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": format!("Refund order ORD-{i:05}")}
        ],
        "tools": tools_payload()
    })
}

/// Build a flat cassette of `node_count` nodes and return it together with
/// the target node's id, its exact request, its edited (fuzzy) request,
/// and its edited prompt text.
fn build_cassette(node_count: usize) -> (Cassette, String, Value, Value, String) {
    let mut cassette = Cassette::new(format!("bench-{node_count}"), THRESHOLD);
    let mut target_id = String::new();
    let mut target_exact = Value::Null;
    let mut target_fuzzy = Value::Null;
    let mut target_edited_prompt = String::new();

    for i in 0..node_count {
        let prompt = prompt_for(i);
        let request = request_payload(i, &prompt);
        let id = cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            &prompt,
            &tools_payload(),
            request.clone(),
            json!({"ok": true}),
            10,
        );
        if i + 1 == node_count {
            target_id = id;
            target_exact = request;
            target_edited_prompt = edited_prompt_for(i);
            target_fuzzy = request_payload(i, &target_edited_prompt);
        }
    }

    (
        cassette,
        target_id,
        target_exact,
        target_fuzzy,
        target_edited_prompt,
    )
}

/// Nearest-rank percentile, returned in microseconds.
fn percentile_us(sorted: &[Duration], p: f64) -> f64 {
    assert!(!sorted.is_empty(), "percentile of empty sample");
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index].as_nanos() as f64 / 1_000.0
}

fn run_timed<F>(iterations: usize, mut query: F) -> Vec<Duration>
where
    F: FnMut(),
{
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        query();
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    samples
}

#[test]
fn benchmark_matching_latency() {
    let sizes = [10usize, 100, 1_000, 10_000];
    println!();
    println!("ChronosTrace matching benchmark (Tier-1 exact vs Tier-2/3 SimHash)");
    println!("threshold={THRESHOLD}, warmup={WARMUP_ITERS} iters per case");
    println!(
        "{:>7} {:>12} {:>6} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>8}",
        "nodes",
        "payload_B",
        "iters",
        "exact_P50_us",
        "exact_P95_us",
        "exact_P99_us",
        "fuzzy_P50_us",
        "fuzzy_P95_us",
        "fuzzy_P99_us",
        "fuzzy_sim"
    );

    for &size in &sizes {
        let iterations = if size <= 1_000 { 1_000 } else { 500 };
        let (cassette, target_id, exact_req, fuzzy_req, fuzzy_prompt) = build_cassette(size);
        let payload_bytes = serde_json::to_string(&cassette)
            .expect("cassette serializes to JSON")
            .len();

        // Warmup (untimed) so the timed loop is not dominated by cold caches.
        for _ in 0..WARMUP_ITERS {
            let _ = cassette.match_request(
                &None,
                &InteractionKind::LlmCompletion,
                &prompt_for(size - 1),
                &tools_payload(),
                &exact_req,
            );
            let _ = cassette.match_request(
                &None,
                &InteractionKind::LlmCompletion,
                &fuzzy_prompt,
                &tools_payload(),
                &fuzzy_req,
            );
        }

        // Tier-1: exact match. Assert the right node is returned each time.
        let exact_samples = run_timed(iterations, || {
            match cassette
                .match_request(
                    &None,
                    &InteractionKind::LlmCompletion,
                    &prompt_for(size - 1),
                    &tools_payload(),
                    &exact_req,
                )
                .expect("exact query must match")
            {
                MatchResult::Exact(node) => assert_eq!(node.node_id, target_id),
                other => panic!("expected Exact, got {other:?}"),
            }
        });

        // Tier-2/3: semantic SimHash match on the edited prompt.
        let mut fuzzy_similarity = 0.0;
        let fuzzy_samples = run_timed(iterations, || {
            match cassette
                .match_request(
                    &None,
                    &InteractionKind::LlmCompletion,
                    &fuzzy_prompt,
                    &tools_payload(),
                    &fuzzy_req,
                )
                .expect("fuzzy query must match")
            {
                MatchResult::SemanticFuzzy { node, similarity } => {
                    assert_eq!(node.node_id, target_id);
                    assert!(
                        similarity >= THRESHOLD,
                        "fuzzy similarity {similarity} below threshold {THRESHOLD}"
                    );
                    fuzzy_similarity = similarity;
                }
                other => panic!("expected SemanticFuzzy, got {other:?}"),
            }
        });

        println!(
            "{size:>7} {payload_bytes:>12} {iterations:>6} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {fuzzy_similarity:>8.4}",
            percentile_us(&exact_samples, 50.0),
            percentile_us(&exact_samples, 95.0),
            percentile_us(&exact_samples, 99.0),
            percentile_us(&fuzzy_samples, 50.0),
            percentile_us(&fuzzy_samples, 95.0),
            percentile_us(&fuzzy_samples, 99.0),
        );
    }
}
