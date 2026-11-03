mod diff_render;

use chronostrace_chaos::{ChaosEngine, ChaosProfile, FaultKind};
use chronostrace_core::diff::{diff_cassettes, TrajectoryDiffReport};
use chronostrace_core::Cassette;
use chronostrace_proxy::mcp_stdio::{run_mcp_stdio_wrapper, McpStdioConfig, McpWrapMode};
use chronostrace_proxy::{build_router, ProxyState, SessionLocks, UpstreamTarget};
use chronostrace_scrub::SecretScrubber;
use clap::{Parser, Subcommand, ValueEnum};
use diff_render::{render_markdown_diff, render_terminal_diff};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

#[derive(Parser)]
#[command(
    name = "chronostrace",
    version,
    about = "Deterministic VCR replay, trajectory diffing & chaos fault injector for AI & MCP workflows"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, ValueEnum)]
enum ReplayModeArg {
    Replay,
    Record,
}

#[derive(Clone, ValueEnum)]
enum WrapModeArg {
    Record,
    Replay,
    Passthrough,
}

/// Chaos fault profiles, shared by `replay` (HTTP) and `mcp-wrap`
/// (stdio frames). One profile per invocation; the engine is
/// deterministic for a given seed.
#[derive(Clone, ValueEnum, PartialEq, Eq)]
enum ChaosProfileArg {
    #[value(name = "http-429")]
    Http429,
    #[value(name = "latency")]
    Latency,
    #[value(name = "truncated")]
    Truncated,
    #[value(name = "drop-field")]
    DropField,
    #[value(name = "service-503")]
    Service503,
}

impl ChaosProfileArg {
    fn name(&self) -> &'static str {
        match self {
            ChaosProfileArg::Http429 => "http-429",
            ChaosProfileArg::Latency => "latency",
            ChaosProfileArg::Truncated => "truncated",
            ChaosProfileArg::DropField => "drop-field",
            ChaosProfileArg::Service503 => "service-503",
        }
    }
}

/// Chaos flags, flattened into both `replay` and `mcp-wrap` so the two
/// transports expose exactly the same surface. `--chaos-delay-ms`,
/// `--chaos-keep-ratio`, and `--chaos-field` are `Option` (not
/// defaulted) so passing one with a profile it does not belong to is
/// a hard error instead of a silently ignored value.
#[derive(clap::Args)]
struct ChaosArgs {
    #[arg(long)]
    chaos_profile: Option<ChaosProfileArg>,
    #[arg(long)]
    chaos_probability: Option<f64>,
    #[arg(long, default_value_t = 42)]
    chaos_seed: u64,
    #[arg(long)]
    chaos_field: Option<String>,
    #[arg(long)]
    chaos_delay_ms: Option<u64>,
    #[arg(long)]
    chaos_keep_ratio: Option<f32>,
}

/// Custom redaction patterns, flattened into `replay`, `mcp-wrap`,
/// and `verify`. Repeatable; an invalid regex is a hard CLI error,
/// never silently ignored.
#[derive(clap::Args)]
struct RedactArgs {
    #[arg(long)]
    redact_regex: Vec<String>,
}

/// Parse `--redact-regex` values, or fail fast on the first invalid one.
fn parse_redact_patterns(args: &RedactArgs) -> Result<Vec<regex::Regex>, String> {
    let mut out = Vec::new();
    for pattern in &args.redact_regex {
        out.push(
            regex::Regex::new(pattern)
                .map_err(|e| format!("invalid --redact-regex {pattern:?}: {e}"))?,
        );
    }
    Ok(out)
}

/// The scrubber for a command: built-ins plus the caller's custom
/// patterns.
fn build_scrubber(args: &RedactArgs) -> Result<SecretScrubber, String> {
    Ok(SecretScrubber::new().with_extra_patterns(&parse_redact_patterns(args)?))
}
fn build_chaos(args: &ChaosArgs) -> Result<Option<ChaosEngine>, String> {
    let (profile, probability) = match (&args.chaos_profile, args.chaos_probability) {
        (None, None) => return Ok(None),
        (None, Some(_)) => {
            return Err("--chaos-probability requires --chaos-profile".to_string());
        }
        (Some(_), None) => {
            return Err("--chaos-profile requires --chaos-probability".to_string());
        }
        (Some(p), Some(prob)) => (p.clone(), prob),
    };
    if !(0.0..=1.0).contains(&probability) {
        return Err(format!(
            "--chaos-probability must be between 0.0 and 1.0 (got {probability})"
        ));
    }
    if args.chaos_delay_ms.is_some() && profile != ChaosProfileArg::Latency {
        return Err("--chaos-delay-ms only applies to --chaos-profile latency".to_string());
    }
    if args.chaos_keep_ratio.is_some() && profile != ChaosProfileArg::Truncated {
        return Err("--chaos-keep-ratio only applies to --chaos-profile truncated".to_string());
    }
    if args.chaos_field.is_some() && profile != ChaosProfileArg::DropField {
        return Err("--chaos-field only applies to --chaos-profile drop-field".to_string());
    }
    if let Some(ratio) = args.chaos_keep_ratio {
        if !(0.0..=1.0).contains(&ratio) || ratio == 0.0 {
            return Err(format!(
                "--chaos-keep-ratio must be between 0.0 (exclusive) and 1.0 (got {ratio})"
            ));
        }
    }
    let fault = match &profile {
        ChaosProfileArg::Http429 => FaultKind::Http429RateLimit {
            retry_after_secs: 60,
        },
        ChaosProfileArg::Latency => FaultKind::LatencySpike {
            delay_ms: args.chaos_delay_ms.unwrap_or(500),
        },
        ChaosProfileArg::Truncated => FaultKind::TruncatedJsonPayload {
            keep_ratio: args.chaos_keep_ratio.unwrap_or(0.5),
        },
        ChaosProfileArg::DropField => FaultKind::DropSchemaField {
            field_name: args
                .chaos_field
                .clone()
                .ok_or("--chaos-profile drop-field requires --chaos-field <NAME>".to_string())?,
        },
        ChaosProfileArg::Service503 => FaultKind::ServiceUnavailable503,
    };
    Ok(Some(ChaosEngine::new(ChaosProfile {
        name: format!("cli_{}", profile.name()),
        seed: args.chaos_seed,
        probability,
        fault,
    })))
}

#[derive(Clone, ValueEnum)]
enum DiffFormatArg {
    Terminal,
    Markdown,
    Json,
}

#[derive(Subcommand)]
enum Commands {
    Replay {
        #[arg(short, long)]
        cassette: PathBuf,
        #[arg(short, long, default_value = "127.0.0.1:8787")]
        bind: String,
        #[command(flatten)]
        chaos: ChaosArgs,
        #[command(flatten)]
        redact: RedactArgs,
        /// Replay recorded responses (default), or forward to a live
        /// upstream and record the scrubbed traffic into the cassette.
        #[arg(short, long, value_enum, default_value_t = ReplayModeArg::Replay)]
        mode: ReplayModeArg,
        /// Upstream base URL for record mode, e.g. http://127.0.0.1:8000.
        #[arg(long)]
        upstream: Option<String>,
        /// Upstream timeout in seconds for record mode. 0 (default)
        /// disables timeouts, preserving the previous behavior
        /// (an unresponsive upstream could stall recording
        /// indefinitely). A positive value bounds waiting for
        /// response headers on every upstream request, and bounds
        /// the full body read for non-SSE responses. SSE streams
        /// are exempt from a total timeout once headers arrive, so
        /// a long stream keeps running. On timeout, recording
        /// fails with 504 CHRONOSTRACE_UPSTREAM_TIMEOUT and no
        /// cassette node is recorded. Replay never contacts the
        /// upstream and is unaffected by this flag.
        #[arg(long, default_value_t = 0)]
        upstream_timeout_secs: u64,
        /// Similarity threshold for a cassette created fresh in record
        /// mode (ignored when appending to an existing file).
        #[arg(long, default_value_t = 0.85)]
        similarity_threshold: f64,
    },
    McpWrap {
        #[arg(short, long)]
        cassette: PathBuf,
        #[arg(short, long, value_enum, default_value_t = WrapModeArg::Replay)]
        mode: WrapModeArg,
        #[arg(long, default_value_t = 0.85)]
        similarity_threshold: f64,
        #[arg(long, default_value_t = false)]
        simulate_latency: bool,
        #[command(flatten)]
        chaos: ChaosArgs,
        #[command(flatten)]
        redact: RedactArgs,
        #[arg(last = true)]
        child_command: Vec<String>,
    },
    Diff {
        baseline: PathBuf,
        candidate: PathBuf,
        #[arg(short, long, value_enum, default_value_t = DiffFormatArg::Terminal)]
        format: DiffFormatArg,
        // Optional-value bool: absent -> true, bare flag -> true,
        // --fail-on-divergence=false -> false.
        #[arg(
            long,
            default_value_t = true,
            default_missing_value = "true",
            num_args = 0..=1,
            require_equals = true
        )]
        fail_on_divergence: bool,
    },
    Verify {
        #[arg(short, long)]
        cassette: PathBuf,
        /// Re-scan every stored payload with the scrubber and report any
        /// secret-pattern or high-entropy hits that remain.
        #[arg(long, default_value_t = false)]
        rescan: bool,
        #[command(flatten)]
        redact: RedactArgs,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match execute_cli(cli).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::from(1)
        }
    }
}

async fn execute_cli(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Replay {
            cassette,
            bind,
            chaos,
            redact,
            mode,
            upstream,
            upstream_timeout_secs,
            similarity_threshold,
        } => {
            let is_record = matches!(mode, ReplayModeArg::Record);
            let upstream_target = if is_record {
                let base = upstream.ok_or("record mode requires --upstream <base-url>")?;
                Some(UpstreamTarget::with_timeout(&base, upstream_timeout_secs)?)
            } else {
                None
            };

            // SECOND AUDIT P1: remember whether the cassette
            // file pre-existed, for the scrubbed-metadata gate at
            // save time (see below).
            let cassette_existed = cassette.exists();
            let loaded = if cassette.exists() {
                Cassette::load_from_file(&cassette)?
            } else if is_record {
                let stem = cassette
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("chronostrace");
                Cassette::new(stem.to_string(), similarity_threshold)
            } else {
                return Err(format!("cassette not found: {}", cassette.display()).into());
            };

            // In record mode, appending to an existing cassette continues
            // the recorded trajectory from its last node.
            let initial_parent = if is_record {
                loaded.nodes.last().map(|n| n.node_id.clone())
            } else {
                None
            };

            let chaos = build_chaos(&chaos).map_err(|e| format!("chaos config: {e}"))?;
            let scrubber = build_scrubber(&redact).map_err(|e| format!("redact config: {e}"))?;

            let state = Arc::new(Mutex::new(ProxyState {
                cassette: loaded,
                current_parent_id: initial_parent,
                sessions: std::collections::HashMap::new(),
                scrubber,
                chaos,
                upstream: upstream_target,
                session_locks: SessionLocks::default(),
            }));

            let state_for_save = state.clone();
            let app = build_router(state);
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            let actual_addr = listener.local_addr()?;
            tracing::info!(
                "ChronosTrace HTTP {} daemon listening on http://{actual_addr}",
                if is_record { "record" } else { "replay" }
            );
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await?;
            if is_record {
                // Graceful shutdown is the save point: recorded nodes live
                // in memory until Ctrl+C / SIGTERM, then the cassette is
                // written once, atomically.
                let mut guard = state_for_save.lock().expect("ProxyState mutex poisoned");
                let policy = guard.scrubber.policy_version();
                let redactions = guard.scrubber.total_redactions();
                // SECOND AUDIT P1: scrubbed metadata must only be
                // set after the complete pipeline succeeds FOR THE
                // WHOLE CASSETTE. Appending to an existing
                // cassette mixes pre-existing nodes (which never
                // passed through this run's scrubber) with new
                // scrubbed nodes, so marking the whole cassette
                // scrubbed would be false. Only mark when the
                // cassette is fresh (every node was scrubbed in
                // this run) or was already marked scrubbed when
                // loaded (old nodes already passed a pipeline,
                // and new nodes are scrubbed too).
                if !cassette_existed || guard.cassette.metadata.scrubbed {
                    guard.cassette.mark_scrubbed(policy, redactions);
                }
                guard.cassette.save_to_file(&cassette)?;
                tracing::info!(
                    "ChronosTrace recorded cassette saved to {} ({} nodes)",
                    cassette.display(),
                    guard.cassette.nodes.len()
                );
            }
            Ok(ExitCode::SUCCESS)
        }

        Commands::McpWrap {
            cassette,
            mode,
            similarity_threshold,
            simulate_latency,
            chaos,
            redact,
            child_command,
        } => {
            let wrap_mode = match mode {
                WrapModeArg::Record => McpWrapMode::Record,
                WrapModeArg::Replay => McpWrapMode::Replay,
                WrapModeArg::Passthrough => McpWrapMode::PassthroughOnMiss,
            };

            let chaos = build_chaos(&chaos).map_err(|e| format!("chaos config: {e}"))?;
            let extra_patterns =
                parse_redact_patterns(&redact).map_err(|e| format!("redact config: {e}"))?;

            let cfg = McpStdioConfig {
                mode: wrap_mode,
                cassette_path: cassette,
                similarity_threshold,
                simulate_recorded_latency: simulate_latency,
                chaos,
                extra_patterns,
                child_command,
            };

            // The wrapper returns Err for child-death, signal interrupt,
            // and I/O failures. Do NOT propagate it with `?`: tokio's
            // stdin blocking-read thread never finishes while the host
            // keeps stdin open, and the runtime's shutdown waits for it,
            // so a `?` return would hang the process after the error was
            // already reported (reproduced: error printed, process alive
            // 20 s+ until stdin closed). The cassette is already flushed
            // and persisted and the child already reaped before the
            // wrapper returns, so exiting directly here loses nothing.
            match run_mcp_stdio_wrapper(cfg).await {
                Ok(()) => Ok(ExitCode::SUCCESS),
                Err(e) => {
                    eprintln!("Error: {e:?}");
                    std::process::exit(1);
                }
            }
        }

        Commands::Diff {
            baseline,
            candidate,
            format,
            fail_on_divergence,
        } => {
            let base_cassette = Cassette::load_from_file(&baseline)?;
            let cand_cassette = Cassette::load_from_file(&candidate)?;
            let report = diff_cassettes(&base_cassette, &cand_cassette);

            match format {
                DiffFormatArg::Terminal => print!("{}", render_terminal_diff(&report)),
                DiffFormatArg::Markdown => println!("{}", render_markdown_diff(&report)),
                DiffFormatArg::Json => println!("{}", serde_json::to_string_pretty(&report)?),
            }

            Ok(ExitCode::from(diff_exit_code(&report, fail_on_divergence)))
        }

        Commands::Verify {
            cassette,
            rescan,
            redact,
        } => {
            let loaded = Cassette::load_from_file(&cassette)?;
            // Validate custom patterns eagerly: an invalid regex is a
            // hard error even when --rescan is off (the flag would
            // otherwise be silently meaningless).
            let scrubber = build_scrubber(&redact).map_err(|e| format!("redact config: {e}"))?;
            println!(
                "Verified cassette '{}' (v{}): {} DAG nodes, threshold >= {:.2}, scrub metadata flag={}",
                loaded.cassette_id,
                loaded.version,
                loaded.nodes.len(),
                loaded.metadata.similarity_threshold,
                loaded.metadata.scrubbed
            );
            if !rescan {
                println!(
                    "Note: this confirms DAG validity and the scrub metadata flag only; stored payloads were not re-scanned. Use --rescan to re-check payloads."
                );
                return Ok(ExitCode::SUCCESS);
            }
            // Best-effort re-check: run the scrubber over every stored
            // payload and report anything it still flags. A clean rescan
            // is not proof that no secret is present: pattern and entropy
            // matching cannot guarantee full discovery.
            let mut flagged: Vec<(String, usize)> = Vec::new();
            let mut total_hits = 0usize;
            for node in &loaded.nodes {
                let mut node_hits = 0usize;
                for payload in [&node.request_payload, &node.response_payload] {
                    let before = scrubber.total_redactions();
                    let scrubbed = scrubber.scrub_json(payload);
                    let hits = scrubber.total_redactions() - before;
                    if hits > 0 || scrubbed != *payload {
                        node_hits += hits.max(1);
                    }
                }
                if node_hits > 0 {
                    total_hits += node_hits;
                    flagged.push((node.node_id.clone(), node_hits));
                }
            }
            if flagged.is_empty() {
                println!(
                    "Rescan: no remaining secret-pattern or high-entropy hits found in stored payloads (best-effort check, not proof of absence)."
                );
                Ok(ExitCode::SUCCESS)
            } else {
                println!(
                    "Rescan: {total_hits} remaining hit(s) in {} node(s):",
                    flagged.len()
                );
                for (node_id, hits) in &flagged {
                    println!("  {node_id}: {hits} hit(s)");
                }
                Ok(ExitCode::from(2))
            }
        }
    }
}

/// Exit code for `diff`: 0 when compatible (or when the caller opts out of
/// failing), 2 on breaking divergence with failing enabled.
fn diff_exit_code(report: &TrajectoryDiffReport, fail_on_divergence: bool) -> u8 {
    if fail_on_divergence && !report.is_compatible() {
        2
    } else {
        0
    }
}

/// Wait for Ctrl+C (or SIGTERM on Unix) so the replay/record daemon can
/// shut down gracefully. In record mode this is the save point: the
/// cassette is written only after this signal arrives.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronostrace_core::InteractionKind;
    use serde_json::{json, Value};

    fn fixture_cassette(id: &str, prompt: &str, response: Value) -> Cassette {
        let mut cassette = Cassette::new(id, 0.80);
        let tools = json!([{"name": "verify_order"}]);
        cassette.record_node(
            None,
            InteractionKind::LlmCompletion,
            prompt,
            &tools,
            json!({"model": "gpt-4o", "messages": [{"content": prompt}], "tools": tools}),
            response,
            10,
        );
        cassette
    }

    fn write_cassette(name: &str, cassette: &Cassette) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "chronostrace-cli-test-{}-{name}",
            std::process::id()
        ));
        cassette.save_to_file(&path).unwrap();
        path
    }

    #[test]
    fn cli_parses_replay_defaults() {
        let cli =
            Cli::try_parse_from(["chronostrace", "replay", "--cassette", "a.ctrace"]).unwrap();
        match cli.command {
            Commands::Replay {
                cassette,
                bind,
                chaos,
                redact,
                mode,
                upstream,
                upstream_timeout_secs,
                similarity_threshold,
            } => {
                assert_eq!(cassette, PathBuf::from("a.ctrace"));
                assert_eq!(bind, "127.0.0.1:8787");
                assert!(chaos.chaos_profile.is_none());
                assert!(chaos.chaos_probability.is_none());
                assert!(redact.redact_regex.is_empty());
                assert!(matches!(mode, ReplayModeArg::Replay));
                assert!(upstream.is_none());
                assert_eq!(upstream_timeout_secs, 0);
                assert_eq!(similarity_threshold, 0.85);
            }
            _ => panic!("expected Replay"),
        }

        let cli = Cli::try_parse_from([
            "chronostrace",
            "replay",
            "--cassette",
            "b.ctrace",
            "--mode",
            "record",
            "--upstream",
            "http://127.0.0.1:9000",
            "--upstream-timeout-secs",
            "120",
        ])
        .unwrap();
        match cli.command {
            Commands::Replay {
                mode,
                upstream,
                upstream_timeout_secs,
                ..
            } => {
                assert!(matches!(mode, ReplayModeArg::Record));
                assert_eq!(upstream.as_deref(), Some("http://127.0.0.1:9000"));
                assert_eq!(upstream_timeout_secs, 120);
            }
            _ => panic!("expected Replay"),
        }
    }

    #[test]
    fn cli_parses_mcp_wrap_defaults_and_modes() {
        let cli = Cli::try_parse_from(["chronostrace", "mcp-wrap", "-c", "x.ctrace"]).unwrap();
        match cli.command {
            Commands::McpWrap {
                mode,
                similarity_threshold,
                simulate_latency,
                chaos,
                child_command,
                ..
            } => {
                assert!(matches!(mode, WrapModeArg::Replay));
                assert_eq!(similarity_threshold, 0.85);
                assert!(!simulate_latency);
                assert!(chaos.chaos_profile.is_none());
                assert!(chaos.chaos_probability.is_none());
                assert!(child_command.is_empty());
            }
            _ => panic!("expected McpWrap"),
        }

        let cli = Cli::try_parse_from([
            "chronostrace",
            "mcp-wrap",
            "-c",
            "x.ctrace",
            "-m",
            "passthrough",
        ])
        .unwrap();
        match cli.command {
            Commands::McpWrap { mode, .. } => assert!(matches!(mode, WrapModeArg::Passthrough)),
            _ => panic!("expected McpWrap"),
        }
    }

    fn chaos_args(
        profile: Option<ChaosProfileArg>,
        probability: Option<f64>,
        field: Option<&str>,
        delay_ms: Option<u64>,
        keep_ratio: Option<f32>,
    ) -> ChaosArgs {
        ChaosArgs {
            chaos_profile: profile,
            chaos_probability: probability,
            chaos_seed: 42,
            chaos_field: field.map(str::to_string),
            chaos_delay_ms: delay_ms,
            chaos_keep_ratio: keep_ratio,
        }
    }

    // CT-HARDEN-008: the unified chaos surface parses on both commands
    // and invalid combinations fail fast in build_chaos.
    #[test]
    fn cli_parses_unified_chaos_flags() {
        let cli = Cli::try_parse_from([
            "chronostrace",
            "replay",
            "--cassette",
            "a.ctrace",
            "--chaos-profile",
            "drop-field",
            "--chaos-probability",
            "0.5",
            "--chaos-field",
            "result",
            "--chaos-seed",
            "7",
        ])
        .unwrap();
        match cli.command {
            Commands::Replay { chaos, .. } => {
                assert!(matches!(
                    chaos.chaos_profile,
                    Some(ChaosProfileArg::DropField)
                ));
                assert_eq!(chaos.chaos_probability, Some(0.5));
                assert_eq!(chaos.chaos_field.as_deref(), Some("result"));
                assert_eq!(chaos.chaos_seed, 7);
                assert!(build_chaos(&chaos).is_ok());
            }
            _ => panic!("expected Replay"),
        }
        // Old single-purpose flags are gone.
        assert!(Cli::try_parse_from([
            "chronostrace",
            "replay",
            "--cassette",
            "a.ctrace",
            "--chaos-rate-limit-prob",
            "0.5",
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "chronostrace",
            "mcp-wrap",
            "-c",
            "x.ctrace",
            "--chaos-service-outage-prob",
            "0.5",
        ])
        .is_err());
    }

    #[test]
    fn build_chaos_rejects_invalid_combinations() {
        // Neither flag: no chaos.
        assert!(build_chaos(&chaos_args(None, None, None, None, None))
            .unwrap()
            .is_none());
        // Profile without probability, and vice versa.
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Http429),
            None,
            None,
            None,
            None
        ))
        .is_err());
        assert!(build_chaos(&chaos_args(None, Some(0.5), None, None, None)).is_err());
        // Probability out of range.
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Http429),
            Some(1.5),
            None,
            None,
            None
        ))
        .is_err());
        // drop-field requires its field.
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::DropField),
            Some(1.0),
            None,
            None,
            None
        ))
        .is_err());
        // Per-profile options on the wrong profile are errors, not
        // silently ignored values.
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Http429),
            Some(1.0),
            Some("x"),
            None,
            None
        ))
        .is_err());
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Latency),
            Some(1.0),
            None,
            None,
            Some(0.5)
        ))
        .is_err());
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Http429),
            Some(1.0),
            None,
            Some(100),
            None
        ))
        .is_err());
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Truncated),
            Some(1.0),
            None,
            None,
            Some(0.0)
        ))
        .is_err());
        // Matching per-profile options build fine (with defaults when
        // omitted, and with explicit values when given).
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Latency),
            Some(1.0),
            None,
            Some(100),
            None
        ))
        .is_ok());
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::Truncated),
            Some(1.0),
            None,
            None,
            Some(0.25)
        ))
        .is_ok());
        // Every profile builds at probability 1.0.
        for profile in [
            ChaosProfileArg::Http429,
            ChaosProfileArg::Latency,
            ChaosProfileArg::Truncated,
            ChaosProfileArg::Service503,
        ] {
            assert!(build_chaos(&chaos_args(Some(profile), Some(1.0), None, None, None)).is_ok());
        }
        assert!(build_chaos(&chaos_args(
            Some(ChaosProfileArg::DropField),
            Some(1.0),
            Some("result"),
            None,
            None
        ))
        .is_ok());
    }

    #[test]
    fn cli_parses_diff_defaults_and_fail_flag_forms() {
        let cli = Cli::try_parse_from(["chronostrace", "diff", "a", "b"]).unwrap();
        match cli.command {
            Commands::Diff {
                format,
                fail_on_divergence,
                ..
            } => {
                assert!(matches!(format, DiffFormatArg::Terminal));
                assert!(fail_on_divergence);
            }
            _ => panic!("expected Diff"),
        }

        let cli = Cli::try_parse_from(["chronostrace", "diff", "a", "b", "--fail-on-divergence"])
            .unwrap();
        match cli.command {
            Commands::Diff {
                fail_on_divergence, ..
            } => assert!(fail_on_divergence),
            _ => panic!("expected Diff"),
        }

        let cli = Cli::try_parse_from([
            "chronostrace",
            "diff",
            "a",
            "b",
            "--fail-on-divergence=false",
        ])
        .unwrap();
        match cli.command {
            Commands::Diff {
                fail_on_divergence, ..
            } => assert!(!fail_on_divergence),
            _ => panic!("expected Diff"),
        }

        let cli = Cli::try_parse_from(["chronostrace", "diff", "a", "b", "-f", "json"]).unwrap();
        match cli.command {
            Commands::Diff { format, .. } => assert!(matches!(format, DiffFormatArg::Json)),
            _ => panic!("expected Diff"),
        }
    }

    #[test]
    fn cli_rejects_missing_and_invalid_args() {
        assert!(Cli::try_parse_from(["chronostrace"]).is_err());
        assert!(Cli::try_parse_from(["chronostrace", "diff"]).is_err());
        assert!(Cli::try_parse_from(["chronostrace", "diff", "a", "b", "-f", "xml"]).is_err());
    }

    #[test]
    fn diff_exit_code_matrix() {
        let same = fixture_cassette("ok", "You are a helpful refund agent.", json!({"ok": true}));
        let compatible = diff_cassettes(&same, &same);
        assert!(compatible.is_compatible());
        assert_eq!(diff_exit_code(&compatible, true), 0);
        assert_eq!(diff_exit_code(&compatible, false), 0);

        let diverged = diff_cassettes(
            &same,
            &fixture_cassette(
                "bad",
                "You are a helpful refund agent.",
                json!({"ok": false}),
            ),
        );
        assert!(!diverged.is_compatible());
        assert_eq!(diff_exit_code(&diverged, true), 2);
        assert_eq!(diff_exit_code(&diverged, false), 0);
    }

    #[tokio::test]
    async fn execute_cli_diff_and_verify() {
        let base = fixture_cassette("ok", "You are a helpful refund agent.", json!({"ok": true}));
        let bad = fixture_cassette(
            "bad",
            "You are a helpful refund agent.",
            json!({"ok": false}),
        );
        let p_base = write_cassette("base", &base);
        let p_bad = write_cassette("bad", &bad);

        let res = execute_cli(Cli {
            command: Commands::Diff {
                baseline: p_base.clone(),
                candidate: p_base.clone(),
                format: DiffFormatArg::Terminal,
                fail_on_divergence: true,
            },
        })
        .await;
        assert!(res.is_ok());

        let res = execute_cli(Cli {
            command: Commands::Diff {
                baseline: p_base.clone(),
                candidate: p_bad.clone(),
                format: DiffFormatArg::Json,
                fail_on_divergence: false,
            },
        })
        .await;
        assert!(res.is_ok());

        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: p_base.clone(),
                rescan: false,
                redact: RedactArgs {
                    redact_regex: vec![],
                },
            },
        })
        .await;
        assert!(res.is_ok());

        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: std::env::temp_dir()
                    .join("chronostrace-cli-test-definitely-missing.ctrace"),
                rescan: false,
                redact: RedactArgs {
                    redact_regex: vec![],
                },
            },
        })
        .await;
        assert!(res.is_err());

        std::fs::remove_file(&p_base).ok();
        std::fs::remove_file(&p_bad).ok();
    }

    #[tokio::test]
    async fn execute_cli_verify_rescan_flags_planted_secret() {
        // record_node does not scrub; scrubbing normally happens before
        // recording. A payload that still contains a secret therefore
        // simulates a cassette that predates scrubbing or slipped
        // through it.
        let mut planted = Cassette::new("rescan_fixture", 0.80);
        let tools = json!([{"name": "noop"}]);
        planted.record_node(
            None,
            InteractionKind::McpToolCall,
            "root",
            &tools,
            json!({"tools": tools}),
            json!({"result": "key sk-proj-abcdefghijklmnopqrstuvwx"}),
            1,
        );
        let p_planted = write_cassette("rescan_planted", &planted);

        // Default verify passes: it only confirms DAG validity and the
        // scrub metadata flag.
        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: p_planted.clone(),
                rescan: false,
                redact: RedactArgs {
                    redact_regex: vec![],
                },
            },
        })
        .await;
        assert_eq!(res.unwrap(), ExitCode::SUCCESS);

        // Rescan must flag the planted secret and exit non-zero.
        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: p_planted.clone(),
                rescan: true,
                redact: RedactArgs {
                    redact_regex: vec![],
                },
            },
        })
        .await;
        assert_eq!(res.unwrap(), ExitCode::from(2));

        // A clean cassette rescans clean.
        let clean = fixture_cassette("ok", "You are a helpful refund agent.", json!({"ok": true}));
        let p_clean = write_cassette("rescan_clean", &clean);
        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: p_clean.clone(),
                rescan: true,
                redact: RedactArgs {
                    redact_regex: vec![],
                },
            },
        })
        .await;
        assert_eq!(res.unwrap(), ExitCode::SUCCESS);

        std::fs::remove_file(&p_planted).ok();
        std::fs::remove_file(&p_clean).ok();
    }

    // CT-HARDEN-009: --redact-regex parses (repeatable) and an invalid
    // regex is a hard error before any serving or scanning happens.
    #[test]
    fn redact_regex_parses_and_rejects_invalid() {
        let args = RedactArgs {
            redact_regex: vec!["INTERNAL-[0-9]+".to_string(), "SECRET-[A-Z]+".to_string()],
        };
        assert_eq!(parse_redact_patterns(&args).unwrap().len(), 2);
        let bad = RedactArgs {
            redact_regex: vec!["[".to_string()],
        };
        assert!(parse_redact_patterns(&bad).is_err());
        // The flag is accepted on all three commands that take it.
        let cli = Cli::try_parse_from([
            "chronostrace",
            "replay",
            "--cassette",
            "a.ctrace",
            "--redact-regex",
            "INTERNAL-[0-9]+",
            "--redact-regex",
            "SECRET-[A-Z]+",
        ])
        .unwrap();
        match cli.command {
            Commands::Replay { redact, .. } => {
                assert_eq!(redact.redact_regex.len(), 2);
            }
            _ => panic!("expected Replay"),
        }
    }

    #[tokio::test]
    async fn execute_cli_verify_rejects_invalid_redact_regex() {
        let clean = fixture_cassette("ok", "You are a helpful refund agent.", json!({"ok": true}));
        let p_clean = write_cassette("redact_bad", &clean);
        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: p_clean.clone(),
                rescan: false,
                redact: RedactArgs {
                    redact_regex: vec!["[".to_string()],
                },
            },
        })
        .await;
        assert!(res.is_err());
        std::fs::remove_file(&p_clean).ok();
    }
}
