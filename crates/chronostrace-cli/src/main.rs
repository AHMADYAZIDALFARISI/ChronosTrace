mod diff_render;

use chronostrace_chaos::{ChaosEngine, ChaosProfile, FaultKind};
use chronostrace_core::diff::{diff_cassettes, TrajectoryDiffReport};
use chronostrace_core::Cassette;
use chronostrace_proxy::mcp_stdio::{run_mcp_stdio_wrapper, McpStdioConfig, McpWrapMode};
use chronostrace_proxy::{build_router, ProxyState};
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
enum WrapModeArg {
    Record,
    Replay,
    Passthrough,
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
        #[arg(long)]
        chaos_rate_limit_prob: Option<f64>,
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
        #[arg(long)]
        chaos_service_outage_prob: Option<f64>,
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
            chaos_rate_limit_prob,
        } => {
            let loaded = Cassette::load_from_file(&cassette)?;
            let chaos = chaos_rate_limit_prob.map(|prob| {
                ChaosEngine::new(ChaosProfile {
                    name: "cli_rate_limit".into(),
                    seed: 42,
                    probability: prob,
                    fault: FaultKind::Http429RateLimit {
                        retry_after_secs: 1,
                    },
                })
            });

            let state = Arc::new(Mutex::new(ProxyState {
                cassette: loaded,
                current_parent_id: None,
                sessions: std::collections::HashMap::new(),
                scrubber: SecretScrubber::new(),
                chaos,
            }));

            let app = build_router(state);
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            let actual_addr = listener.local_addr()?;
            tracing::info!("ChronosTrace HTTP replay daemon listening on http://{actual_addr}");
            axum::serve(listener, app).await?;
            Ok(ExitCode::SUCCESS)
        }

        Commands::McpWrap {
            cassette,
            mode,
            similarity_threshold,
            simulate_latency,
            chaos_service_outage_prob,
            child_command,
        } => {
            let wrap_mode = match mode {
                WrapModeArg::Record => McpWrapMode::Record,
                WrapModeArg::Replay => McpWrapMode::Replay,
                WrapModeArg::Passthrough => McpWrapMode::PassthroughOnMiss,
            };

            let chaos = chaos_service_outage_prob.map(|prob| {
                ChaosEngine::new(ChaosProfile {
                    name: "mcp_outage".into(),
                    seed: 1337,
                    probability: prob,
                    fault: FaultKind::ServiceUnavailable503,
                })
            });

            let cfg = McpStdioConfig {
                mode: wrap_mode,
                cassette_path: cassette,
                similarity_threshold,
                simulate_recorded_latency: simulate_latency,
                chaos,
                child_command,
            };

            run_mcp_stdio_wrapper(cfg).await?;
            Ok(ExitCode::SUCCESS)
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

        Commands::Verify { cassette, rescan } => {
            let loaded = Cassette::load_from_file(&cassette)?;
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
            let scrubber = SecretScrubber::new();
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
                chaos_rate_limit_prob,
            } => {
                assert_eq!(cassette, PathBuf::from("a.ctrace"));
                assert_eq!(bind, "127.0.0.1:8787");
                assert!(chaos_rate_limit_prob.is_none());
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
                chaos_service_outage_prob,
                child_command,
                ..
            } => {
                assert!(matches!(mode, WrapModeArg::Replay));
                assert_eq!(similarity_threshold, 0.85);
                assert!(!simulate_latency);
                assert!(chaos_service_outage_prob.is_none());
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
            },
        })
        .await;
        assert!(res.is_ok());

        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: std::env::temp_dir()
                    .join("chronostrace-cli-test-definitely-missing.ctrace"),
                rescan: false,
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
            },
        })
        .await;
        assert_eq!(res.unwrap(), ExitCode::SUCCESS);

        // Rescan must flag the planted secret and exit non-zero.
        let res = execute_cli(Cli {
            command: Commands::Verify {
                cassette: p_planted.clone(),
                rescan: true,
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
            },
        })
        .await;
        assert_eq!(res.unwrap(), ExitCode::SUCCESS);

        std::fs::remove_file(&p_planted).ok();
        std::fs::remove_file(&p_clean).ok();
    }
}
