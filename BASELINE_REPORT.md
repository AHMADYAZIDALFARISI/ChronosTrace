# BASELINE REPORT (PHASE 0)
Commit SHA: a05d571a940cfd518858ba95985d96c4bd8c7032
Branch: remediation/audit-hardening-v0.1

### Cargo Format Check
```
```
- Exit Code: 0
- Duration: 1s

### Cargo Clippy
```
    Checking chronostrace-core v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-core)
    Checking chronostrace-scrub v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-scrub)
    Checking chronostrace-chaos v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-chaos)
    Checking chronostrace-proxy v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-proxy)
    Checking chronostrace-cli v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-cli)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 7.94s
```
- Exit Code: 0
- Duration: 8s

### Cargo Test Workspace
```
   Compiling chronostrace-core v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-core)
   Compiling chronostrace-chaos v0.1.0 (/home/hatch/workspace/ayasi-factory/chronostrace-repo/crates/chronostrace-chaos)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 4.41s
     Running unittests src/lib.rs (target/debug/deps/chronostrace_chaos-35f7e20a68fe4c7a)

running 1 test
test tests::all_five_chaos_fault_profiles_execute_deterministically ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/main.rs (target/debug/deps/chronostrace-57e03cb5701003d8)

running 11 tests
test tests::build_chaos_rejects_invalid_combinations ... ok
test tests::cli_parses_mcp_wrap_defaults_and_modes ... ok
test tests::cli_parses_replay_defaults ... ok
test tests::cli_parses_diff_defaults_and_fail_flag_forms ... ok
test tests::diff_exit_code_matrix ... ok
test tests::cli_rejects_missing_and_invalid_args ... ok
test tests::cli_parses_unified_chaos_flags ... ok
test tests::redact_regex_parses_and_rejects_invalid ... ok
test tests::execute_cli_verify_rejects_invalid_redact_regex ... ok
test tests::execute_cli_diff_and_verify ... ok
test tests::execute_cli_verify_rescan_flags_planted_secret ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s

     Running tests/mcp_lifecycle.rs (target/debug/deps/mcp_lifecycle-b158f005eb672ff3)

running 5 tests
test duplicate_host_request_ids_keep_first_tracking_only ... ok
test cancelled_request_does_not_block_later_records ... ok
test server_initiated_request_id_collision_is_not_recorded_as_response ... ok
test child_crash_mid_flight_yields_error_frame_and_nonzero_exit ... ok
test signal_during_record_persists_partial_cassette_and_fails ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.36s

     Running unittests src/lib.rs (target/debug/deps/chronostrace_core-6b3a5cf63294a0fc)

running 10 tests
test simhash::tests::minor_prompt_edit_preserves_high_similarity ... ok
test simhash::tests::unicode_normalization_is_consistent ... ok
test diff::tests::reordered_steps_are_topology_diverged_not_mispaired ... ok
test diff::tests::detects_reconciled_prompt_drift_and_tool_arg_mutations ... ok
test tests::canonical_json_normalizes_whole_floats_and_key_order ... ok
test diff::tests::same_target_different_parents_is_topology_diverged_not_identical ... ok
test tests::validate_dag_accepts_branching_chains ... ok
test tests::matches_exact_and_fuzzy_prompt_but_blocks_amount_mutation ... ok
test tests::validate_dag_rejects_parent_cycles ... ok
test tests::concurrent_saves_to_same_target_all_succeed_and_leave_valid_file ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running tests/benchmark_suite.rs (target/debug/deps/benchmark_suite-faed88542a84e02f)

running 1 test
test benchmark_matching_latency ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.91s

     Running unittests src/lib.rs (target/debug/deps/chronostrace_proxy-f3cd03cd4710b877)

running 49 tests
test http_record::tests::parser_handles_openai_style_stream ... ok
test http_record::tests::parser_handles_anthropic_style_events_ids_and_retry ... ok
test http_record::tests::parser_handles_crlf_comments_unknown_fields_and_multiline_data ... ok
test http_record::tests::parser_finish_flushes_unterminated_event_and_ignores_empty_tail ... ok
test http_record::tests::parser_resets_event_name_on_dataless_blocks_but_keeps_id ... ok
test http_record::tests::parser_handles_split_chunks_and_malformed_lines ... ok
test http_record::tests::serialize_round_trips_events_with_fields ... ok
test http_record::tests::v2_marker_round_trips_and_v1_stays_distinct ... ok
test mcp_stdio::tests::cancelled_sequence_unblocks_drain ... ok
test mcp_stdio::tests::ordered_recorder_chains_in_request_order_despite_response_order ... ok
test mcp_stdio::tests::ordered_recorder_flush_drains_leftovers ... ok
test mcp_stdio::tests::prompts_get_structural_includes_full_params ... ok
test mcp_stdio::tests::sampling_create_message_structural_includes_full_params ... ok
test mcp_stdio::tests::prompts_get_param_change_is_a_replay_miss_not_a_match ... ok
test mcp_stdio::tests::harden008_mcp_stdio_supports_all_chaos_profiles ... ok
test tests::extract_prompt_text_keeps_chat_string_content ... ok
test tests::extract_prompt_text_reads_anthropic_content_blocks ... ok
test tests::extract_prompt_text_reads_responses_input ... ok
test http_record::tests::scrub_sse_data_redacts_json_secrets_and_keeps_done ... ok
test tests::harden002_anthropic_headers_reach_upstream_via_dispatch ... ok
test tests::harden001_secret_prompt_reaches_upstream_verbatim ... ok
test tests::harden001_body_credential_reaches_upstream_but_not_cassette ... ok
test tests::harden002_non_allowlisted_headers_are_dropped ... ok
test tests::harden003_interrupted_stream_records_nothing ... ok
test tests::harden003_late_break_records_nothing ... ok
test tests::harden003_sse_replay_ignores_chaos ... ok
test tests::harden003_record_streams_incrementally_and_preserves_event_fields ... ok
test tests::harden003_malformed_stream_lines_are_ignored ... ok
test tests::harden003_v1_recordings_still_replay ... ok
test tests::harden004_old_nodes_replay_as_200 ... ok
test tests::harden005_retry_after_injected_error_matches_same_node ... ok
test tests::harden004_text_error_round_trips ... ok
test tests::harden005_different_sessions_run_in_parallel ... ok
test tests::harden005_sse_pump_guard_keeps_session_chain_linear ... ok
test tests::harden004_error_statuses_round_trip ... ok
test tests::harden007_responses_and_mcp_http_routes_record_and_replay ... ok
test tests::mcp_feature_extractor_handles_tools_call_and_sampling ... ok
test tests::proxy_http_mcp_prompts_get_param_change_misses ... ok
test tests::proxy_injects_http_429_with_retry_after_header_and_preserves_parent_id ... ok
test tests::proxy_returns_200_on_exact_and_fuzzy_match ... ok
test tests::proxy_returns_412_on_safety_critical_amount_mutation ... ok
test tests::proxy_sessions_have_independent_dag_parents ... ok
test tests::record_mode_forwards_json_and_records_node ... ok
test tests::harden005_same_session_records_chain_linearly ... ok
test tests::harden008_http_replay_supports_all_chaos_profiles ... ok
test tests::session_key_parses_header_and_ignores_blank ... ok
test tests::record_mode_records_errors_and_passes_them_through ... ok
test tests::harden005_sessionless_records_chain_linearly ... ok
test tests::record_mode_records_sse_events_and_replays_as_event_stream ... ok

test result: ok. 49 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.50s

     Running unittests src/lib.rs (target/debug/deps/chronostrace_scrub-8317098b73108f09)

running 7 tests
test tests::jwt_pattern_and_anthropic_label ... ok
test tests::passwd_and_pwd_keys_redact ... ok
test tests::custom_patterns_redact_with_custom_label ... ok
test tests::private_key_block_redacts ... ok
test tests::short_low_entropy_unkeyed_secret_survives_by_design ... ok
test tests::token_key_rules_redact_tokens_but_not_usage_counts ... ok
test tests::scrubs_known_patterns_headers_and_high_entropy_secrets ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s

   Doc-tests chronostrace_chaos

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests chronostrace_core

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests chronostrace_proxy

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests chronostrace_scrub

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```
- Exit Code: 0
- Duration: 14s

