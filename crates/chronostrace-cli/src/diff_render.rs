use chronostrace_core::diff::{DiffKind, TrajectoryDiffReport};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const CYAN: &str = "\x1b[36m";

pub fn render_terminal_diff(report: &TrajectoryDiffReport) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "\n{BOLD}ChronosTrace Trajectory Comparison{RESET}\n"
    ));
    out.push_str(&format!(
        "  Baseline : {CYAN}{}{RESET} ({} steps, {} ms)\n",
        report.baseline_id, report.total_baseline_steps, report.baseline_total_latency_ms
    ));
    out.push_str(&format!(
        "  Candidate: {CYAN}{}{RESET} ({} steps, {} ms)\n\n",
        report.candidate_id, report.total_candidate_steps, report.candidate_total_latency_ms
    ));

    for (idx, step) in report.steps.iter().enumerate() {
        let is_last = idx + 1 == report.steps.len();
        let branch_glyph = if is_last { "└──" } else { "├──" };
        let pipe_glyph = if is_last { "   " } else { "│  " };

        let (badge, color) = match step.kind {
            DiffKind::Identical => ("EXACT MATCH", GREEN),
            DiffKind::PromptDriftReconciled => ("PROMPT DRIFT (RECONCILED)", YELLOW),
            DiffKind::ToolSchemaMutated => ("TOOL SCHEMA MUTATED", RED),
            DiffKind::ToolArgumentMutated => ("TOOL ARG MUTATED", RED),
            DiffKind::ResponseDiverged => ("RESPONSE DIVERGED", RED),
            DiffKind::TopologyDiverged => ("TOPOLOGY DIVERGED", RED),
        };

        let latency_delta = (step.candidate_latency_ms as i64) - (step.baseline_latency_ms as i64);
        let latency_str = if latency_delta >= 0 {
            format!("+{latency_delta}ms")
        } else {
            format!("{latency_delta}ms")
        };

        out.push_str(&format!(
            "{branch_glyph} Step {:02} [{color}{BOLD}{badge}{RESET}] {BOLD}{}{RESET} {DIM}(sim={:.2}, Δt={latency_str}){RESET}\n",
            step.step_index + 1,
            step.target_name,
            step.prompt_similarity
        ));

        if matches!(
            step.kind,
            DiffKind::ToolSchemaMutated
                | DiffKind::ToolArgumentMutated
                | DiffKind::TopologyDiverged
        ) {
            for delta in &step.argument_deltas {
                out.push_str(&format!(
                    "{pipe_glyph}   {RED}- {}: {}{RESET}\n",
                    delta.path, delta.baseline_value
                ));
                out.push_str(&format!(
                    "{pipe_glyph}   {GREEN}+ {}: {}{RESET}\n",
                    delta.path, delta.candidate_value
                ));
            }
        }

        if step.kind == DiffKind::ResponseDiverged {
            for delta in step.response_deltas.iter().take(4) {
                out.push_str(&format!(
                    "{pipe_glyph}   {YELLOW}~ {}: {} => {}{RESET}\n",
                    delta.path, delta.baseline_value, delta.candidate_value
                ));
            }
        }
    }

    let verdict = if report.is_compatible() {
        format!("{GREEN}{BOLD}PASS (Trajectory Compatible){RESET}")
    } else {
        format!("{RED}{BOLD}FAIL (Breaking Trajectory Divergence Detected){RESET}")
    };

    out.push_str(&format!(
        "\n{BOLD}Summary:{RESET} {} identical, {} reconciled drift, {} arg/schema mutations, {} response diffs, {} topology breaks -> {verdict}\n",
        report.identical_count,
        report.reconciled_drift_count,
        report.mutated_args_count,
        report.response_diverged_count,
        report.topology_diverged_count
    ));

    out
}

pub fn render_markdown_diff(report: &TrajectoryDiffReport) -> String {
    let status_icon = if report.is_compatible() { "✅" } else { "❌" };
    let mut md = format!(
        "### {status_icon} ChronosTrace Trajectory Diff: `{}` vs `{}`\n\n",
        report.baseline_id, report.candidate_id
    );

    md.push_str("| Step | Target | Classification | Prompt Sim | Latency Delta |\n");
    md.push_str("|---:|---|---|---:|---:|\n");

    for step in &report.steps {
        let label = match step.kind {
            DiffKind::Identical => "🟢 Identical",
            DiffKind::PromptDriftReconciled => "🟡 Prompt Drift (Safe)",
            DiffKind::ToolSchemaMutated => "🔴 Tool Schema Mutated",
            DiffKind::ToolArgumentMutated => "🔴 Tool Arg Mutated",
            DiffKind::ResponseDiverged => "🔴 Response Diverged",
            DiffKind::TopologyDiverged => "🔴 Topology Diverged",
        };
        let delta_ms = (step.candidate_latency_ms as i64) - (step.baseline_latency_ms as i64);
        md.push_str(&format!(
            "| {} | `{}` | {} | {:.2} | {:+} ms |\n",
            step.step_index + 1,
            step.target_name,
            label,
            step.prompt_similarity,
            delta_ms
        ));
    }

    md
}
