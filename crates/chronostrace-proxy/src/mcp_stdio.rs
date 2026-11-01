use chronostrace_chaos::ChaosEngine;
use std::path::PathBuf;

pub struct McpStdioConfig {
    pub cassette_path: PathBuf,
    pub similarity_threshold: f64,
    pub chaos: Option<ChaosEngine>,
}

pub async fn run_mcp_stdio_wrapper(_config: McpStdioConfig) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
