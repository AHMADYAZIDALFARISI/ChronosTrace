use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "chronostrace")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Replay { cassette: PathBuf },
    Verify { cassette: PathBuf },
}

#[tokio::main]
async fn main() -> ExitCode {
    let _cli = Cli::parse();
    ExitCode::SUCCESS
}
