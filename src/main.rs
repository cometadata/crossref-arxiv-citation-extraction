mod cli;
mod commands;
mod common;
mod extract;
mod index;
mod input;
mod streaming;
mod validation;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Commands};
use commands::{run_build_index, run_pipeline, BuildIndexArgs};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pipeline(args) => {
            run_pipeline(*args)?;
        }
        Commands::BuildIndex {
            input,
            output,
            log_level,
        } => {
            let args = BuildIndexArgs {
                input,
                output,
                log_level,
            };
            run_build_index(args)?;
        }
    }

    Ok(())
}
