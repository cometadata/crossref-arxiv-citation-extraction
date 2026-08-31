use anyhow::Result;
use clap::Parser;

use crossref_citation_extraction::cli::{Cli, Commands};
use crossref_citation_extraction::commands::{run_build_index, run_pipeline, BuildIndexArgs};

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
            run_build_index(BuildIndexArgs {
                input,
                output,
                log_level,
            })?;
        }
    }

    Ok(())
}
