use anyhow::Result;
use log::info;
use std::path::Path;

use crate::common::setup_logging;
use crate::index::build_fst_index_from_source;
use crate::input::{detect_datacite_input, open_datacite_source};

/// Arguments for build-index command
#[derive(Debug, Clone)]
pub struct BuildIndexArgs {
    pub input: String,
    pub output: String,
    pub log_level: String,
}

pub fn run_build_index(args: BuildIndexArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Building arXiv FST index");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);

    let temp_dir = std::env::temp_dir();
    let output_path = Path::new(&args.output);

    let input = detect_datacite_input(&args.input)?;
    let source = open_datacite_source(input)?;
    build_fst_index_from_source(source, output_path, &temp_dir)?;

    info!("arXiv FST index built successfully: {}", args.output);

    Ok(())
}
