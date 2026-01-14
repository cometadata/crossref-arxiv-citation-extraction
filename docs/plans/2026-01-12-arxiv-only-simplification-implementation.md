# arXiv-Only Simplification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove Crossref and DataCite DOI extraction, keeping only arXiv reference extraction from Crossref snapshots with validation against arXiv (DataCite) FST index.

**Architecture:** Strip multi-source support, keep only scalable pipeline mode, rename all "datacite" references to "arxiv" throughout codebase.

**Tech Stack:** Rust, Clap CLI, FST indexes, Polars, Parquet

---

## Task 1: Simplify CLI - Remove Source Enum

**Files:**
- Modify: `src/cli.rs`

**Step 1: Remove Source enum and --source flag**

Delete lines 5-40 (Source enum and implementations) and update PipelineArgs to remove source field.

```rust
// Delete this entire block (lines 5-40):
// #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
// pub enum Source { ... }
// impl FromStr for Source { ... }
// impl std::fmt::Display for Source { ... }
```

**Step 2: Remove source field from PipelineArgs**

In PipelineArgs struct, delete:
```rust
    /// Source to extract: all, crossref, datacite, arxiv
    #[arg(long, default_value = "all")]
    pub source: Source,
```

**Step 3: Run cargo check**

Run: `cargo check 2>&1 | head -100`
Expected: Errors about missing Source type (will fix in subsequent tasks)

**Step 4: Commit**

```bash
git add src/cli.rs
git commit -m "refactor: remove Source enum from CLI"
```

---

## Task 2: Rename DataCite CLI Options to arXiv

**Files:**
- Modify: `src/cli.rs`

**Step 1: Rename datacite_records to arxiv_records**

Change in PipelineArgs:
```rust
    /// arXiv records.jsonl.gz file for validation
    #[arg(long)]
    pub arxiv_records: Option<String>,
```

**Step 2: Rename datacite_fst to arxiv_fst**

Change in PipelineArgs:
```rust
    /// Path to pre-built arXiv FST index
    #[arg(long)]
    pub arxiv_fst: Option<String>,
```

**Step 3: Remove Crossref index options**

Delete from PipelineArgs:
```rust
    /// Load Crossref DOI index from Parquet file
    #[arg(long)]
    pub load_crossref_index: Option<String>,

    /// Save Crossref DOI index to Parquet file
    #[arg(long)]
    pub save_crossref_index: Option<String>,

    /// Path to pre-built Crossref FST index (for scalable mode)
    #[arg(long)]
    pub crossref_fst: Option<String>,
```

**Step 4: Remove DataCite index Parquet options (keep only FST)**

Delete from PipelineArgs:
```rust
    /// Load DataCite DOI index from Parquet file
    #[arg(long)]
    pub load_datacite_index: Option<String>,

    /// Save DataCite DOI index to Parquet file
    #[arg(long)]
    pub save_datacite_index: Option<String>,
```

**Step 5: Remove --scalable flag (always scalable)**

Delete from PipelineArgs:
```rust
    /// Use scalable pipeline with FST indexes and streaming aggregation
    #[arg(long, default_value = "false")]
    pub scalable: bool,
```

**Step 6: Remove HTTP fallback options**

Delete from PipelineArgs:
```rust
    /// Enable HTTP fallback for specified sources (comma-separated: crossref,datacite)
    #[arg(long, value_delimiter = ',')]
    pub http_fallback: Vec<String>,

    /// Concurrent HTTP requests for validation
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per validation request
    #[arg(long, default_value = "5")]
    pub timeout: u64,
```

**Step 7: Run cargo check**

Run: `cargo check 2>&1 | head -100`
Expected: Many errors (fields removed, will fix in pipeline.rs)

**Step 8: Commit**

```bash
git add src/cli.rs
git commit -m "refactor: rename datacite CLI options to arxiv"
```

---

## Task 3: Add "all" Alias for Outputs

**Files:**
- Modify: `src/cli.rs`

**Step 1: Update parse_outputs to handle "all" alias**

Replace the parse_outputs method in PipelineArgs:
```rust
    /// Parse outputs into a HashSet.
    /// Supports "all" alias for all output types.
    pub fn parse_outputs(&self) -> Result<HashSet<OutputType>, String> {
        // Check for "all" alias
        if self.outputs.is_empty() || self.outputs.iter().any(|s| s.to_lowercase() == "all") {
            let mut all = HashSet::new();
            all.insert(OutputType::Valid);
            all.insert(OutputType::Failed);
            all.insert(OutputType::Publisher);
            all.insert(OutputType::Crossref);
            all.insert(OutputType::Mined);
            return Ok(all);
        }

        // Auto-link outputs to provenance filter
        let mut outputs = HashSet::new();

        // Always include valid and failed
        outputs.insert(OutputType::Valid);
        outputs.insert(OutputType::Failed);

        if self.provenance.is_empty() {
            // No provenance filter - include all provenance outputs
            outputs.insert(OutputType::Publisher);
            outputs.insert(OutputType::Crossref);
            outputs.insert(OutputType::Mined);
        } else {
            // Link provenance filter to outputs
            for prov in &self.provenance {
                match prov.to_lowercase().as_str() {
                    "publisher" => { outputs.insert(OutputType::Publisher); }
                    "crossref" => { outputs.insert(OutputType::Crossref); }
                    "mined" => { outputs.insert(OutputType::Mined); }
                    _ => {}
                }
            }
        }

        Ok(outputs)
    }
```

**Step 2: Add test for "all" alias**

Add to mod tests:
```rust
    #[test]
    fn test_outputs_all_alias() {
        let args = PipelineArgs {
            input: "test.tar.gz".to_string(),
            arxiv_records: None,
            arxiv_fst: None,
            provenance: vec![],
            outputs: vec!["all".to_string()],
            output_dir: None,
            partitions_dir: None,
            log_level: "INFO".to_string(),
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
        };
        let outputs = args.parse_outputs().unwrap();
        assert!(outputs.contains(&OutputType::Valid));
        assert!(outputs.contains(&OutputType::Failed));
        assert!(outputs.contains(&OutputType::Publisher));
        assert!(outputs.contains(&OutputType::Crossref));
        assert!(outputs.contains(&OutputType::Mined));
    }
```

**Step 3: Run tests**

Run: `cargo test test_outputs_all_alias`
Expected: PASS

**Step 4: Commit**

```bash
git add src/cli.rs
git commit -m "feat: add 'all' alias for --outputs flag"
```

---

## Task 4: Remove Validate Command from CLI

**Files:**
- Modify: `src/cli.rs`

**Step 1: Delete ValidateArgs struct**

Delete the entire ValidateArgs struct and its impl block (lines 250-322).

**Step 2: Remove Validate from Commands enum**

Change Commands enum to:
```rust
#[derive(Subcommand)]
pub enum Commands {
    /// Run the pipeline: extract arXiv IDs, aggregate by cited work, validate
    ///
    /// Streams through the Crossref tar.gz archive, extracts arXiv references,
    /// partitions by arXiv ID prefix, aggregates in parallel, and validates against
    /// arXiv index.
    Pipeline(Box<PipelineArgs>),

    /// Build FST index from arXiv (DataCite) records
    BuildIndex {
        /// Input path (tar.gz, directory, or JSONL file)
        #[arg(short, long)]
        input: String,

        /// Output FST file path
        #[arg(short, long)]
        output: String,

        /// Log level
        #[arg(long, default_value = "INFO")]
        log_level: String,
    },
}
```

**Step 3: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: Errors in main.rs about missing Validate variant

**Step 4: Commit**

```bash
git add src/cli.rs
git commit -m "refactor: remove Validate command from CLI"
```

---

## Task 5: Update main.rs

**Files:**
- Modify: `src/main.rs`

**Step 1: Remove validate import and match arm**

Update main.rs:
```rust
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
```

**Step 2: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: Errors in commands/mod.rs

**Step 3: Commit**

```bash
git add src/main.rs
git commit -m "refactor: remove validate command from main"
```

---

## Task 6: Delete validate.rs and Update commands/mod.rs

**Files:**
- Delete: `src/commands/validate.rs`
- Modify: `src/commands/mod.rs`

**Step 1: Delete validate.rs**

Run: `rm src/commands/validate.rs`

**Step 2: Update commands/mod.rs**

```rust
pub mod build_index;
pub mod pipeline;

pub use build_index::{run_build_index, BuildIndexArgs};
pub use pipeline::run_pipeline;
```

**Step 3: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: Errors in pipeline.rs about Source type

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove validate command module"
```

---

## Task 7: Simplify build_index.rs for arXiv Only

**Files:**
- Modify: `src/commands/build_index.rs`

**Step 1: Remove source_type and simplify to arXiv only**

Replace entire file:
```rust
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
    build_fst_index_from_source(source, "id", output_path, &temp_dir)?;

    info!("arXiv FST index built successfully: {}", args.output);

    Ok(())
}
```

**Step 2: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: Errors in pipeline.rs (next task)

**Step 3: Commit**

```bash
git add src/commands/build_index.rs
git commit -m "refactor: simplify build_index for arxiv only"
```

---

## Task 8: Rewrite pipeline.rs for arXiv-Only Scalable Mode

**Files:**
- Modify: `src/commands/pipeline.rs`

This is a large refactor. The new pipeline.rs should:
1. Only extract arXiv IDs (remove DOI extraction)
2. Only use scalable mode (remove legacy path)
3. Use arxiv_fst and arxiv_records instead of datacite
4. Remove all Source type references

**Step 1: Replace imports section**

```rust
use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::cli::PipelineArgs;
use crate::common::{extract_input_stem, setup_logging, OutputPaths};
use crate::extract::{extract_arxiv_matches_from_text, Provenance};
use crate::index::{build_fst_index_from_source, FstIndex};
use crate::input::{detect_crossref_input, detect_datacite_input, open_crossref_source, open_datacite_source};
use crate::streaming::{aggregate_and_validate, PartitionRow, SegmentedPartitionWriter};
```

**Step 2: Simplify helper functions**

Keep only:
- `has_searchable_content`
- `build_search_text`
- `should_include_citation`
- `determine_provenance`
- `quick_arxiv_likely`

Remove:
- `quick_doi_likely`

**Step 3: Replace run_pipeline function**

```rust
pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting arXiv citation extraction pipeline");
    info!("Input: {}", args.input);
    if !args.provenance.is_empty() {
        info!("Provenance filter: {}", args.provenance.join(", "));
    }

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    run_scalable_pipeline(&args)
}
```

**Step 4: Simplify validate_args**

```rust
fn validate_args(args: &PipelineArgs) -> Result<()> {
    // arXiv mode requires arXiv records or FST index
    if args.arxiv_records.is_none() && args.arxiv_fst.is_none() {
        return Err(anyhow::anyhow!(
            "arXiv extraction requires --arxiv-records or --arxiv-fst"
        ));
    }
    Ok(())
}
```

**Step 5: Simplify run_scalable_pipeline**

```rust
fn run_scalable_pipeline(args: &PipelineArgs) -> Result<()> {
    info!("Running arXiv extraction pipeline");

    // Determine partition directory
    let (partition_dir, skip_extraction) = if let Some(ref existing_dir) = args.partitions_dir {
        let path = PathBuf::from(existing_dir);
        if !path.exists() {
            return Err(anyhow::anyhow!(
                "Partitions directory does not exist: {}",
                existing_dir
            ));
        }
        info!("Using existing partitions from: {}", existing_dir);
        (path, true)
    } else {
        let path = args
            .temp_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!("arxiv-extract-{}", Uuid::new_v4()))
            });
        fs::create_dir_all(&path)?;
        (path, false)
    };

    // Load or build arXiv FST index
    let arxiv_fst = load_or_build_arxiv_fst(args, &partition_dir)?;

    // Phase 1: Extraction
    if skip_extraction {
        info!("=== Skipping Extraction (using existing partitions) ===");
    } else {
        info!("=== Extraction Phase ===");
        run_extraction(args, &partition_dir)?;
    }

    // Parse output selection
    let output_types = args.parse_outputs().map_err(|e| anyhow::anyhow!(e))?;

    let output_dir = args
        .output_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    let input_stem = extract_input_stem(&args.input);
    let outputs = OutputPaths::from_selection(&output_types, &output_dir, &input_stem);

    if !outputs.has_any() {
        return Err(anyhow::anyhow!(
            "No outputs specified. Use --outputs to specify at least one output type."
        ));
    }

    // Phase 2: Aggregation with validation
    info!("=== Aggregation Phase ===");

    let stats = aggregate_and_validate(
        &partition_dir,
        None, // No Crossref index
        arxiv_fst.as_ref(),
        &outputs,
    )?;

    info!("Pipeline complete:");
    info!("  Valid: {}", stats.valid_count);
    info!("  Failed: {}", stats.failed_count);
    info!("  Total citations: {}", stats.total_citations);

    let has_provenance_outputs =
        outputs.publisher.is_some() || outputs.crossref.is_some() || outputs.mined.is_some();
    if has_provenance_outputs {
        info!("  By provenance:");
        info!("    Publisher: {}", stats.publisher_citations);
        info!("    Crossref: {}", stats.crossref_citations);
        info!("    Mined: {}", stats.mined_citations);
    }

    // Log output paths
    if let Some(ref p) = outputs.valid {
        info!("Output valid: {}", p.display());
    }
    if let Some(ref p) = outputs.failed {
        info!("Output failed: {}", p.display());
    }
    if let Some(ref p) = outputs.publisher {
        info!("Output publisher: {}", p.display());
    }
    if let Some(ref p) = outputs.crossref {
        info!("Output crossref: {}", p.display());
    }
    if let Some(ref p) = outputs.mined {
        info!("Output mined: {}", p.display());
    }

    // Cleanup if needed
    if !args.keep_intermediates && args.partitions_dir.is_none() {
        info!("Cleaning up partition directory: {}", partition_dir.display());
        if let Err(e) = fs::remove_dir_all(&partition_dir) {
            warn!("Failed to cleanup partition directory: {}", e);
        }
    }

    Ok(())
}
```

**Step 6: Add load_or_build_arxiv_fst**

```rust
fn load_or_build_arxiv_fst(args: &PipelineArgs, temp_dir: &Path) -> Result<Option<FstIndex>> {
    if let Some(ref path) = args.arxiv_fst {
        info!("Loading arXiv FST from: {}", path);
        return Ok(Some(FstIndex::load(Path::new(path))?));
    }

    if let Some(ref records_path) = args.arxiv_records {
        info!("Building arXiv FST from: {}", records_path);
        let fst_path = temp_dir.join("arxiv.fst");
        let input = detect_datacite_input(records_path)?;
        let source = open_datacite_source(input)?;
        build_fst_index_from_source(source, "id", &fst_path, temp_dir)?;
        return Ok(Some(FstIndex::load(&fst_path)?));
    }

    Ok(None)
}
```

**Step 7: Simplify run_extraction to arXiv only**

```rust
/// Progress logging interval (every N files)
const PROGRESS_LOG_INTERVAL: usize = 100;
/// Divisor for computing flush threshold from batch size
const FLUSH_THRESHOLD_DIVISOR: usize = 100;

fn run_extraction(args: &PipelineArgs, partition_dir: &Path) -> Result<()> {
    let flush_threshold = (args.batch_size / FLUSH_THRESHOLD_DIVISOR).max(10000);
    let mut partition_writer = SegmentedPartitionWriter::new(partition_dir, flush_threshold)?;

    let input = detect_crossref_input(&args.input)?;
    let source = open_crossref_source(input)?;

    let mut items_processed = 0u64;
    let mut refs_extracted = 0u64;
    let mut refs_filtered_provenance = 0u64;
    let mut refs_filtered_self_cite = 0u64;
    let mut files_processed = 0usize;

    info!("Extracting arXiv references from Crossref archive...");

    for entry_result in source {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read entry: {}", e);
                continue;
            }
        };

        debug!("Processing: {}", entry.filename);
        let json = entry.json;

        if let Some(items) = json.get("items").and_then(|v| v.as_array()) {
            for item in items {
                items_processed += 1;

                let work_doi = match item.get("DOI").and_then(|v| v.as_str()) {
                    Some(doi) => doi.to_lowercase(),
                    None => continue,
                };

                if let Some(references) = item.get("reference").and_then(|v| v.as_array()) {
                    for (ref_idx, reference) in references.iter().enumerate() {
                        let ref_index = ref_idx as u32;

                        // Filter chain
                        if !has_searchable_content(reference) {
                            continue;
                        }

                        let search_text = match build_search_text(reference) {
                            Some(text) => text,
                            None => continue,
                        };

                        if !quick_arxiv_likely(&search_text) {
                            continue;
                        }

                        let ref_json = reference.to_string();

                        // Extract arXiv IDs and convert to DOIs
                        let matches = extract_arxiv_as_dois(&search_text);

                        for (doi, _raw) in matches {
                            // Filter self-citations
                            if !should_include_citation(&work_doi, &doi) {
                                refs_filtered_self_cite += 1;
                                continue;
                            }

                            let provenance = determine_provenance(reference, &doi);

                            // Filter by provenance
                            if !args.should_include_provenance(provenance.as_str()) {
                                refs_filtered_provenance += 1;
                                continue;
                            }

                            partition_writer.write(PartitionRow {
                                citing_doi: work_doi.clone(),
                                ref_index,
                                cited_id: doi.clone(),
                                provenance: provenance.as_str().to_string(),
                                ref_json: ref_json.clone(),
                            })?;
                            refs_extracted += 1;
                        }
                    }
                }

                if items_processed % 100_000 == 0 {
                    info!(
                        "Progress: {} items, {} extracted, {} filtered (provenance: {}, self-cite: {})",
                        items_processed, refs_extracted,
                        refs_filtered_provenance + refs_filtered_self_cite,
                        refs_filtered_provenance, refs_filtered_self_cite
                    );
                }
            }
        }

        files_processed += 1;
        if files_processed % PROGRESS_LOG_INTERVAL == 0 {
            info!("Progress: {} files processed", files_processed);
        }
    }

    partition_writer.flush_all()?;

    info!("Extraction complete:");
    info!("  Files processed: {}", files_processed);
    info!("  Items processed: {}", items_processed);
    info!("  Refs extracted: {}", refs_extracted);
    info!("  Refs filtered by provenance: {}", refs_filtered_provenance);
    info!("  Refs filtered (self-citations): {}", refs_filtered_self_cite);

    Ok(())
}

fn extract_arxiv_as_dois(text: &str) -> Vec<(String, String)> {
    extract_arxiv_matches_from_text(text)
        .into_iter()
        .map(|m| (format!("10.48550/arXiv.{}", m.id), m.raw))
        .collect()
}
```

**Step 8: Keep helper functions (simplified)**

```rust
#[inline]
fn has_searchable_content(reference: &Value) -> bool {
    reference.get("DOI").is_some()
        || reference.get("unstructured").is_some()
        || reference.get("URL").is_some()
        || reference.get("article-title").is_some()
        || reference.get("journal-title").is_some()
}

#[inline]
fn quick_arxiv_likely(text: &str) -> bool {
    let text_lower = text.to_ascii_lowercase();
    text_lower.contains("arxiv")
}

fn build_search_text(reference: &Value) -> Option<String> {
    let mut text = String::new();

    if let Some(doi) = reference.get("DOI").and_then(|v| v.as_str()) {
        text.push_str(doi);
        text.push(' ');
    }

    if let Some(url) = reference.get("URL").and_then(|v| v.as_str()) {
        text.push_str(url);
        text.push(' ');
    }

    if let Some(title) = reference.get("article-title").and_then(|v| v.as_str()) {
        text.push_str(title);
        text.push(' ');
    }

    if let Some(journal) = reference.get("journal-title").and_then(|v| v.as_str()) {
        text.push_str(journal);
        text.push(' ');
    }

    if let Some(unstructured) = reference.get("unstructured").and_then(|v| v.as_str()) {
        text.push_str(unstructured);
    }

    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn should_include_citation(citing_doi: &str, cited_id: &str) -> bool {
    citing_doi.to_lowercase() != cited_id.to_lowercase()
}

fn determine_provenance(reference: &Value, extracted_doi: &str) -> Provenance {
    if let Some(doi_field) = reference.get("DOI").and_then(|v| v.as_str()) {
        let doi_field_normalized = doi_field.to_lowercase();
        let extracted_normalized = extracted_doi.to_lowercase();

        if doi_field_normalized == extracted_normalized {
            if let Some(asserted_by) = reference.get("doi-asserted-by").and_then(|v| v.as_str()) {
                return match asserted_by {
                    "publisher" => Provenance::Publisher,
                    "crossref" => Provenance::Crossref,
                    _ => Provenance::Mined,
                };
            }
            return Provenance::Mined;
        }
    }

    Provenance::Mined
}
```

**Step 9: Update tests**

Remove all tests that reference Source enum and update remaining tests.

**Step 10: Run cargo check**

Run: `cargo check`
Expected: PASS (or errors in other files we'll fix next)

**Step 11: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "refactor: simplify pipeline for arxiv-only extraction"
```

---

## Task 9: Delete DOI Extraction Module

**Files:**
- Delete: `src/extract/doi.rs`
- Modify: `src/extract/mod.rs`

**Step 1: Delete doi.rs**

Run: `rm src/extract/doi.rs`

**Step 2: Update extract/mod.rs**

```rust
pub mod arxiv;
mod provenance;

pub use arxiv::*;
pub use provenance::Provenance;
```

**Step 3: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: Errors about missing doi_prefix function

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove DOI extraction module"
```

---

## Task 10: Fix doi_prefix References in Index Module

**Files:**
- Modify: `src/index/mod.rs`

**Step 1: Add arxiv_prefix function locally**

The DoiIndex uses `doi_prefix` from extract. Since we're keeping DoiIndex for now (it's used by legacy code paths), add a local helper or simplify.

Actually, looking at the code, DoiIndex is only used by the non-scalable path which we're removing. Let's check if we can simplify.

For now, add the doi_prefix function locally:

```rust
/// Extract prefix from a DOI (e.g., "10.1234" from "10.1234/example")
fn doi_prefix(doi: &str) -> Option<String> {
    let parts: Vec<&str> = doi.splitn(2, '/').collect();
    if parts.len() == 2 && parts[0].starts_with("10.") {
        Some(parts[0].to_lowercase())
    } else {
        None
    }
}
```

**Step 2: Update DoiIndex::insert to use local function**

Change line 36:
```rust
        if let Some(prefix) = doi_prefix(&doi_lower) {
```

**Step 3: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: PASS or other errors to fix

**Step 4: Commit**

```bash
git add src/index/mod.rs
git commit -m "fix: add local doi_prefix function to index module"
```

---

## Task 11: Delete HTTP Validation Module

**Files:**
- Delete: `src/validation/http.rs`
- Delete: `src/validation/prefix_filter.rs`
- Delete: `src/validation/runner.rs`
- Modify: `src/validation/mod.rs`

**Step 1: Delete files**

```bash
rm src/validation/http.rs
rm src/validation/prefix_filter.rs
rm src/validation/runner.rs
```

**Step 2: Update validation/mod.rs**

```rust
pub mod lookup;

pub use lookup::*;
```

**Step 3: Run cargo check**

Run: `cargo check 2>&1 | head -50`
Expected: Errors about missing validate_citations etc.

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove HTTP validation modules"
```

---

## Task 12: Simplify validation/lookup.rs

**Files:**
- Modify: `src/validation/lookup.rs`

**Step 1: Simplify to arXiv-only lookup**

Replace with simplified version:
```rust
use crate::index::FstIndex;

/// Result of an arXiv DOI lookup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupResult {
    /// DOI found in arXiv index
    Found,
    /// DOI not found
    NotFound,
}

/// Look up an arXiv DOI in the index
pub fn lookup_arxiv(doi: &str, arxiv_index: Option<&FstIndex>) -> LookupResult {
    let doi_lower = doi.to_lowercase();

    if let Some(idx) = arxiv_index {
        if idx.contains(&doi_lower) {
            return LookupResult::Found;
        }
    }

    LookupResult::NotFound
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::FstIndexBuilder;
    use tempfile::tempdir;

    #[test]
    fn test_lookup_arxiv_found() {
        let dir = tempdir().unwrap();
        let fst_path = dir.path().join("test.fst");

        {
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.48550/arxiv.2403.12345").unwrap();
            builder.finish().unwrap();
        }

        let index = FstIndex::load(&fst_path).unwrap();
        let result = lookup_arxiv("10.48550/arXiv.2403.12345", Some(&index));
        assert_eq!(result, LookupResult::Found);
    }

    #[test]
    fn test_lookup_arxiv_not_found() {
        let dir = tempdir().unwrap();
        let fst_path = dir.path().join("test.fst");

        {
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.48550/arxiv.2403.12345").unwrap();
            builder.finish().unwrap();
        }

        let index = FstIndex::load(&fst_path).unwrap();
        let result = lookup_arxiv("10.48550/arXiv.9999.99999", Some(&index));
        assert_eq!(result, LookupResult::NotFound);
    }
}
```

**Step 2: Run cargo check**

Run: `cargo check`
Expected: PASS

**Step 3: Run tests**

Run: `cargo test lookup`
Expected: PASS

**Step 4: Commit**

```bash
git add src/validation/lookup.rs
git commit -m "refactor: simplify validation lookup for arxiv only"
```

---

## Task 13: Update lib.rs Exports

**Files:**
- Modify: `src/lib.rs`

**Step 1: Keep exports minimal**

```rust
pub mod cli;
pub mod common;
pub mod extract;
pub mod index;
pub mod input;
pub mod streaming;
pub mod validation;
```

No changes needed - this is already minimal.

**Step 2: Run cargo check**

Run: `cargo check`
Expected: PASS

**Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "chore: verify lib.rs exports"
```

---

## Task 14: Update Integration Tests

**Files:**
- Modify: `tests/integration_test.rs`

**Step 1: Remove tests that use --source flag**

Delete tests:
- `test_crossref_mode_extraction`
- `test_datacite_requires_index` (if exists)
- Any test using `--source` argument

**Step 2: Update remaining tests to use arXiv flags**

Change `--datacite-records` to `--arxiv-records`
Change `--datacite-fst` to `--arxiv-fst`
Remove `--source` flags

**Step 3: Add new arXiv-focused test**

```rust
#[test]
fn test_arxiv_extraction_end_to_end() {
    use crossref_citation_extraction::index::FstIndexBuilder;
    use std::fs;

    let dir = tempdir().unwrap();

    // Create test Crossref JSON with arXiv references
    let crossref_dir = dir.path().join("crossref");
    fs::create_dir_all(&crossref_dir).unwrap();

    let crossref_file = crossref_dir.join("test.json");
    let crossref_data = serde_json::json!({
        "items": [
            {
                "DOI": "10.1111/citing1",
                "reference": [
                    {"unstructured": "See arXiv:2403.12345 for details"},
                    {"DOI": "10.48550/arXiv.2403.67890", "doi-asserted-by": "publisher"}
                ]
            }
        ]
    });
    fs::write(&crossref_file, serde_json::to_string_pretty(&crossref_data).unwrap()).unwrap();

    // Create arXiv FST index
    let fst_path = dir.path().join("arxiv.fst");
    {
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.48550/arxiv.2403.12345").unwrap();
        builder.insert("10.48550/arxiv.2403.67890").unwrap();
        builder.finish().unwrap();
    }

    let output_path = dir.path().join("crossref_valid.jsonl");
    let partition_dir = dir.path().join("partitions");

    let status = Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input", crossref_dir.to_str().unwrap(),
            "--outputs", "valid",
            "--output-dir", dir.path().to_str().unwrap(),
            "--arxiv-fst", fst_path.to_str().unwrap(),
            "--temp-dir", partition_dir.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to execute command");

    assert!(status.success(), "Pipeline failed");
    assert!(output_path.exists(), "Output file should exist");

    let output_content = fs::read_to_string(&output_path).unwrap();
    assert!(output_content.contains("10.48550/arXiv.2403.12345") ||
            output_content.contains("10.48550/arxiv.2403.12345"));
}
```

**Step 4: Run tests**

Run: `cargo test --test integration_test`
Expected: PASS (some tests may fail if they use removed features)

**Step 5: Commit**

```bash
git add tests/integration_test.rs
git commit -m "test: update integration tests for arxiv-only mode"
```

---

## Task 15: Clean Up Unused Dependencies

**Files:**
- Modify: `Cargo.toml`

**Step 1: Check for unused deps**

Run: `cargo build --release 2>&1 | grep -i "unused"`

**Step 2: Remove reqwest if no longer needed**

If HTTP validation is removed, reqwest may be unused:
```toml
# Remove or comment out:
# reqwest = { version = "0.12", features = ["rustls-tls"], default-features = false }
```

**Step 3: Run cargo build**

Run: `cargo build --release`
Expected: PASS

**Step 4: Commit**

```bash
git add Cargo.toml
git commit -m "chore: remove unused dependencies"
```

---

## Task 16: Run Full Test Suite and Fix Issues

**Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 2: Fix any failing tests**

Address each failure individually.

**Step 3: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No errors

**Step 4: Run fmt**

Run: `cargo fmt`

**Step 5: Final commit**

```bash
git add -A
git commit -m "chore: fix tests and apply formatting"
```

---

## Task 17: Update CLAUDE.md Documentation

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Update architecture section**

Update to reflect arXiv-only focus:

```markdown
## Architecture

This is a Rust CLI that extracts arXiv references from Crossref snapshot data files, aggregates citations by cited arXiv work, and validates them against arXiv (DataCite) records.

### Data Flow

```
Crossref tar.gz → Extract arXiv IDs → Partition by arXiv prefix → Aggregate by cited work → Validate against arXiv index → Output
```
```

**Step 2: Update CLI examples**

Remove `--source` examples, update to use `--arxiv-fst` and `--arxiv-records`.

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md for arxiv-only architecture"
```

---

## Summary

After completing all tasks:

1. **CLI simplified**: No `--source` flag, `--datacite-*` renamed to `--arxiv-*`, `--outputs all` alias added
2. **Scalable-only**: Legacy pipeline path removed, always uses FST + streaming
3. **arXiv extraction only**: DOI extraction removed, only arXiv IDs extracted
4. **Validation simplified**: HTTP fallback removed, FST lookup only
5. **Clean codebase**: Unused modules deleted, tests updated, documentation current
