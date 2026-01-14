# Remove Old Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the old memory-heavy pipeline code, making the scalable FST-based architecture the only code path.

**Architecture:** Delete obsolete files (old index persistence, partition writer, invert, validation runner), update module declarations, remove deprecated CLI flags, and restructure pipeline.rs to use only the scalable code path. Migrate validate command to use FST indexes.

**Tech Stack:** Rust, Clap (CLI), FST (finite state transducer indexes), Polars (DataFrames)

---

## Task 1: Delete Obsolete Files

**Files:**
- Delete: `src/index/persistence.rs`
- Delete: `src/streaming/partition_writer.rs`
- Delete: `src/streaming/partition_invert.rs`
- Delete: `src/validation/runner.rs`
- Delete: `src/validation/lookup.rs`

**Step 1: Delete the files**

```bash
rm src/index/persistence.rs
rm src/streaming/partition_writer.rs
rm src/streaming/partition_invert.rs
rm src/validation/runner.rs
rm src/validation/lookup.rs
```

**Step 2: Verify files are gone**

Run: `ls src/index/ src/streaming/ src/validation/`
Expected: The deleted files should not appear in the listing

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: remove obsolete pipeline files"
```

---

## Task 2: Update Index Module

**Files:**
- Modify: `src/index/mod.rs`
- Modify: `src/index/builder.rs`

**Step 1: Remove DoiIndex and persistence from mod.rs**

Replace entire `src/index/mod.rs` with:

```rust
pub mod builder;
pub mod fst_index;

pub use builder::build_fst_index_from_source;
pub use fst_index::{build_fst_from_iter, FstIndex, FstIndexBuilder};
```

**Step 2: Remove build_index_from_source from builder.rs**

Read `src/index/builder.rs` and remove the `build_index_from_source` function (the one that builds `DoiIndex`). Keep only `build_fst_index_from_source` and related FST building code.

The function to remove has this signature:
```rust
pub fn build_index_from_source(...) -> Result<DoiIndex>
```

Also remove any imports of `DoiIndex` from this file.

**Step 3: Verify build compiles**

Run: `cargo check 2>&1 | head -50`
Expected: Errors about missing imports in other files (this is expected, we'll fix them next)

**Step 4: Commit**

```bash
git add src/index/
git commit -m "refactor: remove DoiIndex from index module"
```

---

## Task 3: Update Streaming Module

**Files:**
- Modify: `src/streaming/mod.rs`

**Step 1: Update module declarations**

Replace entire `src/streaming/mod.rs` with:

```rust
pub mod aggregator;
pub mod checkpoint;
pub mod ref_store;
pub mod segmented_writer;

pub use aggregator::{aggregate_and_validate, AggregationStats};
pub use checkpoint::Checkpoint;
pub use ref_store::{RefJsonLookup, RefJsonStore};
pub use segmented_writer::{PartitionRow, SegmentedPartitionWriter};

/// Extract partition key from a DOI or arXiv ID.
/// For DOIs: uses prefix (e.g., "10.1234" -> "10.1234")
/// For arXiv IDs: uses first 4 chars (existing behavior)
pub fn partition_key(id: &str) -> String {
    // Check if it looks like a DOI (starts with 10.)
    if id.starts_with("10.") {
        // Use the DOI prefix as partition key
        if let Some(slash_pos) = id.find('/') {
            return id[..slash_pos].to_lowercase();
        }
    }

    // Fall back to first 4 chars for arXiv IDs
    id.to_lowercase()
        .chars()
        .take(4)
        .map(|c| if c == '/' { '_' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_modern_format() {
        assert_eq!(partition_key("2403.12345"), "2403");
        assert_eq!(partition_key("2312.00001"), "2312");
        assert_eq!(partition_key("0704.0001"), "0704");
    }

    #[test]
    fn test_partition_key_old_format() {
        assert_eq!(partition_key("hep-ph/9901234"), "hep-");
        assert_eq!(partition_key("cs.dm/9910013"), "cs.d");
        assert_eq!(partition_key("astro-ph/0001001"), "astr");
        assert_eq!(partition_key("cs/9901234"), "cs_9");
        assert_eq!(partition_key("q-bio/0401001"), "q-bi");
    }

    #[test]
    fn test_partition_key_short_id() {
        assert_eq!(partition_key("abc"), "abc");
        assert_eq!(partition_key("a"), "a");
    }

    #[test]
    fn test_partition_key_doi_format() {
        assert_eq!(partition_key("10.1234/example"), "10.1234");
        assert_eq!(partition_key("10.5555/abcd.1234"), "10.5555");
        assert_eq!(partition_key("10.48550/arXiv.2403.12345"), "10.48550");
    }

    #[test]
    fn test_partition_key_doi_no_slash() {
        // DOI without slash falls back to first 4 chars
        assert_eq!(partition_key("10.1"), "10.1");
    }
}
```

**Step 2: Commit**

```bash
git add src/streaming/mod.rs
git commit -m "refactor: remove old partition writer and invert from streaming module"
```

---

## Task 4: Update Validation Module

**Files:**
- Modify: `src/validation/mod.rs`

**Step 1: Update module declarations**

Replace entire `src/validation/mod.rs` with:

```rust
pub mod http;
pub mod prefix_filter;

pub use http::*;
pub use prefix_filter::{has_known_prefix, prefix_source, PrefixMatch};
```

**Step 2: Commit**

```bash
git add src/validation/mod.rs
git commit -m "refactor: remove runner and lookup from validation module"
```

---

## Task 5: Update CLI - Remove Old Flags

**Files:**
- Modify: `src/cli.rs`

**Step 1: Update PipelineArgs struct**

Remove these fields from `PipelineArgs`:
- `scalable: bool`
- `load_crossref_index: Option<String>`
- `save_crossref_index: Option<String>`
- `load_datacite_index: Option<String>`
- `save_datacite_index: Option<String>`

The updated struct should have these fields only:

```rust
#[derive(Parser, Clone)]
pub struct PipelineArgs {
    /// Path to the Crossref snapshot tar.gz file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file for validation
    #[arg(long)]
    pub datacite_records: Option<String>,

    /// Source to extract: all, crossref, datacite, arxiv
    #[arg(long, default_value = "all")]
    pub source: Source,

    /// Output file for Crossref citations (JSONL)
    #[arg(long)]
    pub output_crossref: Option<String>,

    /// Output file for DataCite citations (JSONL)
    #[arg(long)]
    pub output_datacite: Option<String>,

    /// Output file for arXiv citations (JSONL, arxiv mode only)
    #[arg(long)]
    pub output_arxiv: Option<String>,

    /// Output file for failed Crossref validations
    #[arg(long)]
    pub output_crossref_failed: Option<String>,

    /// Output file for failed DataCite validations
    #[arg(long)]
    pub output_datacite_failed: Option<String>,

    /// Output file for failed arXiv validations
    #[arg(long)]
    pub output_arxiv_failed: Option<String>,

    /// Enable HTTP fallback for specified sources (comma-separated: crossref,datacite)
    #[arg(long, value_delimiter = ',')]
    pub http_fallback: Vec<String>,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,

    /// Concurrent HTTP requests for validation
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per validation request
    #[arg(long, default_value = "5")]
    pub timeout: u64,

    /// Keep intermediate files (partitions, temp parquet)
    #[arg(long, default_value = "false")]
    pub keep_intermediates: bool,

    /// Directory for intermediate partition files (default: system temp)
    #[arg(long)]
    pub temp_dir: Option<String>,

    /// Batch size for memory management during streaming
    #[arg(long, default_value = "5000000")]
    pub batch_size: usize,

    /// Path to pre-built Crossref FST index
    #[arg(long)]
    pub crossref_fst: Option<String>,

    /// Path to pre-built DataCite FST index
    #[arg(long)]
    pub datacite_fst: Option<String>,

    /// Directory for ref_json store
    #[arg(long)]
    pub ref_store_dir: Option<String>,
}
```

**Step 2: Update ValidateArgs struct**

Replace the existing `ValidateArgs` with:

```rust
#[derive(Parser, Clone)]
pub struct ValidateArgs {
    /// Input citations JSONL file
    #[arg(short, long, required = true)]
    pub input: String,

    /// DataCite records.jsonl.gz file (for building DataCite FST on-the-fly)
    #[arg(long)]
    pub datacite_records: Option<String>,

    /// Crossref FST index file
    #[arg(long)]
    pub crossref_fst: Option<String>,

    /// DataCite FST index file
    #[arg(long)]
    pub datacite_fst: Option<String>,

    /// Source type of the input file: crossref, datacite, arxiv
    #[arg(long, required = true)]
    pub source: Source,

    /// Output file for valid citations
    #[arg(long, required = true)]
    pub output_valid: String,

    /// Output file for failed citations
    #[arg(long, required = true)]
    pub output_failed: String,

    /// Enable HTTP fallback validation
    #[arg(long, default_value = "false")]
    pub http_fallback: bool,

    /// Concurrent HTTP requests
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Timeout in seconds per request
    #[arg(short, long, default_value = "5")]
    pub timeout: u64,

    /// Logging level (DEBUG, INFO, WARN, ERROR)
    #[arg(short, long, default_value = "INFO")]
    pub log_level: String,
}
```

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "refactor: remove old index flags from CLI"
```

---

## Task 6: Rewrite Pipeline Command

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Replace pipeline.rs with simplified version**

This is a larger change. Replace the entire file with the new implementation that only uses the scalable path. The key changes:
- Remove `run_extraction()` (old), keep `run_scalable_extraction()` renamed to `run_extraction()`
- Remove `run_scalable_pipeline()`, rename its logic to `run_pipeline()`
- Remove `load_indexes()`, `PipelineIndexes`, `should_build_crossref_index()`
- Remove all references to `DoiIndex`
- Remove the `if args.scalable` branch - scalable is now the only path

```rust
use anyhow::{Context, Result};
use log::{info, warn};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::cli::{PipelineArgs, Source};
use crate::common::setup_logging;
use crate::extract::{extract_arxiv_matches_from_text, extract_doi_matches_from_text, Provenance};
use crate::index::{build_fst_index_from_source, FstIndex};
use crate::input::{detect_crossref_input, detect_datacite_input, open_crossref_source, open_datacite_source};
use crate::streaming::{
    aggregate_and_validate, PartitionRow, RefJsonStore, SegmentedPartitionWriter,
};

/// Progress logging interval (every N items)
const PROGRESS_LOG_INTERVAL: usize = 100_000;
/// Divisor for computing flush threshold from batch size
const FLUSH_THRESHOLD_DIVISOR: usize = 100;

/// Check if a citation should be included (filters out self-citations)
fn should_include_citation(citing_doi: &str, cited_id: &str) -> bool {
    citing_doi.to_lowercase() != cited_id.to_lowercase()
}

/// Determine the provenance of a DOI based on how it was found in the reference
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

fn build_search_text(reference: &Value) -> String {
    let mut text = String::new();
    for field in &["DOI", "URL", "article-title", "journal-title", "unstructured"] {
        if let Some(value) = reference.get(*field).and_then(|v| v.as_str()) {
            text.push_str(value);
            text.push(' ');
        }
    }
    text
}

fn extract_arxiv_as_dois(text: &str) -> Vec<(String, String)> {
    extract_arxiv_matches_from_text(text)
        .into_iter()
        .map(|m| (format!("10.48550/arXiv.{}", m.id), m.raw))
        .collect()
}

fn load_or_build_crossref_fst(args: &PipelineArgs, temp_dir: &Path) -> Result<Option<FstIndex>> {
    if let Some(ref path) = args.crossref_fst {
        info!("Loading Crossref FST from: {}", path);
        return Ok(Some(FstIndex::load(Path::new(path))?));
    }

    // For Crossref, we typically need a pre-built index for the full dataset
    // If not provided and source requires it, we could build from the input
    // but that requires streaming through the archive twice
    if matches!(args.source, Source::Crossref | Source::All) {
        info!("Building Crossref FST from input archive...");
        let fst_path = temp_dir.join("crossref.fst");
        let input = detect_crossref_input(&args.input)?;
        let source = open_crossref_source(input)?;
        build_fst_index_from_source(source, "DOI", &fst_path, temp_dir)?;
        return Ok(Some(FstIndex::load(&fst_path)?));
    }

    Ok(None)
}

fn load_or_build_datacite_fst(args: &PipelineArgs, temp_dir: &Path) -> Result<Option<FstIndex>> {
    if let Some(ref path) = args.datacite_fst {
        info!("Loading DataCite FST from: {}", path);
        return Ok(Some(FstIndex::load(Path::new(path))?));
    }

    if let Some(ref records_path) = args.datacite_records {
        info!("Building DataCite FST from: {}", records_path);
        let fst_path = temp_dir.join("datacite.fst");
        let input = detect_datacite_input(records_path)?;
        let source = open_datacite_source(input)?;
        build_fst_index_from_source(source, "id", &fst_path, temp_dir)?;
        return Ok(Some(FstIndex::load(&fst_path)?));
    }

    Ok(None)
}

fn run_extraction(
    args: &PipelineArgs,
    partition_dir: &Path,
    ref_store_dir: &Path,
) -> Result<()> {
    let flush_threshold = (args.batch_size / FLUSH_THRESHOLD_DIVISOR).max(10000);
    let mut partition_writer = SegmentedPartitionWriter::new(partition_dir, flush_threshold)?;
    let mut ref_store = RefJsonStore::new(ref_store_dir, flush_threshold)?;

    let input = detect_crossref_input(&args.input)?;
    let source = open_crossref_source(input)?;

    let mut items_processed = 0;
    let mut refs_extracted = 0;

    info!("Streaming through Crossref archive...");

    for entry_result in source {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read entry: {}", e);
                continue;
            }
        };

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
                        let ref_json = reference.to_string();
                        let ref_index = ref_idx as u32;

                        // Store ref_json (deduplicated)
                        ref_store.write(&work_doi, ref_index, &ref_json)?;

                        // Build search text
                        let search_text = build_search_text(reference);
                        if search_text.is_empty() {
                            continue;
                        }

                        // Extract matches based on source mode
                        let matches = match args.source {
                            Source::Arxiv => extract_arxiv_as_dois(&search_text),
                            _ => extract_doi_matches_from_text(&search_text)
                                .into_iter()
                                .map(|m| (m.doi, m.raw))
                                .collect(),
                        };

                        for (doi, _raw) in matches {
                            if should_include_citation(&work_doi, &doi) {
                                partition_writer.write(PartitionRow {
                                    citing_doi: work_doi.clone(),
                                    ref_index,
                                    cited_id: doi.clone(),
                                    provenance: determine_provenance(reference, &doi)
                                        .as_str()
                                        .to_string(),
                                })?;
                                refs_extracted += 1;
                            }
                        }
                    }
                }

                if items_processed % PROGRESS_LOG_INTERVAL == 0 {
                    info!(
                        "Progress: {} items, {} refs extracted",
                        items_processed, refs_extracted
                    );
                }
            }
        }
    }

    partition_writer.flush_all()?;
    ref_store.flush()?;

    info!(
        "Extraction complete: {} items, {} refs",
        items_processed, refs_extracted
    );

    Ok(())
}

fn validate_args(args: &PipelineArgs) -> Result<()> {
    match args.source {
        Source::All => {
            if args.output_crossref.is_none() || args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'all' requires both --output-crossref and --output-datacite"
                ));
            }
        }
        Source::Crossref => {
            if args.output_crossref.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'crossref' requires --output-crossref"
                ));
            }
        }
        Source::Datacite => {
            if args.output_datacite.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --output-datacite"
                ));
            }
            if args.datacite_records.is_none() && args.datacite_fst.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'datacite' requires --datacite-records or --datacite-fst"
                ));
            }
        }
        Source::Arxiv => {
            if args.output_arxiv.is_none() {
                return Err(anyhow::anyhow!("Source 'arxiv' requires --output-arxiv"));
            }
            if args.datacite_records.is_none() && args.datacite_fst.is_none() {
                return Err(anyhow::anyhow!(
                    "Source 'arxiv' requires --datacite-records or --datacite-fst"
                ));
            }
        }
    }
    Ok(())
}

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting citation extraction pipeline");
    info!("Input: {}", args.input);
    info!("Source mode: {}", args.source);

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    let partition_dir = args
        .temp_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("crossref-pipeline-{}", Uuid::new_v4()))
        });

    let ref_store_dir = args
        .ref_store_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| partition_dir.join("ref_store"));

    fs::create_dir_all(&partition_dir)?;
    fs::create_dir_all(&ref_store_dir)?;

    let cleanup_temp = args.temp_dir.is_none() && !args.keep_intermediates;
    info!("Partition directory: {}", partition_dir.display());

    // Load or build FST indexes
    info!("=== Loading Indexes ===");
    let crossref_fst = load_or_build_crossref_fst(&args, &partition_dir)?;
    let datacite_fst = load_or_build_datacite_fst(&args, &partition_dir)?;

    // Phase 1: Extraction
    info!("=== Extraction Phase ===");
    run_extraction(&args, &partition_dir, &ref_store_dir)?;

    // Phase 2: Aggregation with streaming validation
    info!("=== Aggregation Phase ===");
    let valid_output = args
        .output_crossref
        .as_ref()
        .or(args.output_datacite.as_ref())
        .or(args.output_arxiv.as_ref())
        .ok_or_else(|| anyhow::anyhow!("No output path specified"))?;

    let failed_output = format!("{}.failed", valid_output);

    let stats = aggregate_and_validate(
        &partition_dir,
        &ref_store_dir,
        crossref_fst.as_ref(),
        datacite_fst.as_ref(),
        Path::new(valid_output),
        Path::new(&failed_output),
    )?;

    info!("Pipeline complete:");
    info!("  Valid: {}", stats.valid_count);
    info!("  Failed: {}", stats.failed_count);
    info!("  Total citations: {}", stats.total_citations);

    // Cleanup temp directory if needed
    if cleanup_temp {
        info!("Cleaning up temp directory: {}", partition_dir.display());
        if let Err(e) = fs::remove_dir_all(&partition_dir) {
            warn!("Failed to cleanup temp directory: {}", e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_include_citation() {
        assert!(should_include_citation("10.1234/a", "10.5678/b"));
        assert!(!should_include_citation("10.1234/a", "10.1234/a"));
        assert!(!should_include_citation("10.1234/A", "10.1234/a")); // Case insensitive
    }

    #[test]
    fn test_determine_provenance() {
        use serde_json::json;

        // Publisher asserted
        let ref_publisher = json!({"DOI": "10.1234/test", "doi-asserted-by": "publisher"});
        assert_eq!(
            determine_provenance(&ref_publisher, "10.1234/test"),
            Provenance::Publisher
        );

        // Crossref asserted
        let ref_crossref = json!({"DOI": "10.1234/test", "doi-asserted-by": "crossref"});
        assert_eq!(
            determine_provenance(&ref_crossref, "10.1234/test"),
            Provenance::Crossref
        );

        // DOI present but no doi-asserted-by
        let ref_no_assertion = json!({"DOI": "10.1234/test"});
        assert_eq!(
            determine_provenance(&ref_no_assertion, "10.1234/test"),
            Provenance::Mined
        );

        // Mined from unstructured (DOI not in DOI field)
        let ref_unstructured = json!({"unstructured": "See doi:10.1234/test"});
        assert_eq!(
            determine_provenance(&ref_unstructured, "10.1234/test"),
            Provenance::Mined
        );
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo check 2>&1 | head -100`
Expected: May have errors in validate.rs (we'll fix next)

**Step 3: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "refactor: simplify pipeline to use only scalable path"
```

---

## Task 7: Rewrite Validate Command

**Files:**
- Modify: `src/commands/validate.rs`

**Step 1: Rewrite validate.rs to use FST indexes**

Replace the entire file with:

```rust
use anyhow::Result;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use crate::cli::{Source, ValidateArgs};
use crate::common::setup_logging;
use crate::index::{build_fst_index_from_source, FstIndex};
use crate::input::{detect_datacite_input, open_datacite_source};

#[derive(Debug, Deserialize)]
struct CitationRecord {
    doi: String,
    #[serde(default)]
    citation_count: usize,
    #[serde(default)]
    cited_by: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    doi: String,
    citation_count: usize,
    cited_by: Vec<serde_json::Value>,
}

pub fn run_validate(args: ValidateArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting validation");
    info!("Input: {}", args.input);
    info!("Source: {}", args.source);

    // Load or build the appropriate FST index
    let fst_index = load_or_build_index(&args)?;

    // Open input and output files
    let input_file = File::open(&args.input)?;
    let reader = BufReader::new(input_file);

    let valid_file = File::create(&args.output_valid)?;
    let mut valid_writer = BufWriter::new(valid_file);

    let failed_file = File::create(&args.output_failed)?;
    let mut failed_writer = BufWriter::new(failed_file);

    let mut valid_count = 0;
    let mut failed_count = 0;
    let mut total = 0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        total += 1;

        let record: CitationRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("Failed to parse line {}: {}", total, e);
                continue;
            }
        };

        let is_valid = fst_index.contains(&record.doi);

        let output = OutputRecord {
            doi: record.doi,
            citation_count: record.citation_count,
            cited_by: record.cited_by,
        };

        let json = serde_json::to_string(&output)?;

        if is_valid {
            writeln!(valid_writer, "{}", json)?;
            valid_count += 1;
        } else {
            writeln!(failed_writer, "{}", json)?;
            failed_count += 1;
        }

        if total % 100_000 == 0 {
            info!("Progress: {} records processed", total);
        }
    }

    valid_writer.flush()?;
    failed_writer.flush()?;

    info!("Validation complete:");
    info!("  Total records: {}", total);
    info!("  Valid: {}", valid_count);
    info!("  Failed: {}", failed_count);

    Ok(())
}

fn load_or_build_index(args: &ValidateArgs) -> Result<FstIndex> {
    match args.source {
        Source::Crossref => {
            if let Some(ref path) = args.crossref_fst {
                info!("Loading Crossref FST from: {}", path);
                FstIndex::load(Path::new(path))
            } else {
                Err(anyhow::anyhow!(
                    "Crossref validation requires --crossref-fst"
                ))
            }
        }
        Source::Datacite | Source::Arxiv => {
            if let Some(ref path) = args.datacite_fst {
                info!("Loading DataCite FST from: {}", path);
                FstIndex::load(Path::new(path))
            } else if let Some(ref records_path) = args.datacite_records {
                info!("Building DataCite FST from: {}", records_path);
                let temp_dir = std::env::temp_dir();
                let fst_path = temp_dir.join("datacite_validate.fst");
                let input = detect_datacite_input(records_path)?;
                let source = open_datacite_source(input)?;
                build_fst_index_from_source(source, "id", &fst_path, &temp_dir)?;
                FstIndex::load(&fst_path)
            } else {
                Err(anyhow::anyhow!(
                    "DataCite/arXiv validation requires --datacite-fst or --datacite-records"
                ))
            }
        }
        Source::All => {
            Err(anyhow::anyhow!(
                "Source 'all' not supported for standalone validation. Use 'crossref' or 'datacite'."
            ))
        }
    }
}
```

**Step 2: Commit**

```bash
git add src/commands/validate.rs
git commit -m "refactor: migrate validate command to use FST indexes"
```

---

## Task 8: Update lib.rs Exports

**Files:**
- Modify: `src/lib.rs`

**Step 1: Read current lib.rs**

Check what's currently exported and update to remove references to deleted types.

**Step 2: Update exports**

Remove any exports of:
- `DoiIndex`
- `load_index_from_parquet`, `save_index_to_parquet`
- `PartitionWriter`
- `invert_partitions`, `InvertStats`, `OutputMode`
- `validate_citations`, `ValidationRunner`, `ValidationContext`

Keep exports of:
- `FstIndex`, `build_fst_index_from_source`
- `SegmentedPartitionWriter`, `PartitionRow`
- `RefJsonStore`, `RefJsonLookup`
- `aggregate_and_validate`, `AggregationStats`

**Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "refactor: update lib.rs exports for new architecture"
```

---

## Task 9: Fix Compilation Errors

**Step 1: Run cargo check and fix any remaining errors**

Run: `cargo check 2>&1`

Common issues to fix:
- Unused imports in various files
- Missing type references
- Test code referencing deleted types

**Step 2: Run cargo clippy**

Run: `cargo clippy 2>&1`

Fix any warnings.

**Step 3: Format code**

Run: `cargo fmt`

**Step 4: Commit fixes**

```bash
git add -A
git commit -m "fix: resolve compilation errors after refactoring"
```

---

## Task 10: Update and Run Tests

**Step 1: Run all tests**

Run: `cargo test 2>&1`

**Step 2: Fix any failing tests**

Tests that reference deleted types need to be removed or updated.

**Step 3: Verify integration tests pass**

Run: `cargo test --test '*' 2>&1`

**Step 4: Commit test fixes**

```bash
git add -A
git commit -m "test: update tests for new architecture"
```

---

## Task 11: Final Verification

**Step 1: Full build**

Run: `cargo build --release 2>&1`
Expected: Successful build with no errors

**Step 2: Run the binary with --help**

Run: `./target/release/crossref-citation-extraction pipeline --help`
Expected: Should show new flags, not old ones

**Step 3: Verify old flags are gone**

Check that these flags are NOT in the help output:
- `--scalable`
- `--load-crossref-index`
- `--save-crossref-index`
- `--load-datacite-index`
- `--save-datacite-index`

**Step 4: Run tests one more time**

Run: `cargo test`
Expected: All tests pass

**Step 5: Final commit if any changes**

```bash
git add -A
git commit -m "chore: final cleanup after pipeline refactoring"
```

---

## Summary

After completing all tasks:

1. **Deleted files:** 5 obsolete modules removed
2. **Updated modules:** index, streaming, validation module declarations cleaned up
3. **CLI simplified:** Old Parquet-based flags removed, FST flags remain
4. **Pipeline unified:** Single code path using FST + segmented writer + streaming aggregation
5. **Validate migrated:** Now uses FST indexes instead of DoiIndex
6. **Tests updated:** All tests pass with new architecture

The codebase is now ~30% smaller and uses only the memory-efficient scalable architecture.
