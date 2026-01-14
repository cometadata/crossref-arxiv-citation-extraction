# Flexible Output Handling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace individual output flags with `--outputs` and `--output-dir` for flexible output selection. Add `--partitions-dir` to allow re-running aggregation on existing intermediate files.

**Architecture:** Add `OutputType` enum and `OutputPaths` struct to handle output selection. Modify aggregator to accept optional writers. Add `--partitions-dir` flag to skip extraction phase. Update both pipeline and validate commands.

**Tech Stack:** Rust, clap for CLI parsing

---

## Task 1: Add OutputType enum and parsing

**Files:**
- Modify: `src/cli.rs:1-40`

**Step 1: Write the failing test**

Add to `src/cli.rs` at the end of the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_type_from_str() {
        assert_eq!("valid".parse::<OutputType>().unwrap(), OutputType::Valid);
        assert_eq!("failed".parse::<OutputType>().unwrap(), OutputType::Failed);
        assert_eq!("publisher".parse::<OutputType>().unwrap(), OutputType::Publisher);
        assert_eq!("crossref".parse::<OutputType>().unwrap(), OutputType::Crossref);
        assert_eq!("mined".parse::<OutputType>().unwrap(), OutputType::Mined);
        assert!("invalid".parse::<OutputType>().is_err());
    }

    #[test]
    fn test_output_type_case_insensitive() {
        assert_eq!("VALID".parse::<OutputType>().unwrap(), OutputType::Valid);
        assert_eq!("Valid".parse::<OutputType>().unwrap(), OutputType::Valid);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_output_type_from_str`
Expected: FAIL with "cannot find type `OutputType`"

**Step 3: Write minimal implementation**

Add after the `Source` enum (around line 40):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputType {
    Valid,
    Failed,
    Publisher,
    Crossref,
    Mined,
}

impl FromStr for OutputType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "valid" => Ok(OutputType::Valid),
            "failed" => Ok(OutputType::Failed),
            "publisher" => Ok(OutputType::Publisher),
            "crossref" => Ok(OutputType::Crossref),
            "mined" => Ok(OutputType::Mined),
            _ => Err(format!(
                "Invalid output type: '{}'. Valid options: valid, failed, publisher, crossref, mined",
                s
            )),
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_output_type`
Expected: PASS

**Step 5: Commit**

```bash
git add src/cli.rs
git commit -m "feat: add OutputType enum for flexible output selection"
```

---

## Task 2: Add OutputPaths struct and extract_input_stem

**Files:**
- Modify: `src/common/output.rs`

**Step 1: Write the failing test**

Add to `src/common/output.rs` in the tests module:

```rust
    #[test]
    fn test_extract_input_stem_tar_gz() {
        assert_eq!(extract_input_stem("snapshot.tar.gz"), "snapshot");
        assert_eq!(extract_input_stem("crossref-2024.tar.gz"), "crossref-2024");
    }

    #[test]
    fn test_extract_input_stem_jsonl() {
        assert_eq!(extract_input_stem("data.jsonl.gz"), "data");
        assert_eq!(extract_input_stem("records.jsonl"), "records");
    }

    #[test]
    fn test_extract_input_stem_directory() {
        assert_eq!(extract_input_stem("/path/to/mydir"), "mydir");
        assert_eq!(extract_input_stem("/path/to/mydir/"), "mydir");
    }

    #[test]
    fn test_extract_input_stem_with_path() {
        assert_eq!(extract_input_stem("/data/crossref/snapshot.tar.gz"), "snapshot");
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_extract_input_stem`
Expected: FAIL with "cannot find function `extract_input_stem`"

**Step 3: Write minimal implementation**

Add to `src/common/output.rs` before the tests module:

```rust
use std::collections::HashSet;
use crate::cli::OutputType;

/// Extract the base filename stem from an input path
/// "snapshot.tar.gz" -> "snapshot"
/// "/path/to/dir/" -> "dir"
pub fn extract_input_stem(input: &str) -> String {
    let path = Path::new(input.trim_end_matches('/'));
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output");

    // Strip known compound extensions
    filename
        .strip_suffix(".tar.gz")
        .or_else(|| filename.strip_suffix(".jsonl.gz"))
        .or_else(|| filename.strip_suffix(".jsonl"))
        .or_else(|| filename.strip_suffix(".json"))
        .unwrap_or(filename)
        .to_string()
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_extract_input_stem`
Expected: PASS

**Step 5: Commit**

```bash
git add src/common/output.rs
git commit -m "feat: add extract_input_stem function"
```

---

## Task 3: Add OutputPaths struct

**Files:**
- Modify: `src/common/output.rs`

**Step 1: Write the failing test**

Add to tests module in `src/common/output.rs`:

```rust
    #[test]
    fn test_output_paths_all_outputs() {
        let mut outputs = HashSet::new();
        outputs.insert(OutputType::Valid);
        outputs.insert(OutputType::Failed);
        outputs.insert(OutputType::Mined);

        let paths = OutputPaths::from_selection(&outputs, Path::new("."), "snapshot");

        assert_eq!(paths.valid, Some(PathBuf::from("./snapshot_valid.jsonl")));
        assert_eq!(paths.failed, Some(PathBuf::from("./snapshot_failed.jsonl")));
        assert_eq!(paths.mined, Some(PathBuf::from("./snapshot_mined.jsonl")));
        assert_eq!(paths.publisher, None);
        assert_eq!(paths.crossref, None);
    }

    #[test]
    fn test_output_paths_custom_dir() {
        let mut outputs = HashSet::new();
        outputs.insert(OutputType::Valid);

        let paths = OutputPaths::from_selection(&outputs, Path::new("/tmp/results"), "data");

        assert_eq!(paths.valid, Some(PathBuf::from("/tmp/results/data_valid.jsonl")));
        assert_eq!(paths.failed, None);
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_output_paths`
Expected: FAIL with "cannot find type `OutputPaths`"

**Step 3: Write minimal implementation**

Add to `src/common/output.rs`:

```rust
/// Resolved output paths (None means output is disabled)
#[derive(Debug, Clone, Default)]
pub struct OutputPaths {
    pub valid: Option<PathBuf>,
    pub failed: Option<PathBuf>,
    pub publisher: Option<PathBuf>,
    pub crossref: Option<PathBuf>,
    pub mined: Option<PathBuf>,
}

impl OutputPaths {
    /// Create OutputPaths from a selection of output types
    pub fn from_selection(
        outputs: &HashSet<OutputType>,
        output_dir: &Path,
        input_stem: &str,
    ) -> Self {
        let make_path = |suffix: &str| -> PathBuf {
            output_dir.join(format!("{}_{}.jsonl", input_stem, suffix))
        };

        Self {
            valid: outputs.contains(&OutputType::Valid).then(|| make_path("valid")),
            failed: outputs.contains(&OutputType::Failed).then(|| make_path("failed")),
            publisher: outputs.contains(&OutputType::Publisher).then(|| make_path("publisher")),
            crossref: outputs.contains(&OutputType::Crossref).then(|| make_path("crossref")),
            mined: outputs.contains(&OutputType::Mined).then(|| make_path("mined")),
        }
    }

    /// Check if any output is enabled
    pub fn has_any(&self) -> bool {
        self.valid.is_some()
            || self.failed.is_some()
            || self.publisher.is_some()
            || self.crossref.is_some()
            || self.mined.is_some()
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_output_paths`
Expected: PASS

**Step 5: Commit**

```bash
git add src/common/output.rs
git commit -m "feat: add OutputPaths struct for resolved output paths"
```

---

## Task 4: Update common/mod.rs exports

**Files:**
- Modify: `src/common/mod.rs`

**Step 1: Update exports**

Change line 8 from:
```rust
pub use output::SplitOutputPaths;
```

To:
```rust
pub use output::{extract_input_stem, OutputPaths, SplitOutputPaths};
```

**Step 2: Run cargo check**

Run: `cargo check`
Expected: PASS

**Step 3: Commit**

```bash
git add src/common/mod.rs
git commit -m "feat: export OutputPaths and extract_input_stem from common"
```

---

## Task 5: Update PipelineArgs with new flags

**Files:**
- Modify: `src/cli.rs:82-175`

**Step 1: Replace output flags in PipelineArgs**

Remove these fields from `PipelineArgs`:
- `output_crossref`
- `output_datacite`
- `output_arxiv`
- `output_crossref_failed`
- `output_datacite_failed`
- `output_arxiv_failed`

Add these new fields:

```rust
    /// Output types to generate (comma-separated: valid,failed,publisher,crossref,mined)
    /// Default: all outputs if not specified
    #[arg(long, value_delimiter = ',')]
    pub outputs: Vec<String>,

    /// Directory for output files (default: current directory)
    #[arg(long)]
    pub output_dir: Option<String>,

    /// Path to existing partition directory (skips extraction, runs aggregation only)
    /// Use this to re-run aggregation with different output selections
    #[arg(long)]
    pub partitions_dir: Option<String>,
```

**Step 2: Run cargo check**

Run: `cargo check`
Expected: FAIL with errors in pipeline.rs (expected, will fix in later task)

**Step 3: Add helper method to PipelineArgs**

Add impl block for PipelineArgs:

```rust
impl PipelineArgs {
    /// Parse outputs into a HashSet, defaulting to all outputs if empty
    pub fn parse_outputs(&self) -> Result<HashSet<OutputType>, String> {
        if self.outputs.is_empty() {
            // Default to all outputs
            let mut all = HashSet::new();
            all.insert(OutputType::Valid);
            all.insert(OutputType::Failed);
            all.insert(OutputType::Publisher);
            all.insert(OutputType::Crossref);
            all.insert(OutputType::Mined);
            Ok(all)
        } else {
            self.outputs
                .iter()
                .map(|s| s.parse::<OutputType>())
                .collect()
        }
    }
}
```

**Step 4: Commit (partial - CLI changes)**

```bash
git add src/cli.rs
git commit -m "feat: replace individual output flags with --outputs and --output-dir"
```

---

## Task 6: Update aggregate_and_validate signature

**Files:**
- Modify: `src/streaming/aggregator.rs`

**Step 1: Update function signature**

Change `aggregate_and_validate` signature from:

```rust
pub fn aggregate_and_validate(
    partition_dir: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_output: &Path,
    failed_output: &Path,
    provenance_outputs: Option<&ProvenanceOutputs>,
) -> Result<AggregationStats>
```

To:

```rust
pub fn aggregate_and_validate(
    partition_dir: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    outputs: &crate::common::OutputPaths,
) -> Result<AggregationStats>
```

**Step 2: Update writer creation**

Replace the writer creation (lines 49-61) with:

```rust
    let mut valid_writer = outputs.valid.as_ref()
        .map(|p| BufWriter::new(File::create(p).expect("Failed to create valid output")));
    let mut failed_writer = outputs.failed.as_ref()
        .map(|p| BufWriter::new(File::create(p).expect("Failed to create failed output")));
    let mut publisher_writer = outputs.publisher.as_ref()
        .map(|p| BufWriter::new(File::create(p).expect("Failed to create publisher output")));
    let mut crossref_writer = outputs.crossref.as_ref()
        .map(|p| BufWriter::new(File::create(p).expect("Failed to create crossref output")));
    let mut mined_writer = outputs.mined.as_ref()
        .map(|p| BufWriter::new(File::create(p).expect("Failed to create mined output")));
```

**Step 3: Update all writer usages**

Update `process_partition` calls to pass `&mut Option<BufWriter<File>>` for valid and failed writers.

Update `write_grouped_results` and `process_partition_batched` to handle optional valid/failed writers:

```rust
// Write to main output
if is_valid {
    if let Some(ref mut w) = valid_writer {
        writeln!(w, "{}", record)?;
    }
    stats.valid_count += 1;
} else {
    if let Some(ref mut w) = failed_writer {
        writeln!(w, "{}", record)?;
    }
    stats.failed_count += 1;
}
```

**Step 4: Update flush logic**

Replace lines 104-114 with:

```rust
    if let Some(ref mut w) = valid_writer { w.flush()?; }
    if let Some(ref mut w) = failed_writer { w.flush()?; }
    if let Some(ref mut w) = publisher_writer { w.flush()?; }
    if let Some(ref mut w) = crossref_writer { w.flush()?; }
    if let Some(ref mut w) = mined_writer { w.flush()?; }
```

**Step 5: Run cargo check**

Run: `cargo check`
Expected: FAIL in pipeline.rs (expected)

**Step 6: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "refactor: update aggregate_and_validate to use OutputPaths"
```

---

## Task 7: Update streaming/mod.rs exports

**Files:**
- Modify: `src/streaming/mod.rs`

**Step 1: Remove ProvenanceOutputs export if no longer needed**

Check if `ProvenanceOutputs` is still used. If not, remove from exports.

**Step 2: Run cargo check**

Run: `cargo check`

**Step 3: Commit if changes made**

```bash
git add src/streaming/mod.rs
git commit -m "refactor: update streaming module exports"
```

---

## Task 8: Update run_scalable_pipeline in pipeline.rs

**Files:**
- Modify: `src/commands/pipeline.rs:334-392`

**Step 1: Update imports**

Add to imports:

```rust
use crate::common::{extract_input_stem, OutputPaths};
```

**Step 2: Update run_scalable_pipeline function**

Replace the partition directory setup and add skip-extraction logic:

```rust
    // Determine partition directory - use existing or create new
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
                std::env::temp_dir().join(format!("crossref-scalable-{}", Uuid::new_v4()))
            });
        fs::create_dir_all(&path)?;
        (path, false)
    };

    // Load or build FST indexes
    let crossref_fst = load_or_build_crossref_fst(args, &partition_dir)?;
    let datacite_fst = load_or_build_datacite_fst(args, &partition_dir)?;

    // Phase 1: Extraction (skip if using existing partitions)
    if skip_extraction {
        info!("=== Skipping Extraction (using existing partitions) ===");
    } else {
        info!("=== Extraction Phase (Scalable) ===");
        run_scalable_extraction(args, &partition_dir)?;
    }

    // Parse output selection
    let output_types = args.parse_outputs()
        .map_err(|e| anyhow::anyhow!(e))?;

    let output_dir = args.output_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let input_stem = extract_input_stem(&args.input);
    let outputs = OutputPaths::from_selection(&output_types, &output_dir, &input_stem);

    if !outputs.has_any() {
        return Err(anyhow::anyhow!(
            "No outputs specified. Use --outputs to specify at least one output type."
        ));
    }

    // Phase 2: Aggregation with streaming validation
    info!("=== Aggregation Phase (Scalable) ===");

    let stats = aggregate_and_validate(
        &partition_dir,
        crossref_fst.as_ref(),
        datacite_fst.as_ref(),
        &outputs,
    )?;
```

**Step 3: Update validate_args function**

Remove the old output validation logic in `validate_args()` since outputs are now handled differently.

Replace with simpler validation:

```rust
fn validate_args(args: &PipelineArgs) -> Result<()> {
    // Validate that required indexes are available for the source mode
    match args.source {
        Source::Datacite | Source::Arxiv => {
            if args.datacite_records.is_none()
                && args.load_datacite_index.is_none()
                && args.datacite_fst.is_none()
            {
                return Err(anyhow::anyhow!(
                    "Source '{}' requires --datacite-records, --load-datacite-index, or --datacite-fst",
                    args.source
                ));
            }
        }
        _ => {}
    }
    Ok(())
}
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: PASS (or close)

**Step 5: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "refactor: update pipeline to use OutputPaths"
```

---

## Task 9: Update ValidateArgs with new flags

**Files:**
- Modify: `src/cli.rs:177-218`

**Step 1: Replace output flags in ValidateArgs**

Remove these fields:
- `output_valid`
- `output_failed`

Add these new fields:

```rust
    /// Output types to generate (comma-separated: valid,failed)
    /// Default: both if not specified
    #[arg(long, value_delimiter = ',')]
    pub outputs: Vec<String>,

    /// Directory for output files (default: current directory)
    #[arg(long)]
    pub output_dir: Option<String>,
```

**Step 2: Add helper method to ValidateArgs**

```rust
impl ValidateArgs {
    /// Parse outputs into a HashSet, defaulting to valid and failed
    pub fn parse_outputs(&self) -> Result<HashSet<OutputType>, String> {
        if self.outputs.is_empty() {
            let mut defaults = HashSet::new();
            defaults.insert(OutputType::Valid);
            defaults.insert(OutputType::Failed);
            Ok(defaults)
        } else {
            let parsed: Result<HashSet<OutputType>, String> = self.outputs
                .iter()
                .map(|s| s.parse::<OutputType>())
                .collect();

            // Validate only valid/failed for validate command
            let set = parsed?;
            for t in &set {
                if !matches!(t, OutputType::Valid | OutputType::Failed) {
                    return Err(format!(
                        "Validate command only supports 'valid' and 'failed' outputs, got: {:?}",
                        t
                    ));
                }
            }
            Ok(set)
        }
    }
}
```

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat: update ValidateArgs with --outputs and --output-dir"
```

---

## Task 10: Update run_validate_async

**Files:**
- Modify: `src/commands/validate.rs`

**Step 1: Update imports**

Add:
```rust
use crate::common::{extract_input_stem, OutputPaths};
```

**Step 2: Update output path resolution**

Replace output writing section with:

```rust
    // Resolve output paths
    let output_types = args.parse_outputs()
        .map_err(|e| anyhow::anyhow!(e))?;

    let output_dir = args.output_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let input_stem = extract_input_stem(&args.input);
    let outputs = OutputPaths::from_selection(&output_types, &output_dir, &input_stem);

    // Write results
    if let Some(ref valid_path) = outputs.valid {
        // Write valid results
        write_validation_results(&results.valid, valid_path)?;
    }

    if let Some(ref failed_path) = outputs.failed {
        // Write failed results
        write_validation_results(&results.failed, failed_path)?;
    }
```

**Step 3: Update logging**

```rust
    info!("==================== VALIDATION COMPLETE ====================");
    info!("Total records: {}", results.stats.total_records);
    info!("Valid: {}", results.valid.len());
    info!("Failed: {}", results.failed.len());
    if let Some(ref p) = outputs.valid {
        info!("Output valid: {}", p.display());
    }
    if let Some(ref p) = outputs.failed {
        info!("Output failed: {}", p.display());
    }
    info!("=============================================================");
```

**Step 4: Run cargo check**

Run: `cargo check`
Expected: PASS

**Step 5: Commit**

```bash
git add src/commands/validate.rs
git commit -m "refactor: update validate command to use OutputPaths"
```

---

## Task 11: Update unit tests in pipeline.rs

**Files:**
- Modify: `src/commands/pipeline.rs:794-987`

**Step 1: Update default_args helper**

Replace the old output fields with new ones:

```rust
    fn default_args() -> PipelineArgs {
        PipelineArgs {
            input: "test.tar.gz".to_string(),
            datacite_records: None,
            source: Source::All,
            outputs: vec![],  // Default to all
            output_dir: None,
            http_fallback: vec![],
            load_crossref_index: None,
            save_crossref_index: None,
            load_datacite_index: None,
            save_datacite_index: None,
            log_level: "INFO".to_string(),
            concurrency: 50,
            timeout: 5,
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
            scalable: false,
            crossref_fst: None,
            datacite_fst: None,
        }
    }
```

**Step 2: Update or remove obsolete tests**

Remove tests that validated old output flag requirements:
- `test_validate_args_all_requires_both_outputs`
- `test_validate_args_crossref_requires_output`
- etc.

Add new test:

```rust
    #[test]
    fn test_parse_outputs_default() {
        let args = default_args();
        let outputs = args.parse_outputs().unwrap();
        assert!(outputs.contains(&OutputType::Valid));
        assert!(outputs.contains(&OutputType::Failed));
        assert!(outputs.contains(&OutputType::Publisher));
        assert!(outputs.contains(&OutputType::Crossref));
        assert!(outputs.contains(&OutputType::Mined));
    }

    #[test]
    fn test_parse_outputs_specific() {
        let mut args = default_args();
        args.outputs = vec!["valid".to_string(), "mined".to_string()];
        let outputs = args.parse_outputs().unwrap();
        assert!(outputs.contains(&OutputType::Valid));
        assert!(outputs.contains(&OutputType::Mined));
        assert!(!outputs.contains(&OutputType::Failed));
    }
```

**Step 3: Run tests**

Run: `cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "test: update pipeline unit tests for new output flags"
```

---

## Task 12: Update aggregator unit test

**Files:**
- Modify: `src/streaming/aggregator.rs:559-630`

**Step 1: Update test to use OutputPaths**

Replace the test setup:

```rust
    #[test]
    fn test_aggregate_and_validate() {
        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");

        // Create test data
        {
            let mut partition_writer = SegmentedPartitionWriter::new(&partition_dir, 100).unwrap();
            // ... existing test data creation ...
            partition_writer.flush_all().unwrap();
        }

        // Build FST index
        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.1234/cited").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        // Create OutputPaths
        let outputs = crate::common::OutputPaths {
            valid: Some(dir.path().join("valid.jsonl")),
            failed: Some(dir.path().join("failed.jsonl")),
            publisher: None,
            crossref: None,
            mined: None,
        };

        // Run aggregation
        let stats = aggregate_and_validate(
            &partition_dir,
            Some(&index),
            None,
            &outputs,
        )
        .unwrap();

        assert_eq!(stats.valid_count, 1);
        assert_eq!(stats.failed_count, 0);
        assert_eq!(stats.total_citations, 2);

        // Verify output
        let valid_content = std::fs::read_to_string(outputs.valid.unwrap()).unwrap();
        assert!(valid_content.contains("10.1234/cited"));
    }
```

**Step 2: Run tests**

Run: `cargo test test_aggregate_and_validate`
Expected: PASS

**Step 3: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "test: update aggregator test for OutputPaths"
```

---

## Task 13: Update integration tests

**Files:**
- Modify: `tests/integration_test.rs`

**Step 1: Update test_crossref_mode_extraction**

Replace `--output-crossref` with `--output-dir`:

```rust
#[test]
fn test_crossref_mode_extraction() {
    let dir = tempdir().unwrap();
    let tar_path = create_test_crossref_tar_gz(dir.path());

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path.to_str().unwrap(),
            "--source",
            "crossref",
            "--output-dir",
            dir.path().to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should succeed");

    // Check for auto-generated output file
    let output_path = dir.path().join("test_crossref_valid.jsonl");
    assert!(output_path.exists(), "Valid output file should exist");
    // ... rest of test
}
```

**Step 2: Update test_provenance_end_to_end**

Update to use new flags and check for new output filenames.

**Step 3: Update test_scalable_pipeline_end_to_end**

Replace individual output flags with `--output-dir`.

**Step 4: Update test_batched_processing_for_large_partitions**

Update the `aggregate_and_validate` call to use `OutputPaths`.

**Step 5: Add test for partitions-dir resume**

```rust
#[test]
fn test_partitions_dir_resume() {
    let dir = tempdir().unwrap();
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let partitions_dir = dir.path().join("partitions");
    let output_dir = dir.path().join("outputs");
    std::fs::create_dir(&output_dir).unwrap();

    // First run: extract with --temp-dir and only valid output
    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path.to_str().unwrap(),
            "--source",
            "crossref",
            "--scalable",
            "--temp-dir",
            partitions_dir.to_str().unwrap(),
            "--output-dir",
            output_dir.to_str().unwrap(),
            "--outputs",
            "valid",
        ])
        .status()
        .expect("Failed to run first pipeline");

    assert!(status.success(), "First pipeline run should succeed");
    assert!(partitions_dir.exists(), "Partitions directory should exist");

    // Second run: use existing partitions with different outputs
    let output_dir2 = dir.path().join("outputs2");
    std::fs::create_dir(&output_dir2).unwrap();

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path.to_str().unwrap(),
            "--source",
            "crossref",
            "--scalable",
            "--partitions-dir",
            partitions_dir.to_str().unwrap(),
            "--output-dir",
            output_dir2.to_str().unwrap(),
            "--outputs",
            "failed,mined",
        ])
        .status()
        .expect("Failed to run second pipeline");

    assert!(status.success(), "Second pipeline run should succeed");
    // Verify different outputs were created
    let failed_path = output_dir2.join("test_crossref_failed.jsonl");
    let mined_path = output_dir2.join("test_crossref_mined.jsonl");
    assert!(failed_path.exists() || mined_path.exists(),
            "Should create failed and/or mined outputs");
}
```

**Step 6: Run integration tests**

Run: `cargo test --test integration_test`
Expected: PASS

**Step 7: Commit**

```bash
git add tests/integration_test.rs
git commit -m "test: update integration tests for new output flags"
```

---

## Task 14: Clean up unused code

**Files:**
- Modify: `src/streaming/aggregator.rs`
- Modify: `src/streaming/mod.rs`

**Step 1: Remove ProvenanceOutputs if unused**

Check if `ProvenanceOutputs` struct is still used. If not, remove it from `aggregator.rs` and exports.

**Step 2: Run cargo clippy**

Run: `cargo clippy`
Fix any warnings.

**Step 3: Run full test suite**

Run: `cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add .
git commit -m "chore: remove unused code and fix clippy warnings"
```

---

## Task 15: Final verification

**Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 2: Test CLI help**

Run: `cargo run -- pipeline --help`
Verify: Shows `--outputs` and `--output-dir` flags, not old individual flags

Run: `cargo run -- validate --help`
Verify: Shows `--outputs` and `--output-dir` flags

**Step 3: Test default behavior**

Run: `cargo run -- pipeline --input test.tar.gz --scalable --datacite-fst datacite.fst`
Verify: Creates 5 output files in current directory

**Step 4: Test selective output**

Run: `cargo run -- pipeline --input test.tar.gz --scalable --datacite-fst datacite.fst --outputs valid,mined`
Verify: Creates only `*_valid.jsonl` and `*_mined.jsonl`

**Step 5: Test output directory**

Run: `cargo run -- pipeline --input test.tar.gz --scalable --datacite-fst datacite.fst --output-dir ./results/`
Verify: Creates output files in `./results/` directory

**Step 6: Test partitions-dir (re-run aggregation only)**

First run extracts to a temp directory:
Run: `cargo run -- pipeline --input test.tar.gz --scalable --datacite-fst datacite.fst --temp-dir ./partitions/ --outputs valid`
Verify: Creates `*_valid.jsonl` and `./partitions/` with partition data

Second run uses existing partitions with different outputs:
Run: `cargo run -- pipeline --input test.tar.gz --scalable --datacite-fst datacite.fst --partitions-dir ./partitions/ --outputs failed,mined`
Verify: Creates `*_failed.jsonl` and `*_mined.jsonl` without re-running extraction

**Step 7: Final commit**

```bash
git add .
git commit -m "feat: flexible output handling complete"
```
