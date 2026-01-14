# Flexible Output Handling Design

## Overview

Add flexible output handling to the pipeline and validate commands, allowing users to specify which outputs they want and where they go. All outputs become optional with sensible defaults.

## Output Types

| Type | Description |
|------|-------------|
| `valid` | Citations where the cited DOI exists in an index |
| `failed` | Citations where the cited DOI was not found |
| `publisher` | Valid citations with publisher provenance |
| `crossref` | Valid citations with crossref provenance |
| `mined` | Valid citations with mined provenance |

## CLI Interface

### Pipeline Command

**New flags:**

```
--outputs <LIST>        Comma-separated outputs: valid,failed,publisher,crossref,mined
                        Default: all five if not specified

--output-dir <PATH>     Directory for output files (default: current directory)

--partitions-dir <PATH> Use existing partition directory (skips extraction phase)
                        Allows re-running aggregation with different output selections
```

**Removed flags:**
- `--output-crossref`
- `--output-datacite`
- `--output-arxiv`
- `--output-crossref-failed`
- `--output-datacite-failed`
- `--output-arxiv-failed`

**Example usage:**

```bash
# All outputs (default) - writes 5 files to current dir
crossref-citation-extraction pipeline --input snapshot.tar.gz

# Only valid mined citations
crossref-citation-extraction pipeline --input snapshot.tar.gz --outputs valid,mined

# Only failed, to specific directory
crossref-citation-extraction pipeline --input snapshot.tar.gz --outputs failed --output-dir ./failures/

# First run: extract and keep partitions
crossref-citation-extraction pipeline --input snapshot.tar.gz --temp-dir ./partitions/ --outputs valid

# Re-run with different outputs (skips extraction)
crossref-citation-extraction pipeline --input snapshot.tar.gz --partitions-dir ./partitions/ --outputs failed,mined
```

### Validate Command

**New flags:**

```
--outputs <LIST>      Comma-separated outputs: valid,failed
                      Default: both if not specified

--output-dir <PATH>   Directory for output files (default: current directory)
```

**Removed flags:**
- `--output-valid`
- `--output-failed`

## Filename Generation

**Pattern:** `{input_stem}_{output_type}.jsonl`

**Input stem extraction:**

| Input | Stem |
|-------|------|
| `snapshot.tar.gz` | `snapshot` |
| `crossref-2024.tar.gz` | `crossref-2024` |
| `data.jsonl.gz` | `data` |
| `records.jsonl` | `records` |
| `/path/to/dir/` | `dir` |

**Generated filenames** (for input `snapshot.tar.gz`):
- `snapshot_valid.jsonl`
- `snapshot_failed.jsonl`
- `snapshot_publisher.jsonl`
- `snapshot_crossref.jsonl`
- `snapshot_mined.jsonl`

## Types

### OutputType Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputType {
    Valid,
    Failed,
    Publisher,
    Crossref,
    Mined,
}
```

Parsing:
- `--outputs valid,mined` → `HashSet<OutputType>` containing `Valid` and `Mined`
- If `--outputs` not specified → all types for the command
- Invalid value → error: `"Invalid output type 'foo'. Valid options: valid, failed, publisher, crossref, mined"`

### OutputPaths Struct

```rust
pub struct OutputPaths {
    pub valid: Option<PathBuf>,
    pub failed: Option<PathBuf>,
    pub publisher: Option<PathBuf>,
    pub crossref: Option<PathBuf>,
    pub mined: Option<PathBuf>,
}

impl OutputPaths {
    pub fn from_args(
        outputs: &HashSet<OutputType>,
        output_dir: &Path,
        input_stem: &str,
    ) -> Self { ... }
}
```

### Input Stem Extraction

```rust
fn extract_input_stem(input: &str) -> String {
    let path = Path::new(input);
    let filename = path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output");

    // Strip known compound extensions
    let stem = filename
        .strip_suffix(".tar.gz")
        .or_else(|| filename.strip_suffix(".jsonl.gz"))
        .or_else(|| filename.strip_suffix(".jsonl"))
        .or_else(|| filename.strip_suffix(".json"))
        .unwrap_or(filename);

    stem.to_string()
}
```

## Aggregator Changes

**Current signature:**

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

**New signature:**

```rust
pub fn aggregate_and_validate(
    partition_dir: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    outputs: &OutputPaths,
) -> Result<AggregationStats>
```

**Behavioral changes:**
- Only create writers for outputs that have paths
- Skip writing to outputs not requested
- Only track stats for outputs being written

## File Changes

| File | Changes |
|------|---------|
| `src/cli.rs` | Add `OutputType` enum, `--outputs` and `--output-dir` flags, remove old output flags |
| `src/common/mod.rs` | Add `OutputPaths` struct and `extract_input_stem()` function |
| `src/streaming/aggregator.rs` | Update signature to accept `OutputPaths`, make all writers optional |
| `src/commands/pipeline.rs` | Resolve `OutputPaths` from args, pass to aggregator |
| `src/commands/validate.rs` | Same pattern as pipeline (valid/failed only) |

## No Changes To

- `src/extract/` - extraction logic unchanged
- `src/index/` - index logic unchanged
- `src/streaming/segmented_writer.rs` - partition writing unchanged
- `src/validation/` - validation logic unchanged

## Testing Strategy

### Unit Tests

`src/common/mod.rs`:
- `extract_input_stem()` with various input formats
- `OutputPaths::from_args()` with different output selections

`src/cli.rs`:
- `OutputType` parsing from strings
- Default behavior when `--outputs` not specified

### Integration Tests

- Pipeline with `--outputs valid` only
- Pipeline with `--outputs mined,publisher`
- Pipeline with `--output-dir`
- Validate with `--outputs valid` only
