# Remove Old Pipeline Design

## Overview

Refactor to remove the old memory-heavy pipeline, making the scalable FST-based architecture the only code path. The old pipeline used HashSet-based indexes and read-modify-write partition patterns that couldn't handle 200M+ DOIs on memory-constrained machines.

## Goals

1. Remove dead code from the old pipeline
2. Make the scalable pipeline the default (remove `--scalable` flag)
3. Migrate the `validate` command to use FST indexes
4. Clean break from Parquet-based index format

## Files to Remove

| File | Reason |
|------|--------|
| `src/index/persistence.rs` | Parquet-based DoiIndex save/load - replaced by FST files |
| `src/streaming/partition_writer.rs` | Read-modify-write pattern - replaced by SegmentedPartitionWriter |
| `src/streaming/partition_invert.rs` | Separate inversion phase - merged into aggregator |
| `src/validation/runner.rs` | Separate validation phase - merged into aggregator |
| `src/validation/lookup.rs` | DoiIndex-based lookup - replaced by FstIndex.contains() |

## Types to Remove

| Location | Type/Code |
|----------|-----------|
| `src/index/mod.rs` | `DoiIndex` struct and all its methods |
| `src/index/builder.rs` | `build_index_from_source()` function (builds old DoiIndex) |
| `src/validation/mod.rs` | `ValidationContext` struct (uses DoiIndex) |

## CLI Changes

### PipelineArgs - Flags to Remove

| Flag | Reason |
|------|--------|
| `--scalable` | No longer needed - scalable is the only mode |
| `--load-crossref-index` | Parquet format no longer supported |
| `--save-crossref-index` | Parquet format no longer supported |
| `--load-datacite-index` | Parquet format no longer supported |
| `--save-datacite-index` | Parquet format no longer supported |

### PipelineArgs - Flags to Keep

| Flag | Purpose |
|------|---------|
| `--crossref-fst` | Load pre-built Crossref FST index |
| `--datacite-fst` | Load pre-built DataCite FST index |
| `--ref-store-dir` | Directory for ref_json store |
| `--datacite-records` | Build DataCite FST on-the-fly if no `--datacite-fst` provided |

### ValidateArgs Changes

- Remove `--crossref-index` (was Parquet path)
- Add `--crossref-fst` (FST file path)
- Add `--datacite-fst` (FST file path, alternative to `--datacite-records`)

## Pipeline Restructuring

### Functions to Remove from pipeline.rs

- `run_extraction()` - old extraction with PartitionWriter
- `load_indexes()` - loads old DoiIndex from Parquet
- `should_build_crossref_index()` - logic for building DoiIndex during extraction
- `PipelineIndexes` struct - holds old DoiIndex instances

### Functions to Keep and Rename

| Current Name | New Name |
|--------------|----------|
| `run_scalable_pipeline()` | `run_pipeline()` |
| `run_scalable_extraction()` | `run_extraction()` |
| `load_or_build_crossref_fst()` | keep as-is |
| `load_or_build_datacite_fst()` | keep as-is |

### Utility Functions to Keep

- `build_search_text()`
- `extract_arxiv_as_dois()`
- `determine_provenance()`
- `should_include_citation()`

## Validate Command Migration

The standalone `validate` command needs to work with FST indexes.

**Current flow:**
```
Load DoiIndex from Parquet → validate_citations() → write results
```

**New flow:**
```
Load FstIndex from FST file → validate with FstIndex.contains() → write results
```

The validate command operates on already-extracted JSONL files, so it just needs to check if DOIs exist in the index. No complex aggregation needed.

## Module Cleanup

### src/index/mod.rs

- Remove `pub mod persistence;`
- Remove `DoiIndex` struct and impl
- Remove `build_index_from_source` from builder.rs
- Keep `fst_index` module and `builder` (for FST building only)

### src/streaming/mod.rs

- Remove `pub mod partition_writer;`
- Remove `pub mod partition_invert;`
- Remove associated re-exports (`PartitionWriter`, `invert_partitions`, `OutputMode`, `InvertStats`)
- Keep `aggregator`, `checkpoint`, `ref_store`, `segmented_writer`

### src/validation/mod.rs

- Remove `pub mod runner;`
- Remove `pub mod lookup;`
- Remove `ValidationContext` struct
- Keep `http` (for HTTP fallback), `prefix_filter`

## Migration Path for Users

Users with existing Parquet indexes must rebuild them as FST:

```bash
# Build Crossref FST index
crossref-citation-extraction build-index \
  --input /path/to/crossref-snapshot.tar.gz \
  --output crossref.fst \
  --source-type crossref

# Build DataCite FST index
crossref-citation-extraction build-index \
  --input /path/to/datacite-records.jsonl.gz \
  --output datacite.fst \
  --source-type datacite
```

## Testing Strategy

1. Run existing integration tests to ensure scalable pipeline still works
2. Update tests that reference removed types
3. Test validate command with FST indexes
4. Verify build-index command works for both Crossref and DataCite sources
