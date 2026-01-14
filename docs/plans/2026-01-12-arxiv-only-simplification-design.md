# arXiv-Only Simplification Design

## Overview

Simplify the tool by removing Crossref and DataCite DOI extraction support, keeping only arXiv reference extraction. The tool still reads from Crossref tar.gz snapshots but only extracts arXiv IDs from references.

## Goals

- Remove complexity from multi-source DOI support
- Keep only scalable mode (FST indexes + streaming aggregation)
- Rename DataCite terminology to arXiv throughout
- Maintain provenance tracking (publisher, crossref, mined)

## Scope of Removal

### CLI options to remove

- `--source` flag and Source enum
- `--load-crossref-index`, `--save-crossref-index`, `--crossref-fst`
- `--scalable` flag (becomes the only mode)
- `--http-fallback`, `--concurrency`, `--timeout`

### Code to remove

- `src/extract/doi.rs` - Generic DOI extraction
- `src/validation/http.rs` - HTTP fallback validation
- `src/validation/prefix_filter.rs` - Prefix-based filtering
- `src/validation/runner.rs` - Validation orchestration (merge into lookup.rs)
- `src/commands/validate.rs` - Standalone validate command
- Non-scalable pipeline path in `src/commands/pipeline.rs`
- Crossref index building logic in `src/index/`

### Code to rename

- All "datacite" references → "arxiv"
- `DoiIndex` → `ArxivIndex`
- DataCite FST/index → arXiv index

## Simplified CLI

### Pipeline command

```
crossref-arxiv-citation-extraction pipeline \
  --input <crossref-snapshot.tar.gz> \
  --arxiv-records <arxiv-records.jsonl.gz> \
  --arxiv-fst <arxiv-index.fst> \
  --output-dir <output/> \
  --provenance publisher,crossref,mined \
  --outputs valid,failed,publisher,crossref,mined \
  --temp-dir <temp/> \
  --batch-size 5000000 \
  --keep-intermediates \
  --log-level INFO
```

### Build-index command

```
crossref-arxiv-citation-extraction build-index \
  --input <arxiv-records.jsonl.gz> \
  --output <arxiv-index.fst> \
  --log-level INFO
```

### Output alias

`--outputs all` expands to `valid,failed,publisher,crossref,mined`

Default behavior: if `--outputs` not specified, defaults to `all`.

## Module Structure

```
src/
├── cli.rs                    # Simplified CLI (no Source enum)
├── main.rs
├── commands/
│   └── pipeline.rs           # Scalable-only pipeline
├── extract/
│   └── arxiv.rs              # arXiv ID extraction
├── index/
│   ├── mod.rs                # ArxivIndex type
│   ├── builder.rs            # Build index from arXiv records
│   └── persistence.rs        # FST save/load
├── streaming/
│   ├── partition_writer.rs   # Write to per-partition Parquet
│   ├── aggregator.rs         # Streaming aggregation
│   └── checkpoint.rs         # Resume support
├── validation/
│   ├── mod.rs
│   └── lookup.rs             # FST-based arXiv lookup
└── common/
    ├── mod.rs
    └── output.rs             # Output file handling
```

## Data Flow

```
Crossref tar.gz
    │
    ▼
Extract arXiv IDs (with provenance)
    │
    ▼
Partition by arXiv ID prefix (e.g., "2403", "hep-")
    │
    ▼
Stream-aggregate by cited arXiv ID
    │
    ▼
Validate against arXiv FST index
    │
    ▼
Write outputs (valid, failed, publisher, crossref, mined)
```

## Output Schema

```json
{
  "cited_id": "2403.03542",
  "citing_dois": ["10.1234/example"],
  "citation_count": 1,
  "provenance": "publisher"
}
```

## Implementation Phases

### Phase 1: CLI simplification

- Remove Source enum and `--source` flag
- Rename `--datacite-*` to `--arxiv-*`
- Remove `--scalable` flag (always scalable)
- Remove Crossref index options
- Remove HTTP fallback options
- Add `all` alias for outputs

### Phase 2: Remove dead code

- Delete `src/extract/doi.rs`
- Delete `src/validation/http.rs`
- Delete `src/validation/prefix_filter.rs`
- Delete `src/commands/validate.rs`
- Remove non-scalable pipeline path

### Phase 3: Rename internals

- `DoiIndex` → `ArxivIndex`
- `datacite_fst` → `arxiv_fst` throughout
- Update log messages and error strings

### Phase 4: Cleanup

- Remove unused dependencies from Cargo.toml
- Update tests
- Run `cargo clippy` and `cargo fmt`
