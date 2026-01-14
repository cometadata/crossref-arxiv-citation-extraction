# Flexible Input Handling Design

## Overview

Add auto-detection and flexible input handling for both Crossref and DataCite data sources, allowing the tool to accept directories, individual files, or archives.

## Input Formats

### Crossref (`--input`)

| Format | Detection | Processing |
|--------|-----------|------------|
| `*.tar.gz` | File extension | Stream archive entries |
| Directory with `*.json` | Directory scan | Iterate JSON files |
| Single `*.json` | File extension | Process one file |

### DataCite (`--datacite-records`)

| Format | Detection | Processing |
|--------|-----------|------------|
| `*.jsonl.gz` | File extension | Decompress + iterate lines |
| `*.jsonl` | File extension | Iterate lines |
| Directory with `updated_*/` subdirs | Directory scan | Walk nested structure |
| Directory with `*.jsonl.gz` | Directory scan | Iterate flat directory |

## Input Type Detection

### Types

```rust
enum CrossrefInput {
    TarGz(PathBuf),
    Directory(PathBuf),
    SingleJson(PathBuf),
}

enum DataciteInput {
    SingleJsonlGz(PathBuf),
    SingleJsonl(PathBuf),
    NestedSnapshot(PathBuf),
    FlatDirectory(PathBuf),
}
```

### Detection Logic

For files, use extension matching:
- `*.tar.gz` → `CrossrefInput::TarGz`
- `*.json` → `CrossrefInput::SingleJson`
- `*.jsonl.gz` → `DataciteInput::SingleJsonlGz`
- `*.jsonl` → `DataciteInput::SingleJsonl`

For directories, scan contents:
- Contains `updated_*/` subdirectories → `DataciteInput::NestedSnapshot`
- Contains only `*.json` files → `CrossrefInput::Directory`
- Contains only `*.jsonl.gz` or `*.jsonl` files → `DataciteInput::FlatDirectory`
- Contains mixed types → error with clear message

### Logging

On detection, log what was found:
```
Detected Crossref input: extracted directory with 1,247 JSON files
Detected DataCite input: nested snapshot with 174 monthly directories
```

## Unified Reading Abstraction

### Crossref Source Trait

```rust
trait CrossrefSource {
    fn entries(&mut self) -> impl Iterator<Item = Result<(String, Value)>>;
}
```

Implementations:
- `TarGzSource` - streams tar entries (existing logic)
- `DirectorySource` - iterates `*.json` files, sorted by filename
- `SingleFileSource` - yields one file

### DataCite Source Trait

```rust
trait DataciteSource {
    fn records(&mut self) -> impl Iterator<Item = Result<Value>>;
}
```

Implementations:
- `SingleJsonlGzSource` - decompress and iterate lines (existing logic)
- `SingleJsonlSource` - iterate lines without decompression
- `NestedSnapshotSource` - walk `updated_*/part_*.jsonl.gz`, decompress each
- `FlatDirectorySource` - iterate `*.jsonl.gz` files in directory

### File Ordering

For directories, process files in sorted order for deterministic behavior. For nested snapshots, sort by directory name (chronological) then by part number.

## Module Structure

```
src/input/
├── mod.rs              # Public exports, detection entry points
├── detection.rs        # Input type detection logic
├── crossref.rs         # CrossrefSource trait + implementations
└── datacite.rs         # DataciteSource trait + implementations
```

### Public API

```rust
// Detection
pub fn detect_crossref_input(path: &str) -> Result<CrossrefInput>;
pub fn detect_datacite_input(path: &str) -> Result<DataciteInput>;

// Opening sources
pub fn open_crossref_source(input: CrossrefInput) -> Result<Box<dyn CrossrefSource>>;
pub fn open_datacite_source(input: DataciteInput) -> Result<Box<dyn DataciteSource>>;
```

## Integration

### Pipeline Command

Replace tar.gz-specific code with detection + source abstraction:

```rust
// Before
let file = File::open(&args.input)?;
let gz = GzDecoder::new(file);
let mut archive = Archive::new(gz);

// After
let input = detect_crossref_input(&args.input)?;
let mut source = open_crossref_source(input)?;
```

### Index Builder

Update `build_index_from_jsonl_gz()` to accept any `DataciteSource`:

```rust
pub fn build_index_from_datacite(source: impl DataciteSource) -> Result<DoiIndex>
```

### Changes Required

- `src/index/builder.rs` - accept `DataciteSource`
- `src/commands/pipeline.rs` - use detection + source abstraction
- `src/commands/validate.rs` - same pattern
- `src/lib.rs` - add `pub mod input;`

### No Changes To

- `src/cli.rs` - flags stay the same
- `src/extract/` - extraction logic unchanged
- `src/streaming/` - partition logic unchanged
- `src/validation/` - validation logic unchanged

## Error Handling

Ambiguous directory error:
```
Error: Directory '/path/to/data' contains both .json and .jsonl.gz files.
Specify a path to a specific file or a directory with only one format.
```

Non-existent path:
```
Error: Input path does not exist: /path/to/missing
```

Empty directory:
```
Error: Directory '/path/to/empty' contains no processable files.
```

## Testing Strategy

### Unit Tests

`detection.rs`:
- File extension detection
- Directory content detection
- Ambiguous directory error
- Non-existent path error

`crossref.rs`:
- `DirectorySource` with temp dir of JSON files
- `SingleFileSource` with one JSON file
- Numeric sorting order

`datacite.rs`:
- `SingleJsonlSource` with uncompressed file
- `FlatDirectorySource` with temp dir
- `NestedSnapshotSource` with mock structure

### Integration Test

One test running pipeline with directory input instead of tar.gz.

## Key Decisions

1. Keep separate `--input` and `--datacite-records` flags
2. Auto-detect format at runtime based on path
3. Error on ambiguous directories (mixed file types)
4. Log detected format for transparency
5. Process files in sorted order for determinism
6. No breaking changes to existing CLI usage
