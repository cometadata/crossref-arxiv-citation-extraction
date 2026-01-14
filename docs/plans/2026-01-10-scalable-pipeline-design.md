# Scalable Pipeline Design

## Overview

Redesign the citation extraction pipeline to handle 200M+ Crossref DOIs and 100M+ DataCite DOIs on memory-constrained machines (16-32GB RAM). Current architecture uses in-memory `HashSet<String>` indexes and accumulates all data before writing, causing memory exhaustion at scale.

## Problem Statement

### Current Bottlenecks

1. **DoiIndex as `HashSet<String>`** (`src/index/mod.rs`)
   - Memory cost: ~100 bytes per DOI (string + hash overhead)
   - 200M DOIs = ~20GB for Crossref index alone
   - Exceeds target machine memory

2. **Partition writer read-modify-write** (`src/streaming/partition_writer.rs`)
   - On flush: reads existing parquet, concatenates, rewrites entire file
   - O(n²) I/O pattern as partitions grow
   - Memory spikes during concat operations

3. **Inversion collects all DataFrames** (`src/streaming/partition_invert.rs`)
   - All partition results held in `Vec<DataFrame>` before concatenation
   - With 10K+ partitions, memory grows unbounded

4. **ref_json duplication**
   - Full reference JSON (~500 bytes) stored per extracted citation
   - Same ref_json duplicated when reference cites multiple DOIs
   - Massive I/O and storage overhead

5. **Validation accumulates all records** (`src/validation/runner.rs`)
   - `Vec<CitationRecord>` with full `cited_by` arrays held in memory
   - No streaming output

### Target Constraints

- Machine: 16-32GB RAM, fast SSD
- Data: 200M+ Crossref DOIs, 100M+ DataCite DOIs, 1B+ references
- Requirements: Exact validation (no false positives/negatives), full provenance output
- Usage: Regular batch processing (weekly/monthly), reusable indexes

## Solution Architecture

### High-Level Pipeline

```
Phase 1: Index Building (one-time, reusable)
├── Stream Crossref/DataCite records
├── External sort DOIs (disk-based)
└── Build FST index (~400MB per index)

Phase 2: Extraction
├── Stream Crossref tar.gz
├── Write ref_json to deduplicated store
└── Write slim partition segments (no ref_json)

Phase 3: Aggregation + Validation (merged, streaming)
├── Load FST indexes (~800MB)
├── For each partition:
│   ├── Merge segments
│   ├── Group by cited_id
│   ├── Load ref_json for partition's citing_dois
│   ├── Validate against FST
│   └── Write directly to output
└── Output: valid.jsonl, failed.jsonl
```

### Memory Budget

| Component | Current | Proposed |
|-----------|---------|----------|
| Crossref index | ~20GB | ~400MB (FST) |
| DataCite index | ~10GB | ~200MB (FST) |
| Partition processing | Unbounded | ~200-400MB |
| ref_json lookup | N/A | ~100-200MB |
| Validation results | Unbounded | O(1) streaming |
| **Total peak** | **30GB+** | **~1.5-2GB** |

## Component Designs

### 1. FST-Based DOI Index

Replace `HashSet<String>` with FST (Finite State Transducer) for ~50x memory reduction.

#### Structure

```rust
use fst::{Set, SetBuilder};
use memmap2::Mmap;

pub struct FstIndex {
    set: Set<Mmap>,
}

impl FstIndex {
    pub fn contains(&self, doi: &str) -> bool {
        self.set.contains(doi.to_lowercase().as_bytes())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let set = Set::new(mmap)?;
        Ok(Self { set })
    }
}
```

#### Building with External Sort

FST requires sorted input. For 200M DOIs, use external sort:

```rust
pub fn build_fst_index(
    source: impl Iterator<Item = Result<String>>,
    output_path: &Path,
    temp_dir: &Path,
) -> Result<()> {
    // Phase 1: Write DOIs to temp file
    let temp_file = temp_dir.join("dois_unsorted.txt");
    {
        let mut writer = BufWriter::new(File::create(&temp_file)?);
        for doi_result in source {
            let doi = doi_result?;
            writeln!(writer, "{}", doi.to_lowercase())?;
        }
    }

    // Phase 2: External sort
    let sorted_file = temp_dir.join("dois_sorted.txt");
    Command::new("sort")
        .arg("-u")  // unique
        .arg("-o").arg(&sorted_file)
        .arg(&temp_file)
        .status()?;

    // Phase 3: Stream into FST builder
    let mut builder = SetBuilder::new(BufWriter::new(File::create(output_path)?))?;

    for line in BufReader::new(File::open(&sorted_file)?).lines() {
        let doi = line?;
        builder.insert(doi.as_bytes())?;
    }

    builder.finish()?;

    // Cleanup
    fs::remove_file(&temp_file)?;
    fs::remove_file(&sorted_file)?;

    Ok(())
}
```

#### Characteristics

- **Size**: ~1-2 bytes per DOI due to prefix sharing
- **Lookup**: O(key length), very fast
- **Construction**: O(n log n) for sort, O(n) for FST build
- **Immutable**: Read-only after construction (perfect for batch processing)

### 2. Append-Only Partition Segments

Replace read-modify-write with append-only segment files.

#### Directory Structure

```
partitions/
├── 10.1234/
│   ├── segment_0000.parquet
│   ├── segment_0001.parquet
│   └── segment_0002.parquet
├── 10.5678/
│   └── segment_0000.parquet
└── 10.48550/
    ├── segment_0000.parquet
    └── segment_0001.parquet
```

#### Slim Row Schema

```rust
// No ref_json - references stored separately
struct PartitionRow {
    citing_doi: String,
    ref_index: u32,
    cited_id: String,
    provenance: String,
}
```

#### Writer Implementation

```rust
pub struct SegmentedPartitionWriter {
    partition_dir: PathBuf,
    buffers: HashMap<String, PartitionBuffer>,
    segment_counts: HashMap<String, u32>,
    flush_threshold: usize,
}

impl SegmentedPartitionWriter {
    pub fn new(partition_dir: &Path, flush_threshold: usize) -> Result<Self> {
        fs::create_dir_all(partition_dir)?;
        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            buffers: HashMap::new(),
            segment_counts: HashMap::new(),
            flush_threshold,
        })
    }

    pub fn write(&mut self, row: PartitionRow) -> Result<()> {
        let key = partition_key(&row.cited_id);
        let buffer = self.buffers.entry(key.clone())
            .or_insert_with(PartitionBuffer::new);

        buffer.push(row);

        if buffer.len() >= self.flush_threshold {
            self.flush_partition(&key)?;
        }
        Ok(())
    }

    fn flush_partition(&mut self, key: &str) -> Result<()> {
        let buffer = match self.buffers.get_mut(key) {
            Some(b) if !b.is_empty() => b,
            _ => return Ok(()),
        };

        let segment_num = self.segment_counts.entry(key.to_string()).or_insert(0);

        let partition_path = self.partition_dir.join(key);
        fs::create_dir_all(&partition_path)?;

        let segment_path = partition_path.join(format!("segment_{:04}.parquet", segment_num));
        buffer.write_parquet(&segment_path)?;

        *segment_num += 1;
        buffer.clear();
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self.buffers.keys().cloned().collect();
        for key in keys {
            self.flush_partition(&key)?;
        }
        Ok(())
    }
}
```

#### I/O Comparison

| Aspect | Current | Proposed |
|--------|---------|----------|
| Per-flush I/O | Read N rows + Write N+M rows | Write M rows only |
| Total I/O | O(n²) | O(n) |
| Memory per flush | O(partition size) | O(flush threshold) |

### 3. Deduplicated ref_json Store

Store each reference JSON exactly once, keyed by `(citing_doi, ref_index)`.

#### Storage Structure

```
ref_store/
├── segment_0000.parquet
├── segment_0001.parquet
└── ...

Schema: (citing_doi: String, ref_index: u32, ref_json: String)
```

#### Writer

```rust
pub struct RefJsonStore {
    output_dir: PathBuf,
    buffer: RefJsonBuffer,
    segment_count: u32,
    flush_threshold: usize,
    seen: HashSet<(String, u32)>,
}

impl RefJsonStore {
    pub fn write(&mut self, citing_doi: &str, ref_index: u32, ref_json: &str) -> Result<()> {
        let key = (citing_doi.to_string(), ref_index);

        // Deduplicate within session
        if !self.seen.insert(key) {
            return Ok(());
        }

        self.buffer.push(citing_doi, ref_index, ref_json);

        if self.buffer.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }
}
```

#### Lookup During Aggregation

```rust
pub struct RefJsonLookup {
    index: HashMap<(String, u32), String>,
}

impl RefJsonLookup {
    /// Load ref_json only for the citing DOIs we need
    pub fn load_for_citing_dois(
        store_dir: &Path,
        citing_dois: &HashSet<String>,
    ) -> Result<Self> {
        let mut index = HashMap::new();

        for entry in fs::read_dir(store_dir)? {
            let path = entry?.path();
            if path.extension() != Some("parquet".as_ref()) {
                continue;
            }

            // Filter at scan time - only load matching citing_dois
            let df = LazyFrame::scan_parquet(&path, Default::default())?
                .filter(col("citing_doi").is_in(lit(Series::from_iter(citing_dois.iter().cloned()))))
                .collect()?;

            // Populate index from filtered results
            let citing_col = df.column("citing_doi")?.str()?;
            let ref_idx_col = df.column("ref_index")?.u32()?;
            let ref_json_col = df.column("ref_json")?.str()?;

            for i in 0..df.height() {
                if let (Some(citing), Some(idx), Some(json)) =
                    (citing_col.get(i), ref_idx_col.get(i), ref_json_col.get(i))
                {
                    index.insert((citing.to_string(), idx), json.to_string());
                }
            }
        }

        Ok(Self { index })
    }

    pub fn get(&self, citing_doi: &str, ref_index: u32) -> Option<&str> {
        self.index.get(&(citing_doi.to_string(), ref_index)).map(|s| s.as_str())
    }
}
```

#### Space Savings

If average reference cites 1.5 DOIs:
- Current: 1.5 copies of ref_json per reference
- Proposed: 1 copy per reference
- Savings: ~33% reduction in ref_json storage

### 4. Streaming Aggregation with Integrated Validation

Merge inversion, validation, and output into a single streaming pass.

#### Algorithm

```rust
pub struct AggregationStats {
    pub partitions_processed: usize,
    pub valid_count: usize,
    pub failed_count: usize,
    pub total_citations: usize,
}

pub fn aggregate_and_validate(
    partition_dir: &Path,
    ref_store_dir: &Path,
    crossref_index: &FstIndex,
    datacite_index: Option<&FstIndex>,
    valid_output: &Path,
    failed_output: &Path,
    source_mode: Source,
) -> Result<AggregationStats> {
    let mut valid_writer = BufWriter::new(File::create(valid_output)?);
    let mut failed_writer = BufWriter::new(File::create(failed_output)?);
    let mut stats = AggregationStats::default();

    // Iterate partitions
    for entry in fs::read_dir(partition_dir)? {
        let partition_path = entry?.path();
        if !partition_path.is_dir() {
            continue;
        }

        process_partition(
            &partition_path,
            ref_store_dir,
            crossref_index,
            datacite_index,
            source_mode,
            &mut valid_writer,
            &mut failed_writer,
            &mut stats,
        )?;
    }

    valid_writer.flush()?;
    failed_writer.flush()?;

    Ok(stats)
}

fn process_partition(
    partition_path: &Path,
    ref_store_dir: &Path,
    crossref_index: &FstIndex,
    datacite_index: Option<&FstIndex>,
    source_mode: Source,
    valid_writer: &mut BufWriter<File>,
    failed_writer: &mut BufWriter<File>,
    stats: &mut AggregationStats,
) -> Result<()> {
    // Collect segment files
    let segments: Vec<_> = fs::read_dir(partition_path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension() == Some("parquet".as_ref()))
        .collect();

    if segments.is_empty() {
        return Ok(());
    }

    // Merge all segments
    let lazy_frames: Vec<_> = segments.iter()
        .map(|p| LazyFrame::scan_parquet(p, Default::default()))
        .collect::<Result<_, _>>()?;

    let merged = concat(&lazy_frames, UnionArgs::default())?.collect()?;

    // Group by cited_id
    let grouped = merged.lazy()
        .group_by([col("cited_id")])
        .agg([
            col("citing_doi"),
            col("ref_index"),
            col("provenance"),
        ])
        .collect()?;

    // Collect unique citing_dois for ref_json lookup
    let citing_dois: HashSet<String> = merged.column("citing_doi")?
        .str()?
        .into_iter()
        .filter_map(|s| s.map(|s| s.to_string()))
        .collect();

    let ref_lookup = RefJsonLookup::load_for_citing_dois(ref_store_dir, &citing_dois)?;

    // Process each cited work
    let cited_id_col = grouped.column("cited_id")?.str()?;
    let citing_dois_col = grouped.column("citing_doi")?.list()?;
    let ref_indices_col = grouped.column("ref_index")?.list()?;
    let provenances_col = grouped.column("provenance")?.list()?;

    for i in 0..grouped.height() {
        let cited_id = cited_id_col.get(i).unwrap_or("");

        // Validate against indexes
        let is_valid = match source_mode {
            Source::Crossref => crossref_index.contains(cited_id),
            Source::Datacite | Source::Arxiv => {
                datacite_index.map(|idx| idx.contains(cited_id)).unwrap_or(false)
            }
            Source::All => {
                crossref_index.contains(cited_id) ||
                datacite_index.map(|idx| idx.contains(cited_id)).unwrap_or(false)
            }
        };

        // Build cited_by array
        let cited_by = build_cited_by_array(
            citing_dois_col.get_as_series(i),
            ref_indices_col.get_as_series(i),
            provenances_col.get_as_series(i),
            &ref_lookup,
        )?;

        let citation_count = cited_by.len();
        stats.total_citations += citation_count;

        // Build output record
        let record = serde_json::json!({
            "doi": cited_id,
            "citation_count": citation_count,
            "cited_by": cited_by
        });

        // Write immediately to appropriate output
        if is_valid {
            writeln!(valid_writer, "{}", record)?;
            stats.valid_count += 1;
        } else {
            writeln!(failed_writer, "{}", record)?;
            stats.failed_count += 1;
        }
    }

    stats.partitions_processed += 1;
    Ok(())
}

fn build_cited_by_array(
    citing_dois: Option<Series>,
    ref_indices: Option<Series>,
    provenances: Option<Series>,
    ref_lookup: &RefJsonLookup,
) -> Result<Vec<serde_json::Value>> {
    let citing_dois = citing_dois.ok_or_else(|| anyhow::anyhow!("missing citing_dois"))?;
    let ref_indices = ref_indices.ok_or_else(|| anyhow::anyhow!("missing ref_indices"))?;
    let provenances = provenances.ok_or_else(|| anyhow::anyhow!("missing provenances"))?;

    let dois = citing_dois.str()?;
    let indices = ref_indices.u32()?;
    let provs = provenances.str()?;

    let mut result = Vec::new();

    for j in 0..citing_dois.len() {
        let doi = dois.get(j).unwrap_or("");
        let ref_index = indices.get(j).unwrap_or(0);
        let provenance = provs.get(j).unwrap_or("mined");

        let ref_json_str = ref_lookup.get(doi, ref_index).unwrap_or("null");
        let reference: serde_json::Value = serde_json::from_str(ref_json_str)
            .unwrap_or(serde_json::Value::Null);

        result.push(serde_json::json!({
            "doi": doi,
            "provenance": provenance,
            "reference": reference
        }));
    }

    Ok(result)
}
```

#### Memory Profile

| Data | Size |
|------|------|
| Single partition segments | ~50-100MB |
| Grouped DataFrame | ~50-100MB |
| ref_json lookup (filtered) | ~100-200MB |
| **Peak per partition** | **~200-400MB** |

### 5. Output Format

Output is written in partition order (unsorted). Each line is a complete JSON record:

```json
{"doi":"10.1234/example","citation_count":5,"cited_by":[{"doi":"10.5678/citing1","provenance":"publisher","reference":{...}},{"doi":"10.9999/citing2","provenance":"mined","reference":{...}}]}
```

Downstream consumers can sort if needed. For most use cases (filtering, searching, joining), sorted order is not required.

## Implementation Plan

### New Dependencies

```toml
[dependencies]
fst = "0.4"
memmap2 = "0.9"
```

### Files to Modify

| File | Changes |
|------|---------|
| `src/index/mod.rs` | Replace `DoiIndex` with `FstIndex` |
| `src/index/builder.rs` | External sort + FST construction |
| `src/index/persistence.rs` | Simplify to FST load/save |
| `src/streaming/mod.rs` | Add `ref_store` module |
| `src/streaming/partition_writer.rs` | Segmented append-only writes |
| `src/streaming/partition_invert.rs` | Streaming merge + validation |
| `src/streaming/ref_store.rs` | New: deduplicated ref_json store |
| `src/commands/pipeline.rs` | Wire up new components |

### Files to Remove/Deprecate

| File | Reason |
|------|--------|
| `src/validation/runner.rs` | Merged into aggregation |
| `src/validation/lookup.rs` | Replaced by FST lookup |

### Migration Strategy

1. Implement `FstIndex` alongside existing `DoiIndex`
2. Add `--use-fst` flag to enable new index type
3. Implement segmented partition writer
4. Implement ref_json store
5. Implement streaming aggregation
6. Test with small datasets
7. Test with full datasets
8. Remove old code paths

## Testing Strategy

### Unit Tests

- FST construction and lookup
- Segmented partition writer flush behavior
- ref_json store deduplication
- Streaming aggregation correctness

### Integration Tests

- End-to-end pipeline with small dataset
- Verify output matches current implementation
- Memory profiling with medium dataset

### Performance Tests

- Benchmark FST vs HashSet lookup speed
- Measure I/O reduction from append-only writes
- Profile memory usage at each phase
- Test with full Crossref/DataCite datasets

## Rollback Plan

Keep existing code paths behind feature flags until new implementation is validated:

```rust
pub enum IndexType {
    HashSet,  // Current
    Fst,      // New
}
```

## Success Metrics

- Peak memory usage < 4GB for full dataset
- Total runtime comparable or better than current
- Output identical to current implementation (modulo ordering)
- No increase in validation false positives/negatives
