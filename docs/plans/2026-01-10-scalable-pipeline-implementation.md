# Scalable Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Redesign the pipeline to process 200M+ DOIs with <4GB memory using FST indexes, append-only partition writes, deduplicated ref_json storage, and streaming aggregation.

**Architecture:** Replace in-memory HashSet indexes with disk-backed FST for ~50x memory reduction. Partition extraction uses append-only segments instead of read-modify-write. Aggregation streams through partitions one at a time, validating and writing output immediately.

**Tech Stack:** Rust, fst crate (FST construction/lookup), memmap2 (memory-mapped files), polars (DataFrame operations)

**Design Doc:** `docs/plans/2026-01-10-scalable-pipeline-design.md`

---

## Task 1: Add FST Dependencies

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add fst and memmap2 dependencies**

Add to `Cargo.toml` after the `uuid` dependency:

```toml
# Memory-efficient string set
fst = "0.4"
memmap2 = "0.9"
```

**Step 2: Verify dependencies resolve**

Run: `cargo check`
Expected: Compiles without errors

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: add fst and memmap2 dependencies"
```

---

## Task 2: Create FstIndex Structure

**Files:**
- Create: `src/index/fst_index.rs`
- Modify: `src/index/mod.rs`

**Step 1: Write the failing test for FstIndex**

Create `src/index/fst_index.rs`:

```rust
use anyhow::Result;
use fst::{Set, SetBuilder};
use memmap2::Mmap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

/// Memory-efficient DOI index using FST (Finite State Transducer)
/// Provides ~50x memory reduction compared to HashSet<String>
pub struct FstIndex {
    set: Set<Mmap>,
}

impl FstIndex {
    /// Load an FST index from disk
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let set = Set::new(mmap)?;
        Ok(Self { set })
    }

    /// Check if a DOI exists in the index (case-insensitive)
    pub fn contains(&self, doi: &str) -> bool {
        self.set.contains(doi.to_lowercase().as_bytes())
    }

    /// Get the number of entries in the index
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

/// Builder for creating FST indexes from sorted DOI iterators
pub struct FstIndexBuilder {
    builder: SetBuilder<BufWriter<File>>,
}

impl FstIndexBuilder {
    /// Create a new builder that writes to the given path
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        let builder = SetBuilder::new(writer)?;
        Ok(Self { builder })
    }

    /// Insert a DOI (must be called in sorted order)
    pub fn insert(&mut self, doi: &str) -> Result<()> {
        self.builder.insert(doi.to_lowercase().as_bytes())?;
        Ok(())
    }

    /// Finish building and write the FST to disk
    pub fn finish(self) -> Result<()> {
        self.builder.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_fst_index_build_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fst");

        // Build index with sorted DOIs
        {
            let mut builder = FstIndexBuilder::new(&path).unwrap();
            builder.insert("10.1234/aaa").unwrap();
            builder.insert("10.1234/bbb").unwrap();
            builder.insert("10.5678/ccc").unwrap();
            builder.finish().unwrap();
        }

        // Load and query
        let index = FstIndex::load(&path).unwrap();
        assert_eq!(index.len(), 3);
        assert!(index.contains("10.1234/aaa"));
        assert!(index.contains("10.1234/AAA")); // Case insensitive
        assert!(index.contains("10.5678/ccc"));
        assert!(!index.contains("10.9999/missing"));
    }

    #[test]
    fn test_fst_index_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.fst");

        {
            let builder = FstIndexBuilder::new(&path).unwrap();
            builder.finish().unwrap();
        }

        let index = FstIndex::load(&path).unwrap();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(!index.contains("anything"));
    }
}
```

**Step 2: Add module to index/mod.rs**

Add to `src/index/mod.rs` after existing module declarations:

```rust
pub mod fst_index;
pub use fst_index::{FstIndex, FstIndexBuilder};
```

**Step 3: Run test to verify it passes**

Run: `cargo test fst_index --lib`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/index/fst_index.rs src/index/mod.rs
git commit -m "feat: add FstIndex for memory-efficient DOI lookup"
```

---

## Task 3: Add External Sort Helper

**Files:**
- Modify: `src/index/fst_index.rs`

**Step 1: Write failing test for sorted iterator from file**

Add to `src/index/fst_index.rs` tests module:

```rust
#[test]
fn test_build_fst_from_unsorted_dois() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sorted.fst");

    // Unsorted DOIs
    let dois = vec![
        "10.5678/zzz".to_string(),
        "10.1234/aaa".to_string(),
        "10.1234/bbb".to_string(),
        "10.5678/zzz".to_string(), // Duplicate
    ];

    build_fst_from_iter(dois.into_iter(), &path, dir.path()).unwrap();

    let index = FstIndex::load(&path).unwrap();
    assert_eq!(index.len(), 3); // Deduplicated
    assert!(index.contains("10.1234/aaa"));
    assert!(index.contains("10.5678/zzz"));
}
```

**Step 2: Implement build_fst_from_iter with external sort**

Add to `src/index/fst_index.rs` before the tests module:

```rust
use std::io::{BufRead, BufReader, Write};
use std::process::Command;

/// Build an FST index from an iterator of DOIs (handles sorting and deduplication)
pub fn build_fst_from_iter<I>(dois: I, output_path: &Path, temp_dir: &Path) -> Result<()>
where
    I: Iterator<Item = String>,
{
    let unsorted_path = temp_dir.join("dois_unsorted.txt");
    let sorted_path = temp_dir.join("dois_sorted.txt");

    // Phase 1: Write DOIs to temp file
    {
        let mut writer = BufWriter::new(File::create(&unsorted_path)?);
        for doi in dois {
            writeln!(writer, "{}", doi.to_lowercase())?;
        }
        writer.flush()?;
    }

    // Phase 2: External sort with deduplication
    let status = Command::new("sort")
        .arg("-u") // unique
        .arg("-o")
        .arg(&sorted_path)
        .arg(&unsorted_path)
        .status()?;

    if !status.success() {
        anyhow::bail!("sort command failed with status: {}", status);
    }

    // Phase 3: Stream sorted file into FST builder
    let mut builder = FstIndexBuilder::new(output_path)?;
    let reader = BufReader::new(File::open(&sorted_path)?);

    for line in reader.lines() {
        let doi = line?;
        if !doi.is_empty() {
            builder.insert(&doi)?;
        }
    }

    builder.finish()?;

    // Cleanup temp files
    let _ = std::fs::remove_file(&unsorted_path);
    let _ = std::fs::remove_file(&sorted_path);

    Ok(())
}
```

Add the import at the top of the file:

```rust
use anyhow::Result;
```

**Step 3: Run test to verify it passes**

Run: `cargo test build_fst_from_unsorted --lib`
Expected: PASS

**Step 4: Commit**

```bash
git add src/index/fst_index.rs
git commit -m "feat: add build_fst_from_iter with external sort"
```

---

## Task 4: Add FST Builder Integration for DataCite/Crossref Sources

**Files:**
- Modify: `src/index/fst_index.rs`
- Modify: `src/index/builder.rs`

**Step 1: Write failing test for building FST from source**

Add to `src/index/fst_index.rs` tests:

```rust
#[test]
fn test_build_fst_index_from_source() {
    use crate::input::{open_datacite_source, DataciteInput};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create test JSONL.gz file
    let file = NamedTempFile::new().unwrap();
    let encoder = GzEncoder::new(file.reopen().unwrap(), Compression::default());
    let mut writer = std::io::BufWriter::new(encoder);
    writeln!(writer, r#"{{"id": "10.5678/zzz"}}"#).unwrap();
    writeln!(writer, r#"{{"id": "10.1234/aaa"}}"#).unwrap();
    writeln!(writer, r#"{{"id": "10.1234/bbb"}}"#).unwrap();
    writer.into_inner().unwrap().finish().unwrap();

    let dir = tempdir().unwrap();
    let fst_path = dir.path().join("datacite.fst");

    let input = DataciteInput::SingleJsonlGz(file.path().to_path_buf());
    let source = open_datacite_source(input).unwrap();

    build_fst_index_from_source(source, "id", &fst_path, dir.path()).unwrap();

    let index = FstIndex::load(&fst_path).unwrap();
    assert_eq!(index.len(), 3);
    assert!(index.contains("10.1234/aaa"));
    assert!(index.contains("10.5678/zzz"));
}
```

**Step 2: Implement build_fst_index_from_source**

Add to `src/index/fst_index.rs`:

```rust
use log::info;
use serde_json::Value;
use std::time::Instant;

/// Build an FST index from a source of JSON records
pub fn build_fst_index_from_source<I>(
    source: I,
    id_field: &str,
    output_path: &Path,
    temp_dir: &Path,
) -> Result<()>
where
    I: Iterator<Item = Result<Value>>,
{
    info!("Building FST index from source");
    let start = Instant::now();

    let unsorted_path = temp_dir.join("dois_unsorted.txt");
    let sorted_path = temp_dir.join("dois_sorted.txt");

    // Phase 1: Extract DOIs to temp file
    let mut count = 0;
    {
        let mut writer = BufWriter::new(File::create(&unsorted_path)?);
        for record_result in source {
            match record_result {
                Ok(record) => {
                    if let Some(id) = record.get(id_field).and_then(|v| v.as_str()) {
                        writeln!(writer, "{}", id.to_lowercase())?;
                        count += 1;
                        if count % 500_000 == 0 {
                            info!("  Extracted {} DOIs...", count);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Failed to parse record: {}", e);
                }
            }
        }
        writer.flush()?;
    }

    info!("Extracted {} DOIs, sorting...", count);

    // Phase 2: External sort
    let status = Command::new("sort")
        .arg("-u")
        .arg("-o")
        .arg(&sorted_path)
        .arg(&unsorted_path)
        .status()?;

    if !status.success() {
        anyhow::bail!("sort command failed");
    }

    // Phase 3: Build FST
    let mut builder = FstIndexBuilder::new(output_path)?;
    let reader = BufReader::new(File::open(&sorted_path)?);
    let mut fst_count = 0;

    for line in reader.lines() {
        let doi = line?;
        if !doi.is_empty() {
            builder.insert(&doi)?;
            fst_count += 1;
        }
    }

    builder.finish()?;

    // Cleanup
    let _ = std::fs::remove_file(&unsorted_path);
    let _ = std::fs::remove_file(&sorted_path);

    info!(
        "Built FST index with {} DOIs in {:.1}s",
        fst_count,
        start.elapsed().as_secs_f64()
    );

    Ok(())
}
```

**Step 3: Run test to verify it passes**

Run: `cargo test build_fst_index_from_source --lib`
Expected: PASS

**Step 4: Commit**

```bash
git add src/index/fst_index.rs
git commit -m "feat: add build_fst_index_from_source for DataCite/Crossref"
```

---

## Task 5: Create Segmented Partition Writer

**Files:**
- Create: `src/streaming/segmented_writer.rs`
- Modify: `src/streaming/mod.rs`

**Step 1: Write failing test for segmented partition writer**

Create `src/streaming/segmented_writer.rs`:

```rust
use anyhow::{Context, Result};
use log::debug;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use super::partition_key;

/// A slim row for partitioned storage (no ref_json)
#[derive(Debug, Clone)]
pub struct PartitionRow {
    pub citing_doi: String,
    pub ref_index: u32,
    pub cited_id: String,
    pub provenance: String,
}

/// Buffer for accumulating rows before writing a segment
struct SegmentBuffer {
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    cited_ids: Vec<String>,
    provenances: Vec<String>,
}

impl SegmentBuffer {
    fn new() -> Self {
        Self {
            citing_dois: Vec::new(),
            ref_indices: Vec::new(),
            cited_ids: Vec::new(),
            provenances: Vec::new(),
        }
    }

    fn push(&mut self, row: PartitionRow) {
        self.citing_dois.push(row.citing_doi);
        self.ref_indices.push(row.ref_index);
        self.cited_ids.push(row.cited_id);
        self.provenances.push(row.provenance);
    }

    fn len(&self) -> usize {
        self.citing_dois.len()
    }

    fn is_empty(&self) -> bool {
        self.citing_dois.is_empty()
    }

    fn clear(&mut self) {
        self.citing_dois.clear();
        self.ref_indices.clear();
        self.cited_ids.clear();
        self.provenances.clear();
    }

    fn to_dataframe(&self) -> Result<DataFrame> {
        DataFrame::new(vec![
            Column::new("citing_doi".into(), &self.citing_dois),
            Column::new("ref_index".into(), &self.ref_indices),
            Column::new("cited_id".into(), &self.cited_ids),
            Column::new("provenance".into(), &self.provenances),
        ])
        .map_err(|e| anyhow::anyhow!("Failed to create DataFrame: {}", e))
    }

    fn write_parquet(&self, path: &Path) -> Result<()> {
        let mut df = self.to_dataframe()?;
        let file = File::create(path)
            .with_context(|| format!("Failed to create segment file: {:?}", path))?;

        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)
            .context("Failed to write segment parquet")?;

        Ok(())
    }
}

/// Writes partition rows to append-only segment files
pub struct SegmentedPartitionWriter {
    partition_dir: PathBuf,
    buffers: HashMap<String, SegmentBuffer>,
    segment_counts: HashMap<String, u32>,
    flush_threshold: usize,
    total_rows_written: usize,
}

impl SegmentedPartitionWriter {
    /// Create a new segmented partition writer
    pub fn new(partition_dir: &Path, flush_threshold: usize) -> Result<Self> {
        fs::create_dir_all(partition_dir).with_context(|| {
            format!("Failed to create partition directory: {:?}", partition_dir)
        })?;

        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            buffers: HashMap::new(),
            segment_counts: HashMap::new(),
            flush_threshold,
            total_rows_written: 0,
        })
    }

    /// Write a row to the appropriate partition
    pub fn write(&mut self, row: PartitionRow) -> Result<()> {
        let key = partition_key(&row.cited_id);
        let buffer = self.buffers.entry(key.clone()).or_insert_with(SegmentBuffer::new);

        buffer.push(row);

        if buffer.len() >= self.flush_threshold {
            self.flush_partition(&key)?;
        }

        Ok(())
    }

    /// Flush a specific partition to a new segment file
    fn flush_partition(&mut self, key: &str) -> Result<()> {
        let buffer = match self.buffers.get_mut(key) {
            Some(b) if !b.is_empty() => b,
            _ => return Ok(()),
        };

        let segment_num = self.segment_counts.entry(key.to_string()).or_insert(0);

        // Create partition subdirectory
        let partition_path = self.partition_dir.join(key);
        fs::create_dir_all(&partition_path)?;

        // Write new segment file (append-only, never reads existing)
        let segment_path = partition_path.join(format!("segment_{:04}.parquet", segment_num));
        let rows_in_segment = buffer.len();
        buffer.write_parquet(&segment_path)?;

        debug!(
            "Wrote segment {} for partition {} ({} rows)",
            segment_num, key, rows_in_segment
        );

        *segment_num += 1;
        self.total_rows_written += rows_in_segment;
        buffer.clear();

        Ok(())
    }

    /// Flush all remaining buffered data
    pub fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self.buffers.keys().cloned().collect();
        for key in keys {
            self.flush_partition(&key)?;
        }
        Ok(())
    }

    /// Get total rows written across all partitions
    pub fn total_rows_written(&self) -> usize {
        self.total_rows_written
    }

    /// Get number of partitions
    pub fn partition_count(&self) -> usize {
        self.segment_counts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segmented_writer_creates_segments() {
        let dir = tempdir().unwrap();
        let mut writer = SegmentedPartitionWriter::new(dir.path(), 2).unwrap();

        // Write 5 rows to same partition - should create 2 segments (2+2) + 1 on flush
        for i in 0..5 {
            writer
                .write(PartitionRow {
                    citing_doi: format!("10.1111/citing{}", i),
                    ref_index: i as u32,
                    cited_id: "10.1234/cited".to_string(),
                    provenance: "mined".to_string(),
                })
                .unwrap();
        }
        writer.flush_all().unwrap();

        // Check segments exist
        let partition_dir = dir.path().join("10.1234");
        assert!(partition_dir.exists());
        assert!(partition_dir.join("segment_0000.parquet").exists());
        assert!(partition_dir.join("segment_0001.parquet").exists());
        assert!(partition_dir.join("segment_0002.parquet").exists());

        assert_eq!(writer.total_rows_written(), 5);
    }

    #[test]
    fn test_segmented_writer_multiple_partitions() {
        let dir = tempdir().unwrap();
        let mut writer = SegmentedPartitionWriter::new(dir.path(), 10).unwrap();

        writer
            .write(PartitionRow {
                citing_doi: "10.1111/a".to_string(),
                ref_index: 0,
                cited_id: "10.1234/cited1".to_string(),
                provenance: "publisher".to_string(),
            })
            .unwrap();

        writer
            .write(PartitionRow {
                citing_doi: "10.1111/b".to_string(),
                ref_index: 0,
                cited_id: "10.5678/cited2".to_string(),
                provenance: "mined".to_string(),
            })
            .unwrap();

        writer.flush_all().unwrap();

        assert!(dir.path().join("10.1234").exists());
        assert!(dir.path().join("10.5678").exists());
        assert_eq!(writer.partition_count(), 2);
    }
}
```

**Step 2: Add module to streaming/mod.rs**

Add to `src/streaming/mod.rs`:

```rust
pub mod segmented_writer;
pub use segmented_writer::{PartitionRow, SegmentedPartitionWriter};
```

**Step 3: Run tests to verify they pass**

Run: `cargo test segmented_writer --lib`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/streaming/segmented_writer.rs src/streaming/mod.rs
git commit -m "feat: add SegmentedPartitionWriter with append-only segments"
```

---

## Task 6: Create RefJson Store

**Files:**
- Create: `src/streaming/ref_store.rs`
- Modify: `src/streaming/mod.rs`

**Step 1: Write failing test for ref_json store**

Create `src/streaming/ref_store.rs`:

```rust
use anyhow::{Context, Result};
use log::debug;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

/// Stores reference JSON deduplicated by (citing_doi, ref_index)
pub struct RefJsonStore {
    output_dir: PathBuf,
    citing_dois: Vec<String>,
    ref_indices: Vec<u32>,
    ref_jsons: Vec<String>,
    segment_count: u32,
    flush_threshold: usize,
    seen: HashSet<(String, u32)>,
}

impl RefJsonStore {
    /// Create a new ref_json store
    pub fn new(output_dir: &Path, flush_threshold: usize) -> Result<Self> {
        fs::create_dir_all(output_dir)
            .with_context(|| format!("Failed to create ref_store directory: {:?}", output_dir))?;

        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            citing_dois: Vec::new(),
            ref_indices: Vec::new(),
            ref_jsons: Vec::new(),
            segment_count: 0,
            flush_threshold,
            seen: HashSet::new(),
        })
    }

    /// Write a reference JSON (deduplicates by citing_doi + ref_index)
    pub fn write(&mut self, citing_doi: &str, ref_index: u32, ref_json: &str) -> Result<bool> {
        let key = (citing_doi.to_string(), ref_index);

        // Skip if already seen
        if !self.seen.insert(key) {
            return Ok(false);
        }

        self.citing_dois.push(citing_doi.to_string());
        self.ref_indices.push(ref_index);
        self.ref_jsons.push(ref_json.to_string());

        if self.citing_dois.len() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(true)
    }

    /// Flush buffered data to a new segment file
    pub fn flush(&mut self) -> Result<()> {
        if self.citing_dois.is_empty() {
            return Ok(());
        }

        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), &self.citing_dois),
            Column::new("ref_index".into(), &self.ref_indices),
            Column::new("ref_json".into(), &self.ref_jsons),
        ])?;

        let segment_path = self.output_dir.join(format!("segment_{:04}.parquet", self.segment_count));
        let file = File::create(&segment_path)?;

        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)?;

        debug!(
            "Wrote ref_store segment {} ({} refs)",
            self.segment_count,
            self.citing_dois.len()
        );

        self.segment_count += 1;
        self.citing_dois.clear();
        self.ref_indices.clear();
        self.ref_jsons.clear();

        Ok(())
    }

    /// Get count of unique references stored
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

/// Lookup for loading ref_json on demand during aggregation
pub struct RefJsonLookup {
    index: HashMap<(String, u32), String>,
}

impl RefJsonLookup {
    /// Load ref_json for a specific set of citing DOIs
    pub fn load_for_citing_dois(store_dir: &Path, citing_dois: &HashSet<String>) -> Result<Self> {
        let mut index = HashMap::new();

        if citing_dois.is_empty() {
            return Ok(Self { index });
        }

        for entry in fs::read_dir(store_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }

            let df = LazyFrame::scan_parquet(&path, Default::default())?
                .collect()?;

            let citing_col = df.column("citing_doi")?.str()?;
            let ref_idx_col = df.column("ref_index")?.u32()?;
            let ref_json_col = df.column("ref_json")?.str()?;

            for i in 0..df.height() {
                if let (Some(citing), Some(idx), Some(json)) =
                    (citing_col.get(i), ref_idx_col.get(i), ref_json_col.get(i))
                {
                    if citing_dois.contains(citing) {
                        index.insert((citing.to_string(), idx), json.to_string());
                    }
                }
            }
        }

        Ok(Self { index })
    }

    /// Get ref_json for a specific citation
    pub fn get(&self, citing_doi: &str, ref_index: u32) -> Option<&str> {
        self.index
            .get(&(citing_doi.to_string(), ref_index))
            .map(|s| s.as_str())
    }

    /// Get count of loaded references
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_ref_store_deduplicates() {
        let dir = tempdir().unwrap();
        let mut store = RefJsonStore::new(dir.path(), 100).unwrap();

        // Write same ref twice
        assert!(store.write("10.1234/a", 0, r#"{"key": "value1"}"#).unwrap());
        assert!(!store.write("10.1234/a", 0, r#"{"key": "value1"}"#).unwrap()); // Duplicate

        // Write different ref
        assert!(store.write("10.1234/a", 1, r#"{"key": "value2"}"#).unwrap());

        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_ref_store_and_lookup() {
        let dir = tempdir().unwrap();

        // Write refs
        {
            let mut store = RefJsonStore::new(dir.path(), 100).unwrap();
            store.write("10.1234/a", 0, r#"{"ref": "first"}"#).unwrap();
            store.write("10.1234/a", 1, r#"{"ref": "second"}"#).unwrap();
            store.write("10.5678/b", 0, r#"{"ref": "third"}"#).unwrap();
            store.flush().unwrap();
        }

        // Load only for specific citing DOIs
        let mut dois = HashSet::new();
        dois.insert("10.1234/a".to_string());

        let lookup = RefJsonLookup::load_for_citing_dois(dir.path(), &dois).unwrap();

        assert_eq!(lookup.len(), 2);
        assert_eq!(lookup.get("10.1234/a", 0), Some(r#"{"ref": "first"}"#));
        assert_eq!(lookup.get("10.1234/a", 1), Some(r#"{"ref": "second"}"#));
        assert_eq!(lookup.get("10.5678/b", 0), None); // Not in filter set
    }
}
```

**Step 2: Add module to streaming/mod.rs**

Add to `src/streaming/mod.rs`:

```rust
pub mod ref_store;
pub use ref_store::{RefJsonLookup, RefJsonStore};
```

**Step 3: Run tests to verify they pass**

Run: `cargo test ref_store --lib`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/streaming/ref_store.rs src/streaming/mod.rs
git commit -m "feat: add RefJsonStore for deduplicated reference storage"
```

---

## Task 7: Create Streaming Aggregator

**Files:**
- Create: `src/streaming/aggregator.rs`
- Modify: `src/streaming/mod.rs`

**Step 1: Write failing test for streaming aggregation**

Create `src/streaming/aggregator.rs`:

```rust
use anyhow::{Context, Result};
use log::info;
use polars::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use super::ref_store::RefJsonLookup;
use crate::index::FstIndex;

/// Statistics from aggregation
#[derive(Debug, Clone, Default)]
pub struct AggregationStats {
    pub partitions_processed: usize,
    pub valid_count: usize,
    pub failed_count: usize,
    pub total_citations: usize,
}

/// Aggregate partitions with streaming validation
pub fn aggregate_and_validate(
    partition_dir: &Path,
    ref_store_dir: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_output: &Path,
    failed_output: &Path,
) -> Result<AggregationStats> {
    let mut valid_writer = BufWriter::new(File::create(valid_output)?);
    let mut failed_writer = BufWriter::new(File::create(failed_output)?);
    let mut stats = AggregationStats::default();

    // Iterate partition directories
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
            &mut valid_writer,
            &mut failed_writer,
            &mut stats,
        )?;
    }

    valid_writer.flush()?;
    failed_writer.flush()?;

    info!(
        "Aggregation complete: {} valid, {} failed, {} total citations",
        stats.valid_count, stats.failed_count, stats.total_citations
    );

    Ok(stats)
}

fn process_partition(
    partition_path: &Path,
    ref_store_dir: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_writer: &mut BufWriter<File>,
    failed_writer: &mut BufWriter<File>,
    stats: &mut AggregationStats,
) -> Result<()> {
    // Collect segment files
    let segments: Vec<_> = fs::read_dir(partition_path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .collect();

    if segments.is_empty() {
        return Ok(());
    }

    // Merge all segments
    let lazy_frames: Result<Vec<_>, _> = segments
        .iter()
        .map(|p| LazyFrame::scan_parquet(p, Default::default()))
        .collect();

    let merged = concat(&lazy_frames?, UnionArgs::default())?.collect()?;

    // Group by cited_id
    let grouped = merged
        .lazy()
        .group_by([col("cited_id")])
        .agg([
            col("citing_doi"),
            col("ref_index"),
            col("provenance"),
        ])
        .collect()?;

    // Collect unique citing_dois for ref_json lookup
    let citing_dois: HashSet<String> = merged
        .column("citing_doi")?
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
        let is_valid = crossref_index.map(|idx| idx.contains(cited_id)).unwrap_or(false)
            || datacite_index.map(|idx| idx.contains(cited_id)).unwrap_or(false);

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

        // Write immediately
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
        let reference: serde_json::Value =
            serde_json::from_str(ref_json_str).unwrap_or(serde_json::Value::Null);

        result.push(serde_json::json!({
            "doi": doi,
            "provenance": provenance,
            "reference": reference
        }));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::{PartitionRow, RefJsonStore, SegmentedPartitionWriter};
    use tempfile::tempdir;

    #[test]
    fn test_aggregate_and_validate() {
        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");
        let ref_store_dir = dir.path().join("ref_store");
        let valid_output = dir.path().join("valid.jsonl");
        let failed_output = dir.path().join("failed.jsonl");

        // Create test data
        {
            let mut partition_writer =
                SegmentedPartitionWriter::new(&partition_dir, 100).unwrap();
            let mut ref_store = RefJsonStore::new(&ref_store_dir, 100).unwrap();

            // Two citations to same work
            partition_writer
                .write(PartitionRow {
                    citing_doi: "10.1111/citing1".to_string(),
                    ref_index: 0,
                    cited_id: "10.1234/cited".to_string(),
                    provenance: "publisher".to_string(),
                })
                .unwrap();
            ref_store
                .write("10.1111/citing1", 0, r#"{"unstructured": "ref 1"}"#)
                .unwrap();

            partition_writer
                .write(PartitionRow {
                    citing_doi: "10.1111/citing2".to_string(),
                    ref_index: 0,
                    cited_id: "10.1234/cited".to_string(),
                    provenance: "mined".to_string(),
                })
                .unwrap();
            ref_store
                .write("10.1111/citing2", 0, r#"{"unstructured": "ref 2"}"#)
                .unwrap();

            partition_writer.flush_all().unwrap();
            ref_store.flush().unwrap();
        }

        // Build a simple FST index containing the cited DOI
        let fst_path = dir.path().join("test.fst");
        {
            use crate::index::FstIndexBuilder;
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.1234/cited").unwrap();
            builder.finish().unwrap();
        }
        let index = FstIndex::load(&fst_path).unwrap();

        // Run aggregation
        let stats = aggregate_and_validate(
            &partition_dir,
            &ref_store_dir,
            Some(&index),
            None,
            &valid_output,
            &failed_output,
        )
        .unwrap();

        assert_eq!(stats.valid_count, 1);
        assert_eq!(stats.failed_count, 0);
        assert_eq!(stats.total_citations, 2);

        // Verify output
        let valid_content = std::fs::read_to_string(&valid_output).unwrap();
        assert!(valid_content.contains("10.1234/cited"));
        assert!(valid_content.contains("citation_count\":2"));
    }
}
```

**Step 2: Add module to streaming/mod.rs**

Add to `src/streaming/mod.rs`:

```rust
pub mod aggregator;
pub use aggregator::{aggregate_and_validate, AggregationStats};
```

**Step 3: Run test to verify it passes**

Run: `cargo test aggregator --lib`
Expected: PASS

**Step 4: Commit**

```bash
git add src/streaming/aggregator.rs src/streaming/mod.rs
git commit -m "feat: add streaming aggregator with integrated validation"
```

---

## Task 8: Add CLI Flag for New Pipeline Mode

**Files:**
- Modify: `src/cli.rs`

**Step 1: Add --scalable flag to PipelineArgs**

Find the `PipelineArgs` struct in `src/cli.rs` and add:

```rust
    /// Use scalable pipeline with FST indexes and streaming aggregation
    #[arg(long, default_value = "false")]
    pub scalable: bool,

    /// Path to pre-built Crossref FST index (for scalable mode)
    #[arg(long)]
    pub crossref_fst: Option<String>,

    /// Path to pre-built DataCite FST index (for scalable mode)
    #[arg(long)]
    pub datacite_fst: Option<String>,

    /// Directory for ref_json store (for scalable mode)
    #[arg(long)]
    pub ref_store_dir: Option<String>,
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: Compiles without errors

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat: add --scalable flag and FST path options to CLI"
```

---

## Task 9: Integrate Scalable Pipeline into Commands

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Add scalable pipeline execution path**

Add a new function to `src/commands/pipeline.rs`:

```rust
use crate::index::{FstIndex, build_fst_index_from_source, build_fst_from_iter};
use crate::streaming::{
    aggregate_and_validate, AggregationStats, PartitionRow, RefJsonStore,
    SegmentedPartitionWriter,
};

/// Run the scalable pipeline with FST indexes and streaming aggregation
fn run_scalable_pipeline(args: &PipelineArgs) -> Result<()> {
    info!("Running scalable pipeline mode");

    let partition_dir = args.temp_dir.as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("crossref-scalable-{}", Uuid::new_v4())));

    let ref_store_dir = args.ref_store_dir.as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| partition_dir.join("ref_store"));

    fs::create_dir_all(&partition_dir)?;
    fs::create_dir_all(&ref_store_dir)?;

    // Load or build FST indexes
    let crossref_fst = load_or_build_crossref_fst(args, &partition_dir)?;
    let datacite_fst = load_or_build_datacite_fst(args, &partition_dir)?;

    // Phase 1: Extraction with segmented partitions
    info!("=== Extraction Phase (Scalable) ===");
    run_scalable_extraction(args, &partition_dir, &ref_store_dir)?;

    // Phase 2: Aggregation with streaming validation
    info!("=== Aggregation Phase (Scalable) ===");
    let valid_output = args.output_crossref.as_ref()
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

    info!("Scalable pipeline complete:");
    info!("  Valid: {}", stats.valid_count);
    info!("  Failed: {}", stats.failed_count);
    info!("  Total citations: {}", stats.total_citations);

    Ok(())
}

fn load_or_build_crossref_fst(args: &PipelineArgs, temp_dir: &Path) -> Result<Option<FstIndex>> {
    if let Some(ref path) = args.crossref_fst {
        info!("Loading Crossref FST from: {}", path);
        return Ok(Some(FstIndex::load(Path::new(path))?));
    }

    // Build from input during extraction (collected DOIs)
    // For now, return None - will be built during extraction
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

fn run_scalable_extraction(
    args: &PipelineArgs,
    partition_dir: &Path,
    ref_store_dir: &Path,
) -> Result<()> {
    let flush_threshold = args.batch_size / 100;
    let mut partition_writer = SegmentedPartitionWriter::new(partition_dir, flush_threshold.max(10000))?;
    let mut ref_store = RefJsonStore::new(ref_store_dir, flush_threshold.max(10000))?;

    let input = detect_crossref_input(&args.input)?;
    let source = open_crossref_source(input)?;

    let mut items_processed = 0;
    let mut refs_extracted = 0;

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

                        // Extract DOIs
                        let search_text = build_search_text(reference);
                        if search_text.is_empty() {
                            continue;
                        }

                        let matches = match args.source {
                            Source::Arxiv => extract_arxiv_as_dois(&search_text),
                            _ => extract_doi_matches_from_text(&search_text),
                        };

                        for m in matches {
                            if should_include_citation(&work_doi, &m.doi) {
                                partition_writer.write(PartitionRow {
                                    citing_doi: work_doi.clone(),
                                    ref_index,
                                    cited_id: m.doi,
                                    provenance: determine_provenance(reference, &m.raw).as_str().to_string(),
                                })?;
                                refs_extracted += 1;
                            }
                        }
                    }
                }

                if items_processed % 100_000 == 0 {
                    info!("Progress: {} items, {} refs extracted", items_processed, refs_extracted);
                }
            }
        }
    }

    partition_writer.flush_all()?;
    ref_store.flush()?;

    info!("Extraction complete: {} items, {} refs", items_processed, refs_extracted);

    Ok(())
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

fn extract_arxiv_as_dois(text: &str) -> Vec<DoiMatch> {
    extract_arxiv_matches_from_text(text)
        .into_iter()
        .map(|m| DoiMatch {
            doi: format!("10.48550/arXiv.{}", m.id),
            raw: m.raw,
            provenance: Provenance::Mined,
        })
        .collect()
}
```

**Step 2: Update run_pipeline to check scalable flag**

Modify the `run_pipeline` function to add at the beginning:

```rust
pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting citation extraction pipeline");
    info!("Input: {}", args.input);
    info!("Source mode: {}", args.source);

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    // Use scalable pipeline if requested
    if args.scalable {
        return run_scalable_pipeline(&args);
    }

    // ... rest of existing pipeline code ...
```

**Step 3: Verify compilation**

Run: `cargo check`
Expected: Compiles (may have warnings for unused imports that will be resolved)

**Step 4: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "feat: integrate scalable pipeline with --scalable flag"
```

---

## Task 10: Add Build-Index Command

**Files:**
- Create: `src/commands/build_index.rs`
- Modify: `src/commands/mod.rs`
- Modify: `src/cli.rs`
- Modify: `src/main.rs`

**Step 1: Create build_index command**

Create `src/commands/build_index.rs`:

```rust
use anyhow::Result;
use log::info;
use std::path::Path;

use crate::common::setup_logging;
use crate::index::build_fst_index_from_source;
use crate::input::{detect_crossref_input, detect_datacite_input, open_crossref_source, open_datacite_source};

/// Arguments for build-index command
#[derive(Debug, Clone)]
pub struct BuildIndexArgs {
    pub input: String,
    pub output: String,
    pub source_type: String, // "crossref" or "datacite"
    pub log_level: String,
}

pub fn run_build_index(args: BuildIndexArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Building FST index");
    info!("Input: {}", args.input);
    info!("Output: {}", args.output);
    info!("Source type: {}", args.source_type);

    let temp_dir = std::env::temp_dir();
    let output_path = Path::new(&args.output);

    match args.source_type.as_str() {
        "crossref" => {
            let input = detect_crossref_input(&args.input)?;
            let source = open_crossref_source(input)?;

            // Extract DOIs from Crossref items
            let dois = source.flat_map(|entry| {
                entry.ok().and_then(|e| {
                    e.json.get("items").and_then(|items| {
                        items.as_array().map(|arr| {
                            arr.iter()
                                .filter_map(|item| {
                                    item.get("DOI").and_then(|d| d.as_str()).map(|s| Ok(serde_json::json!({"id": s})))
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                }).into_iter().flatten()
            });

            build_fst_index_from_source(dois, "id", output_path, &temp_dir)?;
        }
        "datacite" => {
            let input = detect_datacite_input(&args.input)?;
            let source = open_datacite_source(input)?;
            build_fst_index_from_source(source, "id", output_path, &temp_dir)?;
        }
        _ => {
            anyhow::bail!("Unknown source type: {}. Use 'crossref' or 'datacite'", args.source_type);
        }
    }

    info!("FST index built successfully: {}", args.output);

    Ok(())
}
```

**Step 2: Add to commands/mod.rs**

Add to `src/commands/mod.rs`:

```rust
pub mod build_index;
pub use build_index::{run_build_index, BuildIndexArgs};
```

**Step 3: Add CLI subcommand**

Add to `src/cli.rs` in the `Commands` enum:

```rust
    /// Build FST index from Crossref or DataCite data
    BuildIndex {
        /// Input path (tar.gz, directory, or JSONL file)
        #[arg(short, long)]
        input: String,

        /// Output FST file path
        #[arg(short, long)]
        output: String,

        /// Source type: crossref or datacite
        #[arg(short, long, default_value = "datacite")]
        source_type: String,

        /// Log level
        #[arg(long, default_value = "INFO")]
        log_level: String,
    },
```

**Step 4: Handle in main.rs**

Add to `src/main.rs` in the match statement:

```rust
        Commands::BuildIndex {
            input,
            output,
            source_type,
            log_level,
        } => {
            let args = commands::BuildIndexArgs {
                input,
                output,
                source_type,
                log_level,
            };
            commands::run_build_index(args)
        }
```

**Step 5: Verify compilation**

Run: `cargo build`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/commands/build_index.rs src/commands/mod.rs src/cli.rs src/main.rs
git commit -m "feat: add build-index command for pre-building FST indexes"
```

---

## Task 11: Add Integration Test for Scalable Pipeline

**Files:**
- Modify: `tests/integration_test.rs`

**Step 1: Add integration test**

Add to `tests/integration_test.rs`:

```rust
#[test]
fn test_scalable_pipeline_end_to_end() {
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();

    // Create test Crossref JSON file
    let crossref_dir = dir.path().join("crossref");
    fs::create_dir_all(&crossref_dir).unwrap();

    let crossref_file = crossref_dir.join("test.json");
    let crossref_data = serde_json::json!({
        "items": [
            {
                "DOI": "10.1111/citing1",
                "reference": [
                    {"DOI": "10.1234/cited1", "doi-asserted-by": "publisher"},
                    {"unstructured": "See also 10.1234/cited2 for details"}
                ]
            },
            {
                "DOI": "10.1111/citing2",
                "reference": [
                    {"DOI": "10.1234/cited1"}
                ]
            }
        ]
    });
    fs::write(&crossref_file, serde_json::to_string_pretty(&crossref_data).unwrap()).unwrap();

    // Create test DataCite index (FST)
    let fst_path = dir.path().join("datacite.fst");
    {
        use crossref_citation_extraction::index::FstIndexBuilder;
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.1234/cited1").unwrap();
        builder.insert("10.1234/cited2").unwrap();
        builder.finish().unwrap();
    }

    // Run scalable pipeline
    let output_path = dir.path().join("output.jsonl");
    let partition_dir = dir.path().join("partitions");
    let ref_store_dir = dir.path().join("ref_store");

    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crossref-citation-extraction"))
        .args([
            "pipeline",
            "--input", crossref_dir.to_str().unwrap(),
            "--source", "all",
            "--output-crossref", output_path.to_str().unwrap(),
            "--output-datacite", dir.path().join("datacite.jsonl").to_str().unwrap(),
            "--scalable",
            "--crossref-fst", fst_path.to_str().unwrap(),
            "--datacite-fst", fst_path.to_str().unwrap(),
            "--temp-dir", partition_dir.to_str().unwrap(),
            "--ref-store-dir", ref_store_dir.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to execute command");

    assert!(status.success(), "Scalable pipeline failed");

    // Verify output exists and contains expected data
    let output_content = fs::read_to_string(&output_path).unwrap();
    assert!(output_content.contains("10.1234/cited1"));
}
```

**Step 2: Run integration test**

Run: `cargo test test_scalable_pipeline --test integration_test`
Expected: PASS (or helpful failure message to debug)

**Step 3: Commit**

```bash
git add tests/integration_test.rs
git commit -m "test: add integration test for scalable pipeline"
```

---

## Task 12: Documentation Update

**Files:**
- Modify: `README.md`

**Step 1: Add scalable pipeline documentation**

Add a new section to `README.md`:

```markdown
## Scalable Pipeline Mode

For processing large datasets (100M+ DOIs) on memory-constrained machines, use the scalable pipeline:

### Pre-build FST Indexes (Recommended)

```bash
# Build DataCite FST index
./target/release/crossref-citation-extraction build-index \
    --input /path/to/datacite-records.jsonl.gz \
    --output datacite.fst \
    --source-type datacite

# Build Crossref FST index
./target/release/crossref-citation-extraction build-index \
    --input /path/to/crossref-snapshot.tar.gz \
    --output crossref.fst \
    --source-type crossref
```

### Run Scalable Pipeline

```bash
./target/release/crossref-citation-extraction pipeline \
    --input /path/to/crossref-snapshot.tar.gz \
    --source all \
    --output-crossref crossref-citations.jsonl \
    --output-datacite datacite-citations.jsonl \
    --scalable \
    --crossref-fst crossref.fst \
    --datacite-fst datacite.fst \
    --temp-dir /path/to/temp \
    --ref-store-dir /path/to/ref-store
```

### Memory Usage

| Mode | Peak Memory |
|------|-------------|
| Standard | 20-30GB+ |
| Scalable | 1.5-2GB |

The scalable pipeline uses:
- **FST indexes**: ~400MB for 200M DOIs (vs ~20GB for HashSet)
- **Append-only partitions**: O(n) I/O instead of O(n²)
- **Streaming aggregation**: Bounded memory per partition
- **Deduplicated ref_json**: Each reference stored once
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add scalable pipeline documentation"
```

---

## Summary

| Task | Description | Est. Time |
|------|-------------|-----------|
| 1 | Add FST dependencies | 2 min |
| 2 | Create FstIndex structure | 10 min |
| 3 | Add external sort helper | 10 min |
| 4 | Add FST builder for sources | 10 min |
| 5 | Create segmented partition writer | 15 min |
| 6 | Create ref_json store | 15 min |
| 7 | Create streaming aggregator | 20 min |
| 8 | Add CLI flags | 5 min |
| 9 | Integrate scalable pipeline | 20 min |
| 10 | Add build-index command | 15 min |
| 11 | Integration test | 10 min |
| 12 | Documentation | 5 min |

**Total: ~2-3 hours**

After completing all tasks, the scalable pipeline will:
- Process 200M+ DOIs with <4GB memory
- Use FST indexes for ~50x memory reduction
- Write append-only partition segments
- Store ref_json deduplicated
- Stream aggregation with integrated validation
