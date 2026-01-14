# External Sort Aggregation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace memory-hungry batched aggregation with external sort + streaming merge to handle partitions with hundreds of millions of rows.

**Architecture:** For large partitions (>100 segments), sort all segments to a temp file using Polars' out-of-core sort, then stream through sorted data grouping consecutive rows by `cited_id`. Memory bounded to single group size (~100MB max) instead of entire partition (~136GB).

**Tech Stack:** Rust, Polars 0.46 (lazy, parquet, streaming features), existing FstIndex validation

---

## Task 1: Add GroupedCitation Struct

**Files:**
- Modify: `src/streaming/aggregator.rs:225-231`

**Step 1: Write the struct**

Replace `AccumulatedCitation` with simpler `GroupedCitation` (we no longer need it for the batched path, and the new struct is cleaner):

```rust
/// Citation data for streaming aggregation
#[derive(Clone)]
struct GroupedCitation {
    citing_doi: String,
    provenance: String,
    ref_json: String,
}
```

**Step 2: Verify compilation**

Run: `cargo check`
Expected: Success (struct is unused for now, that's fine)

**Step 3: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "refactor: rename AccumulatedCitation to GroupedCitation"
```

---

## Task 2: Add sort_partition_to_temp Function

**Files:**
- Modify: `src/streaming/aggregator.rs` (add after line 16, before AggregationStats)

**Step 1: Write the failing test**

Add to the test module at the bottom of the file:

```rust
#[test]
fn test_sort_partition_to_temp() {
    let dir = tempdir().unwrap();
    let partition_dir = dir.path().join("partitions/10.1234");
    fs::create_dir_all(&partition_dir).unwrap();

    // Create multiple segment files with unsorted data
    for (seg_num, cited_suffix) in [(0, "zzz"), (1, "aaa"), (2, "mmm")] {
        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), &[format!("10.1111/citing{}", seg_num)]),
            Column::new("ref_index".into(), &[0u32]),
            Column::new("cited_id".into(), &[format!("10.1234/{}", cited_suffix)]),
            Column::new("provenance".into(), &["mined"]),
            Column::new("ref_json".into(), &[r#"{"test": true}"#]),
        ])
        .unwrap();

        let path = partition_dir.join(format!("segment_{:04}.parquet", seg_num));
        let file = File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    let segments: Vec<_> = fs::read_dir(&partition_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .collect();

    // Sort to temp file
    let sorted_path = sort_partition_to_temp(&segments, &partition_dir).unwrap();

    // Verify sorted file exists
    assert!(sorted_path.exists());
    assert_eq!(sorted_path.file_name().unwrap(), "_sorted.parquet");

    // Verify data is sorted by cited_id
    let sorted_df = LazyFrame::scan_parquet(&sorted_path, Default::default())
        .unwrap()
        .collect()
        .unwrap();

    assert_eq!(sorted_df.height(), 3);
    let cited_ids: Vec<_> = sorted_df
        .column("cited_id")
        .unwrap()
        .str()
        .unwrap()
        .into_iter()
        .map(|s| s.unwrap().to_string())
        .collect();

    // Should be sorted: aaa, mmm, zzz
    assert_eq!(cited_ids, vec!["10.1234/aaa", "10.1234/mmm", "10.1234/zzz"]);

    // Cleanup
    fs::remove_file(&sorted_path).unwrap();
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_sort_partition_to_temp -- --nocapture`
Expected: FAIL with "cannot find function `sort_partition_to_temp`"

**Step 3: Write the implementation**

Add after the constants (after line 16):

```rust
use std::path::PathBuf;
use log::warn;

/// Sort all segments in a partition to a single temp file, sorted by cited_id.
/// Applies deduplication and self-citation filtering during the sort.
fn sort_partition_to_temp(segments: &[PathBuf], partition_path: &Path) -> Result<PathBuf> {
    let temp_path = partition_path.join("_sorted.parquet");

    // Lazy scan all segments
    let lazy_frames: Result<Vec<_>, PolarsError> = segments
        .iter()
        .map(|p| LazyFrame::scan_parquet(p, Default::default()))
        .collect();

    let lazy_frames = lazy_frames.map_err(|e| anyhow::anyhow!("Failed to scan segments: {}", e))?;

    // Concat, deduplicate, filter self-citations, sort by cited_id
    let sorted = concat(&lazy_frames, UnionArgs::default())?
        .unique(
            Some(vec![
                "citing_doi".into(),
                "ref_index".into(),
                "cited_id".into(),
            ]),
            UniqueKeepStrategy::First,
        )
        .filter(col("citing_doi").neq(col("cited_id")))
        .sort(["cited_id"], SortMultipleOptions::default());

    // Write to temp file using sink_parquet for out-of-core operation
    let options = ParquetWriteOptions {
        compression: ParquetCompression::Zstd(None),
        ..Default::default()
    };

    sorted
        .sink_parquet(&temp_path, options)
        .map_err(|e| anyhow::anyhow!("Failed to write sorted parquet: {}", e))?;

    Ok(temp_path)
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_sort_partition_to_temp -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "feat: add sort_partition_to_temp for external sort"
```

---

## Task 3: Add stream_sorted_and_write Function

**Files:**
- Modify: `src/streaming/aggregator.rs`

**Step 1: Write the failing test**

Add to the test module:

```rust
#[test]
fn test_stream_sorted_and_write() {
    let dir = tempdir().unwrap();
    let sorted_path = dir.path().join("sorted.parquet");
    let valid_output = dir.path().join("valid.jsonl");

    // Create sorted test data with multiple citations to same DOI
    let mut df = DataFrame::new(vec![
        Column::new(
            "citing_doi".into(),
            &["10.1111/a", "10.1111/b", "10.2222/x"],
        ),
        Column::new("ref_index".into(), &[0u32, 1, 0]),
        Column::new(
            "cited_id".into(),
            &["10.5555/target", "10.5555/target", "10.6666/other"],
        ),
        Column::new("provenance".into(), &["publisher", "mined", "mined"]),
        Column::new(
            "ref_json".into(),
            &[r#"{"key":"a"}"#, r#"{"key":"b"}"#, r#"{"key":"x"}"#],
        ),
    ])
    .unwrap();

    // Sort by cited_id (as the real function would produce)
    df = df.sort(["cited_id"], SortMultipleOptions::default()).unwrap();

    let file = File::create(&sorted_path).unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();

    // Build FST index with both target DOIs
    let fst_path = dir.path().join("test.fst");
    {
        use crate::index::FstIndexBuilder;
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.5555/target").unwrap();
        builder.insert("10.6666/other").unwrap();
        builder.finish().unwrap();
    }
    let index = FstIndex::load(&fst_path).unwrap();

    let outputs = OutputPaths {
        valid: Some(valid_output.clone()),
        failed: None,
        publisher: None,
        crossref: None,
        mined: None,
    };

    let mut valid_writer = Some(BufWriter::new(File::create(&valid_output).unwrap()));
    let mut stats = AggregationStats::default();

    stream_sorted_and_write(
        &sorted_path,
        Some(&index),
        None,
        &mut valid_writer,
        &mut None,
        &mut None,
        &mut None,
        &mut None,
        &mut stats,
    )
    .unwrap();

    valid_writer.as_mut().unwrap().flush().unwrap();

    // Verify output
    let content = fs::read_to_string(&valid_output).unwrap();
    let lines: Vec<_> = content.lines().collect();

    assert_eq!(lines.len(), 2); // Two unique cited DOIs
    assert_eq!(stats.valid_count, 2);
    assert_eq!(stats.total_citations, 3);

    // First line should be 10.5555/target with 2 citations
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["doi"], "10.5555/target");
    assert_eq!(first["citation_count"], 2);

    // Second line should be 10.6666/other with 1 citation
    let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(second["doi"], "10.6666/other");
    assert_eq!(second["citation_count"], 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_stream_sorted_and_write -- --nocapture`
Expected: FAIL with "cannot find function `stream_sorted_and_write`"

**Step 3: Write the implementation**

Add after `sort_partition_to_temp`:

```rust
/// Stream through a sorted parquet file, grouping by cited_id and writing output.
/// Memory usage is bounded to the size of a single group (all citations to one DOI).
fn stream_sorted_and_write(
    sorted_path: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    // Read the entire sorted file - it's been deduplicated and filtered,
    // so it's smaller than the original segments combined
    let df = LazyFrame::scan_parquet(sorted_path, Default::default())?
        .collect()
        .map_err(|e| anyhow::anyhow!("Failed to read sorted parquet: {}", e))?;

    if df.height() == 0 {
        return Ok(());
    }

    let cited_col = df.column("cited_id")?.str()?;
    let citing_col = df.column("citing_doi")?.str()?;
    let prov_col = df.column("provenance")?.str()?;
    let json_col = df.column("ref_json")?.str()?;

    let mut current_cited_id: Option<String> = None;
    let mut current_group: Vec<GroupedCitation> = Vec::new();

    for i in 0..df.height() {
        let cited_id = cited_col.get(i).unwrap_or("");

        // Check if we've moved to a new group
        if current_cited_id.as_deref() != Some(cited_id) {
            // Flush the previous group
            if let Some(ref id) = current_cited_id {
                flush_group(
                    id,
                    &current_group,
                    crossref_index,
                    datacite_index,
                    valid_writer,
                    failed_writer,
                    publisher_writer,
                    crossref_writer,
                    mined_writer,
                    stats,
                )?;
            }
            current_cited_id = Some(cited_id.to_string());
            current_group.clear();
        }

        // Add citation to current group
        current_group.push(GroupedCitation {
            citing_doi: citing_col.get(i).unwrap_or("").to_string(),
            provenance: prov_col.get(i).unwrap_or("mined").to_string(),
            ref_json: json_col.get(i).unwrap_or("null").to_string(),
        });
    }

    // Flush the final group
    if let Some(ref id) = current_cited_id {
        flush_group(
            id,
            &current_group,
            crossref_index,
            datacite_index,
            valid_writer,
            failed_writer,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )?;
    }

    Ok(())
}

/// Flush a group of citations for a single cited DOI to output files.
fn flush_group(
    cited_id: &str,
    citations: &[GroupedCitation],
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    if citations.is_empty() {
        return Ok(());
    }

    // Validate against indexes
    let is_valid = crossref_index
        .map(|idx| idx.contains(cited_id))
        .unwrap_or(false)
        || datacite_index
            .map(|idx| idx.contains(cited_id))
            .unwrap_or(false);

    // Group by provenance
    let mut by_provenance: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for cit in citations {
        let reference: serde_json::Value =
            serde_json::from_str(&cit.ref_json).unwrap_or(serde_json::Value::Null);
        let citation_json = serde_json::json!({
            "doi": cit.citing_doi,
            "provenance": cit.provenance,
            "reference": reference
        });
        by_provenance
            .entry(cit.provenance.clone())
            .or_default()
            .push(citation_json);
    }

    // Flatten for main output
    let all_cited_by: Vec<_> = by_provenance.values().flatten().cloned().collect();
    let citation_count = all_cited_by.len();
    stats.total_citations += citation_count;

    // Build main output record
    let record = serde_json::json!({
        "doi": cited_id,
        "citation_count": citation_count,
        "cited_by": all_cited_by
    });

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

    // Write to provenance-specific files (only valid records)
    let split_by_provenance = publisher_writer.is_some()
        || crossref_writer.is_some()
        || mined_writer.is_some();

    if split_by_provenance && is_valid {
        write_provenance_outputs(
            cited_id,
            &by_provenance,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )?;
    }

    Ok(())
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_stream_sorted_and_write -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "feat: add stream_sorted_and_write for streaming aggregation"
```

---

## Task 4: Add process_partition_external_sort Function

**Files:**
- Modify: `src/streaming/aggregator.rs`

**Step 1: Write the failing test**

Add to the test module:

```rust
#[test]
fn test_process_partition_external_sort() {
    let dir = tempdir().unwrap();
    let partition_path = dir.path().join("partitions/10.1234");
    fs::create_dir_all(&partition_path).unwrap();
    let valid_output = dir.path().join("valid.jsonl");

    // Create 3 segment files (simulating a partition that would trigger external sort)
    for seg_num in 0..3 {
        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), &[format!("10.1111/citing{}", seg_num)]),
            Column::new("ref_index".into(), &[0u32]),
            Column::new("cited_id".into(), &["10.1234/target"]),
            Column::new("provenance".into(), &["mined"]),
            Column::new("ref_json".into(), &[r#"{"seg": "test"}"#]),
        ])
        .unwrap();

        let path = partition_path.join(format!("segment_{:04}.parquet", seg_num));
        let file = File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    let segments: Vec<_> = fs::read_dir(&partition_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
        .collect();

    // Build FST index
    let fst_path = dir.path().join("test.fst");
    {
        use crate::index::FstIndexBuilder;
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.1234/target").unwrap();
        builder.finish().unwrap();
    }
    let index = FstIndex::load(&fst_path).unwrap();

    let mut valid_writer = Some(BufWriter::new(File::create(&valid_output).unwrap()));
    let mut stats = AggregationStats::default();

    process_partition_external_sort(
        &segments,
        &partition_path,
        Some(&index),
        None,
        &mut valid_writer,
        &mut None,
        &mut None,
        &mut None,
        &mut None,
        &mut stats,
    )
    .unwrap();

    valid_writer.as_mut().unwrap().flush().unwrap();

    // Verify temp file was cleaned up
    assert!(!partition_path.join("_sorted.parquet").exists());

    // Verify output
    let content = fs::read_to_string(&valid_output).unwrap();
    let record: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

    assert_eq!(record["doi"], "10.1234/target");
    assert_eq!(record["citation_count"], 3);
    assert_eq!(stats.partitions_processed, 1);
    assert_eq!(stats.valid_count, 1);
    assert_eq!(stats.total_citations, 3);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_process_partition_external_sort -- --nocapture`
Expected: FAIL with "cannot find function `process_partition_external_sort`"

**Step 3: Write the implementation**

Add after `flush_group`:

```rust
/// Process a large partition using external sort.
/// Sorts all segments to a temp file, then streams through sorted data.
fn process_partition_external_sort(
    segments: &[PathBuf],
    partition_path: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    info!(
        "External sort for partition with {} segments: {}",
        segments.len(),
        partition_path.display()
    );

    // Phase 1: Sort to temp file
    let sorted_path = sort_partition_to_temp(segments, partition_path)?;

    // Phase 2: Stream and aggregate
    let result = stream_sorted_and_write(
        &sorted_path,
        crossref_index,
        datacite_index,
        valid_writer,
        failed_writer,
        publisher_writer,
        crossref_writer,
        mined_writer,
        stats,
    );

    // Phase 3: Cleanup temp file (even on error)
    if sorted_path.exists() {
        if let Err(e) = fs::remove_file(&sorted_path) {
            warn!("Failed to delete temp file {}: {}", sorted_path.display(), e);
        }
    }

    result?;

    stats.partitions_processed += 1;
    Ok(())
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_process_partition_external_sort -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "feat: add process_partition_external_sort"
```

---

## Task 5: Wire Up External Sort in process_partition

**Files:**
- Modify: `src/streaming/aggregator.rs:136-166`

**Step 1: Write test for large partition routing**

Add to the test module:

```rust
#[test]
fn test_aggregate_routes_large_partition_to_external_sort() {
    // This test verifies that partitions with >100 segments use external sort
    // We do this by creating 101 segments and checking the temp file gets created/cleaned
    let dir = tempdir().unwrap();
    let partition_dir = dir.path().join("partitions");
    let partition_path = partition_dir.join("10.9999");
    fs::create_dir_all(&partition_path).unwrap();
    let valid_output = dir.path().join("valid.jsonl");

    // Create 101 segment files (exceeds SEGMENT_BATCH_THRESHOLD of 100)
    for seg_num in 0..101 {
        let mut df = DataFrame::new(vec![
            Column::new("citing_doi".into(), &[format!("10.1111/citing{}", seg_num)]),
            Column::new("ref_index".into(), &[0u32]),
            Column::new("cited_id".into(), &["10.9999/target"]),
            Column::new("provenance".into(), &["mined"]),
            Column::new("ref_json".into(), &[r#"{"test": true}"#]),
        ])
        .unwrap();

        let path = partition_path.join(format!("segment_{:04}.parquet", seg_num));
        let file = File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    // Build FST index
    let fst_path = dir.path().join("test.fst");
    {
        use crate::index::FstIndexBuilder;
        let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
        builder.insert("10.9999/target").unwrap();
        builder.finish().unwrap();
    }
    let index = FstIndex::load(&fst_path).unwrap();

    let outputs = OutputPaths {
        valid: Some(valid_output.clone()),
        failed: None,
        publisher: None,
        crossref: None,
        mined: None,
    };

    let stats = aggregate_and_validate(&partition_dir, Some(&index), None, &outputs).unwrap();

    // Verify temp file was cleaned up
    assert!(!partition_path.join("_sorted.parquet").exists());

    // Verify aggregation worked
    assert_eq!(stats.valid_count, 1);
    assert_eq!(stats.total_citations, 101);

    // Verify output
    let content = fs::read_to_string(&valid_output).unwrap();
    let record: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
    assert_eq!(record["citation_count"], 101);
}
```

**Step 2: Run test to verify current behavior**

Run: `cargo test test_aggregate_routes_large_partition_to_external_sort -- --nocapture`
Expected: Currently uses batched processing (may pass or fail depending on memory)

**Step 3: Update process_partition to use external sort**

Replace lines 136-166 in `process_partition`:

```rust
fn process_partition(
    partition_path: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    valid_writer: &mut Option<BufWriter<File>>,
    failed_writer: &mut Option<BufWriter<File>>,
    publisher_writer: &mut Option<BufWriter<File>>,
    crossref_writer: &mut Option<BufWriter<File>>,
    mined_writer: &mut Option<BufWriter<File>>,
    stats: &mut AggregationStats,
) -> Result<()> {
    // Collect segment files (exclude temp files starting with _)
    let segments: Vec<_> = fs::read_dir(partition_path)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|e| e.to_str()) == Some("parquet")
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with('_'))
                    .unwrap_or(false)
        })
        .collect();

    if segments.is_empty() {
        return Ok(());
    }

    // Use external sort for large partitions to avoid OOM
    if segments.len() > SEGMENT_BATCH_THRESHOLD {
        process_partition_external_sort(
            &segments,
            partition_path,
            crossref_index,
            datacite_index,
            valid_writer,
            failed_writer,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )
    } else {
        process_partition_inmemory(
            &segments,
            crossref_index,
            datacite_index,
            valid_writer,
            failed_writer,
            publisher_writer,
            crossref_writer,
            mined_writer,
            stats,
        )
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test test_aggregate_routes_large_partition_to_external_sort -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "feat: route large partitions to external sort"
```

---

## Task 6: Remove Old Batched Processing Code

**Files:**
- Modify: `src/streaming/aggregator.rs`

**Step 1: Run all tests to ensure everything works**

Run: `cargo test`
Expected: All tests pass

**Step 2: Remove unused code**

Delete the following:
- `SEGMENTS_PER_BATCH` constant (line 16)
- `AccumulatedCitation` struct (if not renamed in Task 1)
- `process_partition_batched` function (lines 233-370)

**Step 3: Remove unused imports**

Remove `HashSet` from imports if no longer used:

```rust
use std::collections::HashMap;  // Remove HashSet
```

**Step 4: Run tests and clippy**

Run: `cargo test && cargo clippy`
Expected: All tests pass, no warnings about unused code

**Step 5: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "refactor: remove old batched processing code"
```

---

## Task 7: Add Stale Temp File Cleanup

**Files:**
- Modify: `src/streaming/aggregator.rs`

**Step 1: Write the test**

Add to the test module:

```rust
#[test]
fn test_cleanup_stale_temp_files() {
    let dir = tempdir().unwrap();
    let partition_dir = dir.path().join("partitions");
    let partition_path = partition_dir.join("10.1234");
    fs::create_dir_all(&partition_path).unwrap();

    // Create a stale temp file (simulating interrupted previous run)
    let stale_temp = partition_path.join("_sorted.parquet");
    fs::write(&stale_temp, "stale data").unwrap();
    assert!(stale_temp.exists());

    // Run cleanup
    cleanup_stale_temp_files(&partition_dir).unwrap();

    // Verify temp file was deleted
    assert!(!stale_temp.exists());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_cleanup_stale_temp_files -- --nocapture`
Expected: FAIL with "cannot find function `cleanup_stale_temp_files`"

**Step 3: Write the implementation**

Add before `aggregate_and_validate`:

```rust
/// Clean up any stale _sorted.parquet temp files from interrupted runs.
fn cleanup_stale_temp_files(partition_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(partition_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let temp_file = path.join("_sorted.parquet");
            if temp_file.exists() {
                info!("Cleaning up stale temp file: {}", temp_file.display());
                fs::remove_file(&temp_file)?;
            }
        }
    }
    Ok(())
}
```

**Step 4: Wire up in aggregate_and_validate**

Add at the start of `aggregate_and_validate`, after creating writers:

```rust
// Clean up any stale temp files from interrupted runs
cleanup_stale_temp_files(partition_dir)?;
```

**Step 5: Run test to verify it passes**

Run: `cargo test test_cleanup_stale_temp_files -- --nocapture`
Expected: PASS

**Step 6: Commit**

```bash
git add src/streaming/aggregator.rs
git commit -m "feat: add cleanup for stale temp files on startup"
```

---

## Task 8: Full Integration Test

**Files:**
- Modify: `src/streaming/aggregator.rs`

**Step 1: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy`
Expected: No warnings

**Step 3: Build release**

Run: `cargo build --release`
Expected: Success

**Step 4: Commit any final fixes**

If any issues found:
```bash
git add -A
git commit -m "fix: address test/clippy issues"
```

---

## Task 9: Manual Verification with Real Data (Optional)

**Step 1: Test with small partition**

Run against a small partition to verify in-memory path still works:

```bash
cargo run --release -- pipeline \
    --input <small-test-file.tar.gz> \
    --source datacite \
    --scalable \
    --datacite-fst ./test-datacite.fst \
    --partitions-dir ./test-partitions \
    --outputs mined \
    --output-dir ./test-results/
```

**Step 2: Test with large partition subset**

If you have a subset of the problematic data:

```bash
# Run with RUST_LOG=debug to see external sort messages
RUST_LOG=debug cargo run --release -- pipeline ...
```

Look for log messages:
- "External sort for partition with N segments: ..."
- "Cleaning up stale temp file: ..."

---

## Summary

| Task | Description | Test |
|------|-------------|------|
| 1 | Add GroupedCitation struct | Compilation |
| 2 | Add sort_partition_to_temp | test_sort_partition_to_temp |
| 3 | Add stream_sorted_and_write | test_stream_sorted_and_write |
| 4 | Add process_partition_external_sort | test_process_partition_external_sort |
| 5 | Wire up in process_partition | test_aggregate_routes_large_partition |
| 6 | Remove old batched code | Full test suite |
| 7 | Add stale temp cleanup | test_cleanup_stale_temp_files |
| 8 | Full integration test | cargo test + clippy |
| 9 | Manual verification | Optional real data test |
