# External Sort Aggregation Design

## Problem

The current batched aggregation approach OOMs on large partitions. For partition `10.1016` with 9640 segments (~482 million rows), the code attempts to accumulate all data in memory:

- `seen: HashSet<(String, u32, String)>` - ~26GB
- `citations_by_cited: HashMap<String, Vec<AccumulatedCitation>>` - ~110GB
- **Total: ~136GB**

The batching only limits parquet files open concurrently, not the accumulated data.

## Solution

Replace memory-hungry batched aggregation with external sort + streaming merge. After sorting by `cited_id`, all citations to the same work are consecutive. We only hold one group in memory at a time.

### High-Level Flow

```
Segments (9640 files)
        |
   [Sort Phase]
   Polars lazy scan all segments
   Deduplicate + filter self-citations
   Sort by cited_id
   sink_parquet to _sorted.parquet
        |
   [Stream Phase]
   Read sorted file in 50K row batches
   Group consecutive rows (same cited_id)
   Write each group immediately
   Delete temp file
        |
   Output files
```

## Sorting Phase

```rust
fn sort_partition_to_temp(segments: &[PathBuf], partition_path: &Path) -> Result<PathBuf> {
    let temp_path = partition_path.join("_sorted.parquet");

    // Lazy scan all segments
    let lazy_frames: Vec<LazyFrame> = segments
        .iter()
        .map(|p| LazyFrame::scan_parquet(p, Default::default()))
        .collect::<Result<_, _>>()?;

    // Concat, deduplicate, filter self-citations, sort
    concat(&lazy_frames, UnionArgs::default())?
        .unique(Some(vec!["citing_doi", "ref_index", "cited_id"]), UniqueKeepStrategy::First)
        .filter(col("citing_doi").neq(col("cited_id")))
        .sort(["cited_id"], SortMultipleOptions::default())
        .sink_parquet(&temp_path, ParquetWriteOptions::default())?;

    Ok(temp_path)
}
```

Key decisions:

1. **Deduplication during sort**: Apply `unique()` and self-citation filter in the lazy pipeline. Polars handles this efficiently during the sort pass.

2. **`sink_parquet` vs `collect`**: Using `sink_parquet` streams results to disk without materializing the full DataFrame in memory.

3. **Temp file location**: `_sorted.parquet` inside the partition directory. Underscore prefix distinguishes it from segment files.

4. **Sort key**: Just `cited_id`. No secondary sort needed since we're grouping, not ordering within groups.

## Streaming Aggregation Phase

```rust
fn stream_sorted_and_write(
    sorted_path: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    writers: &mut OutputWriters,
    stats: &mut AggregationStats,
) -> Result<()> {
    let file = File::open(sorted_path)?;
    let reader = ParquetReader::new(file);
    let batched = reader.batched(50_000)?;  // Read in 50K row batches

    let mut current_cited_id: Option<String> = None;
    let mut current_group: Vec<GroupedCitation> = Vec::new();

    while let Some(batch_df) = batched.next()? {
        // Process batch, handling group splits across batch boundaries
        process_batch(&batch_df, &mut current_cited_id, &mut current_group, /* ... */)?;
    }

    // Flush final group
    flush_final_group(&current_cited_id, &current_group, /* ... */)?;

    Ok(())
}
```

Key points:

1. **Chunked reading**: Process 50K rows at a time, not entire file
2. **Group continuity**: A group may span batch boundaries - accumulate across batches
3. **Flush on transition**: Write output immediately when `cited_id` changes
4. **Memory**: Only holds current group + one batch in memory

## Integration

```rust
fn process_partition(
    partition_path: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    writers: &mut OutputWriters,
    stats: &mut AggregationStats,
) -> Result<()> {
    let segments: Vec<PathBuf> = collect_segment_files(partition_path)?;

    if segments.is_empty() {
        return Ok(());
    }

    if segments.len() <= SEGMENT_BATCH_THRESHOLD {
        // Small partition - existing in-memory path (fast)
        process_partition_inmemory(&segments, /* ... */)
    } else {
        // Large partition - external sort path (bounded memory)
        process_partition_external_sort(&segments, partition_path, /* ... */)
    }
}

fn process_partition_external_sort(
    segments: &[PathBuf],
    partition_path: &Path,
    crossref_index: Option<&FstIndex>,
    datacite_index: Option<&FstIndex>,
    writers: &mut OutputWriters,
    stats: &mut AggregationStats,
) -> Result<()> {
    info!("External sort for partition with {} segments: {}",
          segments.len(), partition_path.display());

    // Phase 1: Sort to temp file
    let sorted_path = sort_partition_to_temp(segments, partition_path)?;

    // Phase 2: Stream and aggregate
    let result = stream_sorted_and_write(
        &sorted_path, crossref_index, datacite_index, writers, stats
    );

    // Phase 3: Cleanup temp file (even on error)
    if sorted_path.exists() {
        fs::remove_file(&sorted_path).ok();
    }

    result
}
```

### Changes Summary

| File | Change |
|------|--------|
| `aggregator.rs` | Replace `process_partition_batched` with `process_partition_external_sort` |
| `aggregator.rs` | Add `sort_partition_to_temp` function |
| `aggregator.rs` | Add `stream_sorted_and_write` function |
| `aggregator.rs` | Add `GroupedCitation` struct (simpler than `AccumulatedCitation`) |
| `aggregator.rs` | Remove `seen` HashSet (dedup handled in sort phase) |

### Unchanged

- `process_partition_inmemory` - unchanged for small partitions
- `write_provenance_outputs` - reused for output
- `aggregate_and_validate` entry point - unchanged API
- All validation logic against FST indexes

## Memory Analysis

### Current Approach (batched)

| Data Structure | Size for 10.1016 |
|----------------|------------------|
| `seen: HashSet<(String, u32, String)>` | ~26GB |
| `citations_by_cited: HashMap<...>` | ~110GB |
| **Total** | **~136GB** |

### New Approach (external sort)

| Data Structure | Size |
|----------------|------|
| One batch from sorted file | ~12MB |
| Current group accumulator | ~23MB worst case |
| Polars sort buffer | Spills to disk |
| **Total** | **~50MB typical, ~100MB peak** |

### Temporary Disk Space

| File | Size |
|------|------|
| `_sorted.parquet` | ~33GB (deleted after streaming) |

### Performance Expectations

| Phase | Time for 10.1016 |
|-------|------------------|
| Sort (Polars out-of-core) | 10-20 minutes |
| Stream + write | 5-10 minutes |
| **Total** | **15-30 minutes** |

vs current: OOM crash after ~3 hours.

## Error Handling

### Temp File Cleanup

Always delete temp file, even on error:

```rust
let result = stream_sorted_and_write(&sorted_path, /* ... */);

if sorted_path.exists() {
    if let Err(e) = fs::remove_file(&sorted_path) {
        warn!("Failed to delete temp file {}: {}", sorted_path.display(), e);
    }
}

result
```

### Edge Cases

| Case | Handling |
|------|----------|
| Empty partition | Return early |
| Single segment large partition | Use external sort |
| Sort fails (disk full) | Error propagates, temp cleaned |
| Stream fails mid-way | Partial output, temp cleaned |
| Batch boundary splits group | Accumulator carries across |

### Interrupt Recovery

On restart, delete any `_sorted.parquet` files before processing:

```rust
// In aggregate_and_validate, before main loop
cleanup_stale_temp_files(partition_dir)?;
```
