use anyhow::Result;
use log::{info, warn};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::cli::PipelineArgs;
use crate::common::{extract_input_stem, setup_logging, OutputPaths};
use crate::extract::{extract_arxiv_matches_from_text, Provenance};
use crate::index::{build_fst_index_from_source, FstIndex};
use crate::input::{
    detect_crossref_input, detect_datacite_input, open_crossref_source, open_datacite_source,
};
use crate::streaming::{
    aggregate_and_validate, ExtractionCheckpoint, PartitionRow, SegmentedPartitionWriter,
};

const FLUSH_THRESHOLD_DIVISOR: usize = 100;

/// Files processed between durable checkpoint commits during extraction.
const CHECKPOINT_FILES_INTERVAL: usize = 25;

/// Make all buffered rows durable, then mark pending files complete.
/// Order matters: a file checkpointed before its rows are flushed would be
/// skipped on resume with those rows lost.
fn commit_progress(
    writer: &mut SegmentedPartitionWriter,
    checkpoint: &mut Option<ExtractionCheckpoint>,
    pending: &mut Vec<String>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    writer.flush_all()?;
    if let Some(cp) = checkpoint {
        for filename in pending.iter() {
            cp.mark_completed(filename)?;
        }
    }
    pending.clear();
    Ok(())
}

/// Quick check if reference has any fields worth searching.
#[inline]
fn has_searchable_content(reference: &Value) -> bool {
    reference.get("DOI").is_some()
        || reference.get("unstructured").is_some()
        || reference.get("URL").is_some()
        || reference.get("article-title").is_some()
        || reference.get("journal-title").is_some()
}

#[inline]
fn quick_arxiv_likely(text: &str) -> bool {
    text.to_ascii_lowercase().contains("arxiv")
}

fn build_search_text(reference: &Value, buffer: &mut String) -> bool {
    buffer.clear();

    if let Some(doi) = reference.get("DOI").and_then(|v| v.as_str()) {
        buffer.push_str(doi);
        buffer.push(' ');
    }

    if let Some(url) = reference.get("URL").and_then(|v| v.as_str()) {
        buffer.push_str(url);
        buffer.push(' ');
    }

    if let Some(title) = reference.get("article-title").and_then(|v| v.as_str()) {
        buffer.push_str(title);
        buffer.push(' ');
    }

    if let Some(journal) = reference.get("journal-title").and_then(|v| v.as_str()) {
        buffer.push_str(journal);
        buffer.push(' ');
    }

    if let Some(unstructured) = reference.get("unstructured").and_then(|v| v.as_str()) {
        buffer.push_str(unstructured);
    }

    !buffer.is_empty()
}

#[inline]
fn should_include_citation(citing_doi: &str, cited_id: &str) -> bool {
    !citing_doi.eq_ignore_ascii_case(cited_id)
}

#[inline]
fn determine_provenance(reference: &Value, extracted_doi: &str) -> Provenance {
    if let Some(doi_field) = reference.get("DOI").and_then(|v| v.as_str()) {
        if doi_field.eq_ignore_ascii_case(extracted_doi) {
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

pub fn run_pipeline(args: PipelineArgs) -> Result<()> {
    setup_logging(&args.log_level)?;

    info!("Starting arXiv citation extraction pipeline");
    info!("Input: {}", args.input);
    if !args.provenance.is_empty() {
        info!("Provenance filter: {}", args.provenance.join(", "));
    }

    validate_args(&args)?;

    if !Path::new(&args.input).exists() {
        return Err(anyhow::anyhow!("Input file does not exist: {}", args.input));
    }

    run_scalable_pipeline(&args)
}

fn validate_args(args: &PipelineArgs) -> Result<()> {
    if args.arxiv_records.is_none() && args.arxiv_fst.is_none() {
        return Err(anyhow::anyhow!(
            "arXiv extraction requires --arxiv-records or --arxiv-fst"
        ));
    }
    Ok(())
}

/// Run the scalable pipeline with FST indexes and streaming aggregation
fn run_scalable_pipeline(args: &PipelineArgs) -> Result<()> {
    info!("Running scalable pipeline mode");

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
                std::env::temp_dir().join(format!("arxiv-scalable-{}", Uuid::new_v4()))
            });
        fs::create_dir_all(&path)?;
        (path, false)
    };

    let arxiv_fst = load_or_build_arxiv_fst(args, &partition_dir)?;

    if skip_extraction {
        info!("=== Skipping Extraction (using existing partitions) ===");
    } else {
        info!("=== Extraction Phase ===");
        run_scalable_extraction(args, &partition_dir)?;
    }

    let output_types = args.parse_outputs().map_err(|e| anyhow::anyhow!(e))?;

    let output_dir = args
        .output_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
    }

    let input_stem = extract_input_stem(&args.input);
    let outputs = OutputPaths::from_selection(&output_types, &output_dir, &input_stem);

    if !outputs.has_any() {
        return Err(anyhow::anyhow!(
            "No outputs specified. Use --outputs to specify at least one output type."
        ));
    }

    info!("=== Aggregation Phase ===");

    let stats = aggregate_and_validate(
        &partition_dir,
        arxiv_fst.as_ref(),
        &outputs,
        args.resume,
        args.checkpoint_interval,
    )?;

    info!("Pipeline complete:");
    info!("  Valid: {}", stats.valid_count);
    info!("  Failed: {}", stats.failed_count);
    info!("  Total citations: {}", stats.total_citations);

    let has_provenance_outputs =
        outputs.publisher.is_some() || outputs.crossref.is_some() || outputs.mined.is_some();
    if has_provenance_outputs {
        info!("  By provenance:");
        info!("    Publisher: {}", stats.publisher_citations);
        info!("    Crossref: {}", stats.crossref_citations);
        info!("    Mined: {}", stats.mined_citations);
    }

    if let Some(ref p) = outputs.valid {
        info!("Output valid: {}", p.display());
    }
    if let Some(ref p) = outputs.failed {
        info!("Output failed: {}", p.display());
    }
    if let Some(ref p) = outputs.publisher {
        info!("Output publisher: {}", p.display());
    }
    if let Some(ref p) = outputs.crossref {
        info!("Output crossref: {}", p.display());
    }
    if let Some(ref p) = outputs.mined {
        info!("Output mined: {}", p.display());
    }

    Ok(())
}

fn load_or_build_arxiv_fst(args: &PipelineArgs, temp_dir: &Path) -> Result<Option<FstIndex>> {
    if let Some(ref path) = args.arxiv_fst {
        info!("Loading arXiv FST from: {}", path);
        return Ok(Some(FstIndex::load(Path::new(path))?));
    }

    if let Some(ref records_path) = args.arxiv_records {
        info!("Building arXiv FST from: {}", records_path);
        let fst_path = temp_dir.join("arxiv.fst");
        let input = detect_datacite_input(records_path)?;
        let source = open_datacite_source(input)?;
        build_fst_index_from_source(source, "id", &fst_path, temp_dir)?;
        return Ok(Some(FstIndex::load(&fst_path)?));
    }

    Ok(None)
}

fn run_scalable_extraction(args: &PipelineArgs, partition_dir: &Path) -> Result<()> {
    let flush_threshold = (args.batch_size / FLUSH_THRESHOLD_DIVISOR).max(10000);
    let mut partition_writer = SegmentedPartitionWriter::new(partition_dir, flush_threshold)?;

    let input = detect_crossref_input(&args.input)?;
    let source = open_crossref_source(input)?;

    let checkpoint_path = partition_dir.join("extraction.checkpoint");
    let mut checkpoint = if args.resume {
        let cp = ExtractionCheckpoint::new(&checkpoint_path)?;
        if cp.completed_count() > 0 {
            info!(
                "Resuming extraction: {} files already processed",
                cp.completed_count()
            );
        }
        Some(cp)
    } else {
        if checkpoint_path.exists() {
            fs::remove_file(&checkpoint_path)?;
        }
        None
    };

    let mut items_processed = 0u64;
    let mut files_processed = 0u64;
    let mut files_skipped = 0u64;
    let mut refs_extracted = 0u64;
    let mut refs_filtered_provenance = 0u64;
    let mut refs_filtered_self_cite = 0u64;
    let mut refs_no_match = 0u64;
    let mut pending_files: Vec<String> = Vec::new();

    let mut search_text = String::with_capacity(512);

    info!("Extracting arXiv IDs from references...");

    for entry_result in source {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read entry: {}", e);
                continue;
            }
        };

        if let Some(ref cp) = checkpoint {
            if cp.is_completed(&entry.filename) {
                files_skipped += 1;
                continue;
            }
        }

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
                        let ref_index = ref_idx as u32;

                        if !has_searchable_content(reference) {
                            continue;
                        }

                        if !build_search_text(reference, &mut search_text) {
                            continue;
                        }

                        if !quick_arxiv_likely(&search_text) {
                            continue;
                        }

                        let matches = extract_arxiv_as_dois(&search_text);

                        if matches.is_empty() {
                            refs_no_match += 1;
                            continue;
                        }

                        let mut ref_json_cache: Option<String> = None;

                        for (doi, _raw) in matches {
                            if !should_include_citation(&work_doi, &doi) {
                                refs_filtered_self_cite += 1;
                                continue;
                            }

                            let provenance = determine_provenance(reference, &doi);

                            if !args.should_include_provenance(provenance.as_str()) {
                                refs_filtered_provenance += 1;
                                continue;
                            }

                            let ref_json =
                                ref_json_cache.get_or_insert_with(|| reference.to_string());

                            partition_writer.write(PartitionRow {
                                citing_doi: work_doi.clone(),
                                ref_index,
                                cited_id: doi, // Move instead of clone - we own this
                                provenance: provenance.as_str().to_string(),
                                ref_json: ref_json.clone(),
                            })?;
                            refs_extracted += 1;
                        }
                    }
                }

                if items_processed.is_multiple_of(100_000) {
                    info!(
                        "Progress: {} items, {} extracted, {} filtered (provenance: {}, self-cite: {})",
                        items_processed, refs_extracted,
                        refs_filtered_provenance + refs_filtered_self_cite,
                        refs_filtered_provenance, refs_filtered_self_cite
                    );
                }
            }
        }

        pending_files.push(entry.filename);
        files_processed += 1;

        // Periodic durable commit only matters when checkpointing; without
        // --resume, segment flushing stays purely threshold-driven.
        if checkpoint.is_some() && pending_files.len() >= CHECKPOINT_FILES_INTERVAL {
            commit_progress(&mut partition_writer, &mut checkpoint, &mut pending_files)?;
        }
    }

    commit_progress(&mut partition_writer, &mut checkpoint, &mut pending_files)?;
    partition_writer.flush_all()?; // no-op if commit_progress just ran; kept for the checkpoint-disabled path

    info!("Extraction complete:");
    info!("  Files processed: {}", files_processed);
    if files_skipped > 0 {
        info!("  Files skipped (resumed): {}", files_skipped);
    }
    info!("  Items processed: {}", items_processed);
    info!("  Refs extracted: {}", refs_extracted);
    info!(
        "  Refs filtered by provenance: {}",
        refs_filtered_provenance
    );
    info!(
        "  Refs filtered (self-citations): {}",
        refs_filtered_self_cite
    );
    info!("  Refs with no arXiv match: {}", refs_no_match);

    if let Some(cp) = checkpoint {
        cp.cleanup()?;
    }

    Ok(())
}

fn extract_arxiv_as_dois(text: &str) -> Vec<(String, String)> {
    extract_arxiv_matches_from_text(text)
        .into_iter()
        .map(|m| (format!("10.48550/arXiv.{}", m.id), m.raw))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PipelineArgs;

    fn default_args() -> PipelineArgs {
        PipelineArgs {
            input: "test.tar.gz".to_string(),
            arxiv_records: Some("arxiv.jsonl.gz".to_string()),
            arxiv_fst: None,
            provenance: vec![],
            outputs: vec![],
            output_dir: None,
            partitions_dir: None,
            log_level: "INFO".to_string(),
            keep_intermediates: false,
            temp_dir: None,
            batch_size: 5000000,
            resume: false,
            checkpoint_interval: 50,
        }
    }

    #[test]
    fn test_validate_args_requires_arxiv_index() {
        let mut args = default_args();
        args.arxiv_records = None;
        args.arxiv_fst = None;
        let result = validate_args(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("--arxiv-records"));
    }

    #[test]
    fn test_validate_args_with_records() {
        let mut args = default_args();
        args.arxiv_records = Some("records.jsonl.gz".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_args_with_fst() {
        let mut args = default_args();
        args.arxiv_records = None;
        args.arxiv_fst = Some("arxiv.fst".to_string());
        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_include_citation() {
        assert!(should_include_citation("10.1234/a", "10.5678/b"));
        assert!(!should_include_citation("10.1234/a", "10.1234/a"));
        assert!(!should_include_citation("10.1234/A", "10.1234/a")); // Case insensitive
    }

    #[test]
    fn test_determine_provenance() {
        use crate::extract::Provenance;
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

    #[test]
    fn test_has_searchable_content() {
        use serde_json::json;

        // Has DOI field
        assert!(has_searchable_content(&json!({"DOI": "10.1234/test"})));

        // Has unstructured field
        assert!(has_searchable_content(
            &json!({"unstructured": "Some text"})
        ));

        // Has URL field
        assert!(has_searchable_content(
            &json!({"URL": "https://example.com"})
        ));

        // Has article-title field
        assert!(has_searchable_content(&json!({"article-title": "A Paper"})));

        // Has journal-title field
        assert!(has_searchable_content(&json!({"journal-title": "Nature"})));

        // Empty reference
        assert!(!has_searchable_content(&json!({})));

        // Only has irrelevant fields
        assert!(!has_searchable_content(
            &json!({"key": "ref1", "author": "Smith"})
        ));
    }

    #[test]
    fn test_quick_arxiv_likely() {
        // Contains arXiv (various cases)
        assert!(quick_arxiv_likely("See arXiv:2403.12345"));
        assert!(quick_arxiv_likely("ARXIV paper reference"));
        assert!(quick_arxiv_likely("Available at arxiv.org/abs/2403.12345"));

        // Does not contain arXiv
        assert!(!quick_arxiv_likely("Just a regular paper"));
        assert!(!quick_arxiv_likely("10.1234/example"));
        assert!(!quick_arxiv_likely("")); // Empty string
    }

    #[test]
    fn test_build_search_text() {
        use serde_json::json;

        let mut buffer = String::new();

        // Single field
        let has_text = build_search_text(&json!({"DOI": "10.1234/test"}), &mut buffer);
        assert!(has_text);
        assert!(buffer.contains("10.1234/test"));

        // Multiple fields
        let has_text = build_search_text(
            &json!({
                "DOI": "10.1234/test",
                "unstructured": "A paper about something"
            }),
            &mut buffer,
        );
        assert!(has_text);
        assert!(buffer.contains("10.1234/test"));
        assert!(buffer.contains("A paper about something"));

        // Empty reference
        let has_text = build_search_text(&json!({}), &mut buffer);
        assert!(!has_text);

        // Only irrelevant fields
        let has_text = build_search_text(&json!({"key": "ref1", "author": "Smith"}), &mut buffer);
        assert!(!has_text);
    }

    #[test]
    fn test_provenance_filter() {
        // No filter (empty) - includes all
        let mut args = default_args();
        assert!(args.should_include_provenance("publisher"));
        assert!(args.should_include_provenance("crossref"));
        assert!(args.should_include_provenance("mined"));

        // Filter to mined only
        args.provenance = vec!["mined".to_string()];
        assert!(!args.should_include_provenance("publisher"));
        assert!(!args.should_include_provenance("crossref"));
        assert!(args.should_include_provenance("mined"));

        // Filter to publisher only
        args.provenance = vec!["publisher".to_string()];
        assert!(args.should_include_provenance("publisher"));
        assert!(!args.should_include_provenance("crossref"));
        assert!(!args.should_include_provenance("mined"));

        // Filter to multiple
        args.provenance = vec!["publisher".to_string(), "crossref".to_string()];
        assert!(args.should_include_provenance("publisher"));
        assert!(args.should_include_provenance("crossref"));
        assert!(!args.should_include_provenance("mined"));
    }

    #[test]
    fn test_extract_arxiv_as_dois() {
        // Test basic arXiv extraction and DOI conversion
        let text = "See arXiv:2403.12345 for details";
        let results = extract_arxiv_as_dois(text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "10.48550/arXiv.2403.12345");

        // Test with URL format
        let text = "Available at https://arxiv.org/abs/2312.00001";
        let results = extract_arxiv_as_dois(text);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "10.48550/arXiv.2312.00001");

        // Test with no arXiv
        let text = "Just a regular DOI 10.1234/test";
        let results = extract_arxiv_as_dois(text);
        assert!(results.is_empty());
    }

    #[test]
    fn test_commit_progress_flushes_before_marking() {
        use crate::streaming::{ExtractionCheckpoint, PartitionRow, SegmentedPartitionWriter};
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let partition_dir = dir.path().join("partitions");
        // Threshold high enough that nothing auto-flushes.
        let mut writer = SegmentedPartitionWriter::new(&partition_dir, 1_000_000).unwrap();
        writer
            .write(PartitionRow {
                citing_doi: "10.1111/a".to_string(),
                ref_index: 0,
                cited_id: "10.48550/arxiv.2403.00001".to_string(),
                provenance: "mined".to_string(),
                ref_json: "{}".to_string(),
            })
            .unwrap();
        assert_eq!(writer.total_rows_written(), 0, "row is only buffered");

        let cp_path = partition_dir.join("extraction.checkpoint");
        let mut checkpoint = Some(ExtractionCheckpoint::new(&cp_path).unwrap());
        let mut pending = vec!["file1.json".to_string()];

        commit_progress(&mut writer, &mut checkpoint, &mut pending).unwrap();

        assert_eq!(writer.total_rows_written(), 1, "buffered row hit disk");
        assert!(pending.is_empty());
        // Reload checkpoint from disk: the mark must be durable.
        drop(checkpoint);
        let reloaded = ExtractionCheckpoint::new(&cp_path).unwrap();
        assert!(reloaded.is_completed("file1.json"));
    }
}
