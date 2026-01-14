use anyhow::Result;
use fst::{Set, SetBuilder};
use log::info;
use memmap2::Mmap;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

/// Memory-efficient DOI index using FST (Finite State Transducer)
/// Provides ~50x memory reduction compared to HashSet<String>
#[allow(dead_code)]
pub struct FstIndex {
    set: Set<Mmap>,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct FstIndexBuilder {
    builder: SetBuilder<BufWriter<File>>,
}

#[allow(dead_code)]
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

/// Build an FST index from an iterator of DOIs (handles sorting and deduplication)
#[allow(dead_code)]
pub fn build_fst_from_iter<I>(dois: I, output_path: &Path, temp_dir: &Path) -> Result<()>
where
    I: Iterator<Item = String>,
{
    let unsorted_path = temp_dir.join("dois_unsorted.txt");
    let sorted_path = temp_dir.join("dois_sorted.txt");

    {
        let mut writer = BufWriter::new(File::create(&unsorted_path)?);
        for doi in dois {
            writeln!(writer, "{}", doi.to_lowercase())?;
        }
        writer.flush()?;
    }

    // Use LC_ALL=C to ensure byte-order sorting (required for FST)
    let status = Command::new("sort")
        .env("LC_ALL", "C")
        .arg("-u") // unique
        .arg("-o")
        .arg(&sorted_path)
        .arg(&unsorted_path)
        .status()?;

    if !status.success() {
        anyhow::bail!("sort command failed with status: {}", status);
    }

    let mut builder = FstIndexBuilder::new(output_path)?;
    let reader = BufReader::new(File::open(&sorted_path)?);

    for line in reader.lines() {
        let doi = line?;
        if !doi.is_empty() {
            builder.insert(&doi)?;
        }
    }

    builder.finish()?;

    let _ = std::fs::remove_file(&unsorted_path);
    let _ = std::fs::remove_file(&sorted_path);

    Ok(())
}

/// Build an FST index from a source of JSON records
#[allow(dead_code)]
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

    // Use LC_ALL=C to ensure byte-order sorting (required for FST)
    let status = Command::new("sort")
        .env("LC_ALL", "C")
        .arg("-u")
        .arg("-o")
        .arg(&sorted_path)
        .arg(&unsorted_path)
        .status()?;

    if !status.success() {
        anyhow::bail!("sort command failed");
    }

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

    let _ = std::fs::remove_file(&unsorted_path);
    let _ = std::fs::remove_file(&sorted_path);

    info!(
        "Built FST index with {} DOIs in {:.1}s",
        fst_count,
        start.elapsed().as_secs_f64()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
}
