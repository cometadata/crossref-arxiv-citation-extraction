//! DataCite input source implementations.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::info;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use walkdir::WalkDir;

use super::DataciteInput;

/// Trait for iterating over DataCite records
pub trait DataciteSource: Iterator<Item = Result<Value>> {}

/// Open a DataCite source based on detected input type
pub fn open_datacite_source(input: DataciteInput) -> Result<Box<dyn DataciteSource>> {
    match input {
        DataciteInput::SingleJsonlGz(path) => {
            info!(
                "Detected DataCite input: gzipped JSONL file at {}",
                path.display()
            );
            Ok(Box::new(SingleJsonlGzSource::new(path)?))
        }
        DataciteInput::SingleJsonl(path) => {
            info!("Detected DataCite input: JSONL file at {}", path.display());
            Ok(Box::new(SingleJsonlSource::new(path)?))
        }
        DataciteInput::NestedSnapshot(path) => Ok(Box::new(NestedSnapshotSource::new(path)?)),
        DataciteInput::FlatDirectory(path) => Ok(Box::new(FlatDirectorySource::new(path)?)),
    }
}

/// Source that reads from a gzipped JSONL file
pub struct SingleJsonlGzSource {
    reader: std::io::Lines<BufReader<GzDecoder<File>>>,
}

impl SingleJsonlGzSource {
    pub fn new(path: PathBuf) -> Result<Self> {
        let file =
            File::open(&path).with_context(|| format!("Failed to open: {}", path.display()))?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder).lines();
        Ok(Self { reader })
    }
}

impl Iterator for SingleJsonlGzSource {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.reader.next()? {
                Ok(l) => l,
                Err(e) => return Some(Err(e.into())),
            };

            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str(&line) {
                Ok(v) => return Some(Ok(v)),
                Err(_) => continue, // Skip invalid lines
            }
        }
    }
}

impl DataciteSource for SingleJsonlGzSource {}

/// Source that reads from an uncompressed JSONL file
pub struct SingleJsonlSource {
    reader: std::io::Lines<BufReader<File>>,
}

impl SingleJsonlSource {
    pub fn new(path: PathBuf) -> Result<Self> {
        let file =
            File::open(&path).with_context(|| format!("Failed to open: {}", path.display()))?;
        let reader = BufReader::new(file).lines();
        Ok(Self { reader })
    }
}

impl Iterator for SingleJsonlSource {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.reader.next()? {
                Ok(l) => l,
                Err(e) => return Some(Err(e.into())),
            };

            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str(&line) {
                Ok(v) => return Some(Ok(v)),
                Err(_) => continue,
            }
        }
    }
}

impl DataciteSource for SingleJsonlSource {}

/// Enum to handle both compressed and uncompressed file readers
enum LineReader {
    Compressed(std::io::Lines<BufReader<GzDecoder<File>>>),
    Uncompressed(std::io::Lines<BufReader<File>>),
}

impl LineReader {
    fn next_line(&mut self) -> Option<std::io::Result<String>> {
        match self {
            LineReader::Compressed(lines) => lines.next(),
            LineReader::Uncompressed(lines) => lines.next(),
        }
    }
}

/// Source that reads from a flat directory of .jsonl.gz files
pub struct FlatDirectorySource {
    files: std::vec::IntoIter<PathBuf>,
    current_reader: Option<LineReader>,
}

impl FlatDirectorySource {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&path)
            .with_context(|| format!("Failed to read directory: {}", path.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let s = p.to_string_lossy().to_lowercase();
                s.ends_with(".jsonl.gz") || (s.ends_with(".jsonl") && !s.ends_with(".jsonl.gz"))
            })
            .collect();

        files.sort();

        info!(
            "Detected DataCite input: flat directory with {} files",
            files.len()
        );

        Ok(Self {
            files: files.into_iter(),
            current_reader: None,
        })
    }

    fn open_next_file(&mut self) -> Option<()> {
        let path = self.files.next()?;
        let file = File::open(&path).ok()?;

        if path.to_string_lossy().ends_with(".gz") {
            let decoder = GzDecoder::new(file);
            self.current_reader = Some(LineReader::Compressed(BufReader::new(decoder).lines()));
        } else {
            self.current_reader = Some(LineReader::Uncompressed(BufReader::new(file).lines()));
        }
        Some(())
    }
}

impl Iterator for FlatDirectorySource {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_reader.is_none() {
                self.open_next_file()?;
            }

            if let Some(ref mut reader) = self.current_reader {
                match reader.next_line() {
                    Some(Ok(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str(&line) {
                            Ok(v) => return Some(Ok(v)),
                            Err(_) => continue,
                        }
                    }
                    Some(Err(e)) => return Some(Err(e.into())),
                    None => {
                        self.current_reader = None;
                        continue;
                    }
                }
            }
        }
    }
}

impl DataciteSource for FlatDirectorySource {}

/// Source that reads from a nested snapshot directory (updated_*/ subdirs)
pub struct NestedSnapshotSource {
    files: std::vec::IntoIter<PathBuf>,
    current_reader: Option<std::io::Lines<BufReader<GzDecoder<File>>>>,
}

impl NestedSnapshotSource {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut files: Vec<PathBuf> = WalkDir::new(&path)
            .into_iter()
            .filter_map(|e| e.ok())
            .map(|e| e.path().to_path_buf())
            .filter(|p| p.is_file() && p.to_string_lossy().to_lowercase().ends_with(".jsonl.gz"))
            .collect();

        // Sort by path to ensure consistent ordering (by month dir, then by part number)
        files.sort();

        info!(
            "Detected DataCite input: nested snapshot with {} files across directories",
            files.len()
        );

        Ok(Self {
            files: files.into_iter(),
            current_reader: None,
        })
    }

    fn open_next_file(&mut self) -> Option<()> {
        let path = self.files.next()?;
        let file = File::open(&path).ok()?;
        let decoder = GzDecoder::new(file);
        self.current_reader = Some(BufReader::new(decoder).lines());
        Some(())
    }
}

impl Iterator for NestedSnapshotSource {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_reader.is_none() {
                self.open_next_file()?;
            }

            if let Some(ref mut reader) = self.current_reader {
                match reader.next() {
                    Some(Ok(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match serde_json::from_str(&line) {
                            Ok(v) => return Some(Ok(v)),
                            Err(_) => continue,
                        }
                    }
                    Some(Err(e)) => return Some(Err(e.into())),
                    None => {
                        self.current_reader = None;
                        continue;
                    }
                }
            }
        }
    }
}

impl DataciteSource for NestedSnapshotSource {}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_single_jsonl_gz_source() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.jsonl.gz");

        let file = File::create(&path).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut writer = std::io::BufWriter::new(encoder);
        writeln!(writer, r#"{{"id": "10.1234/first"}}"#).unwrap();
        writeln!(writer, r#"{{"id": "10.1234/second"}}"#).unwrap();
        writer.into_inner().unwrap().finish().unwrap();

        let input = DataciteInput::SingleJsonlGz(path);
        let source = open_datacite_source(input).unwrap();
        let records: Vec<_> = source.collect();

        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_single_jsonl_source() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.jsonl");

        std::fs::write(
            &path,
            r#"{"id": "10.1234/first"}
{"id": "10.1234/second"}"#,
        )
        .unwrap();

        let input = DataciteInput::SingleJsonl(path);
        let source = open_datacite_source(input).unwrap();
        let records: Vec<_> = source.collect();

        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_flat_directory_source() {
        let dir = tempdir().unwrap();
        let flat_dir = dir.path().join("datacite");
        std::fs::create_dir(&flat_dir).unwrap();

        for i in 0..2 {
            let path = flat_dir.join(format!("part_{:04}.jsonl.gz", i));
            let file = File::create(&path).unwrap();
            let encoder = GzEncoder::new(file, Compression::default());
            let mut writer = std::io::BufWriter::new(encoder);
            writeln!(writer, r#"{{"id": "10.1234/part{}-a"}}"#, i).unwrap();
            writeln!(writer, r#"{{"id": "10.1234/part{}-b"}}"#, i).unwrap();
            writer.into_inner().unwrap().finish().unwrap();
        }

        let input = DataciteInput::FlatDirectory(flat_dir);
        let source = open_datacite_source(input).unwrap();
        let records: Vec<_> = source.collect();

        assert_eq!(records.len(), 4);
    }

    #[test]
    fn test_nested_snapshot_source() {
        let dir = tempdir().unwrap();
        let snapshot_dir = dir.path().join("datacite");
        std::fs::create_dir(&snapshot_dir).unwrap();

        for month in ["2024-01", "2024-02"] {
            let month_dir = snapshot_dir.join(format!("updated_{}", month));
            std::fs::create_dir(&month_dir).unwrap();

            let path = month_dir.join("part_0000.jsonl.gz");
            let file = File::create(&path).unwrap();
            let encoder = GzEncoder::new(file, Compression::default());
            let mut writer = std::io::BufWriter::new(encoder);
            writeln!(writer, r#"{{"id": "10.1234/{}"}}"#, month).unwrap();
            writer.into_inner().unwrap().finish().unwrap();
        }

        let input = DataciteInput::NestedSnapshot(snapshot_dir);
        let source = open_datacite_source(input).unwrap();
        let records: Vec<_> = source.collect();

        assert_eq!(records.len(), 2);
    }
}
