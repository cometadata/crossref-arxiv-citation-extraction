//! Crossref input source implementations.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::info;
use std::fs::File;
use std::path::PathBuf;
use tar::Archive;

use super::CrossrefInput;

/// A single entry from a Crossref source (filename + raw JSON bytes).
pub struct CrossrefEntry {
    pub filename: String,
    pub bytes: Vec<u8>,
}

/// Trait for iterating over Crossref JSON files
pub trait CrossrefSource: Iterator<Item = Result<CrossrefEntry>> {}

/// Open a Crossref source based on detected input type
pub fn open_crossref_source(input: CrossrefInput) -> Result<Box<dyn CrossrefSource>> {
    match input {
        CrossrefInput::TarGz(path) => {
            info!(
                "Detected Crossref input: tar.gz archive at {}",
                path.display()
            );
            Ok(Box::new(TarGzSource::new(path)?))
        }
        CrossrefInput::Directory(path) => Ok(Box::new(DirectorySource::new(path)?)),
        CrossrefInput::SingleJson(path) => Ok(Box::new(SingleJsonSource::new(path)?)),
    }
}

/// Source that reads from a tar.gz archive
pub struct TarGzSource {
    archive: Archive<GzDecoder<File>>,
    entries: Option<tar::Entries<'static, GzDecoder<File>>>,
}

impl TarGzSource {
    pub fn new(path: PathBuf) -> Result<Self> {
        let file =
            File::open(&path).with_context(|| format!("Failed to open: {}", path.display()))?;
        let gz = GzDecoder::new(file);
        let archive = Archive::new(gz);

        // We need to use unsafe to create a self-referential struct
        // Alternative: just store the archive and create entries on iteration
        Ok(Self {
            archive,
            entries: None,
        })
    }
}

impl Iterator for TarGzSource {
    type Item = Result<CrossrefEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        // Initialize entries on first call
        if self.entries.is_none() {
            // Safety: we're creating a self-referential struct but the archive lives as long as entries
            let archive_ptr = &mut self.archive as *mut Archive<GzDecoder<File>>;
            let entries = unsafe { (*archive_ptr).entries() };
            match entries {
                Ok(e) => {
                    // Transmute lifetime - this is safe because we own the archive
                    self.entries = Some(unsafe {
                        std::mem::transmute::<
                            tar::Entries<'_, GzDecoder<File>>,
                            tar::Entries<'static, GzDecoder<File>>,
                        >(e)
                    });
                }
                Err(e) => return Some(Err(e.into())),
            }
        }

        let entries = self.entries.as_mut()?;

        loop {
            let entry = match entries.next()? {
                Ok(e) => e,
                Err(e) => return Some(Err(e.into())),
            };

            let path = match entry.path() {
                Ok(p) => p.to_path_buf(),
                Err(e) => return Some(Err(e.into())),
            };

            let path_str = path.to_string_lossy();
            if !path_str.ends_with(".json") {
                continue;
            }

            let filename = path_str.to_string();
            let mut entry = entry;
            let mut bytes = Vec::with_capacity(entry.size() as usize);
            use std::io::Read;
            if let Err(e) = entry.read_to_end(&mut bytes) {
                return Some(Err(e.into()));
            }
            return Some(Ok(CrossrefEntry { filename, bytes }));
        }
    }
}

impl CrossrefSource for TarGzSource {}

/// Source that reads from a directory of .json files
pub struct DirectorySource {
    files: std::vec::IntoIter<PathBuf>,
}

impl DirectorySource {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&path)
            .with_context(|| format!("Failed to read directory: {}", path.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
            .collect();

        // Sort numerically by filename (0.json, 1.json, ..., 10.json)
        files.sort_by(|a, b| {
            let a_num = a
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok());
            let b_num = b
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok());
            match (a_num, b_num) {
                (Some(a), Some(b)) => a.cmp(&b),
                _ => a.cmp(b),
            }
        });

        info!(
            "Detected Crossref input: directory with {} JSON files",
            files.len()
        );

        Ok(Self {
            files: files.into_iter(),
        })
    }
}

impl Iterator for DirectorySource {
    type Item = Result<CrossrefEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.files.next()?;
        let filename = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        match std::fs::read(&path) {
            Ok(bytes) => Some(Ok(CrossrefEntry { filename, bytes })),
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl CrossrefSource for DirectorySource {}

/// Source that reads a single .json file
pub struct SingleJsonSource {
    path: Option<PathBuf>,
}

impl SingleJsonSource {
    pub fn new(path: PathBuf) -> Result<Self> {
        info!(
            "Detected Crossref input: single JSON file at {}",
            path.display()
        );
        Ok(Self { path: Some(path) })
    }
}

impl Iterator for SingleJsonSource {
    type Item = Result<CrossrefEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.path.take()?;
        let filename = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        match std::fs::read(&path) {
            Ok(bytes) => Some(Ok(CrossrefEntry { filename, bytes })),
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl CrossrefSource for SingleJsonSource {}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::fs::File;
    use tar::Builder;
    use tempfile::tempdir;

    fn create_test_tar_gz(dir: &std::path::Path) -> std::path::PathBuf {
        let tar_path = dir.join("test.tar.gz");
        let file = File::create(&tar_path).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = Builder::new(encoder);

        let json_content = r#"{"items": [{"DOI": "10.1234/test"}]}"#;
        let json_bytes = json_content.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_path("0.json").unwrap();
        header.set_size(json_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, json_bytes).unwrap();

        builder.into_inner().unwrap().finish().unwrap();
        tar_path
    }

    #[test]
    fn test_tar_gz_source() {
        let dir = tempdir().unwrap();
        let tar_path = create_test_tar_gz(dir.path());

        let input = CrossrefInput::TarGz(tar_path);
        let mut source = open_crossref_source(input).unwrap();

        let entry = source.next().unwrap().unwrap();
        assert_eq!(entry.filename, "0.json");
        let v: serde_json::Value = serde_json::from_slice(&entry.bytes).unwrap();
        assert!(v.get("items").is_some());
    }

    #[test]
    fn test_directory_source() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();

        std::fs::write(
            json_dir.join("0.json"),
            r#"{"items": [{"DOI": "10.1234/first"}]}"#,
        )
        .unwrap();
        std::fs::write(
            json_dir.join("1.json"),
            r#"{"items": [{"DOI": "10.1234/second"}]}"#,
        )
        .unwrap();

        let input = CrossrefInput::Directory(json_dir);
        let source = open_crossref_source(input).unwrap();
        let entries: Vec<_> = source.collect();

        assert_eq!(entries.len(), 2);
        // Should be sorted by filename
        assert_eq!(entries[0].as_ref().unwrap().filename, "0.json");
        assert_eq!(entries[1].as_ref().unwrap().filename, "1.json");
        let v: serde_json::Value =
            serde_json::from_slice(&entries[0].as_ref().unwrap().bytes).unwrap();
        assert!(v.get("items").is_some());
    }

    #[test]
    fn test_single_json_source() {
        let dir = tempdir().unwrap();
        let json_path = dir.path().join("test.json");
        std::fs::write(&json_path, r#"{"items": [{"DOI": "10.1234/single"}]}"#).unwrap();

        let input = CrossrefInput::SingleJson(json_path);
        let mut source = open_crossref_source(input).unwrap();

        let entry = source.next().unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&entry.bytes).unwrap();
        assert!(v.get("items").is_some());
        assert!(source.next().is_none());
    }

    #[test]
    fn test_directory_numeric_sort() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();

        // Create files that would sort wrong lexically: 1, 10, 2
        for i in [1, 2, 10] {
            std::fs::write(
                json_dir.join(format!("{}.json", i)),
                format!(r#"{{"items": [{{"DOI": "10.1234/{}"}}]}}"#, i),
            )
            .unwrap();
        }

        let input = CrossrefInput::Directory(json_dir);
        let source = open_crossref_source(input).unwrap();
        let filenames: Vec<_> = source.map(|e| e.unwrap().filename).collect();

        // Should be numerically sorted: 1, 2, 10
        assert_eq!(filenames, vec!["1.json", "2.json", "10.json"]);
    }
}
