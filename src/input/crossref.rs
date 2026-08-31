//! Crossref input source implementations.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::info;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use tar::Archive;

use super::CrossrefInput;

/// A single entry from a Crossref source (filename + raw JSON bytes).
pub struct CrossrefEntry {
    pub filename: String,
    pub bytes: Vec<u8>,
}

/// Visit every JSON entry in a Crossref input, in source order.
/// The closure receives filename + raw bytes; parse failures are the
/// caller's concern (it has the bytes), I/O failures abort the visit.
pub fn visit_crossref_entries(
    input: CrossrefInput,
    mut f: impl FnMut(CrossrefEntry) -> Result<()>,
) -> Result<()> {
    match input {
        CrossrefInput::TarGz(path) => {
            info!(
                "Detected Crossref input: tar.gz archive at {}",
                path.display()
            );
            let file =
                File::open(&path).with_context(|| format!("Failed to open: {}", path.display()))?;
            let mut archive = Archive::new(GzDecoder::new(file));
            for entry in archive.entries()? {
                let mut entry = entry?;
                let path = entry.path()?.to_path_buf();
                let path_str = path.to_string_lossy();
                if !path_str.ends_with(".json") {
                    continue;
                }
                let filename = path_str.to_string();
                // The size comes from the tar header, so it is only a hint: a
                // corrupt one could otherwise ask for a huge allocation up
                // front. read_to_end grows past the hint when the entry is
                // genuinely larger.
                const MAX_PREALLOC: usize = 16 * 1024 * 1024;
                let mut bytes = Vec::with_capacity((entry.size() as usize).min(MAX_PREALLOC));
                entry.read_to_end(&mut bytes)?;
                f(CrossrefEntry { filename, bytes })?;
            }
            Ok(())
        }
        CrossrefInput::Directory(path) => {
            let mut files: Vec<PathBuf> = std::fs::read_dir(&path)
                .with_context(|| format!("Failed to read directory: {}", path.display()))?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
                .collect();
            files.sort_by(|a, b| {
                let num = |p: &PathBuf| {
                    p.file_stem()
                        .and_then(|s| s.to_str())
                        .and_then(|s| s.parse::<u64>().ok())
                };
                match (num(a), num(b)) {
                    (Some(a), Some(b)) => a.cmp(&b),
                    _ => a.cmp(b),
                }
            });
            info!(
                "Detected Crossref input: directory with {} JSON files",
                files.len()
            );
            for path in files {
                let filename = path
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let bytes = std::fs::read(&path)?;
                f(CrossrefEntry { filename, bytes })?;
            }
            Ok(())
        }
        CrossrefInput::SingleJson(path) => {
            info!(
                "Detected Crossref input: single JSON file at {}",
                path.display()
            );
            let filename = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            let bytes = std::fs::read(&path)?;
            f(CrossrefEntry { filename, bytes })
        }
    }
}

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

    fn collect_entries(input: CrossrefInput) -> Vec<CrossrefEntry> {
        let mut entries = Vec::new();
        visit_crossref_entries(input, |entry| {
            entries.push(entry);
            Ok(())
        })
        .unwrap();
        entries
    }

    #[test]
    fn test_tar_gz_source() {
        let dir = tempdir().unwrap();
        let tar_path = create_test_tar_gz(dir.path());

        let entries = collect_entries(CrossrefInput::TarGz(tar_path));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].filename, "0.json");
        let v: serde_json::Value = serde_json::from_slice(&entries[0].bytes).unwrap();
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

        let entries = collect_entries(CrossrefInput::Directory(json_dir));

        assert_eq!(entries.len(), 2);
        // Should be sorted by filename
        assert_eq!(entries[0].filename, "0.json");
        assert_eq!(entries[1].filename, "1.json");
        let v: serde_json::Value = serde_json::from_slice(&entries[0].bytes).unwrap();
        assert!(v.get("items").is_some());
    }

    #[test]
    fn test_single_json_source() {
        let dir = tempdir().unwrap();
        let json_path = dir.path().join("test.json");
        std::fs::write(&json_path, r#"{"items": [{"DOI": "10.1234/single"}]}"#).unwrap();

        let entries = collect_entries(CrossrefInput::SingleJson(json_path));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].filename, "test.json");
        let v: serde_json::Value = serde_json::from_slice(&entries[0].bytes).unwrap();
        assert!(v.get("items").is_some());
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

        let filenames: Vec<String> = collect_entries(CrossrefInput::Directory(json_dir))
            .into_iter()
            .map(|e| e.filename)
            .collect();

        // Should be numerically sorted: 1, 2, 10
        assert_eq!(filenames, vec!["1.json", "2.json", "10.json"]);
    }

    #[test]
    fn test_visitor_error_aborts_visit() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();
        for i in [0, 1, 2] {
            std::fs::write(json_dir.join(format!("{}.json", i)), "{}").unwrap();
        }

        let mut seen = 0usize;
        let err = visit_crossref_entries(CrossrefInput::Directory(json_dir), |_entry| {
            seen += 1;
            Err(anyhow::anyhow!("consumer hung up"))
        })
        .unwrap_err();

        assert_eq!(seen, 1, "visit stops at the first closure error");
        assert!(err.to_string().contains("consumer hung up"));
    }
}
