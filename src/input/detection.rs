//! Input type detection based on path.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

/// Detected Crossref input type
#[derive(Debug, Clone)]
pub enum CrossrefInput {
    /// A tar.gz archive
    TarGz(PathBuf),
    /// A directory containing .json files
    Directory(PathBuf),
    /// A single .json file
    SingleJson(PathBuf),
}

/// Detected DataCite input type
#[derive(Debug, Clone)]
pub enum DataciteInput {
    /// A single .jsonl.gz file
    SingleJsonlGz(PathBuf),
    /// A single uncompressed .jsonl file
    SingleJsonl(PathBuf),
    /// A directory with updated_*/ subdirectories (nested snapshot)
    NestedSnapshot(PathBuf),
    /// A flat directory with .jsonl.gz files
    FlatDirectory(PathBuf),
}

/// Detect Crossref input type from path
pub fn detect_crossref_input(path: &str) -> Result<CrossrefInput> {
    let path = Path::new(path);

    if !path.exists() {
        bail!("Input path does not exist: {}", path.display());
    }

    if path.is_file() {
        let path_str = path.to_string_lossy().to_lowercase();
        if path_str.ends_with(".tar.gz") {
            return Ok(CrossrefInput::TarGz(path.to_path_buf()));
        } else if path_str.ends_with(".json") {
            return Ok(CrossrefInput::SingleJson(path.to_path_buf()));
        }
        bail!(
            "Unrecognized Crossref file type: {}. Expected .tar.gz or .json",
            path.display()
        );
    }

    // Directory: scan for .json files
    detect_crossref_directory(path)
}

fn detect_crossref_directory(path: &Path) -> Result<CrossrefInput> {
    let entries: Vec<_> = std::fs::read_dir(path)
        .with_context(|| format!("Failed to read directory: {}", path.display()))?
        .filter_map(|e| e.ok())
        .collect();

    let has_json = entries.iter().any(|e| {
        e.path()
            .extension()
            .map(|ext| ext == "json")
            .unwrap_or(false)
    });

    let has_jsonl_gz = entries.iter().any(|e| {
        e.path()
            .to_string_lossy()
            .to_lowercase()
            .ends_with(".jsonl.gz")
    });

    let has_jsonl = entries.iter().any(|e| {
        let p = e.path().to_string_lossy().to_lowercase();
        p.ends_with(".jsonl") && !p.ends_with(".jsonl.gz")
    });

    if has_json && (has_jsonl_gz || has_jsonl) {
        bail!(
            "Ambiguous directory '{}': contains both .json and .jsonl files. \
             Specify a path to a specific file or a directory with only one format.",
            path.display()
        );
    }

    if has_json {
        return Ok(CrossrefInput::Directory(path.to_path_buf()));
    }

    if !has_json && !has_jsonl_gz && !has_jsonl {
        // Check if empty or has no relevant files
        let has_any_files = entries.iter().any(|e| e.path().is_file());
        if !has_any_files {
            bail!(
                "Directory '{}' contains no processable files.",
                path.display()
            );
        }
    }

    bail!(
        "Directory '{}' does not contain .json files for Crossref input.",
        path.display()
    );
}

/// Detect DataCite input type from path
pub fn detect_datacite_input(path: &str) -> Result<DataciteInput> {
    let path = Path::new(path);

    if !path.exists() {
        bail!("Input path does not exist: {}", path.display());
    }

    if path.is_file() {
        let path_str = path.to_string_lossy().to_lowercase();
        if path_str.ends_with(".jsonl.gz") {
            return Ok(DataciteInput::SingleJsonlGz(path.to_path_buf()));
        } else if path_str.ends_with(".jsonl") {
            return Ok(DataciteInput::SingleJsonl(path.to_path_buf()));
        }
        bail!(
            "Unrecognized DataCite file type: {}. Expected .jsonl.gz or .jsonl",
            path.display()
        );
    }

    // Directory: check for nested snapshot or flat structure
    detect_datacite_directory(path)
}

fn detect_datacite_directory(path: &Path) -> Result<DataciteInput> {
    let entries: Vec<_> = std::fs::read_dir(path)
        .with_context(|| format!("Failed to read directory: {}", path.display()))?
        .filter_map(|e| e.ok())
        .collect();

    // Check for nested snapshot structure (updated_*/ subdirectories)
    let has_updated_dirs = entries
        .iter()
        .any(|e| e.path().is_dir() && e.file_name().to_string_lossy().starts_with("updated_"));

    if has_updated_dirs {
        return Ok(DataciteInput::NestedSnapshot(path.to_path_buf()));
    }

    // Check for flat directory with .jsonl.gz or .jsonl files
    let has_jsonl_gz = entries.iter().any(|e| {
        e.path()
            .to_string_lossy()
            .to_lowercase()
            .ends_with(".jsonl.gz")
    });

    let has_jsonl = entries.iter().any(|e| {
        let p = e.path().to_string_lossy().to_lowercase();
        p.ends_with(".jsonl") && !p.ends_with(".jsonl.gz")
    });

    let has_json = entries.iter().any(|e| {
        e.path()
            .extension()
            .map(|ext| ext == "json")
            .unwrap_or(false)
    });

    if has_json && (has_jsonl_gz || has_jsonl) {
        bail!(
            "Ambiguous directory '{}': contains both .json and .jsonl files. \
             Specify a path to a specific file or a directory with only one format.",
            path.display()
        );
    }

    if has_jsonl_gz || has_jsonl {
        return Ok(DataciteInput::FlatDirectory(path.to_path_buf()));
    }

    bail!(
        "Directory '{}' does not contain .jsonl or .jsonl.gz files for DataCite input.",
        path.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn test_detect_crossref_tar_gz() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tar.gz");
        File::create(&path).unwrap();

        let result = detect_crossref_input(path.to_str().unwrap()).unwrap();
        assert!(matches!(result, CrossrefInput::TarGz(_)));
    }

    #[test]
    fn test_detect_crossref_single_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.json");
        File::create(&path).unwrap();

        let result = detect_crossref_input(path.to_str().unwrap()).unwrap();
        assert!(matches!(result, CrossrefInput::SingleJson(_)));
    }

    #[test]
    fn test_detect_datacite_jsonl_gz() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.jsonl.gz");
        File::create(&path).unwrap();

        let result = detect_datacite_input(path.to_str().unwrap()).unwrap();
        assert!(matches!(result, DataciteInput::SingleJsonlGz(_)));
    }

    #[test]
    fn test_detect_datacite_jsonl() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        File::create(&path).unwrap();

        let result = detect_datacite_input(path.to_str().unwrap()).unwrap();
        assert!(matches!(result, DataciteInput::SingleJsonl(_)));
    }

    #[test]
    fn test_detect_nonexistent_path() {
        let result = detect_crossref_input("/nonexistent/path.tar.gz");
        assert!(result.is_err());
    }

    #[test]
    fn test_detect_crossref_directory() {
        let dir = tempdir().unwrap();
        let json_dir = dir.path().join("crossref");
        std::fs::create_dir(&json_dir).unwrap();
        File::create(json_dir.join("0.json")).unwrap();
        File::create(json_dir.join("1.json")).unwrap();

        let result = detect_crossref_input(json_dir.to_str().unwrap()).unwrap();
        assert!(matches!(result, CrossrefInput::Directory(_)));
    }

    #[test]
    fn test_detect_datacite_nested_snapshot() {
        let dir = tempdir().unwrap();
        let snapshot_dir = dir.path().join("datacite");
        std::fs::create_dir(&snapshot_dir).unwrap();
        let updated_dir = snapshot_dir.join("updated_2024-01");
        std::fs::create_dir(&updated_dir).unwrap();
        File::create(updated_dir.join("part_0000.jsonl.gz")).unwrap();

        let result = detect_datacite_input(snapshot_dir.to_str().unwrap()).unwrap();
        assert!(matches!(result, DataciteInput::NestedSnapshot(_)));
    }

    #[test]
    fn test_detect_datacite_flat_directory() {
        let dir = tempdir().unwrap();
        let flat_dir = dir.path().join("datacite_flat");
        std::fs::create_dir(&flat_dir).unwrap();
        File::create(flat_dir.join("part_0000.jsonl.gz")).unwrap();
        File::create(flat_dir.join("part_0001.jsonl.gz")).unwrap();

        let result = detect_datacite_input(flat_dir.to_str().unwrap()).unwrap();
        assert!(matches!(result, DataciteInput::FlatDirectory(_)));
    }

    #[test]
    fn test_detect_ambiguous_directory_error() {
        let dir = tempdir().unwrap();
        let mixed_dir = dir.path().join("mixed");
        std::fs::create_dir(&mixed_dir).unwrap();
        File::create(mixed_dir.join("test.json")).unwrap();
        File::create(mixed_dir.join("test.jsonl.gz")).unwrap();

        // Should error because it contains both .json and .jsonl.gz
        let result = detect_crossref_input(mixed_dir.to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_detect_empty_directory_error() {
        let dir = tempdir().unwrap();
        let empty_dir = dir.path().join("empty");
        std::fs::create_dir(&empty_dir).unwrap();

        let result = detect_crossref_input(empty_dir.to_str().unwrap());
        assert!(result.is_err());
    }
}
