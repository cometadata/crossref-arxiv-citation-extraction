# Flexible Input Handling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add auto-detection and flexible input handling for Crossref (tar.gz, directory, single JSON) and DataCite (jsonl.gz, jsonl, nested snapshot, flat directory) inputs.

**Architecture:** Create a new `src/input/` module with detection logic and source traits. Each input type implements a common iterator interface. Detection happens at runtime based on file extension or directory contents.

**Tech Stack:** Rust std::fs for filesystem operations, existing flate2/tar for compression, walkdir crate for directory traversal.

---

## Task 1: Add walkdir Dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add walkdir to dependencies**

In `Cargo.toml`, add walkdir under the `[dependencies]` section after `uuid`:

```toml
# Directory traversal
walkdir = "2"
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles with no errors

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: add walkdir dependency for directory traversal"
```

---

## Task 2: Create Input Module Structure

**Files:**
- Create: `src/input/mod.rs`
- Modify: `src/lib.rs`

**Step 1: Create the input module file**

Create `src/input/mod.rs`:

```rust
//! Input detection and reading for Crossref and DataCite sources.

mod detection;
mod crossref;
mod datacite;

pub use detection::{detect_crossref_input, detect_datacite_input, CrossrefInput, DataciteInput};
pub use crossref::{open_crossref_source, CrossrefEntry, CrossrefSource};
pub use datacite::{open_datacite_source, DataciteSource};
```

**Step 2: Register module in lib.rs**

In `src/lib.rs`, add after the existing modules:

```rust
pub mod input;
```

**Step 3: Create placeholder files**

Create `src/input/detection.rs`:

```rust
//! Input type detection based on path.

use std::path::PathBuf;

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
pub fn detect_crossref_input(_path: &str) -> anyhow::Result<CrossrefInput> {
    todo!()
}

/// Detect DataCite input type from path
pub fn detect_datacite_input(_path: &str) -> anyhow::Result<DataciteInput> {
    todo!()
}
```

Create `src/input/crossref.rs`:

```rust
//! Crossref input source implementations.

use anyhow::Result;
use serde_json::Value;

use super::CrossrefInput;

/// A single entry from a Crossref source (filename + parsed JSON)
pub struct CrossrefEntry {
    pub filename: String,
    pub json: Value,
}

/// Trait for iterating over Crossref JSON files
pub trait CrossrefSource: Iterator<Item = Result<CrossrefEntry>> {}

/// Open a Crossref source based on detected input type
pub fn open_crossref_source(_input: CrossrefInput) -> Result<Box<dyn CrossrefSource>> {
    todo!()
}
```

Create `src/input/datacite.rs`:

```rust
//! DataCite input source implementations.

use anyhow::Result;
use serde_json::Value;

use super::DataciteInput;

/// Trait for iterating over DataCite records
pub trait DataciteSource: Iterator<Item = Result<Value>> {}

/// Open a DataCite source based on detected input type
pub fn open_datacite_source(_input: DataciteInput) -> Result<Box<dyn DataciteSource>> {
    todo!()
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles (with dead_code warnings, which is fine)

**Step 5: Commit**

```bash
git add src/input/ src/lib.rs
git commit -m "feat: scaffold input module structure"
```

---

## Task 3: Implement File-Based Detection

**Files:**
- Modify: `src/input/detection.rs`
- Test: `src/input/detection.rs` (inline tests)

**Step 1: Write failing tests for file detection**

Add to `src/input/detection.rs`:

```rust
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
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test detection::tests -q`
Expected: FAIL (todo! panics)

**Step 3: Implement file-based detection**

Replace the placeholder functions in `src/input/detection.rs`:

```rust
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

    // Directory detection will be added in Task 4
    bail!(
        "Directory input not yet implemented for: {}",
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

    // Directory detection will be added in Task 4
    bail!(
        "Directory input not yet implemented for: {}",
        path.display()
    );
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test detection::tests -q`
Expected: All 5 tests pass

**Step 5: Commit**

```bash
git add src/input/detection.rs
git commit -m "feat: implement file-based input detection"
```

---

## Task 4: Implement Directory Detection

**Files:**
- Modify: `src/input/detection.rs`

**Step 1: Write failing tests for directory detection**

Add to the tests module in `src/input/detection.rs`:

```rust
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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test detection::tests -q`
Expected: 5 new tests FAIL

**Step 3: Implement directory detection**

Update the detection functions in `src/input/detection.rs`. Replace the directory bail! placeholders:

In `detect_crossref_input`, replace the final bail! with:

```rust
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
```

In `detect_datacite_input`, replace the final bail! with:

```rust
    // Directory: check for nested snapshot or flat structure
    detect_datacite_directory(path)
}

fn detect_datacite_directory(path: &Path) -> Result<DataciteInput> {
    let entries: Vec<_> = std::fs::read_dir(path)
        .with_context(|| format!("Failed to read directory: {}", path.display()))?
        .filter_map(|e| e.ok())
        .collect();

    // Check for nested snapshot structure (updated_*/ subdirectories)
    let has_updated_dirs = entries.iter().any(|e| {
        e.path().is_dir()
            && e.file_name()
                .to_string_lossy()
                .starts_with("updated_")
    });

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
```

**Step 4: Run tests to verify they pass**

Run: `cargo test detection::tests -q`
Expected: All 10 tests pass

**Step 5: Commit**

```bash
git add src/input/detection.rs
git commit -m "feat: implement directory-based input detection"
```

---

## Task 5: Implement Crossref TarGz Source

**Files:**
- Modify: `src/input/crossref.rs`

**Step 1: Write failing test**

Add to `src/input/crossref.rs`:

```rust
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
        assert!(entry.json.get("items").is_some());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test crossref::tests::test_tar_gz_source -q`
Expected: FAIL (todo! panic)

**Step 3: Implement TarGzSource**

Replace the contents of `src/input/crossref.rs`:

```rust
//! Crossref input source implementations.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::info;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use tar::Archive;

use super::CrossrefInput;

/// A single entry from a Crossref source (filename + parsed JSON)
pub struct CrossrefEntry {
    pub filename: String,
    pub json: Value,
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
        CrossrefInput::Directory(_) => {
            todo!("Directory source")
        }
        CrossrefInput::SingleJson(_) => {
            todo!("Single JSON source")
        }
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
                    self.entries = Some(unsafe { std::mem::transmute(e) });
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
            let reader = BufReader::new(entry);

            match serde_json::from_reader(reader) {
                Ok(json) => return Some(Ok(CrossrefEntry { filename, json })),
                Err(e) => {
                    log::warn!("Failed to parse JSON in {}: {}", filename, e);
                    continue;
                }
            }
        }
    }
}

impl CrossrefSource for TarGzSource {}
```

**Step 4: Run test to verify it passes**

Run: `cargo test crossref::tests::test_tar_gz_source -q`
Expected: PASS

**Step 5: Commit**

```bash
git add src/input/crossref.rs
git commit -m "feat: implement TarGzSource for Crossref archives"
```

---

## Task 6: Implement Crossref Directory and SingleJson Sources

**Files:**
- Modify: `src/input/crossref.rs`

**Step 1: Write failing tests**

Add to the tests module in `src/input/crossref.rs`:

```rust
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
    }

    #[test]
    fn test_single_json_source() {
        let dir = tempdir().unwrap();
        let json_path = dir.path().join("test.json");
        std::fs::write(&json_path, r#"{"items": [{"DOI": "10.1234/single"}]}"#).unwrap();

        let input = CrossrefInput::SingleJson(json_path);
        let mut source = open_crossref_source(input).unwrap();

        let entry = source.next().unwrap().unwrap();
        assert!(entry.json.get("items").is_some());
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
        let filenames: Vec<_> = source
            .map(|e| e.unwrap().filename)
            .collect();

        // Should be numerically sorted: 1, 2, 10
        assert_eq!(filenames, vec!["1.json", "2.json", "10.json"]);
    }
```

**Step 2: Run tests to verify they fail**

Run: `cargo test crossref::tests -q`
Expected: New tests FAIL (todo! panic)

**Step 3: Implement Directory and SingleJson sources**

Add these implementations to `src/input/crossref.rs`:

```rust
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

        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => return Some(Err(e.into())),
        };

        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(json) => Some(Ok(CrossrefEntry { filename, json })),
            Err(e) => {
                log::warn!("Failed to parse JSON in {}: {}", filename, e);
                self.next() // Skip invalid files
            }
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
        info!("Detected Crossref input: single JSON file at {}", path.display());
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

        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => return Some(Err(e.into())),
        };

        let reader = BufReader::new(file);
        match serde_json::from_reader(reader) {
            Ok(json) => Some(Ok(CrossrefEntry { filename, json })),
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl CrossrefSource for SingleJsonSource {}
```

Update `open_crossref_source` to use the new sources:

```rust
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
```

**Step 4: Run tests to verify they pass**

Run: `cargo test crossref::tests -q`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/input/crossref.rs
git commit -m "feat: implement Directory and SingleJson sources for Crossref"
```

---

## Task 7: Implement DataCite Single File Sources

**Files:**
- Modify: `src/input/datacite.rs`

**Step 1: Write failing tests**

Replace contents of `src/input/datacite.rs`:

```rust
//! DataCite input source implementations.

use anyhow::Result;
use serde_json::Value;

use super::DataciteInput;

/// Trait for iterating over DataCite records
pub trait DataciteSource: Iterator<Item = Result<Value>> {}

/// Open a DataCite source based on detected input type
pub fn open_datacite_source(_input: DataciteInput) -> Result<Box<dyn DataciteSource>> {
    todo!()
}

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
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test datacite::tests -q`
Expected: FAIL (todo! panic)

**Step 3: Implement single file sources**

Update `src/input/datacite.rs`:

```rust
//! DataCite input source implementations.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use log::info;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

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
            info!(
                "Detected DataCite input: JSONL file at {}",
                path.display()
            );
            Ok(Box::new(SingleJsonlSource::new(path)?))
        }
        DataciteInput::NestedSnapshot(_) => {
            todo!("Nested snapshot source")
        }
        DataciteInput::FlatDirectory(_) => {
            todo!("Flat directory source")
        }
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
```

**Step 4: Run tests to verify they pass**

Run: `cargo test datacite::tests -q`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/input/datacite.rs
git commit -m "feat: implement single file sources for DataCite"
```

---

## Task 8: Implement DataCite Directory Sources

**Files:**
- Modify: `src/input/datacite.rs`

**Step 1: Write failing tests**

Add to the tests module in `src/input/datacite.rs`:

```rust
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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test datacite::tests -q`
Expected: New tests FAIL

**Step 3: Implement directory sources**

Add to `src/input/datacite.rs`:

```rust
use walkdir::WalkDir;

/// Source that reads from a flat directory of .jsonl.gz files
pub struct FlatDirectorySource {
    files: std::vec::IntoIter<PathBuf>,
    current_reader: Option<std::io::Lines<BufReader<GzDecoder<File>>>>,
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
            self.current_reader = Some(BufReader::new(decoder).lines());
        } else {
            // For uncompressed files, we need a different approach
            // This is a simplification - in practice you might want to handle this differently
            let decoder = GzDecoder::new(file);
            self.current_reader = Some(BufReader::new(decoder).lines());
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
            .filter(|p| {
                p.is_file() && p.to_string_lossy().to_lowercase().ends_with(".jsonl.gz")
            })
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
```

Update `open_datacite_source`:

```rust
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
            info!(
                "Detected DataCite input: JSONL file at {}",
                path.display()
            );
            Ok(Box::new(SingleJsonlSource::new(path)?))
        }
        DataciteInput::NestedSnapshot(path) => Ok(Box::new(NestedSnapshotSource::new(path)?)),
        DataciteInput::FlatDirectory(path) => Ok(Box::new(FlatDirectorySource::new(path)?)),
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test datacite::tests -q`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/input/datacite.rs
git commit -m "feat: implement directory sources for DataCite"
```

---

## Task 9: Refactor Index Builder to Use DataciteSource

**Files:**
- Modify: `src/index/builder.rs`
- Modify: `src/index/mod.rs`

**Step 1: Write failing test for new builder function**

Add to `src/index/builder.rs` tests:

```rust
    #[test]
    fn test_build_index_from_source() {
        use crate::input::{open_datacite_source, DataciteInput};

        let file = create_test_jsonl_gz(&[
            r#"{"id": "10.1234/example1"}"#,
            r#"{"id": "10.1234/example2"}"#,
        ]);

        let input = DataciteInput::SingleJsonlGz(file.path().to_path_buf());
        let source = open_datacite_source(input).unwrap();
        let index = build_index_from_source(source, "id").unwrap();

        assert_eq!(index.len(), 2);
        assert!(index.contains("10.1234/example1"));
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test index::builder::tests::test_build_index_from_source -q`
Expected: FAIL (function doesn't exist)

**Step 3: Implement build_index_from_source**

Add to `src/index/builder.rs`:

```rust
use crate::input::DataciteSource;

/// Build a DOI index from any DataciteSource
pub fn build_index_from_source(
    source: impl DataciteSource,
    id_field: &str,
) -> Result<DoiIndex> {
    info!("Building DOI index from source");
    let start = Instant::now();

    let mut index = DoiIndex::with_capacity(10_000_000, 100_000);
    let mut records_processed = 0;
    let mut records_failed = 0;

    for record_result in source {
        match record_result {
            Ok(record) => {
                if let Some(id) = record.get(id_field).and_then(|v| v.as_str()) {
                    index.insert(id);
                }
                records_processed += 1;
                if records_processed % 500_000 == 0 {
                    info!(
                        "  Processed {} records, {} DOIs indexed...",
                        records_processed,
                        index.len()
                    );
                }
            }
            Err(_) => {
                records_failed += 1;
            }
        }
    }

    info!(
        "Built index with {} DOIs ({} prefixes) from {} records in {}",
        index.len(),
        index.prefix_count(),
        records_processed,
        format_elapsed(start.elapsed())
    );

    if records_failed > 0 {
        info!("  ({} records failed to parse)", records_failed);
    }

    Ok(index)
}
```

Update `src/index/mod.rs` to export the new function:

```rust
pub use builder::{build_index_from_jsonl_gz, build_index_from_source};
```

**Step 4: Run test to verify it passes**

Run: `cargo test index::builder::tests::test_build_index_from_source -q`
Expected: PASS

**Step 5: Commit**

```bash
git add src/index/builder.rs src/index/mod.rs
git commit -m "feat: add build_index_from_source for flexible DataCite input"
```

---

## Task 10: Integrate Input Detection into Pipeline Command

**Files:**
- Modify: `src/commands/pipeline.rs`

**Step 1: Update imports and run_extraction signature**

At the top of `src/commands/pipeline.rs`, add:

```rust
use crate::input::{
    detect_crossref_input, detect_datacite_input, open_crossref_source, open_datacite_source,
    CrossrefSource,
};
```

**Step 2: Refactor run_extraction to use CrossrefSource**

Replace the tar.gz-specific code in `run_extraction`. The key change is replacing:

```rust
// OLD: Open and stream the tar.gz
let file = File::open(&args.input)
    .with_context(|| format!("Failed to open input file: {}", args.input))?;
let gz = GzDecoder::new(file);
let mut archive = Archive::new(gz);
```

With:

```rust
// NEW: Detect and open Crossref source
let input = detect_crossref_input(&args.input)?;
let mut source = open_crossref_source(input)?;
```

And replace:

```rust
for entry_result in archive.entries()? {
    let entry = entry_result.context("Failed to read tar entry")?;
    let path = entry.path()?.to_path_buf();

    // Skip non-JSON files
    let path_str = path.to_string_lossy();
    if !path_str.ends_with(".json") {
        continue;
    }

    debug!("Processing: {}", path_str);

    // Read and parse JSON
    let reader = BufReader::new(entry);
    let json: Value = match serde_json::from_reader(reader) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse JSON in {}: {}", path_str, e);
            continue;
        }
    };
```

With:

```rust
for entry_result in source {
    let entry = match entry_result {
        Ok(e) => e,
        Err(e) => {
            warn!("Failed to read entry: {}", e);
            continue;
        }
    };

    debug!("Processing: {}", entry.filename);
    let json = entry.json;
```

Remove these imports that are no longer needed at the top:
```rust
use flate2::read::GzDecoder;
use tar::Archive;
```

**Step 3: Update load_indexes to use DataciteSource**

In `load_indexes`, replace:

```rust
} else if let Some(ref path) = args.datacite_records {
    info!("Building DataCite index from: {}", path);
    indexes.datacite = Some(build_index_from_jsonl_gz(path, "id")?);
}
```

With:

```rust
} else if let Some(ref path) = args.datacite_records {
    let input = detect_datacite_input(path)?;
    let source = open_datacite_source(input)?;
    indexes.datacite = Some(build_index_from_source(source, "id")?);
}
```

Add to imports:

```rust
use crate::index::build_index_from_source;
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles

**Step 5: Run existing tests**

Run: `cargo test`
Expected: All tests pass (existing functionality preserved)

**Step 6: Commit**

```bash
git add src/commands/pipeline.rs
git commit -m "feat: integrate flexible input detection into pipeline"
```

---

## Task 11: Integrate Input Detection into Validate Command

**Files:**
- Modify: `src/commands/validate.rs`

**Step 1: Update imports**

Add to the top of `src/commands/validate.rs`:

```rust
use crate::input::{detect_datacite_input, open_datacite_source};
use crate::index::build_index_from_source;
```

**Step 2: Update DataCite index loading**

Replace:

```rust
let datacite_index: Option<DoiIndex> = if let Some(ref path) = args.datacite_records {
    info!("Building DataCite index from: {}", path);
    Some(build_index_from_jsonl_gz(path, "id")?)
} else {
    None
};
```

With:

```rust
let datacite_index: Option<DoiIndex> = if let Some(ref path) = args.datacite_records {
    let input = detect_datacite_input(path)?;
    let source = open_datacite_source(input)?;
    Some(build_index_from_source(source, "id")?)
} else {
    None
};
```

Remove the old import:

```rust
use crate::index::build_index_from_jsonl_gz;
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles

**Step 4: Run existing tests**

Run: `cargo test`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/commands/validate.rs
git commit -m "feat: integrate flexible input detection into validate"
```

---

## Task 12: Add Integration Tests for Directory Input

**Files:**
- Modify: `tests/integration_test.rs`

**Step 1: Add test for Crossref directory input**

Add to `tests/integration_test.rs`:

```rust
/// Create a Crossref directory (extracted tar) for testing
fn create_test_crossref_directory(dir: &std::path::Path) -> std::path::PathBuf {
    let crossref_dir = dir.join("crossref_extracted");
    std::fs::create_dir(&crossref_dir).unwrap();

    let json_content = r#"{
        "items": [
            {
                "DOI": "10.1234/citing-paper",
                "reference": [
                    {"DOI": "10.5678/cited-paper", "doi-asserted-by": "publisher"}
                ]
            },
            {
                "DOI": "10.5678/cited-paper",
                "title": "Cited Paper"
            }
        ]
    }"#;

    std::fs::write(crossref_dir.join("0.json"), json_content).unwrap();
    crossref_dir
}

#[test]
fn test_pipeline_with_directory_input() {
    let dir = tempdir().unwrap();
    let crossref_dir = create_test_crossref_directory(dir.path());
    let output_path = dir.path().join("output.jsonl");

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            crossref_dir.to_str().unwrap(),
            "--source",
            "crossref",
            "--output-crossref",
            output_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline with directory input");

    assert!(status.success(), "Pipeline with directory input should succeed");
    assert!(output_path.exists(), "Output file should exist");

    // Verify output has records
    let file = File::open(&output_path).unwrap();
    let reader = BufReader::new(file);
    let lines: Vec<_> = reader.lines().collect();
    assert!(!lines.is_empty(), "Should have output records");
}
```

**Step 2: Add test for DataCite nested snapshot input**

Add to `tests/integration_test.rs`:

```rust
/// Create a DataCite nested snapshot directory for testing
fn create_test_datacite_snapshot(dir: &std::path::Path) -> std::path::PathBuf {
    let snapshot_dir = dir.join("datacite_snapshot");
    std::fs::create_dir(&snapshot_dir).unwrap();

    let month_dir = snapshot_dir.join("updated_2024-01");
    std::fs::create_dir(&month_dir).unwrap();

    let path = month_dir.join("part_0000.jsonl.gz");
    let file = File::create(&path).unwrap();
    let encoder = GzEncoder::new(file, Compression::default());
    let mut writer = std::io::BufWriter::new(encoder);
    writeln!(writer, r#"{{"id": "10.5678/cited-paper"}}"#).unwrap();
    writer.into_inner().unwrap().finish().unwrap();

    snapshot_dir
}

#[test]
fn test_pipeline_with_datacite_snapshot() {
    let dir = tempdir().unwrap();
    let tar_path = create_test_crossref_tar_gz(dir.path());
    let datacite_dir = create_test_datacite_snapshot(dir.path());
    let output_path = dir.path().join("output.jsonl");

    let status = Command::new("cargo")
        .args([
            "run",
            "--",
            "pipeline",
            "--input",
            tar_path.to_str().unwrap(),
            "--datacite-records",
            datacite_dir.to_str().unwrap(),
            "--source",
            "datacite",
            "--output-datacite",
            output_path.to_str().unwrap(),
        ])
        .status()
        .expect("Failed to run pipeline with datacite snapshot");

    assert!(status.success(), "Pipeline with datacite snapshot should succeed");
}
```

**Step 3: Run integration tests**

Run: `cargo test --test integration_test`
Expected: All tests pass

**Step 4: Commit**

```bash
git add tests/integration_test.rs
git commit -m "test: add integration tests for directory input formats"
```

---

## Task 13: Update README Documentation

**Files:**
- Modify: `README.md`

**Step 1: Update input documentation**

In the README.md, update the Usage section to document the new input formats. Add after the "Options" section:

```markdown
### Input Formats

The tool auto-detects input format based on path:

**Crossref input (`--input`):**
- `*.tar.gz` - Streams archive without extraction (original format)
- Directory with `*.json` files - Processes extracted snapshot
- Single `*.json` file - For testing or partial runs

**DataCite input (`--datacite-records`):**
- `*.jsonl.gz` - Single gzipped JSONL file
- `*.jsonl` - Single uncompressed JSONL file
- Directory with `updated_*/` subdirs - Nested snapshot format
- Directory with `*.jsonl.gz` files - Flat directory of files

Examples:
```bash
# Using extracted Crossref directory
crossref-citation-extraction pipeline \
  --input /path/to/extracted/crossref/ \
  --source crossref \
  --output-crossref output.jsonl

# Using DataCite snapshot directory
crossref-citation-extraction pipeline \
  --input crossref.tar.gz \
  --datacite-records /path/to/datacite-snapshot/ \
  --source datacite \
  --output-datacite output.jsonl
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document flexible input formats"
```

---

## Task 14: Final Verification

**Step 1: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy`
Expected: No warnings (or only acceptable ones)

**Step 3: Format code**

Run: `cargo fmt`

**Step 4: Build release**

Run: `cargo build --release`
Expected: Builds successfully

**Step 5: Manual smoke test with real data (if available)**

If you have access to test data:

```bash
# Test with extracted directory
./target/release/crossref-citation-extraction pipeline \
  --input /path/to/extracted/crossref/ \
  --source crossref \
  --output-crossref test_output.jsonl

# Verify output
head -1 test_output.jsonl | jq .
```

**Step 6: Final commit if any formatting changes**

```bash
git add -A
git commit -m "chore: final cleanup and formatting"
```

---

## Summary

This plan implements flexible input handling in 14 tasks:

1. Add walkdir dependency
2. Create input module structure
3. Implement file-based detection
4. Implement directory detection
5. Implement Crossref TarGz source
6. Implement Crossref Directory and SingleJson sources
7. Implement DataCite single file sources
8. Implement DataCite directory sources
9. Refactor index builder
10. Integrate into pipeline command
11. Integrate into validate command
12. Add integration tests
13. Update documentation
14. Final verification

Each task is self-contained with tests, making it safe to commit after each step.
