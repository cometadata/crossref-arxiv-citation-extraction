use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::cli::OutputType;

/// Resolved output paths (None means output is disabled)
#[derive(Debug, Clone, Default)]
pub struct OutputPaths {
    pub valid: Option<PathBuf>,
    pub failed: Option<PathBuf>,
    pub publisher: Option<PathBuf>,
    pub crossref: Option<PathBuf>,
    pub mined: Option<PathBuf>,
}

impl OutputPaths {
    /// Create OutputPaths from a selection of output types
    pub fn from_selection(
        outputs: &HashSet<OutputType>,
        output_dir: &Path,
        input_stem: &str,
    ) -> Self {
        let make_path = |suffix: &str| -> PathBuf {
            output_dir.join(format!("{}_{}.jsonl", input_stem, suffix))
        };

        Self {
            valid: outputs
                .contains(&OutputType::Valid)
                .then(|| make_path("valid")),
            failed: outputs
                .contains(&OutputType::Failed)
                .then(|| make_path("failed")),
            publisher: outputs
                .contains(&OutputType::Publisher)
                .then(|| make_path("publisher")),
            crossref: outputs
                .contains(&OutputType::Crossref)
                .then(|| make_path("crossref")),
            mined: outputs
                .contains(&OutputType::Mined)
                .then(|| make_path("mined")),
        }
    }

    /// Check if any output is enabled
    pub fn has_any(&self) -> bool {
        self.valid.is_some()
            || self.failed.is_some()
            || self.publisher.is_some()
            || self.crossref.is_some()
            || self.mined.is_some()
    }
}

/// Extract the base filename stem from an input path
/// "snapshot.tar.gz" -> "snapshot"
/// "/path/to/dir/" -> "dir"
pub fn extract_input_stem(input: &str) -> String {
    let path = Path::new(input.trim_end_matches('/'));
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("output");

    // Strip known compound extensions
    filename
        .strip_suffix(".tar.gz")
        .or_else(|| filename.strip_suffix(".jsonl.gz"))
        .or_else(|| filename.strip_suffix(".jsonl"))
        .or_else(|| filename.strip_suffix(".json"))
        .unwrap_or(filename)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_input_stem_tar_gz() {
        assert_eq!(extract_input_stem("snapshot.tar.gz"), "snapshot");
        assert_eq!(extract_input_stem("crossref-2024.tar.gz"), "crossref-2024");
    }

    #[test]
    fn test_extract_input_stem_jsonl() {
        assert_eq!(extract_input_stem("data.jsonl.gz"), "data");
        assert_eq!(extract_input_stem("records.jsonl"), "records");
    }

    #[test]
    fn test_extract_input_stem_directory() {
        assert_eq!(extract_input_stem("/path/to/mydir"), "mydir");
        assert_eq!(extract_input_stem("/path/to/mydir/"), "mydir");
    }

    #[test]
    fn test_extract_input_stem_with_path() {
        assert_eq!(
            extract_input_stem("/data/crossref/snapshot.tar.gz"),
            "snapshot"
        );
    }

    #[test]
    fn test_output_paths_all_outputs() {
        let mut outputs = HashSet::new();
        outputs.insert(OutputType::Valid);
        outputs.insert(OutputType::Failed);
        outputs.insert(OutputType::Mined);

        let paths = OutputPaths::from_selection(&outputs, Path::new("."), "snapshot");

        assert_eq!(paths.valid, Some(PathBuf::from("./snapshot_valid.jsonl")));
        assert_eq!(paths.failed, Some(PathBuf::from("./snapshot_failed.jsonl")));
        assert_eq!(paths.mined, Some(PathBuf::from("./snapshot_mined.jsonl")));
        assert_eq!(paths.publisher, None);
        assert_eq!(paths.crossref, None);
    }

    #[test]
    fn test_output_paths_custom_dir() {
        let mut outputs = HashSet::new();
        outputs.insert(OutputType::Valid);

        let paths = OutputPaths::from_selection(&outputs, Path::new("/tmp/results"), "data");

        assert_eq!(
            paths.valid,
            Some(PathBuf::from("/tmp/results/data_valid.jsonl"))
        );
        assert_eq!(paths.failed, None);
    }
}
