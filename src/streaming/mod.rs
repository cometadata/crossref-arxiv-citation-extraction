pub mod aggregator;
pub mod checkpoint;
pub mod segmented_writer;

pub use aggregator::aggregate_and_validate;
pub use checkpoint::ExtractionCheckpoint;
pub use segmented_writer::{PartitionRow, SegmentedPartitionWriter};

/// Extract partition key from a DOI or arXiv ID.
/// For DOIs: uses prefix (e.g., "10.1234" -> "10.1234")
/// For arXiv IDs: uses first 4 chars
pub fn partition_key(id: &str) -> String {
    if id.starts_with("10.") {
        if let Some(slash_pos) = id.find('/') {
            return id[..slash_pos].to_lowercase();
        }
    }

    id.to_lowercase()
        .chars()
        .take(4)
        .map(|c| if c == '/' { '_' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_modern_format() {
        assert_eq!(partition_key("2403.12345"), "2403");
        assert_eq!(partition_key("2312.00001"), "2312");
        assert_eq!(partition_key("0704.0001"), "0704");
    }

    #[test]
    fn test_partition_key_old_format() {
        assert_eq!(partition_key("hep-ph/9901234"), "hep-");
        assert_eq!(partition_key("cs.dm/9910013"), "cs.d");
        assert_eq!(partition_key("astro-ph/0001001"), "astr");
        assert_eq!(partition_key("cs/9901234"), "cs_9");
        assert_eq!(partition_key("q-bio/0401001"), "q-bi");
    }

    #[test]
    fn test_partition_key_short_id() {
        assert_eq!(partition_key("abc"), "abc");
        assert_eq!(partition_key("a"), "a");
    }

    #[test]
    fn test_partition_key_doi_format() {
        assert_eq!(partition_key("10.1234/example"), "10.1234");
        assert_eq!(partition_key("10.5555/abcd.1234"), "10.5555");
        assert_eq!(partition_key("10.48550/arXiv.2403.12345"), "10.48550");
    }

    #[test]
    fn test_partition_key_doi_no_slash() {
        assert_eq!(partition_key("10.1"), "10.1");
    }
}
