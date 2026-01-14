use crate::index::FstIndex;

/// Result of an arXiv DOI lookup
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupResult {
    /// DOI found in arXiv index
    Found,
    /// DOI not found
    NotFound,
}

/// Look up an arXiv DOI in the index
#[allow(dead_code)]
pub fn lookup_arxiv(doi: &str, arxiv_index: Option<&FstIndex>) -> LookupResult {
    let doi_lower = doi.to_lowercase();

    if let Some(idx) = arxiv_index {
        if idx.contains(&doi_lower) {
            return LookupResult::Found;
        }
    }

    LookupResult::NotFound
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::FstIndexBuilder;
    use tempfile::tempdir;

    #[test]
    fn test_lookup_arxiv_found() {
        let dir = tempdir().unwrap();
        let fst_path = dir.path().join("test.fst");

        {
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.48550/arxiv.2403.12345").unwrap();
            builder.finish().unwrap();
        }

        let index = FstIndex::load(&fst_path).unwrap();
        let result = lookup_arxiv("10.48550/arXiv.2403.12345", Some(&index));
        assert_eq!(result, LookupResult::Found);
    }

    #[test]
    fn test_lookup_arxiv_not_found() {
        let dir = tempdir().unwrap();
        let fst_path = dir.path().join("test.fst");

        {
            let mut builder = FstIndexBuilder::new(&fst_path).unwrap();
            builder.insert("10.48550/arxiv.2403.12345").unwrap();
            builder.finish().unwrap();
        }

        let index = FstIndex::load(&fst_path).unwrap();
        let result = lookup_arxiv("10.48550/arXiv.9999.99999", Some(&index));
        assert_eq!(result, LookupResult::NotFound);
    }
}
