use serde::{Deserialize, Serialize};

/// Represents a single arXiv match with normalized ID and raw matched text
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivMatch {
    pub id: String,
    pub raw: String,
    pub arxiv_doi: String,
}

impl ArxivMatch {
    pub fn new(id: String, raw: String) -> Self {
        let arxiv_doi = format!("10.48550/arXiv.{}", id);
        Self { id, raw, arxiv_doi }
    }
}
