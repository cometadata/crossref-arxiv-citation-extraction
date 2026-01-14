//! Input detection and reading for Crossref and DataCite sources.

mod crossref;
mod datacite;
mod detection;

pub use crossref::open_crossref_source;
pub use datacite::open_datacite_source;
pub use detection::{detect_crossref_input, detect_datacite_input, CrossrefInput, DataciteInput};
