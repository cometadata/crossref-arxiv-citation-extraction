# compare_citations

Verify arXiv citation coverage in Crossref, OpenAlex, DataCite, and OpenCitations.

## Overview

This script samples citation pairs from the [cometadata/crossref-arxiv-citations](https://huggingface.co/datasets/cometadata/crossref-arxiv-citations) HuggingFace dataset and verifies whether each citation exists in the reference metadata of external APIs.

Citations are categorized as:
- **Asserted**: The arXiv DOI appeared in the structured reference metadata (with `DOI` field)
- **Mined**: The arXiv ID was extracted from unstructured text only

## Usage

Run with uv (dependencies are declared inline):

```bash
uv run scripts/compare_citations/compare_citations.py \
  --sample 100 \
  --mailto you@example.com \
  --output-csv results.csv \
  --output-json summary.json
```

### With Crossref Plus API Token

For higher rate limits (50 RPS vs 5 RPS):

```bash
uv run scripts/compare_citations/compare_citations.py \
  --sample 1000 \
  --mailto you@example.com \
  --crossref-token $CROSSREF_API_TOKEN \
  --output-csv results.csv \
  --output-json summary.json
```

The token can also be set via the `CROSSREF_API_TOKEN` environment variable.

### Skip Slow APIs

DataCite and OpenCitations can be slow; skip them for faster runs:

```bash
uv run scripts/compare_citations/compare_citations.py \
  --sample 100 \
  --mailto you@example.com \
  --skip-datacite \
  --skip-opencitations \
  --output-csv results.csv \
  --output-json summary.json
```

## Options

| Option | Description |
|--------|-------------|
| `--sample` | Number of citation pairs to sample (required) |
| `--mailto` | Email for API polite pool access (required) |
| `--output-csv` | Path for CSV output file (required) |
| `--output-json` | Path for JSON summary file (required) |
| `--dataset` | HuggingFace dataset name (default: cometadata/crossref-arxiv-citations) |
| `--split` | Dataset split to use (default: train) |
| `--seed` | Random seed for reproducible sampling (default: 42) |
| `--crossref-token` | Crossref Plus API token |
| `--crossref-rps` | Crossref requests/sec (default: 50 with token, 5 without) |
| `--openalex-rps` | OpenAlex requests/sec (default: 10) |
| `--datacite-rps` | DataCite requests/sec (default: 5) |
| `--opencitations-rps` | OpenCitations requests/sec (default: 5) |
| `--skip-openalex` | Skip OpenAlex verification |
| `--skip-datacite` | Skip DataCite verification |
| `--skip-opencitations` | Skip OpenCitations verification |

## What It Verifies

For each sampled citation pair (citing DOI → arXiv paper):

### Crossref
- Fetches the citing work's reference list
- Checks if the arXiv DOI appears in a structured `DOI` field
- Checks if the arXiv ID appears in unstructured text or title fields

### OpenAlex
- Looks up both the citing work and arXiv work
- Checks if the arXiv work ID appears in the citing work's `referenced_works`

### DataCite Event Data
- Queries events for the arXiv DOI
- Checks if any event's `subj-id` matches the citing DOI

### OpenCitations
- Fetches references for the citing work
- Checks if any reference's `cited` field contains the arXiv DOI

## Output Format

### CSV (per-citation results)

Each row contains verification results for one citation pair:

| Field | Description |
|-------|-------------|
| `citing_doi` | DOI of the citing work |
| `arxiv_id` | arXiv ID being cited |
| `source` | "asserted" or "mined" |
| `doi_asserted_by` | "publisher", "crossref", or null |
| `crossref_found` | Whether citing work exists in Crossref |
| `crossref_has_references` | Whether citing work has reference metadata |
| `crossref_structured_doi_match` | arXiv DOI found in structured reference |
| `crossref_unstructured_match` | arXiv ID found in unstructured text |
| `crossref_reference_key` | Key of matching reference |
| `openalex_citing_found` | Whether citing work exists in OpenAlex |
| `openalex_arxiv_in_refs` | arXiv work found in referenced_works |
| `openalex_arxiv_work_id` | OpenAlex ID of arXiv work |
| `datacite_event_found` | Citation event found in DataCite |
| `datacite_total_events` | Total events for the arXiv DOI |
| `opencitations_citing_found` | Whether citing work has references in OpenCitations |
| `opencitations_arxiv_in_refs` | arXiv DOI found in OpenCitations references |
| `opencitations_oci` | OpenCitations Citation Identifier |

### JSON (summary statistics)

```json
{
  "sample_size": {
    "asserted": 45,
    "asserted_publisher": 30,
    "asserted_crossref": 15,
    "mined": 55,
    "total": 100
  },
  "asserted_results": {
    "count": 45,
    "crossref_citing_found": 44,
    "crossref_has_references": 42,
    "crossref_structured_doi_match": 38,
    "crossref_structured_match_rate": 0.844,
    "crossref_unstructured_only": 3,
    "crossref_unstructured_only_rate": 0.067,
    "crossref_no_match": 1,
    "crossref_no_match_rate": 0.022,
    "openalex_citing_found": 43,
    "openalex_arxiv_in_refs": 40,
    "openalex_match_rate": 0.889,
    "datacite_event_found": 35,
    "datacite_event_match_rate": 0.778,
    "opencitations_citing_found": 38,
    "opencitations_arxiv_in_refs": 32,
    "opencitations_match_rate": 0.711
  },
  "mined_results": {
    "count": 55,
    "crossref_structured_doi_match": 2,
    "crossref_structured_match_rate": 0.036,
    ...
  }
}
```

## Sampling

The script uses reservoir sampling to select random citation pairs while streaming the dataset. This allows sampling from the full dataset without loading it entirely into memory.

Sampling stops after seeing 10x the requested sample size to ensure a good distribution while avoiding unnecessarily long streaming.
