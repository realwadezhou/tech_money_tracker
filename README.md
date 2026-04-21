# Tech Money

This project tracks the tech sector's influence in American politics via money. Currently, we have data on campaign contributions via the FEC. Future updates will include lobbying data (via the LDA) and what stocks members of Congress hold.

## Data

Current structure:

- `data/fec/raw/<cycle>/`
  Official bulk ZIP downloads
- `data/fec/interim/<cycle>/`
  Extracted FEC working files used by the pipeline
- `data/fec/derived/<cycle>/`
  Cycle-specific analytical tables produced by the validated pipeline
- `data/lda/raw/`, `data/lda/interim/`, `data/lda/derived/`
  Reserved for lobbying-disclosure ingestion and normalized outputs
- `data/reference/tech_employers/`
  Curated employer-tagging lookup files used to identify tech-linked donors
- `exports/site/<cycle>/`
  Frontend-ready cycle exports consumed by the static site builder

Most of these datasets are too big for GitHub.


## FEC Files
The key FEC files for each active cycle are:

- `data/fec/interim/2024/indiv24/itcont.txt` - itemized individual contributions
- `data/fec/interim/2024/oth24/itoth.txt` - committee-to-committee and IE transactions
- `data/fec/interim/2024/cm24/cm.txt` - committee master
- `data/fec/interim/2024/cn24/cn.txt` - candidate master
- `data/fec/interim/2024/ccl24/ccl.txt` - candidate-committee linkage
- `data/fec/interim/2024/pas224/itpas2.txt` - contributions to candidates

Same structure under `2026/` for the current cycle.

Historical bulk data outside the active working set is not required for the current public build.

## Code organization

The pipeline is organized by source first, then by shared/publish layers:

- `pipeline/common/`
  Shared environment loading and project paths
- `pipeline/fec/`
  Downloads and processes FEC data
- `pipeline/lda/`
  Downloads and processes LDA data
- `pipeline/build_*.py`, `pipeline/classify_partisan.py`
  Cross-source or presentation-oriented build steps that still power the current site

## Build outputs

- `python -m pipeline.build_frontend_exports 2024 2026`
  Rebuilds cycle-specific derived tables and site export bundles
- `python -m frontend.build_site`
  Rebuilds the static HTML site from `exports/site/<cycle>/`
- `python scripts/publish_site_to_docs.py`
  Copies the built static site into `docs/` for GitHub Pages publishing

## Deploy

GitHub Pages is currently published from the committed `docs/` snapshot.

- Local rebuilds still happen on your machine because the full data pipeline depends on large repo-external working data.
- GitHub Actions handles deployment of the committed `docs/` folder after pushes to `main`.
