# Tech Money

Fresh-start validation of FEC bulk data counting rules for tracking tech-linked money in US federal politics.

## Why this exists

An earlier project (`../spending_tracker/`) did exploratory work on FEC data but left open questions about whether the transaction type and memo code filters were excluding real dollars. This project re-derives counting rules from scratch against 2024 data (where public benchmarks exist), then applies validated rules to 2026.

## Data

The active working data for this project now lives inside this repository.

Current structure:

- `data/fec/raw/<cycle>/`
  Official bulk ZIP downloads
- `data/fec/interim/<cycle>/`
  Extracted FEC working files used by the pipeline
- `data/fec/derived/<cycle>/`
  Cycle-specific analytical tables produced by the validated pipeline
- `data/reference/tech_employers/`
  Curated employer-tagging lookup files used to identify tech-linked donors
- `exports/site/<cycle>/`
  Frontend-ready cycle exports consumed by the static site builder

The key FEC files for each active cycle are:

- `data/fec/interim/2024/indiv24/itcont.txt` - itemized individual contributions
- `data/fec/interim/2024/oth24/itoth.txt` - committee-to-committee and IE transactions
- `data/fec/interim/2024/cm24/cm.txt` - committee master
- `data/fec/interim/2024/cn24/cn.txt` - candidate master
- `data/fec/interim/2024/ccl24/ccl.txt` - candidate-committee linkage
- `data/fec/interim/2024/pas224/itpas2.txt` - contributions to candidates

Same structure under `2026/` for the current cycle.

Historical bulk data outside the active working set is not required for the current public build.

## Approach

1. Start with known donors (Musk, etc.) in 2024 data with zero filters
2. Inspect every transaction type, memo code, and recipient to understand what each row represents
3. Build counting rules bottom-up from evidence, not top-down from assumptions
4. Validate against public benchmarks (OpenSecrets, FEC candidate pages, news reports)
5. Apply validated rules to 2026 data for the full tech-money pipeline

## Build outputs

- `python -m pipeline.build_frontend_exports 2024 2026`
  Rebuilds cycle-specific derived tables and site export bundles
- `python -m frontend.build_site`
  Rebuilds the static HTML site from `exports/site/<cycle>/`
