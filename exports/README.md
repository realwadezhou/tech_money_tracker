# Exports

The presentation layer's input. Files here are generated; do not edit by hand.

## What lives here

- `exports/site/<cycle>/` — site-ready bundles for one cycle. Produced by
  `python -m pipeline.build_frontend_exports <cycle>`, consumed by
  `python -m frontend.build_site`.

Contents of a cycle bundle include:

```
site_metadata.json
source_manifest.json
homepage_summary.json
companies.json
committees.json
major_donors.json
candidate_race_summary.csv
candidate_state_summary.csv
candidate_senate_summary.csv
candidate_house_district_summary.csv
weekly_totals.csv
weekly_by_company.csv
weekly_by_recipient_bucket.csv
weekly_by_recipient_party.csv
entity_party_lean*.csv
charts/home_weekly_totals.json
charts/companies/<slug>.json
companies/<slug>.json
```

## How this relates to the pipeline

```
data/fec/derived/<cycle>/   →   exports/site/<cycle>/   →   frontend/site/<cycle>/   →   docs/<cycle>/
(analytical CSVs)               (site-ready JSON/CSV)      (rendered HTML)               (published snapshot)
```

Exports are a thin presentation layer on top of the derived analytical tables.
They don't introduce new transaction-type rules or new ingestion logic — that
all happens earlier in the pipeline.
