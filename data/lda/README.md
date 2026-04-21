# LDA Data

This source namespace is reserved for lobbying disclosure data from LDA.gov.

Important caveat for tech-focused analysis:

- The LDA source data itself is broad, but any future "tech client" or "tech registrant" tagging layer in this project should be treated as a project-curated working list.
- That tagging layer will be useful for filtering and exploratory work, but it should not be described as complete or authoritative unless separately validated.
- Keep source data and tagging logic conceptually separate: LDA filings are the source of record, while tech-entity tagging is an analytical overlay.

Stage layout:

- `data/lda/raw/`
  Raw API snapshots or other untouched source captures.
- `data/lda/interim/`
  Cleaned source-native tables prepared for analysis.
- `data/lda/derived/`
  Source-level analytical outputs derived from lobbying data.

Current workflow:

- `python -m pipeline.lda.ingest <year>`
  Fetches raw paginated LDA API snapshots into `data/lda/raw/<year>/`.
- `python -m pipeline.lda.reconcile <year>`
  Builds deduped endpoint snapshots and repairs known page-boundary instability for filings before downstream processing.
- `python -m pipeline.lda.normalize <year>`
  Flattens nested filing and contribution payloads into interim CSV tables.
  If a reconciled `snapshot.jsonl` exists, normalization reads that instead of raw pages.
- `python -m pipeline.lda.build_summaries <year>`
  Builds exploratory summary tables from the interim flat tables.
- `python -m pipeline.lda.build_tech_overlay <year>`
  Builds a review-oriented "likely tech clients / registrants" layer from the existing project alias list.
  This is an analytical overlay, not source truth.
- `python -m pipeline.lda.profile <year>`
  Writes a structure report showing how the nested API payloads map to flat tables.

Completeness model:

- Raw paginated pages are not automatically a trustworthy complete snapshot for active years.
- The API can rate-limit and live pagination can shift while records are being posted.
- Treat `data/lda/raw/<year>/<endpoint>/snapshot.jsonl` plus `snapshot_manifest.json` as the local source of truth for downstream work, not the raw page files alone.
- For active years, "complete" means complete as of the most recent reconciliation/top-up time, not permanently frozen.

Current interim tables:

- `filings.csv`
- `filing_activities.csv`
- `filing_activity_lobbyists.csv`
- `filing_activity_government_entities.csv`
- `filing_foreign_entities.csv`
- `filing_affiliated_organizations.csv`
- `filing_conviction_disclosures.csv`
- `contributions.csv`
- `contribution_items.csv`
- `clients.csv`
- `registrants.csv`
- `lobbyists.csv`

Current derived exploratory outputs:

- `client_quarter_summary.csv`
- `client_issue_summary.csv`
- `issue_quarter_summary.csv`
- `client_lobbyist_summary.csv`
- `government_entity_issue_summary.csv`
- `contribution_summary.csv`
- `tech_entity_match_candidates.csv`
- `tech_entity_review.csv`
- `tech_client_review.csv`
- `tech_registrant_review.csv`
- `tech_overlay_manifest.json`
- `structure_profile.json`
- `table_shapes.csv`
- `flattening_guide.csv`
