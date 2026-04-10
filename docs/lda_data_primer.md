# LDA Data Primer

This note is the working guide for the local LDA lobbying data now stored and processed in this repo.

It is written to answer three questions:

1. What data do we actually have locally?
2. Which files should we use for analysis?
3. How do we answer common reporting questions without getting lost?

## Scope

This primer reflects the reconciled local LDA snapshots built from the LDA API and processed on April 9, 2026 and April 10, 2026.

Important caveats:

- The source of record is the LDA data itself.
- Any future "tech entity" layer is an analytical overlay, not source truth.
- For active years, "complete" means complete as of the most recent reconciliation time, not permanently frozen forever.

## The Four Layers

Think of the LDA data in this repo as four layers:

1. Raw page captures
   Location: `data/lda/raw/<year>/<endpoint>/page_*.json`
   Use for: troubleshooting ingestion, not normal analysis.

2. Reconciled raw snapshots
   Location: `data/lda/raw/<year>/<endpoint>/snapshot.jsonl`
   Use for: the local source of truth for downstream processing.
   Why: raw paginated pages can duplicate or miss rows at page boundaries while the live API is changing.

3. Interim flat tables
   Location: `data/lda/interim/<year>/`
   Use for: most analysis.
   Why: these are clean tabular tables built from the reconciled snapshots.

4. Derived summaries and profiles
   Location: `data/lda/derived/<year>/`
   Use for: quick exploration and understanding structure before custom analysis.

## Current Local Scale

### 2025

- `filings.csv`: 108,227 rows
- `filing_activities.csv`: 203,408 rows
- `filing_activity_lobbyists.csv`: 543,967 rows
- `filing_activity_government_entities.csv`: 506,572 rows
- `filing_foreign_entities.csv`: 1,092 rows
- `filing_affiliated_organizations.csv`: 990 rows
- `filing_conviction_disclosures.csv`: 349 rows
- `contributions.csv`: 39,428 rows
- `contribution_items.csv`: 149,164 rows
- `clients.csv`: 28,246 rows
- `registrants.csv`: 5,626 rows
- `lobbyists.csv`: 15,895 rows

### 2026

- `filings.csv`: 3,772 rows
- `filing_activities.csv`: 6,195 rows
- `filing_activity_lobbyists.csv`: 13,033 rows
- `filing_activity_government_entities.csv`: 8,277 rows
- `filing_foreign_entities.csv`: 131 rows
- `filing_affiliated_organizations.csv`: 80 rows
- `filing_conviction_disclosures.csv`: 8 rows
- `contributions.csv`: 100 rows
- `contribution_items.csv`: 61 rows
- `clients.csv`: 3,571 rows
- `registrants.csv`: 1,054 rows
- `lobbyists.csv`: 2,476 rows

## The Main Mental Model

The lobbying side is not one giant spreadsheet.

It is a small relational dataset:

- `filings.csv`
  One row per filing.
- `filing_activities.csv`
  One row per issue/activity inside a filing.
- `filing_activity_lobbyists.csv`
  One row per lobbyist attached to an activity.
- `filing_activity_government_entities.csv`
  One row per government entity attached to an activity.
- `clients.csv`
  One row per client entity seen in the year.
- `registrants.csv`
  One row per lobbying firm or in-house filer seen in the year.
- `lobbyists.csv`
  One row per lobbyist person seen in the year.
- `contributions.csv`
  One row per LD-203 contribution filing.
- `contribution_items.csv`
  One row per item disclosed inside an LD-203 contribution filing.

This is "flat tabular data" in the useful sense.
The only reason it is not one single table is that the source contains repeated arrays, so the clean representation is multiple related tables.

## Best Source For Each Kind Of Work

### If the question is about lobbying

Start with:

- `data/lda/interim/<year>/filings.csv`

Then join to:

- `filing_activities.csv` for issue codes and issue descriptions
- `filing_activity_lobbyists.csv` for lobbyist people and covered positions
- `filing_activity_government_entities.csv` for targeted agencies and bodies

### If the question is about LD-203 contribution disclosures

Start with:

- `data/lda/interim/<year>/contributions.csv`

Then join to:

- `contribution_items.csv`

### If the question is "who is this entity?"

Use:

- `clients.csv`
- `registrants.csv`
- `lobbyists.csv`

## Primary Keys And Join Keys

Use these keys consistently:

- Filing key: `filing_uuid`
- Activity key: `activity_id`
- Client key: `client_api_id`
- Registrant key: `registrant_api_id`
- Lobbyist key: `lobbyist_api_id`
- Contribution item key: `contribution_item_id`

Important caution:

- Prefer `client_api_id`, not `client_source_id`, as the safer local entity key.
- Prefer `registrant_api_id`, not `house_registrant_id`, as the join key.

## Biggest Gotchas

### 1. Filing year is not the same as posting year

A `filing_year = 2025` record can have `dt_posted` in 2026.

That is normal.
For example, fourth-quarter 2025 filings can be posted in early 2026.

### 2. "No activity" filings exist

Some filings have zero activities.

That means:

- the filing row exists in `filings.csv`
- but it may not create rows in `filing_activities.csv`

### 3. Do not analyze the raw paginated pages directly

Use:

- `snapshot.jsonl` as the reconciled raw source
- or the interim CSVs for almost all work

### 4. Contributions are not lobbying spend

`contributions.csv` and `contribution_items.csv` are LD-203 ethics/contribution disclosures.

They answer different questions from `filings.csv`.

### 5. The tech-company universe is not authoritative

If we later flag some clients or registrants as "tech," that is a project-curated overlay, not ground truth.

## Short How-To Guides

### Question: Which clients reported the most lobbying activity?

Use:

- `data/lda/derived/<year>/client_quarter_summary.csv`

Look at:

- `client_name`
- `n_filings`
- `total_activities`
- `total_income`
- `total_expenses`
- `total_reported_spend`

### Question: What issues did a given client lobby on?

Fast path:

- Use `data/lda/derived/<year>/client_issue_summary.csv`

Detailed path:

1. Filter `filings.csv` to the client.
2. Join on `filing_uuid` to `filing_activities.csv`.

Key columns:

- `client_name`
- `general_issue_code`
- `general_issue_code_display`
- `description`

### Question: Which lobbyists worked for a given client?

Use:

- `data/lda/derived/<year>/client_lobbyist_summary.csv`

Or join:

1. `filings.csv`
2. `filing_activity_lobbyists.csv`

Key columns:

- `client_name`
- `lobbyist_first_name`
- `lobbyist_last_name`
- `covered_position`
- `is_new`

### Question: Which agencies or political bodies were targeted?

Use:

- `data/lda/derived/<year>/government_entity_issue_summary.csv`

Or join:

1. `filing_activities.csv`
2. `filing_activity_government_entities.csv`

Key columns:

- `government_entity_name`
- `general_issue_code`
- `general_issue_code_display`
- `n_activities`

### Question: What contribution disclosures did a lobbyist or registrant make?

Use:

- `data/lda/interim/<year>/contributions.csv`
- `data/lda/interim/<year>/contribution_items.csv`

Key columns:

- `registrant_name`
- `lobbyist_name`
- `contributor_name`
- `payee_name`
- `honoree_name`
- `amount`
- `date`

### Question: How many unique clients, registrants, or lobbyists are in scope?

Use:

- `clients.csv`
- `registrants.csv`
- `lobbyists.csv`

### Question: How do I understand the nesting without reading raw JSON?

Use:

- `data/lda/derived/<year>/flattening_guide.csv`
- `data/lda/derived/<year>/table_shapes.csv`
- `data/lda/derived/<year>/structure_profile.json`

These files explain:

- what the raw endpoint contains
- which arrays were exploded
- what each flat table means

## Derived Summary Files

These are the best "start here" files for exploration:

- `client_quarter_summary.csv`
- `client_issue_summary.csv`
- `issue_quarter_summary.csv`
- `client_lobbyist_summary.csv`
- `government_entity_issue_summary.csv`
- `contribution_summary.csv`

## Recommended Workflow For Future Work

If we refresh data:

1. `python -m pipeline.lda.ingest 2025 2026`
2. `python -m pipeline.lda.reconcile 2025 2026`
3. `python -m pipeline.lda.normalize 2025 2026`
4. `python -m pipeline.lda.build_summaries 2025 2026`
5. `python -m pipeline.lda.profile 2025 2026`

If we build a tech-entity layer:

1. Start from `clients.csv` and `registrants.csv`
2. Keep tech tagging in its own reference file
3. Never overwrite or redefine the source tables themselves

## GitHub / Repo Policy

Recommended rule:

- Commit pipeline code, docs, and small reference files.
- Do not commit large generated LDA raw/interim/derived data by default.

This repo is now set up that way in `.gitignore`:

- LDA generated data stays local
- pipeline code and markdown notes can be committed and pushed

If we later want to publish a small curated output, we should whitelist that specific output intentionally rather than committing the whole generated tree.
