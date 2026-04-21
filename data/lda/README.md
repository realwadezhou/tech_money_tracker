# LDA Data

Lobbying disclosure data from <https://lda.senate.gov/>. Federal lobbyists file
quarterly reports listing their clients, the issues they lobbied on, the
government entities they contacted, and contributions they made to political
committees. The LDA publishes these as a paginated JSON API.

**Status:** Ingested into interim and exploratory summary tables. **Not yet
surfaced on the public site.** The `/federal-lobbying/` page is a placeholder.

## The tech-tagging caveat (read this first)

The same caveat that applies to FEC employer matching applies here, with
different failure modes:

- LDA filings are the source of record. Tagging a client or registrant as
  "tech" is a project-level analytical choice, not a property of the filing.
- The current "likely tech client/registrant" overlay comes from reusing the
  same alias list used for FEC employer matching. It will miss entities that
  don't overlap with employer strings, and it may tag entities that use a
  tech-sounding name but aren't tech in this context.
- Treat the overlay as exploratory. Source data and tagging logic are kept
  conceptually separate: source filings live in the normal `raw` / `interim`
  / `derived` stages, and the tech overlay is a review layer on top.

## Layout

- `data/lda/raw/<year>/<endpoint>/` — raw paginated API snapshots plus the
  reconciled `snapshot.jsonl` and `snapshot_manifest.json`
- `data/lda/interim/<year>/` — flattened CSV tables (one per endpoint shape)
- `data/lda/derived/<year>/` — exploratory summaries and the tech overlay

## Pipeline commands

```bash
python -m pipeline.lda.ingest <year>             # fetch raw paginated pages
python -m pipeline.lda.reconcile <year>          # dedupe pages into snapshot.jsonl
python -m pipeline.lda.normalize <year>          # flatten nested JSON into CSVs
python -m pipeline.lda.build_summaries <year>    # exploratory summary tables
python -m pipeline.lda.build_tech_overlay <year> # likely tech clients/registrants
python -m pipeline.lda.profile <year>            # structure report on payloads
```

Normalization reads from `snapshot.jsonl` if it exists; otherwise from the raw
pages. Always reconcile before normalizing for an active year.

## Completeness model

LDA is not a static archive. For any active year:

- Raw paginated pages captured at a single time are not necessarily a complete
  snapshot — the API rate-limits, and live pagination can shift while records
  are being posted.
- The pairing of `snapshot.jsonl` + `snapshot_manifest.json` is the local
  source of truth for downstream work, not the raw pages alone.
- "Complete" for an active year means "complete as of the last reconciliation,"
  not permanently frozen.

## Current interim tables

Produced by `pipeline.lda.normalize`:

```
filings.csv
filing_activities.csv
filing_activity_lobbyists.csv
filing_activity_government_entities.csv
filing_foreign_entities.csv
filing_affiliated_organizations.csv
filing_conviction_disclosures.csv
contributions.csv
contribution_items.csv
clients.csv
registrants.csv
lobbyists.csv
```

## Current derived outputs

Produced by `pipeline.lda.build_summaries`, `build_tech_overlay`, and `profile`:

```
client_quarter_summary.csv
client_issue_summary.csv
issue_quarter_summary.csv
client_lobbyist_summary.csv
government_entity_issue_summary.csv
contribution_summary.csv
tech_entity_match_candidates.csv
tech_entity_review.csv
tech_client_review.csv
tech_registrant_review.csv
tech_overlay_manifest.json
structure_profile.json
table_shapes.csv
flattening_guide.csv
```

None of these are surfaced on the site yet; they are working artifacts for
exploration.
