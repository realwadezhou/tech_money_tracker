# LDA Data

Lobbying disclosure data from <https://lda.gov/>. Federal lobbyists file
quarterly reports listing their clients, the issues they lobbied on, the
government entities they contacted, and contributions they made to political
committees. The LDA publishes these as a paginated JSON API.

**Status:** Ingested into interim tables. The public lobbying page is
`/lobbying/spending/` (quarterly spending by tracked company). The AI issue
explorer is built and validated locally but not published; see Decision 10 in
[DECISIONS.md](../reference/companies/DECISIONS.md). Neither uses the older
exploratory spending summaries described below.

Build it from installed years with `python -m pipeline.lda.build_explorer 2025 2026`, then
`python -m frontend.lobbying --site-root docs`. See the separate
[topic definitions, data dictionary, and review workflow](../reference/lobbying/README.md).

The `/lobbying/spending/` page shows quarterly spending by tracked company. Build
it with `python -m pipeline.lda.build_spending`, then
`python -m frontend.lobbying --site-root docs`. It uses the reviewed name list in
[`data/reference/companies/`](../reference/companies/README.md), not the
exploratory overlay described below.

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

### Full collection and manual refresh

The requested collection covers **2020 onward**, across all clients and sectors,
including registrations/quarterly activity filings and LD-203 contribution
reports. Refreshes run only when requested; no recurring refresh is scheduled.
The full 2020–2026 collection finished on September 19, 2026 at 04:22 UTC;
the [audit report](../../notes/AUDIT_2026-09-18.md) records each year's actual
snapshot cutoff and verification. The 2026 snapshot excludes postings after
its September 18 cutoff. For any later refresh, use the saved job status,
rather than this README, to determine which years are ready:

```bash
python -m pipeline.lda.refresh --status
python -m pipeline.lda.refresh 2026 2025 2024 2023 2022 2021 2020
```

The second command starts or resumes the same job. After it finishes, add
`--new-run` to start a new full refresh. Do not run another ingestion or
reconciliation command against a year while the refresh job owns it.

The refresh command:

- Downloads current years first, followed by the historical backfill.
- Uses one shared request limiter: 110 requests/minute with a configured key,
  or 14/minute anonymously, including retries. The source permits 120/minute
  authenticated and returns at most 25 records per page. See the
  [official API documentation](https://lda.gov/api/redoc/v1/).
- Fixes a posting-time cutoff for each year. Requests overlap at posting-time
  boundaries, then deduplicate by filing UUID. Large timestamp ties are fetched
  and checked separately. This avoids offset shifts across the full year.
- Saves every response page and a checkpoint under `data/lda/refresh/`. Network
  interruption preserves progress; rerun the same command to resume. A source
  count mismatch stops replacement and leaves staging available for inspection.
- Requires unique IDs to match source counts both before and after the download
  for its cutoff. Checks are explicitly count-based, not an independent comparison
  of all live record contents or a guarantee that every required report was filed.
- Creates a hashed `snapshot.jsonl`, with cutoff, fetch, and verification dates.
  Only verified endpoints replace installed data. Previous endpoints are retained
  under `data/lda/archive/<run>/<year>/`; they are not deleted.
- Normalizes a year only after both endpoints pass, rebuilds the explorer using
  all installed years, and validates its counts, CSVs, and evidence spans.
  The local `docs/` and `frontend/site/` copies update; this does not publish them.

`data/lda/refresh/job.json` records the run, completed years, and any error.
Each endpoint's `download_state.json` records its latest progress. Successful
endpoints also have a `verification.json` with source counts and a snapshot hash.
Raw response pages intentionally contain some duplicate boundary records;
`snapshot.jsonl` is the deduplicated input to normalization. Do not sum raw page
lengths to count distinct filings. Full refreshes are used instead of assuming
that unchanged counts imply unchanged filing contents.

Current-year quarters that have not ended remain partial even after a successful
download. The explorer distinguishes elapsed, current, and future quarters.
Always cite the per-year source cutoff, rather than the website build date.

### Individual legacy stages

```bash
python -m pipeline.lda.ingest <year>             # fetch raw paginated pages
python -m pipeline.lda.reconcile <year>          # dedupe pages into snapshot.jsonl
python -m pipeline.lda.normalize <year>          # flatten nested JSON into CSVs
python -m pipeline.lda.build_summaries <year>    # exploratory summary tables
python -m pipeline.lda.build_tech_overlay <year> # likely tech clients/registrants
python -m pipeline.lda.profile <year>            # structure report on payloads
```

To update a previously downloaded year, use
`python -m pipeline.lda.ingest <year> --refresh`, then reconcile, normalize,
and rebuild the summaries. Without `--refresh`, ingest reuses endpoints whose
saved manifest is already complete. Refresh downloads each endpoint into a
temporary directory and replaces its old pages, supplemental records, and
snapshot only after the download passes row-count and UUID checks. Failed or
partial refreshes preserve that endpoint's previous files. Replacement is per
endpoint, so check the outcome for both filings and contributions before
building downstream tables.

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
- Reconciliation compares the number of unique filing IDs with the live API
  count. Matching counts do not guarantee identical IDs or current record
  contents; the snapshot manifest states this verification scope.
- The API returned at most 25 records per page in the September 7, 2026 audit,
  even with `--page-size 100`. Reconciliation's tail top-up checks only the
  final 12 pages (300 records at that limit). A larger backlog requires a full
  staged `--refresh`; the current pipeline has no timestamp-based incremental
  refresh command.

Before the requested backfill, the September 7, 2026 audit checked live counts
and fixed refresh handling, but did **not** refresh the full LDA dataset. The
starting snapshots were dated April 9, 2026. These are historical audit figures,
not the progress or outcome of the new refresh:

| Year / endpoint | Local unique filings | Live API records |
|---|---:|---:|
| 2025 filings | 108,227 | 108,974 |
| 2025 contributions | 39,428 | 40,454 |
| 2026 filings | 3,772 | 56,345 |
| 2026 contributions | 100 | 18,066 |

At 25 records per page, a full refresh of both years requires approximately
8,955 page requests, plus verification and lookup requests. These exploratory
LDA files remain separate from the public site's refreshed FEC data.

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

These older derived outputs remain working artifacts for exploration. The AI
explorer reads normalized issue entries and selected quarterly reports directly;
it does not use the spending summaries or fuzzy tech overlay. Its generated
exports live in `exports/lobbying/`.
