# AI lobbying explorer: definitions and review

This is an issue-description index of saved federal LDA quarterly reports. It
is independent of the FEC employer lookup and of election-cycle accounting.
The original raw snapshots and normalized tables remain the source of record.

The collection target is reporting years 2020 onward. The explorer includes
years after their source data have been installed and normalized; consult its
coverage manifest for the actual years and source dates currently available.
The first release used April 9, 2026 snapshots for 2025 and 2026; a manual
refresh/backfill replaces those as each year passes verification. Rebuilding
the explorer alone does not refresh the source.
No company or issue spending estimates are published by this explorer.

## Data model

- **Source client:** an identity within an LDA registrant's reporting records.
  Use registrant ID and client ID together. These are not necessarily distinct
  real-world organizations; the same company can have several source IDs.
- **Organization:** a project identity joined to source clients through exact
  name seeds or reviewed source-ID decisions. Names are case/whitespace
  normalized only. No substring matching or corporate suffix stripping occurs.
- **Watchlist:** a named list of organization IDs. Membership does not limit
  ingestion or topic matching. Parent/subsidiary, intermediary, and trade
  association membership relationships are not inferred. The seed mapping
  includes explicitly listed Amazon/Google operating names, not a general
  ownership graph. Add names deliberately and document corporate scope.
- **Quarterly report:** reporting year/quarter, source registrant/client, source
  UUID, posting timestamp, and source document. Amounts belong to the report.
- **Issue entry:** one reported issue category and its descriptive text, within
  a report. It can mention several subjects. Lobbyists and government entities
  remain linked to this entry, not expanded into asserted meetings or positions.
  There is a source exception: the LDA API repeats government entities from the
  entire filing for records posted before February 14, 2021. Those are marked
  `government_entity_scope=filing`; the transition date is conservatively marked
  `unknown` because the API documentation gives no cutover time. Later records
  are marked `issue_entry`. Scope follows posting date, not reporting year.
  See the [official API limitations](https://lda.gov/api/redoc/v1/).
- **Topic:** a versioned set of rules applied to issue descriptions only.
- **Match:** a topic assignment with exact text spans, matched rule IDs, match
  kind, and a separately recorded review decision. An entry can have many topics.

### Report selection

The latest posting timestamp is selected for each source registrant/client/year/
quarter. This resolves original/amended/termination reports together. Source
UUID deduplication alone does not resolve amendments. Registration forms (RR,
RA) never enter the topic index. No-activity reports can supersede an earlier
report, but their entries are not treated as activity evidence.

Groups with missing or tied latest timestamps are excluded for review. Missing
source identities, unknown types, and period/type mismatches are excluded and
counted in the manifest. This selection is an explicit analytical rule based
on the saved records, not an official amendment-chain identifier. Full version
selection decisions are preserved locally in `exports/lobbying/report_versions.csv`.

## Editable references

`topics.json` defines topic IDs, display names, definitions, and regex rules.
Each rule has a unique ID, a pattern, a match kind (`direct`, `ambiguous`, or
`context`), and optional case sensitivity. Dependencies (`requires`) must refer
to an earlier topic. Current subtopics require an AI match in the same issue
entry, which establishes co-occurrence only, not a causal or policy relationship.

Change the version string whenever topic rules change. Rebuild every comparison
year together so changes in the definition are not mistaken for time trends.
Keep previous definitions in version control. Company names and business
descriptions are not used as AI topic evidence. `AI`/`A.I.` and `LLM` are
case-sensitive whole-token abbreviation candidates; other phrases match without
case sensitivity. Automatic matches are never labeled as human-reviewed.

**Companies and their name spellings come from the shared company list** in
[`data/reference/companies/`](../companies/README.md): `companies.csv` (the
companies) and `lda_clients.csv` (reviewed lobbying client names for each).
That is the same list the private search tool and the FEC pipeline use. The
rules behind it are in [DECISIONS.md](../companies/DECISIONS.md). Names match
exactly after ignoring case and spacing. Subsidiaries roll up to the parent,
and subcontractor ("on behalf of") reports are excluded. Look-alikes such as
U.S. Apple Association are explicitly rejected there. Unmapped clients remain
in the topic index under their reported names.

`watchlists.json` in this folder only groups company IDs into named lists
("Big Tech", "AI firms and infrastructure"). The explorer adds an "All tracked
companies" list automatically. The organization version in the manifest is a
fingerprint of these three files, so any edit changes it; rebuild after editing.
The generated `organizations.json` export shows exactly what a build used.

Before October 2026 this folder held its own 10-company `organizations.json`.
Every name in it is in the shared list; see git history for the old file.

## Review workflow

1. Download/open `topic_review_queue.csv`. It includes the original passage,
   source link, topic ID, rule version, and passage hash. Read the source context.
2. Set `decision` to `accepted`, `rejected`, or `uncertain`; supply `reviewer`,
   `reviewed_at` (ISO date), and `notes` explaining ambiguous decisions.
3. Copy only decided rows into `data/reference/lobbying/topic_reviews.csv`,
   preserving its header and key columns. Leave undecided rows in the generated
   worksheet. The builder never overwrites the reference file.
4. A decision applies to exactly one activity/topic/rule version and description
   hash. A changed passage causes the build to fail rather than silently reusing
   the review. Decisions on older rule versions or superseded reports remain in
   the reference history but are not applied to the new index.
5. A rejected AI prerequisite also excludes its automatic dependent subtopics
   from the default view. Rejected evidence remains available in the audit filter.

To inspect false negatives, use `nonmatch_sample.csv`: a deterministic sample
of approximately 0.2% of current, nonmatching issue entries. To add a missed
entry manually, add an accepted topic review with the supplied activity ID,
description hash, current rules version, reviewer, date, and explanation. For a
dependent subtopic, also supply an accepted prerequisite topic if it is missing.
This sample is for qualitative auditing, not a claimed recall estimate.

For company identity review, use `organization_review_queue.csv` and record
decisions in `data/reference/lobbying/organization_reviews.csv`. An accepted row
maps the exact registrant/client IDs to a company's `canonical_name` in `companies.csv`;
a rejected row prevents the name seed from being applied to that source client.
Include reviewer, date, and rationale. Source-ID decisions override name seeds.
Future ownership changes may require effective-dated mappings; this first
release does not infer ownership history.

## Export dictionary

`matches.csv` has one row per issue entry/topic, including rejected matches:

| Field | Meaning |
|---|---|
| activity_id | Source filing UUID plus the issue entry's original position |
| filing_uuid | Source quarterly report ID; count distinct IDs for report counts |
| year / quarter | Reporting period, not posting date or election cycle |
| client_name / registrant_name | Names as reported in the selected filing |
| organization_name / organization_status | Project group, with name_seed, reviewed, unmapped, or rejected_mapping status |
| issue_code | Official general issue code; not our AI classification |
| description | Original issue description; multiple subjects may appear |
| description_sha256 | SHA-256 of the exact UTF-8 description before spreadsheet escaping |
| topic_id / rules_version | Our topic definition and version |
| match_kind | direct phrase, ambiguous abbreviation, context co-occurrence, or manual review |
| decision | unreviewed, accepted, rejected, or uncertain |
| dependency_rejected | Prerequisite rejected/missing; excluded from the default explorer view |
| matched_phrases | Matching source text; detailed rules/spans are retained in the JSON index |
| filing_url | Official original source document |
| government_entity_scope | `filing` for legacy filing-wide government entities; `issue_entry` for issue-linked entities; `unknown` on the documented transition date |

The filtered browser CSV also includes lobbyist names and government entities
listed in that issue entry. CSV downloads prefix formula-like text with an
apostrophe for spreadsheet safety; the JSON index preserves exact text. Offsets
in JSON are Unicode code-point offsets into that exact description.

`manifest.json` records source cutoff dates, selected/excluded report counts,
source-file hashes, reference-file hashes, and build/rule versions. Build date
and snapshot date have different meanings. Local manifest reconciliation does
not establish completeness against the current API or identical current content.

## Rebuild

```powershell
python -m pipeline.lda.build_explorer 2025 2026
python -m frontend.lobbying --site-root docs
python scripts/validate_site.py
```

The second command updates only the shared `lobbying/` explorer and the existing
cycle lobbying entry pages. A normal `python -m frontend.build_site` also includes
the explorer when its export is present. Generated exports are ignored by Git;
the built `docs/lobbying/` snapshot is included in the normal GitHub Pages tree.
Committing/pushing remains a separate publication step.

For a resumable full refresh, run `python -m pipeline.lda.refresh 2026 2025 2024 2023 2022 2021 2020`.
Inspect progress with `python -m pipeline.lda.refresh --status`. Once that job
finishes, use `--new-run` for a subsequent refresh. Refreshes run only on request.
This command also normalizes, rebuilds, and validates completed years. See the
collection workflow in `data/lda/README.md`. You can also run the individual
LDA `ingest --refresh`, `reconcile`, and `normalize` commands, then rebuild the explorer.
The old spending summaries
and fuzzy tech overlay are not inputs to this index. Contribution reports
(LD-203) remain separate and are not indexed as lobbying activity.

## Interpretation

Counts represent reported issue entries and distinct source reports, not
meetings, lobbying intensity, influence, or support/opposition. Distinct client
source records are not counts of consolidated organizations. An AI mention does
not allocate any share of a report's dollar amount to AI. Missing matches do not
establish that a client did no AI lobbying. Review both keyword coverage and
source completeness before making comparisons.
