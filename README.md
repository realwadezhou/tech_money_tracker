# Tech Money

This is a project to track the influence of the tech sector on politics.

The public site lives at <https://realwadezhou.github.io/tech_money_tracker/>.
This repo is the data pipeline and site generator behind it. The raw data
are too big to upload to GitHub.

## What this project does

Takes raw Federal Election Commission bulk data, identifies contributions whose
reported employer matches a tracked tech company, and publishes the
results as a plain static site. The scope right now is federal campaign finance for
the 2024 and 2026 cycles. 

The lobbying collection targets reporting years 2020 onward, across all sectors.
A resumable manual backfill refreshes 2026/2025 first, then adds earlier years.
The AI lobbying explorer searches installed issue descriptions with company
watchlists, versioned topic rules, and filing-level evidence. Its coverage
manifest records the actual available years and source dates; run
`python -m pipeline.lda.refresh --status` to inspect collection progress.

Stock holdings of members of Congress are on the wishlist but not started.

## Why this is not trivial

FEC filings publish the donor's *employer* as free text. "Google," "Google
LLC," "google inc," "goog," and "alphabet" all arrive as different strings.
Identifying tech donors means maintaining a **hand-tagged** lookup that maps
these messy strings to clean company names, one by one.

Employer cleaning is only one part of the problem. FEC rows can also describe
the same money as a receipt, a transfer, an attribution, or an adjustment.
The right selection depends on whether the question concerns a donor's
original gift or the share attributed to a particular campaign. The current
site is useful for exploration; its totals are not a generally reconciled
answer to every employee-to-candidate question.

## Data flow

```
FEC bulk zips
    │  pipeline.fec.update_bulk
    ▼
data/fec/raw/<cycle>/                       (original ZIPs)
    │  (extracted during download)
    ▼
data/fec/interim/<cycle>/                   (FEC text files: itcont, cm, cn, ccl, itpas2, itoth)
    │  pipeline.build_frontend_exports (per cycle) — runs:
    │    - pipeline.fec.load           → apply transaction-type rules, join to the curated company-alias lookup
    │    - pipeline.classify_partisan  → label committees / donors D, R, Mixed
    │    - pipeline.build_summaries    → build analytical tables
    ▼
data/fec/derived/<cycle>/                   (analytical CSVs: tech_donor_summary, tech_company_summary, …)
    +
exports/site/<cycle>/                       (site-ready JSON + CSV, written in the same pass)
    │  frontend.build_site
    ▼
frontend/site/                              (static HTML)
    │  scripts/publish_site_to_docs.py
    ▼
docs/                                       (what GitHub Pages serves)
```

## How to use this project

Prerequisites: Python 3.10+ and the dependencies in `requirements.txt`:

```bash
python -m pip install -r requirements.txt
```

Node.js is also needed to run the frontend regression tests.

Before using the numbers in reporting or changing the FEC joins and filters,
read the [developer guide to FEC data and our assumptions](notes/FEC_DEVELOPER_GUIDE.md).
It explains how to investigate a company-to-candidate question, which totals
are comparable, and where source attribution remains unresolved.
Start with the shorter [FEC confidence review](notes/FEC_CONFIDENCE_REVIEW.md) for
what is established, what is still unproven, and the evidence needed before
quoting a figure.
The [methodology validation](notes/FEC_METHODOLOGY_VALIDATION.md) explains the expanded
source checks and every observed difference from the old campaign calculations.

### Rebuild everything from scratch

```bash
# 1. Pull the latest FEC bulk files for one or more cycles
python -m pipeline.fec.update_bulk 2024 2026

# Refresh FEC-confirmed historical campaign-to-PAC conversions
python -m pipeline.fec.committee_history 2024 2026

# 2. Load, classify, summarize, and export for the site (one pass, per cycle)
python -m pipeline.build_frontend_exports 2024 2026

# 3. Render the static HTML
python -m frontend.build_site

# 4. Copy into docs/ so it can be previewed locally and deployed
python scripts/publish_site_to_docs.py

# 5. Verify links, JSON, and reconciliation of exported totals
python scripts/validate_site.py
```

`build_frontend_exports` is the real work step. It imports the summary-building
functions from `pipeline.build_summaries` and the classification logic from
`pipeline.classify_partisan`, runs them per cycle, and writes both the
analytical CSVs (under `data/fec/derived/<cycle>/`) and the site-ready bundle
(under `exports/site/<cycle>/`) in one pass.

### Open the site without a local server

Open [Tech Money](https://realwadezhou.github.io/tech_money_tracker/) in your
browser, or double-click `Open Tech Money.url` in this folder on Windows.
GitHub Pages serves the site; Python and a running terminal are not needed to
browse it.

The hosted site shows the last published snapshot, not uncommitted local
changes. To publish an updated snapshot, run the build and validation steps
above, review and commit the changes, then push to `main`. The existing Pages
deployment publishes `docs/`. Data refreshes and site generation still run
locally when you request an update.

### Preview unpublished changes locally

Use this only when checking local changes before publication:

```bash
python -m http.server 8000 -d docs
# then open http://localhost:8000/
```

Opening `docs/index.html` directly is not a complete preview: charts fetch JSON
and browsers restrict those requests for local files.

### Just rebuild the site after a copy change

If only copy in `frontend/build_site.py` changed (no pipeline or data change):

```bash
python -m frontend.build_site && python scripts/publish_site_to_docs.py
```

### Committee registry (super PACs and PACs tracked by name)

`data/reference/committees/registry.csv` lists the AI, crypto and tech
committees the project tracks by name. To rebuild their full money-in and
money-out profiles into `exports/committees/` (about three minutes):

```bash
python -m pipeline.fec.committee_profiles 2024 2026
```

To look for committees that should be added (writes a review queue):

```bash
python -m pipeline.tagging.committees
```

See [data/reference/committees/README.md](data/reference/committees/README.md).
These profiles are not on the public site yet.

### Lobbying spending page

Quarterly lobbying spending for each tracked company, at `docs/lobbying/spending/`:

```bash
python -m pipeline.lda.build_spending
python -m frontend.lobbying --site-root docs
```

The first command adds up the installed lobbying reports; the second renders the
page (and the AI explorer, if its export exists). Neither downloads anything.
Which reported names count as which company is set in
`data/reference/companies/lda_clients.csv`; the counting rule is Decision 9 in
[DECISIONS.md](data/reference/companies/DECISIONS.md). Run both again after a
lobbying refresh or after editing that file.

### AI lobbying explorer (built locally, not published)

The explorer is switched off for the public site: `PUBLISH_AI_EXPLORER = False`
in `frontend/lobbying.py`. It is still built and checked on every lobbying
refresh. Decision 10 in
[DECISIONS.md](data/reference/companies/DECISIONS.md) says why and how to turn
it on. The commands below build its export; its page only appears in `docs/`
when the switch is on.

Start or resume the requested full collection with
`python -m pipeline.lda.refresh 2026 2025 2024 2023 2022 2021 2020`.
After completion, add `--new-run` for a subsequent refresh. It downloads,
verifies, normalizes, and rebuilds each completed year. Refreshes run only on
request. See [the collection workflow](data/lda/README.md).

To rebuild only from already installed source years (example):

```bash
python -m pipeline.lda.build_explorer 2025 2026
python -m frontend.lobbying --site-root docs
```

This updates the shared `docs/lobbying/` explorer and cycle lobbying entry pages
without rebuilding FEC exports. The full site generator also includes it when
the lobbying export is present. Rebuilding does not download newer filings.
See [definitions, data dictionary, and review workflow](data/reference/lobbying/README.md).

### Private lobbying search (local only)

To explore all lobbying reports by phrase or company name on your own computer:

```bash
python -m tools.lobbying_search
```

See [tools/lobbying_search/README.md](tools/lobbying_search/README.md). It is not published.

### Regression checks

```bash
python -m unittest discover -s tests -v
node --test tests/frontend_assets.test.cjs tests/lobbying_assets.test.cjs tests/lobbying_spending_assets.test.cjs
python scripts/validate_site.py
```

The regression tests use small fixtures; they do not require bulk downloads.
The site validator checks the built snapshot in `docs/`. Weekly totals plus
contributions with invalid or missing dates must reconcile to the headline.
The public committee table lists positive net recipients, so its sum excludes
committees whose refunds exceed their receipts.

After changing only candidate attribution rules, reuse the current bulk snapshot
and existing committee summaries with `python -m pipeline.rebuild_candidate_exports
2024 2026`, then render and copy the site again. This command reads only the
transaction columns needed by the candidate summaries.

Historical campaign accounts that later became PACs are identified through
FEC-confirmed conversion records in `data/reference/committees/`. Ordinary builds
read this committed supplement without making history requests. Refresh it after
updating the bulk committee and candidate directories.

## Directory map

| Path | What it holds |
|---|---|
| `data/fec/raw/<cycle>/` | Original FEC bulk ZIPs |
| `data/fec/interim/<cycle>/` | Extracted FEC text files used as pipeline input |
| `data/fec/derived/<cycle>/` | Analytical CSVs produced by the pipeline |
| `data/lda/` | Lobbying Disclosure Act source snapshots and exploratory tables |
| `data/reference/lobbying/` | AI topic rules, company watchlists, and separate review decisions |
| `exports/lobbying/` | Generated lobbying spending tables, AI issue index, evidence CSVs, and review worksheets |
| `data/reference/companies/` | **The hand-tagged employer → tech-company alias lookup.** The heart of the cleaning work. |
| `data/reference/committees/` | Committee registry (tracked super PACs and PACs) and campaign-conversion records |
| `data/reference/individuals/` | Donor-name consolidation layer (skeleton; not yet wired into the pipeline). |
| `pipeline/tagging/` | Generators that produce `candidates.csv` / `review_queue.csv` for the alias layer |
| `pipeline/` | All ingest, load, classify, and summarize code |
| `pipeline/fec/` | FEC-specific ingest and loading |
| `pipeline/lda/` | LDA-specific ingest and normalization, plus `build_spending.py` and `build_explorer.py` |
| `pipeline/build_summaries.py` | Turns loaded FEC rows into derived analytical tables |
| `pipeline/build_frontend_exports.py` | Turns derived tables into the JSON/CSV the site consumes |
| `pipeline/classify_partisan.py` | D/R/Mixed labels for committees and donors |
| `frontend/build_site.py` | Static-site generator (every page's HTML comes from here) |
| `frontend/assets/` | Stylesheet and JS for the site |
| `exports/site/<cycle>/` | Site-ready bundles produced by `build_frontend_exports` |
| `docs/` | The committed snapshot GitHub Pages serves |
| `tools/lobbying_search/` | Private local search page for all LDA reports (not published) |
| `scripts/` | Site publisher, site validator, and FEC case-audit tools |
| `notes/` | Audit reports, FEC developer guide, confidence review, methodology validation |

## Key FEC files (per cycle)

Once extracted, the pipeline reads these from `data/fec/interim/<cycle>/`:

| File | What it is |
|---|---|
| `indiv<yy>/itcont.txt` | Itemized individual contributions |
| `oth<yy>/itoth.txt` | Committee-to-committee transfers and independent expenditures |
| `cm<yy>/cm.txt` | Committee master (every committee and its filer info) |
| `cn<yy>/cn.txt` | Candidate master (every candidate) |
| `ccl<yy>/ccl.txt` | Candidate-committee links (which committees are a candidate's) |
| `pas2<yy>/itpas2.txt` | Contributions to candidates (from committees) |

These files are big (multi-GB in aggregate) and are not committed to the repo.

## Known weaknesses

These are real. They affect every number on the site.

- **False negatives from employer matching.** If a donor wrote "Self-employed,"
  left the field blank, used an unusual abbreviation, or worked at a tech
  company not in the lookup, their contribution is invisible to this project.
  The tracked-company list is curated, not exhaustive. False positives and
  refund-attribution gaps also exist, so the result is not a guaranteed lower
  bound on actual employee giving.
- **False positives from fuzzy matches.** The employer lookup is built by hand
  against a broad candidate-matches list. Some strings are ambiguous ("Apple"
  is usually the company but sometimes isn't; "Meta" is used by non-tech
  entities too). Entries get reviewed, but mistakes slip through.
- **No unitemized giving.** The FEC only requires a contributor's name,
  address, and employer when the person gives more than $200 in a cycle.
  Gifts that remain unitemized cannot be matched to an employer. Some smaller
  gifts are itemized, including gifts from donors who cross the reporting
  threshold; those can appear in the source data and are included when matched.
- **Committee party lean is inferred.** For PACs and Super PACs without a
  direct party affiliation, lean is inferred from their candidate-facing
  spending. That inference is a heuristic, not a fact on the filing.
- **Manual work is point-in-time.** Each rebuild of the employer lookup is a
  snapshot. Newly added companies or newly mapped employer strings only appear
  after the next rebuild.
- **Donors are grouped by exact reported name.** Name variants can split one
  person, and identical names can combine different people. The separate
  individual-identity review layer is not yet wired into the pipeline.
- **Refunds need attributable employers.** A refund without a matched employer
  affects the overall receipt pool but cannot automatically reduce a company's
  matched total. We do not assign it to a company using name alone.
- **Earmarked memo attribution is unresolved.** Type `15E` memo rows mix
  destination evidence and money already represented upstream. The current
  exclusion is a conservative rule, not proof that every excluded row is
  irrelevant. Candidate-specific reporting requires inspecting these cases.
- **Historical campaign accounts may later be PACs.** Confirmed former accounts
  are retained separately, but the current supplement has no transaction-level
  conversion cutoff. Combined account receipts are not necessarily money the
  campaign received before conversion.
- **Lobbying spending is a floor, for listed names only.** The spending page
  counts reports whose client name is on the reviewed list, and takes the larger
  of in-house and outside-firm sums per quarter. Dollars are not assigned to
  topics. The AI explorer's automatic topic matches have not been reviewed and it
  is not published. Congressional stock disclosures are not in scope yet.

If you're using these numbers for anything more serious than browsing, read
`data/reference/companies/README.md` for more on the matching layer, and
the [Methodology page on the site](https://realwadezhou.github.io/tech_money_tracker/2026/methodology/)
for what's counted and what isn't.

## Deploying

GitHub Pages publishes from the committed `docs/` folder. The
`.github/workflows/deploy-pages.yml` workflow triggers on pushes to `main` that
touch `docs/`. Local rebuilds happen on your machine because the full pipeline
depends on large repo-external bulk data.
