# Tech Money

This is a project to track the influence of the tech sector on politics.

The public site lives at <https://realwadezhou.github.io/tech_money_tracker/>.
This repo is the data pipeline and site generator behind it. The raw data
are too big to upload to GitHub.

## What this project does

Takes raw Federal Election Commission bulk data, identifies contributions that
came from employees (or PACs) of tracked tech companies, and publishes the
results as a plain static site. The scope right now is federal campaign finance for
the 2024 and 2026 cycles. 

Lobbying (LDA) data are partially ingested but not yet surfaced on the site. 

Stock holdings of members of Congress are on the wishlist but not started.

## Why this is not trivial

FEC filings publish the donor's *employer* as free text. "Google," "Google
LLC," "google inc," "goog," and "alphabet" all arrive as different strings.
Identifying tech donors means maintaining a **hand-tagged** lookup that maps
these messy strings to clean company names, one by one.

That manual cleaning is the core of this project and also its biggest
limitation — see **Known weaknesses** below.

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

Prerequisites: Python 3 and `pandas`. Everything else the code uses is in the
standard library. There is no `requirements.txt` yet.

### Rebuild everything from scratch

```bash
# 1. Pull the latest FEC bulk files for one or more cycles
python -m pipeline.fec.update_bulk 2024 2026

# 2. Load, classify, summarize, and export for the site (one pass, per cycle)
python -m pipeline.build_frontend_exports 2024 2026

# 3. Render the static HTML
python -m frontend.build_site

# 4. Copy into docs/ so it can be previewed locally and deployed
python scripts/publish_site_to_docs.py
```

`build_frontend_exports` is the real work step. It imports the summary-building
functions from `pipeline.build_summaries` and the classification logic from
`pipeline.classify_partisan`, runs them per cycle, and writes both the
analytical CSVs (under `data/fec/derived/<cycle>/`) and the site-ready bundle
(under `exports/site/<cycle>/`) in one pass.

### Preview locally

```bash
python -m http.server 8000 -d docs
# then open http://localhost:8000/
```

### Just rebuild the site after a copy change

If only copy in `frontend/build_site.py` changed (no pipeline or data change):

```bash
python -m frontend.build_site && python scripts/publish_site_to_docs.py
```

## Directory map

| Path | What it holds |
|---|---|
| `data/fec/raw/<cycle>/` | Original FEC bulk ZIPs |
| `data/fec/interim/<cycle>/` | Extracted FEC text files used as pipeline input |
| `data/fec/derived/<cycle>/` | Analytical CSVs produced by the pipeline |
| `data/lda/` | Lobbying Disclosure Act data (ingested, not yet on the site) |
| `data/reference/companies/` | **The hand-tagged employer → tech-company alias lookup.** The heart of the cleaning work. |
| `data/reference/individuals/` | Donor-name consolidation layer (skeleton; not yet wired into the pipeline). |
| `pipeline/tagging/` | Generators that produce `candidates.csv` / `review_queue.csv` for the alias layer |
| `pipeline/` | All ingest, load, classify, and summarize code |
| `pipeline/fec/` | FEC-specific ingest and loading |
| `pipeline/lda/` | LDA-specific ingest and normalization |
| `pipeline/build_summaries.py` | Turns loaded FEC rows into derived analytical tables |
| `pipeline/build_frontend_exports.py` | Turns derived tables into the JSON/CSV the site consumes |
| `pipeline/classify_partisan.py` | D/R/Mixed labels for committees and donors |
| `frontend/build_site.py` | Static-site generator (every page's HTML comes from here) |
| `frontend/assets/` | Stylesheet and JS for the site |
| `exports/site/<cycle>/` | Site-ready bundles produced by `build_frontend_exports` |
| `docs/` | The committed snapshot GitHub Pages serves |
| `scripts/` | Ad-hoc investigation scripts and the site → docs publisher |

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
  The tracked-company list is curated, not exhaustive — real tech giving is
  always larger than what appears here.
- **False positives from fuzzy matches.** The employer lookup is built by hand
  against a broad candidate-matches list. Some strings are ambiguous ("Apple"
  is usually the company but sometimes isn't; "Meta" is used by non-tech
  entities too). Entries get reviewed, but mistakes slip through.
- **No unitemized giving.** The FEC only requires a contributor's name,
  address, and employer when the person gives more than $200 in a cycle.
  Smaller gifts aren't reported by name, so they cannot be matched to any
  employer. This is a hard limit of the source data, not something cleaning
  can fix.
- **Committee party lean is inferred.** For PACs and Super PACs without a
  direct party affiliation, lean is inferred from their candidate-facing
  spending. That inference is a heuristic, not a fact on the filing.
- **Manual work is point-in-time.** Each rebuild of the employer lookup is a
  snapshot. Newly added companies or newly mapped employer strings only appear
  after the next rebuild.
- **LDA and stock-holding data are not integrated.** The LDA pipeline exists
  but the site does not yet surface it. Congressional stock disclosures are not
  in scope yet.

If you're using these numbers for anything more serious than browsing, read
`data/reference/companies/README.md` for more on the matching layer, and
the [Methodology page on the site](https://realwadezhou.github.io/tech_money_tracker/2026/methodology/)
for what's counted and what isn't.

## Deploying

GitHub Pages publishes from the committed `docs/` folder. The
`.github/workflows/deploy-pages.yml` workflow triggers on pushes to `main` that
touch `docs/`. Local rebuilds happen on your machine because the full pipeline
depends on large repo-external bulk data.