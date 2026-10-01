# Lobbying Search (private, local)

A search page for every federal lobbying quarterly report (LD-2) we have
downloaded, 2020 onward. It runs only on your computer and is not part of the
public site.

## Start it

From the project folder:

```bash
python -m tools.lobbying_search
```

Your browser opens at <http://127.0.0.1:8765/>. Press Ctrl+C in the terminal to stop.
The first start reads all the CSVs (about 25 seconds) and saves a cache to
`data/lda/derived/lobbying_search_cache.pkl`. Later starts take a few seconds.
The cache rebuilds itself whenever the LDA data is refreshed.

## What it shows

**Phrase trends:** type phrases, see how often they appear in the issue
descriptions each quarter. Click a point to see which clients said it and the
actual text, with links to the original filings.

**Company lookup:** type part of a name, see every client name that contains it
(one company usually appears under several spellings), with report counts and
reported dollars by quarter. Click a name to read its reports.

## Words used on the page

- **Report:** one lobbying firm (or a company lobbying for itself) for one
  client for one quarter. When a report was amended, only the latest version is
  used, the same rule as the AI explorer (`pipeline/lda/build_explorer.py`).
- **Issue entry:** one topic paragraph inside a report. A report can have many.
- **Client:** who paid for the lobbying, grouped by name as written (only
  capitalization and spacing are ignored).
- **Outside firms' income** vs **in-house expenses:** a lobbying firm reports what
  a client paid it; a company lobbying for itself reports its own costs, which
  usually already include what it paid outside firms. Adding both can double count.

## Limits

- Dollars belong to the whole report, not to individual issues.
- Shows what was lobbied *on*, not the position taken.
- Only as current as the last LDA refresh (`python -m pipeline.lda.refresh --status`).
  Quarters whose reports weren't all due by then are shaded and hidden by default.
