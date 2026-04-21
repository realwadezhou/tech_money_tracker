# Company tagging

This directory is the **company alias layer**. It turns messy free-text
`employer` fields from FEC filings (e.g. `GOOGLE LLC`, `google inc`,
`Alphabet`) into clean, project-internal company names (e.g. `google`).

The site's company pages and every "tech money" total depend on this file
being accurate.

## The three files

| File | What it is | Who writes it | Can I delete it? |
|---|---|---|---|
| `curated.csv` | **Your hand-tagged decisions.** Source of truth. | You (by hand). | **No — this is the only file the pipeline reads.** |
| `candidates.csv` | Every employer string the crude regex surfaced, with stats. | The generator, automatically. Overwritten on every run. | Yes — will be regenerated. |
| `review_queue.csv` | Candidates **not yet** in `curated.csv`. Your worklist. | The generator, automatically. Overwritten on every run. | Yes — will be regenerated. |

The pipeline **only reads `curated.csv`**. Nothing else in this directory affects the published numbers.

## The golden rule

**Never let automation write to `curated.csv`.** Every script in this repo
leaves it alone. Everything you have tagged is preserved across FEC data
refreshes, forever, unless you choose to edit it.

## Schema: `curated.csv`

One row per employer string. Columns:

| Column | Required? | Meaning |
|---|---|---|
| `employer` | yes | The raw employer string as it appears in FEC data. Matching against contributions is done after upper-casing + trimming. |
| `include` | yes | `TRUE` = count this string as tech giving. `FALSE` = explicitly exclude (don't re-ask me). No other values. |
| `canonical_name` | yes when `include=TRUE` | Project-internal slug for the company, e.g. `google`, `meta`, `openai`. Must match whatever `canonical_name` is used in other company-pages. |
| `sector` | no | Freeform tag like `tech_giant`, `ai`, `vc`, `semiconductors`. Useful for grouping but not required. |
| `notes` | no | Anything you want to remember about this row. |
| `matched_searches` | no | Which regex label surfaced this row originally (e.g. `google`). Pipeline uses this as an extra alias in the lobbying overlay. |

## How the workflow works

You will repeat this cycle whenever new FEC data arrives:

### 1. Pull fresh FEC data

Whatever your usual update command is (e.g. `python -m pipeline.update_fec_bulk`).
That populates `data/fec/interim/<cycle>/` but touches nothing in this directory.

### 2. Regenerate candidates and review queue

```
python -m pipeline.tagging.companies
```

This reads every `itcont.txt` under `data/fec/interim/*/`, runs the broad
regex searches in `pipeline/tagging/companies.py`, and writes:

- `candidates.csv` — every employer string that matched any pattern, sorted
  by total dollar volume.
- `review_queue.csv` — candidates whose key is not already in `curated.csv`.
  This is your worklist. Each row has empty `include` / `canonical_name` /
  `sector` / `notes` columns waiting to be filled in.

**`curated.csv` is not touched.**

### 3. Review rows in `review_queue.csv`

Open it in your spreadsheet tool, or feed rows to an LLM for first-pass
suggestions. For each row:

- If it's one of the tracked tech companies → set `include=TRUE`, pick a
  `canonical_name`, optionally set `sector`.
- If it's noise → set `include=FALSE`. This records "I decided this is not
  tech" so it never re-appears in the review queue.
- If you're not sure → leave blank. It'll stay in the queue next regen.

**Do not invent new `canonical_name` values lightly.** If you add a brand-new
canonical_name, a new company page needs to exist for it to show up on the
site. Check existing values in `curated.csv` first and re-use them.

### 4. Move your decisions into `curated.csv`

Two options — whichever you prefer:

**Option A — append by hand.** Copy every row you've filled in from
`review_queue.csv` into `curated.csv`, then delete those rows from
`review_queue.csv`. Save. Commit. Done.

**Option B — promote script.** (If this ever gets tedious enough to
automate, add a `scripts/promote_reviewed.py` — but until then, by-hand is
fine and auditable.)

### 5. Rebuild the site

Your normal build commands (`python frontend/build_site.py` then
`python scripts/publish_site_to_docs.py`). The pipeline reads the updated
`curated.csv` and you're done.

## What if I change my mind about a row?

Three scenarios:

1. **I want to change a tag** (e.g. `APPLE BANK` from `apple` → exclude).
   Edit the row in `curated.csv` in place. Flip `include` or change
   `canonical_name`. Rebuild the site.

2. **I want to re-review it from scratch.** Delete the row from
   `curated.csv`. Next time you run `python -m pipeline.tagging.companies`,
   the row will reappear in `review_queue.csv` because the key is no longer
   in curated.

3. **I made a mistake and want to undo.** `curated.csv` is in git. Revert
   the change.

## "Don't show me this again" — using `include=FALSE`

If you decide `BOB'S PIZZA` is not a tech employer, **keep the row in
`curated.csv` with `include=FALSE`.** Do not just delete it — if you do,
the generator will re-queue it every time it reappears in FEC data.

The pipeline ignores `include=FALSE` rows. The generator skips any
already-known key. So marking `FALSE` is the way to say "decided: not tech,
stop bothering me."

## How `employer` is matched against contributions

The pipeline upper-cases and strips whitespace on both sides before joining.
So `"Google, LLC"` in `curated.csv` will match `"GOOGLE, LLC"`,
`"google, llc"`, and `"  Google, LLC  "` in the raw FEC data — but not
`"GOOGLE LLC"` (no comma). If you want to match both, add two rows.

## Where the generator's regex patterns live

`pipeline/tagging/companies.py`. If you find you're routinely adding the
same variant for a company, it's faster to add a new pattern to
`TECH_SEARCHES` so the generator surfaces it automatically next cycle.

## What this does not cover

- **Individual donors.** See `data/reference/individuals/README.md` for the
  sibling system that consolidates donor names.
- **Committee / PAC names.** PACs are linked to companies through
  `connected_org_nm` on the FEC committee master (`cm.txt`), not through
  this file.
