# Individual-donor tagging

This directory is the **individual donor consolidation layer**. It will
eventually let us group donor name variants (e.g. `MUSK, ELON` and
`MUSK, ELON R`) into a single consolidated identity (`Elon Musk`), and
optionally link that identity to a tech company.

**Status as of 2026-04-21: skeleton only. `curated.csv` is empty.**
No hand-tagging has been done yet and no generator exists. The pipeline
does not yet read anything from this directory — donor names are currently
grouped only by exact `contributor_name` string. Fixing this is the next
project.

## Intended layout (matches the companies dir)

| File | What it will be | Written by |
|---|---|---|
| `curated.csv` | Hand-tagged consolidated identities. Source of truth. | You |
| `candidates.csv` | Donor-name variants surfaced by a generator. Overwritten each run. | Generator (TODO) |
| `review_queue.csv` | Candidates not yet in `curated.csv`. | Generator (TODO) |

See `data/reference/companies/README.md` for the full workflow — the
individual side will work identically.

## Open design question: the key

Companies use the raw `employer` string as a natural key. Individuals
don't have as clean a key, because `SMITH, JOHN` in Texas and
`SMITH, JOHN` in California are almost certainly different people.

The current plan is to use `(contributor_name, state)` as the key. That
prevents mistakenly consolidating two unrelated people with the same name,
at the cost of some real duplication (a donor who moved mid-cycle has
rows under both states).

This needs to be confirmed before building the generator. If you come back
to this cold: read the companies README first to understand the shape of
the system, then decide the key before writing code.

## Schema: `curated.csv`

One row per (contributor_name, state) pair you've decided on. Columns:

| Column | Required? | Meaning |
|---|---|---|
| `contributor_name` | yes | The raw contributor_name string from FEC data (e.g. `MUSK, ELON`). Upper-cased + trimmed when matched. |
| `state` | yes | Two-letter state code from the same FEC row. Leave blank only if you intend the decision to apply to this name in every state. |
| `consolidated_name` | yes | The human-readable name used on the site, e.g. `Elon Musk`. |
| `is_tech` | yes | `TRUE` or `FALSE`. Use `FALSE` for "decided: not a tech person, don't re-queue." |
| `tech_company` | no | If `is_tech=TRUE`, the `canonical_name` of the associated company (must match a value used in `data/reference/companies/curated.csv`). |
| `notes` | no | Anything you want to remember. |

## When this gets built

When you come back to this:

1. Re-read this file.
2. Confirm or change the key decision above.
3. Create `pipeline/tagging/individuals.py` modeled on
   `pipeline/tagging/companies.py`. It should rank top donors by total
   giving, cross-reference against `tech_employers` for a first-pass
   `is_tech` suggestion, and write `candidates.csv` + `review_queue.csv`.
4. Wire consolidation into the pipeline — likely a new function in
   `pipeline/fec/load.py` analogous to `tag_tech_donors`.
5. Remove this "status" paragraph once the system is live.
