# FEC Data

Bulk data from the Federal Election Commission. This is the main source for
everything on the site today.

Active cycles: **2024** and **2026**. Older cycles are not needed for the
current build.

## Stages

- **`raw/<cycle>/`** — original bulk ZIPs downloaded from
  <https://www.fec.gov/data/browse-data/?tab=bulk-data>. Mirrored by
  `python -m pipeline.fec.update_bulk <cycle>`.
- **`interim/<cycle>/`** — ZIPs extracted to text files. These are the files
  the pipeline reads from.
- **`derived/<cycle>/`** — analytical CSVs produced by
  `pipeline.build_summaries` (tech donor summaries, company summaries,
  committee receipts, party-lean tables, and so on).

## Files the pipeline cares about

Under `interim/<cycle>/`:

| File | Contents |
|---|---|
| `indiv<yy>/itcont.txt` | Itemized individual contributions |
| `oth<yy>/itoth.txt` | Committee-to-committee transfers and independent expenditures |
| `cm<yy>/cm.txt` | Committee master |
| `cn<yy>/cn.txt` | Candidate master |
| `ccl<yy>/ccl.txt` | Candidate-committee links |
| `pas2<yy>/itpas2.txt` | Contributions to candidates |

## Not committed

Raw and interim files are multi-GB per cycle and are not in the repo. Run the
update-bulk script to download them locally.
