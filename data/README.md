# Data

Note this may be largely empty on GitHub since most the datasets are too big
to upload.

Everything in `data/` is either input to the pipeline or a working artifact
produced by it. Large raw, interim, and derived datasets are rebuilt locally
and are not committed. The small curated reference lookups are committed.

## Layout

```
data/
├── fec/                    # Federal Election Commission data (currently the main source)
│   ├── raw/<cycle>/        # Original bulk ZIPs as downloaded from the FEC
│   ├── interim/<cycle>/    # Extracted text files used as pipeline input
│   └── derived/<cycle>/    # Analytical CSVs produced by the pipeline
├── lda/                    # Lobbying Disclosure Act inputs and site explorer data
│   ├── raw/
│   ├── interim/
│   └── derived/
└── reference/              # Small curated lookups that live with the code
    ├── companies/          # Hand-tagged employer → tech company alias layer
    ├── committees/         # Source-confirmed former campaign accounts
    ├── transactions/       # Exact filing-reviewed repeated-receipt exclusions
    ├── lobbying/           # Topics, organization mappings, and source reviews
    └── individuals/        # Donor-name consolidation layer (skeleton)
```

## The three-stage convention

Every source directory follows the same `raw → interim → derived` shape:

- **`raw/`** — the data as the source publishes it. Untouched.
- **`interim/`** — cleaned or extracted form, still source-native (one file
  per source file, same schema). This is what the pipeline reads from.
- **`derived/`** — analytical outputs that combine, filter, or aggregate the
  interim files. This is the pipeline's output.

## Size

`data/fec/raw/` and `data/fec/interim/` are multi-GB per cycle. They live
locally only.

`data/reference/` is small enough to commit and *does* get committed — it is
the hand-curated part of the project.

## See also

- [`../FEC_DEVELOPER_GUIDE.md`](../FEC_DEVELOPER_GUIDE.md) — counting assumptions,
  API comparisons, joins, and source-review workflow
- [`fec/README.md`](fec/README.md) — details on the FEC pipeline stages
- [`lda/README.md`](lda/README.md) — details on lobbying ingestion
- [`reference/companies/README.md`](reference/companies/README.md) —
  the employer → tech-company alias layer (the core cleaning layer)
- [`reference/individuals/README.md`](reference/individuals/README.md) —
  donor-name consolidation (skeleton; not yet wired into the pipeline)
