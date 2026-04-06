# Data

This project keeps repo-local data under `data/`, organized by domain and stage.

Current layout:

- `data/fec/raw/`
  Official bulk ZIP downloads kept as local source files.
- `data/fec/interim/`
  Extracted working files used by the pipeline.
- `data/fec/derived/`
  Cycle-specific analytical tables produced by the validated FEC pipeline.
- `data/reference/`
  Smaller curated lookup assets that are part of the project itself.

Near-term intent:

- FEC remains the first domain under `data/`.
- Future domains such as lobbying and congressional assets should follow the same pattern:
  `data/<domain>/raw`, `data/<domain>/interim`, `data/<domain>/derived`.
