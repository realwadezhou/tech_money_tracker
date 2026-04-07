# Data

This project keeps repo-local data under `data/`, organized by domain and stage.

Current layout:

- `data/fec/raw/`
  Official bulk ZIP downloads kept as local source files.
- `data/fec/interim/`
  Extracted working files used by the pipeline.
- `data/fec/derived/`
  Cycle-specific analytical tables produced by the validated FEC pipeline.
- `data/lda/raw/`
  Reserved for raw API snapshots or other source captures from LDA.gov.
- `data/lda/interim/`
  Reserved for cleaned lobbying tables that still remain source-native.
- `data/lda/derived/`
  Reserved for source-level analytical outputs derived from lobbying data.
- `data/reference/`
  Smaller curated lookup assets that are part of the project itself.

Project convention:

- Every source gets its own top-level domain directory under `data/`.
- Each source should use the same three-stage lifecycle:
  `data/<domain>/raw`, `data/<domain>/interim`, `data/<domain>/derived`.
- Cross-source lookup assets belong under `data/reference/`, not under any one source.
