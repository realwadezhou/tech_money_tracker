# Tech Money

Fresh-start validation of FEC bulk data counting rules for tracking tech-linked money in US federal politics.

## Why this exists

An earlier project (`../spending_tracker/`) did exploratory work on FEC data but left open questions about whether the transaction type and memo code filters were excluding real dollars. This project re-derives counting rules from scratch against 2024 data (where public benchmarks exist), then applies validated rules to 2026.

## Data

Raw and extracted FEC bulk files live in `../spending_tracker/data/`. We reference them in-place rather than copying ~15GB of files. The key files:

- `data/interim/fec/2024/indiv24/itcont.txt` - itemized individual contributions
- `data/interim/fec/2024/oth24/itoth.txt` - other (committee-to-committee) receipts
- `data/interim/fec/2024/cm24/cm.txt` - committee master
- `data/interim/fec/2024/cn24/cn.txt` - candidate master
- `data/interim/fec/2024/ccl24/ccl.txt` - candidate-committee linkage
- `data/interim/fec/2024/pas224/itpas2.txt` - contributions to candidates

Same structure under `2026/` for the current cycle.

## Approach

1. Start with known donors (Musk, etc.) in 2024 data with zero filters
2. Inspect every transaction type, memo code, and recipient to understand what each row represents
3. Build counting rules bottom-up from evidence, not top-down from assumptions
4. Validate against public benchmarks (OpenSecrets, FEC candidate pages, news reports)
5. Apply validated rules to 2026 data for the full tech-money pipeline
