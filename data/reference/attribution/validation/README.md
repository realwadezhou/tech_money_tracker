# Frozen 2026 campaign-source comparisons

This is address-free evidence for a bounded methodology audit, not a replacement campaign-finance dataset. The final snapshot covers **32 company/campaign cases and 429 exact transaction comparisons**: El-Sayed 18/282, DelBene 4/89, Rounds 10/58. It preserves the evidence even when local `outputs/` audit files are removed.

- `bridge_snapshot.json`: all 32 company results, component bridges, nullable combined/net amounts, issue counts, original filing hashes and latest IDs, manifest hashes, and canonical review-annotation hashes. Receipt/refund issue arrays are omitted. Its source/review/account scopes come from the named manifest, while employer matching deliberately uses **all 447 included curated aliases across 37 companies**.
- `audit_provenance.json`: installed raw-bulk hash, source size/date/row count, actual production selector rules, alias/reference hashes, and code hashes.
- `*_record_comparison.csv`: the exact comparison outputs. They join committee ID + filing number + transaction ID, never donor names or amounts. Raw amount strings and public source IDs remain visible. CSV hashes are in the snapshot.
- `*_original_only_context.json`: filed aggregate amounts and explicit source-parent context for records absent from bulk. Original record hashes are included; full-file hashes are in the snapshot.
- `rounds_audit_manifest.json`: the exact audit-only source/review manifest, retained here because its working copy was under ignored `outputs/`. Its ownership review deliberately remains unresolved.
- `reconciliation_note.md`: the final human-readable findings and all 32 amounts.

The final component bridges are:

| Campaign account | Legacy selected-record sum | Supported source components | Explanation of difference | Combined receipts | Net |
|---|---:|---:|---|---|---|
| El-Sayed, C00902668 | $277,666.00 | $277,852.46 | $185 original-only direct records + $1.46 exact raw-cent differences | All 18 cases supported for this report scope | All unavailable |
| DelBene, C00459099 | $28,100.00 | $65,125.00 | $300 original-only direct records + $36,725 reviewed JFC allocations | All four cases supported for this report scope | All unavailable |
| Rounds, C00532465 | $144,725.00 | $165,725.59 | $14,000 reviewed JFC allocations + $0.59 raw-cent differences + $7,000 unresolved negative corrections withheld | All ten unavailable | All unavailable |

The Rounds $7,000 component increase is **not additional giving**: two Google-matched negative corrections remain unresolved. Its 84 receipt issues per company consist of 83 campaign-wide unresolved negative-origin records and one unreviewed account-ownership scope. Shared scope issues must not be added across companies as distinct transactions. Refund issue counts likewise do not prove any target-company donor received a refund.

The reported periods end July 15, 2026 for El-Sayed/DelBene and June 30, 2026 for Rounds. These are the specified full-report accounts and processed inventories; they do not establish all candidate accounts, later notices, current giving, employment, or complete refund-adjusted giving. In particular the El-Sayed case covers C00902668 only. It does not include the separately identified C00961805 account.

## Reproduce the comparisons

Run from the repository root at the code/reference revision whose hashes appear in `audit_provenance.json`. First run the focused tests:

```powershell
python -m unittest tests.test_fec_attribution tests.test_campaign_reports tests.test_fec_filings tests.test_campaign_reconciliation
```

The original installed `data/fec/interim/2026/indiv26/itcont.txt` must match SHA-256 `8d2c57f932a354a65f42d7b7f6031e3006cb34473eeb4ca882d0e7e70ba13f71`. A newer FEC bulk snapshot is a different comparison, not a byte-for-byte reproduction. The scan also uses the installed 2026 committee directory and the tracked curated-employer/exact-exclusion references. It verifies the reviewed exclusions against the streamed raw records.

```powershell
Get-FileHash data/fec/interim/2026/indiv26/itcont.txt -Algorithm SHA256
python -m scripts.reconcile_campaign_sources scan --cycle 2026 --committee C00902668 --committee C00459099 --output outputs/reproduce_campaign_comparison/bulk
```

This reads the large bulk file once. It writes exact raw extracts **only under local ignored `outputs/`**; those `.raw.psv.gz` files contain source addresses and must never be copied into this directory or the website. Derive the small Rounds matched-record cache from that same scan:

```powershell
@'
import csv, gzip
from pathlib import Path
base = Path('outputs/reproduce_campaign_comparison/bulk')
with gzip.open(base / 'all_curated_employer_matches.public.csv.gz', 'rt', encoding='utf-8', newline='') as src:
    reader = csv.DictReader(src)
    with gzip.open(base / 'C00532465.matched.public.csv.gz', 'wt', encoding='utf-8', newline='') as dst:
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(row for row in reader if row['cmte_id'] == 'C00532465')
'@ | python -
```

Use existing local original filings or download the exact immutable filing IDs below. Verify every original against the manifest hash before parsing. This recipe downloads into a local ignored folder and produces no site export:

```powershell
@'
import hashlib, json
from pathlib import Path
from urllib.request import Request, urlopen
manifests = [
    Path('data/reference/attribution/nvidia_el_sayed.json'),
    Path('data/reference/attribution/meta_delbene_full_reports.json'),
    Path('data/reference/attribution/validation/rounds_audit_manifest.json'),
]
folder = Path('outputs/reproduce_campaign_comparison/source_filings')
folder.mkdir(parents=True, exist_ok=True)
for manifest in manifests:
    for source in json.loads(manifest.read_text(encoding='utf-8'))['sources']:
        number = source['file_number']
        path = folder / f'{number}.fec'
        if path.exists():
            raw = path.read_bytes()
        else:
            url = f'https://docquery.fec.gov/dcdev/posted/{number}.fec'
            with urlopen(Request(url, headers={'User-Agent': 'tech-money-audit/1.0'}), timeout=60) as response:
                raw = response.read()
        if hashlib.sha256(raw).hexdigest() != source['sha256']:
            raise ValueError(f'Source {number} changed; do not reuse the old review')
        if not path.exists():
            path.write_bytes(raw)
'@ | python -
```

Run all-company comparisons. The script overrides the manifest's single displayed company with each relevant curated company while retaining the full source/account/review scope:

```powershell
python -m scripts.reconcile_campaign_sources compare --manifest data/reference/attribution/nvidia_el_sayed.json --bulk-index outputs/reproduce_campaign_comparison/bulk/C00902668.public.csv.gz --source-dir outputs/reproduce_campaign_comparison/source_filings --output outputs/reproduce_campaign_comparison/el_sayed
python -m scripts.reconcile_campaign_sources compare --manifest data/reference/attribution/meta_delbene_full_reports.json --bulk-index outputs/reproduce_campaign_comparison/bulk/C00459099.public.csv.gz --source-dir outputs/reproduce_campaign_comparison/source_filings --output outputs/reproduce_campaign_comparison/delbene
python -m scripts.reconcile_campaign_sources compare --manifest data/reference/attribution/validation/rounds_audit_manifest.json --bulk-index outputs/reproduce_campaign_comparison/bulk/C00532465.matched.public.csv.gz --source-dir outputs/reproduce_campaign_comparison/source_filings --output outputs/reproduce_campaign_comparison/rounds
```

Compare the resulting `record_comparison.csv` hashes to the corresponding frozen files. The semantic results and CSV bytes should match with identical inputs/code; timestamps, working paths and gzip container hashes need not. `uncompressed_sha256` records the bulk-cache content independently of gzip timestamps. A future source, alias, review or engine change requires fresh comparison outputs and explanations; never keep a stale supported-status claim by silently replacing its inputs.

## What the evidence does and does not explain

All nonzero record differences reconcile exactly to the company bridges. No multiple-sub-ID ambiguity, bulk-only key, employer/identity fact change or unexplained numeric delta occurs in these scopes. That does not resolve Rounds' donor corrections or any campaign's refund relationships.

The original-only direct records have filed aggregate values of $0–$200, consistent with the FEC individual bulk file's documented greater-than-$200 aggregate threshold. This is source-scope evidence, not a name-based inference about a missing donor. The [official FEC individual-file description](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/) allows cents in `TRANSACTION_AMT`; observed exact-key raw differences establish precision loss only for those specific pairs, not a universal bulk-rounding rule.

Large partnership memo cases are separate bounded source fixtures: `tests/fixtures/fec_attribution/a16z_partnership.json` and `tests/fixtures/fec_attribution/musk_partnership_in_kind.json`. They demonstrate why blanket memo deletion is wrong and why organizational parents must not be added to their donor-attribution children. Neither fixture establishes complete current giving or refunds.
