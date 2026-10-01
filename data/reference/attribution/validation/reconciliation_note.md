# Bulk-to-original campaign reconciliation

Final snapshot: 2026-09-19T11:14:04.671613+00:00. Frozen address-free provenance, all 32 company results and 429 exact transaction comparisons are in `data/reference/attribution/validation/`. The snapshot has 22 supported receipt cases, 10 unresolved receipt cases and no resolved net cases. Supported is limited to the stated processed original-report scope; it is not complete current giving.

The installed 2026 bulk file was scanned once: **31,894,597 rows**, SHA-256 `8d2c57f932a354a65f42d7b7f6031e3006cb34473eeb4ca882d0e7e70ba13f71`. The production employer/type/memo/refund/exact-exclusion rules reproduce **117,595 selected employer-matched rows totaling $249,074,477**, exactly the unchanged legacy headline. No export or legacy selector was changed. All 447 included curated aliases (37 canonical companies) were considered.

Comparisons join **committee ID + filing number + transaction ID**, never names or amounts. Exact raw bulk amount strings, sub-IDs, source-row numbers, original source hashes, and dispositions are retained in record_comparison.csv. Equal-looking gifts remain separate. Duplicate keys are exposed rather than guessed. Published comparison evidence omits source location fields; raw target extracts are local audit files only.

The columns below distinguish a legacy selected-record sum from supported components. A difference caused by withholding an unresolved negative adjustment is **not newly established giving**. A null combined amount remains unknown even when some components are supported. Refunds have a separate review scope; unknown net amounts are never zero.

## El-Sayed

18 original report versions select 6 latest reports, covering 2025-01-01 through 2026-07-15. 18 company cases and 282 relevant transaction keys. Legacy selected-record sum **$277,666.00**; supported-component sum **$277,852.46**.

| Company | Legacy selected records | Supported source components | Component difference | Combined before refunds | Net |
|---|---:|---:|---:|---:|---:|
| amazon | $19,800.00 | $19,800.00 | $0.00 | $19,800.00 | Unavailable |
| amd | $7,500.00 | $7,500.00 | $0.00 | $7,500.00 | Unavailable |
| anthropic | $21,250.00 | $21,250.00 | $0.00 | $21,250.00 | Unavailable |
| apple | $56,475.00 | $56,475.00 | $0.00 | $56,475.00 | Unavailable |
| coreweave | $1,650.00 | $1,650.00 | $0.00 | $1,650.00 | Unavailable |
| google | $53,725.00 | $53,725.00 | $0.00 | $53,725.00 | Unavailable |
| ibm | $1,750.00 | $1,750.00 | $0.00 | $1,750.00 | Unavailable |
| meta | $7,960.00 | $7,960.47 | $0.47 | $7,960.47 | Unavailable |
| microsoft | $48,255.00 | $48,255.00 | $0.00 | $48,255.00 | Unavailable |
| nvidia | $33,500.00 | $33,500.00 | $0.00 | $33,500.00 | Unavailable |
| openai | $500.00 | $500.00 | $0.00 | $500.00 | Unavailable |
| oracle | $4,750.00 | $4,750.99 | $0.99 | $4,750.99 | Unavailable |
| palantir | $0.00 | $185.00 | $185.00 | $185.00 | Unavailable |
| qualcomm | $500.00 | $500.00 | $0.00 | $500.00 | Unavailable |
| salesforce | $3,951.00 | $3,951.00 | $0.00 | $3,951.00 | Unavailable |
| stripe | $1,100.00 | $1,100.00 | $0.00 | $1,100.00 | Unavailable |
| tesla | $7,000.00 | $7,000.00 | $0.00 | $7,000.00 | Unavailable |
| uber | $8,000.00 | $8,000.00 | $0.00 | $8,000.00 | Unavailable |

Exact-key categories: `{"bulk_precision_difference": 3, "new_supported_record_not_in_legacy_selection": 4, "original_only": 4, "same_source_facts": 275}`.

Receipt issues per company: 0; refund/coverage issues per company: 140. These are shared campaign-scope issues, not distinct gifts or proof of employer-specific refunds. Full issue arrays are deliberately omitted from the frozen compact evidence.

Evidence: `el_sayed_record_comparison.csv`, `el_sayed_original_only_context.json`, and this case in `bridge_snapshot.json`.

## DelBene

7 original report versions select 7 latest reports, covering 2025-01-01 through 2026-07-15. 4 company cases and 89 relevant transaction keys. Legacy selected-record sum **$28,100.00**; supported-component sum **$65,125.00**.

| Company | Legacy selected records | Supported source components | Component difference | Combined before refunds | Net |
|---|---:|---:|---:|---:|---:|
| amazon | $5,000.00 | $24,000.00 | $19,000.00 | $24,000.00 | Unavailable |
| google | $3,750.00 | $3,750.00 | $0.00 | $3,750.00 | Unavailable |
| meta | $4,800.00 | $4,925.00 | $125.00 | $4,925.00 | Unavailable |
| microsoft | $14,550.00 | $32,450.00 | $17,900.00 | $32,450.00 | Unavailable |

Exact-key categories: `{"new_supported_record_not_in_legacy_selection": 15, "original_only": 15, "same_source_facts": 74}`.

Receipt issues per company: 0; refund/coverage issues per company: 227. These are shared campaign-scope issues, not distinct gifts or proof of employer-specific refunds. Full issue arrays are deliberately omitted from the frozen compact evidence.

Evidence: `delbene_record_comparison.csv`, `delbene_original_only_context.json`, and this case in `bridge_snapshot.json`.

## Rounds

11 original report versions select 7 latest reports, covering 2025-01-01 through 2026-06-30. 10 company cases and 58 relevant transaction keys. Legacy selected-record sum **$144,725.00**; supported-component sum **$165,725.59**.

| Company | Legacy selected records | Supported source components | Component difference | Combined before refunds | Net |
|---|---:|---:|---:|---:|---:|
| a16z | $7,000.00 | $7,000.00 | $0.00 | Unavailable | Unavailable |
| anduril | $0.00 | $7,000.00 | $7,000.00 | Unavailable | Unavailable |
| anthropic | $61,600.00 | $61,600.00 | $0.00 | Unavailable | Unavailable |
| coinbase | $7,143.00 | $7,143.56 | $0.56 | Unavailable | Unavailable |
| google | $49,400.00 | $56,400.00 | $7,000.00 | Unavailable | Unavailable |
| meta | $2,082.00 | $2,082.03 | $0.03 | Unavailable | Unavailable |
| microsoft | $7,000.00 | $7,000.00 | $0.00 | Unavailable | Unavailable |
| openai | $3,500.00 | $3,500.00 | $0.00 | Unavailable | Unavailable |
| palantir | $0.00 | $7,000.00 | $7,000.00 | Unavailable | Unavailable |
| stripe | $7,000.00 | $7,000.00 | $0.00 | Unavailable | Unavailable |

Exact-key categories: `{"bulk_precision_difference": 2, "legacy_selected_now_unresolved": 2, "new_supported_record_not_in_legacy_selection": 4, "original_only": 4, "same_source_facts": 52}`.

Receipt issues per company: 84; refund/coverage issues per company: 7. These are shared campaign-scope issues, not distinct gifts or proof of employer-specific refunds. Full issue arrays are deliberately omitted from the frozen compact evidence.

Evidence: `rounds_record_comparison.csv`, `rounds_original_only_context.json`, and this case in `bridge_snapshot.json`.

## Explained differences

**El-Sayed:** Four original-file-only Palantir contributions total $185 ($30, $30, $25, $100). Their filed aggregate fields are $0, $0, $125, $125. Their absence is consistent with the FEC bulk file's documented contribution-aggregate threshold of greater than $200; this is a source-scope difference, not an employer-alias failure. Three exact-key original/bulk pairs differ in raw cents: Oracle $500.99 vs $500, Meta $33.27 vs $33 and $107.20 vs $107. Together these account for the remaining $1.46. There are no other source-amount or employer/name/entity/date/election/memo differences among the relevant matched keys. The $33,500 NVIDIA supported component is unchanged. The final reviewed correction families resolve all 18 receipt cases for this source scope; every refund-adjusted net remains unavailable.

**DelBene:** Four original-file-only direct records contribute $300 beyond bulk: Meta $100 (1903193:10494475), Microsoft $100 (1887226:10353372), and Microsoft $50 each (1903193:10477726 and 10505076). Their reported aggregates are $100, $100, $150, and $200, respectively; they are consistent with the strict greater-than-$200 bulk threshold. Eleven source-only SA12 memo allocations total $36,725: Meta $25, Microsoft $17,700, Amazon $19,000. Each must have an explicit source-backed transfer/attribution review before it enters supported components. Six legacy Microsoft memo adjustments sum to zero in three signed pairs; they must retain their individual signs and supported donor-origin relationships, not be treated as six new gifts or silently discarded. The final comparison resolves all 11 JFC allocations and all four receipt cases: Amazon $24,000, Google $3,750, Meta $4,925, Microsoft $32,450, totaling $65,125 before refunds. Meta is $4,800 in legacy bulk, versus $4,900 direct original receipts plus the separately reviewed $25 allocation: the full bridge is $125, not merely $25. No raw cent discrepancies were found in this campaign's exact-key matched records.

**Rounds:** Four original-file-only reviewed SA12 allocations add $14,000 of supported attributions, split $7,000 Anduril and $7,000 Palantir. Their aggregate transfer parents are context and are not added again. Two Google nonmemo negative source records ($3,700 and $3,300) are retained by legacy bulk but withheld by the new engine pending a verified origin; this causes a $7,000 increase in supported positive components while combined amounts remain unknown. It is not evidence of $7,000 of additional giving. Two exact-key raw precision differences add $0.59: Coinbase $143.56 vs bulk $143 and Meta $2,082.03 vs bulk $2,082. These three categories explain the diagnostic $21,000.59 component difference in the final audit snapshot. Rounds ownership is deliberately unresolved in the audit-only comparison manifest.

The official FEC bulk schema permits cents (NUMBER(14,2)); the observed source pairs establish precision differences for these records, not a general promise that bulk amounts are rounded. Reference: https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/ .

## Largest memo exposure review

The unchanged legacy selection contains 476 employer-matched memo rows totaling $81,451,521 net. This is an audit-priority measure, not proof that those rows are errors. The two largest groups were checked against full original sources and current processed inventories:

- **a16z / Leading the Future:** $50 million in four children of two $25 million organizational parents. Each pair of $12.5 million children exactly conserves its parent. Parent employers are blank; they are not also in this employer-matched numerator.
- **Musk / America PAC:** 14 filed in-kind partner-attribution children, one per explicit parent, source total $30,209,514.37 vs bulk $30,209,509. Eight exact source pairs account for the $5.37 difference. These are noncash reported attributions, not independent employee cash checks. Parent plus child would double-count the same receipt. A changed-coverage amendment was explicitly inspected; all 46 transaction rows were identical and the generic amendment validator was not relaxed.

Recommendation: retain the selected donor attributions once with their filed scope and noncash descriptions; do not blanket-exclude memos or combine organizational parents with their children. The source-backed fixtures are `tests/fixtures/fec_attribution/musk_partnership_in_kind.json` and `tests/fixtures/fec_attribution/a16z_partnership.json`; detailed local source inventories are in `outputs/audit_20260919/methodology/memo_exposure/`. No complete-cycle or refund-adjusted claim follows from these bounded source reviews.

All nonzero dollar differences in the 429 comparison rows reconcile exactly to the company bridges. No ambiguous multiple-sub-ID join, unmatched bulk-only row, employer/identity fact change, or unexplained numeric delta remains in these three scopes. Rounds still has 83 unresolved negative-origin receipt records plus the unresolved account-ownership review; the $7,000 withheld Google corrections are part of that unresolved set, not a proven increase in giving.
