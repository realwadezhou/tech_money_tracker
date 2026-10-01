# Audit and data refresh — September 7, 2026

Reviewed the active FEC loaders, counting and classification logic, exports, static-site generator, browser scripts, source-refresh handling, curated alias integrity, and the dormant LDA ingestion/reconciliation path. Fixed the issues below and rebuilt the 2024/2026 public-site datasets from the current available FEC releases.

## Refreshed data

| Cycle | Previous latest transaction | Current latest transaction | Previous tech-linked total | Current tech-linked total | Matched donor names |
|---|---|---|---:|---:|---:|
| 2024 | 2024-12-31 | 2024-12-31 | $421,446,050 | $421,446,050 | 31,306 |
| 2026 | 2026-04-25 | 2026-08-27 | $207,142,915 | $247,949,117 | 15,406 |

The transaction date is the latest valid date among matched tech contributions; it is not a claim that all filings through that date are complete. Source manifests record the download release dates separately. All six required FEC source files per cycle were checked; newer files were downloaded. The 2024 transaction archives were already current, while its committee/candidate directories changed. The 2026 refresh included newer transaction archives.

## Bugs fixed

- **Candidate attribution:** restricted campaign-account receipts to principal/authorized committees plus FEC-confirmed former campaign accounts, excluded other joint fundraising/leadership/unauthorized affiliations, resolved shared campaign ownership using the committee master, and counted each donor name once per candidate across included accounts. Verified 25 historical conversions for 2024 and 30 for 2026 against official FEC notices. This preserves historical accounts that the latest FEC directories now label as PACs. Their cycle totals can include activity after conversion; account attribution does not set a PAC's partisan label. Broader affiliations remain visible as metadata.
- **Partisan classification:** removed unsupported direct PAC party labels, excluded independent-expenditure memo rows from behavioral evidence, included off-cycle candidate party evidence, and avoided arbitrary attribution of shared committees. Refund-distorted party ratios are undefined instead of producing misleading directional labels.
- **Amounts and percentages:** preserved undated contributions in totals, kept refund outflows out of gross-positive receipts, retained recipient rows with missing metadata, distinguished committees with identical names by ID, and suppressed invalid Tech Share percentages while preserving signed dollar amounts. Actual snapshots contained Tech Share values as high as 7,929% before the correction.
- **Website behavior:** preserved excluded-candidate flags, represented missing chart weeks at their proper spacing, handled negative/zero/single-point chart series, and allowed table searches across column boundaries. Browser testing exposed stale cached JavaScript after rebuilds; generated styles and scripts now use content-hashed URLs so changed assets refresh correctly.
- **Refresh reliability:** rejected incomplete or non-ZIP FEC downloads before replacing existing archives; validated expected extracted files before replacing existing datasets; handled HTTP header casing correctly. LDA refreshes now support staged replacement with UUID checks and preserve recovered records during reconciliation.
- **Explanations and maintenance:** corrected descriptions of employer matching, campaign receipts, outside spending, small itemized gifts, donor-name grouping, and percentage denominators. Added dependency setup, regression tests, a CI workflow, and a site validator. Reduced peak export memory by releasing obsolete frames and parsing dates only for charted tech rows.

Rules were checked against the [FEC linkage documentation](https://www.fec.gov/campaign-finance-data/candidate-committee-linkage-file-description/) and [committee type definitions](https://www.fec.gov/campaign-finance-data/committee-type-code-descriptions/). Downloads come from the [FEC bulk-data service](https://www.fec.gov/data/browse-data/?tab=bulk-data).

## Verification

- 52 Python regression tests and 5 JavaScript tests pass.

- Generated HTML links, strict JSON, company detail/chart totals, headline totals, weekly cumulative totals, source status, and percentage bounds pass validation.
- Checked 1,192 HTML pages and 160 JSON files.
- Reviewed the rebuilt homepage, company chart, candidate navigation, and table filtering/sorting in the local browser.
- Candidate/state/House/Senate totals reconcile for both cycles, all historical conversions are attributed once, and all four candidate CSV pairs match between analytical and site exports.
- Compared the memory-optimized 2026 rebuild against its preliminary full export: company totals, donor amounts, weekly charts, and committee dollars remained unchanged; the intended invalid-share corrections were isolated.
- Curated lookup integrity: 447 included aliases across 37 companies, with no conflicting normalized aliases or empty included employer keys. The reviewed alias mappings were retained.
- Runtime used: Python 3.12.7, pandas 2.2.3, NumPy 1.26.4. Some pandas future-compatibility warnings remain; this run used pandas 2.2.3, and requirements.txt limits future installs to pandas 2.x.

## Remaining scope and limits

The exploratory LDA snapshots remain dated April 9, 2026. They are not used by the public site. Their refresh bugs were repaired and live counts checked, but a full refresh of both years (about 8,955 API pages at the observed 25-row limit) was not performed. The [current official download guidance](https://lda.gov/api/) points to the REST API; the [legacy Senate XML archive](https://www.senate.gov/legislative/Public_Disclosure/database_download.htm) stops at 2022 Q1. No current bulk replacement was found. See [LDA status and refresh instructions](../data/lda/README.md).

Employer coverage and donor identity still depend on curated aliases and exact reported names. New alias review, individual identity consolidation, and lobbying integration remain separate work.

The refreshed site is in `docs/`, with matching local analytical/export datasets. It has not been committed, pushed, or published. The pre-existing working-tree site was preserved in `outputs/audit_20260907T223013Z/docs_before.zip`. Build logs, test logs, numerical comparisons, and validation output are in the same audit directory.
