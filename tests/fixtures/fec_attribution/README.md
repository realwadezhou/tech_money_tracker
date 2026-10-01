# Source-backed FEC attribution cases

These six bounded cases are independent accounting oracles for the attribution
engine. Their expected amounts were established by inspecting actual FEC
electronic filings before implementing the engine. They are not generated from
production selection rules, and passing them does not validate every filing or
establish complete donor/candidate totals.

| Fixture | What the selected source records establish | What must not be inferred |
| --- | --- | --- |
| `thayer_jfc_allocation.json` | $25 of Thayer donor attribution to the candidate, with -$25 primary / +$25 general adjustments. The old selected individual-file rows sum to zero. | The $124,357 JFC transfer is for many donors. It is not Thayer's cash amount. This chain does not establish her original upstream gift or her share of net proceeds after expenses. |
| `a16z_partnership.json` | One $25 million partnership receipt plus two $12.5 million partner attributions. Committee cash in this excerpt is $25 million; partner attribution is also $25 million. | Adding parent and children does not establish $50 million of cash. Employer-matched partner attributions do not establish independent employee checks or a candidate contribution. |
| `vandiver_offsetting_adjustments.json` | Two distinct $3,500 nonmemo originals and balanced -$3,500 / +$3,500 memos preserve $7,000. | Equal name, date and amount do not prove two originals are duplicates. The adjustment relationship requires review because the filed transaction back-references are blank. |
| `flores_repeated_original.json` | One $6,600 original is repeated as a later explanatory memo. Keeping the original and both signed redesignation children preserves $6,600. | Reports from different periods cannot be discarded as duplicate report versions. The repeated memo can be suppressed only with its exact retained original and reviewed evidence. |
| `unresolved_same_value_memo.json` | One $3,300 nonmemo receipt and one unexplained $3,300 memo. Cash in the excerpt is $3,300; complete donor attribution is unresolved. | Neither deleting the memo nor confidently counting it as another gift is source-proven. Similarity is insufficient. |
| `nvidia_el_sayed_direct_receipts.json` | 23 exact reported-employer receipts total $33,500, with 23 explicitly linked ActBlue context memos. | The context memos are not another $33,500 of giving. 23 rows do not establish 23 people. The receipt excerpt cannot establish employment or complete net giving. |

Amounts are decimal strings in US dollars. `null` means not established by this
evidence, not zero. An `expected` total applies only to the explicitly selected
chain or excerpt. In particular, no fixture certifies a donor's giving across
all recipients, a full committee cycle total, or refunds absent from the
excerpt. `campaign_cash_receipts_in_excerpt` includes nonmemo context parents;
it is not interchangeable with employer-attributed receipts.

## Files and provenance

Each JSON contains:

- `sources`: official filing URLs, complete original-file SHA-256 hashes,
  original byte sizes and format headers, plus separately named redacted
  snippet hashes. These are historical source versions, not a live latestness
  assertion.
- `records`: address-free records in the shared engine input shape, plus
  `source_fields`, the exact selected filed strings. `record_id` is
  `file_number:transaction_id`. `source_record_number` is the one-based CSV
  logical record number in the original complete filing.
- `reviewed_context`: explicit interpretations, exact related record IDs,
  supporting source references, reasons and original row hashes. These are
  reviewer findings, not data supplied by the filer. An absent relationship
  must not be filled in by fuzzy name or amount matching.
- `expected`: literal, question-specific amounts and unresolved outcomes.
- `limits`: the bounds of the source review.

`identity_key` is empty in the source records. Where a review supplies one, it
is local to that reviewed chain. It is not a global donor identity or an
assertion that similarly named people elsewhere are the same person.
`donor_name` is a display value assembled from the original name fields.
Employer strings are as reported; no employment verification is implied.

The 11 files under `redacted_schedule_a/` are **excerpts, not original filings**.
They retain the first seven `HDR` fields and the selected 45-field Schedule A
rows. Only columns enumerated in each fixture's `field_layout.columns_one_based`
are retained; every other Schedule A field is blank. Street, city, state and
ZIP fields, the complete report form, and unrelated receipts are omitted.
The source's `X` memo flag remains in `source_fields.memo_code`; the engine
input `memo` is its Boolean translation.

Original and redacted hashes have different purposes:

- `source_sha256`: SHA-256 of the original complete `.fec` response bytes.
- `source_row_sha256`: SHA-256 of the complete original parsed 45-field list,
  serialized as `json.dumps(row, ensure_ascii=False, separators=(',', ':'))`
  and encoded as UTF-8. This binds source reviews to all original fields
  without publishing contributor addresses. Strings, including amount
  formatting, are preserved exactly.
- `redacted_snippet_sha256`: SHA-256 of the committed excerpt bytes.
- `redacted_row_sha256`: the same canonical list hash applied to the redacted
  45-field row. It is deliberately not used as an original-source review hash.

Do not substitute a redacted hash for the original source hash when applying
reviews to downloaded data. Original file or reviewed row changes require
fresh adjudication, not automatically regenerated expectations.

## Layout and source interpretation

The literal column positions were checked against the official
[Schedule A field specification](https://fecgov.github.io/fecfile-validate/SchA_spec.html)
and the FEC's
[electronic filing workbook](https://docquery.fec.gov/formatspecs/FEC_EFO_Format_Specifications.xlsx).
The reviewed workbook is version 8.5 and its hash is pinned in each fixture.
The historical source headers declare 8.4 or 8.5; the selected Schedule A
columns have the same observed layout in these reviewed records. This does
not authorize applying that layout to arbitrary versions or other schedules.
The separator is ASCII 28, described by the FEC's
[data conversion documentation](https://www.fec.gov/help-candidates-and-committees/filing-reports/data-conversion-tools/).

Parse with `csv.reader(..., delimiter='\x1c')` using real newline handling.
Do not call `str.splitlines()` first: Python treats ASCII 28 as a line boundary
and would destroy the records.

The FEC's [joint fundraising reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/joint-fundraising-transfers/)
distinguishes the transfer from the memo entries identifying donor allocations.
The [partnership reporting guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/partnership-contributions/)
distinguishes the partnership contribution from its partner attributions.
Those distinctions explain the different cash and attribution expectations;
they do not justify adding every memo or deleting every memo.

Two filed irregularities are intentionally preserved:

- Thayer transaction `10732455` references `10732436` by ID but labels its
  back-reference schedule `SA11AI`; the target is filed on `SA12`. The reviewed
  connection uses the exact record IDs and records the discrepancy.
- Vandiver's memo text calls the change reattribution, while the donor/employer
  stay the same and the election changes from primary to general. No source
  transaction back-reference is present. The reviewed interpretation must
  not be presented as a literal filed relationship.

For the unresolved Mansuri pair, the reviewed
[November 13 correspondence](https://docquery.fec.gov/pdf/982/202411130300227982/202411130300227982.pdf)
addresses Schedule B reimbursements. It does not explain this Schedule A pair.
The filing's cash summary excludes memos, but that does not resolve the memo's
donor-attribution role.

## Nvidia scope and refunds

The Nvidia case preserves 23 receipts and their 23 exact conduit links from
five of the six latest reports in the saved inventory. The sixth latest report
has no matching receipts, so it has no Schedule A excerpt. `report_chain_inventory`
contains the non-address metadata and source hashes of all 18 saved report
versions, including the six selected latest versions. The known coverage ends
July 15, 2026. No assertion is made that this historical catalog is exhaustive
or still current.

The case also retains an amendment counterexample: transaction `7982813`
reports employer `Nvda` in earlier versions and `Nurse` in the latest saved
version. The latest receipt is excluded from the $33,500 employer total.
Old and amended records must not be unioned or treated as independent gifts.

The six full reports contain $61,682.15 of itemized Schedule B individual
refunds and $68,392.39 in the corresponding report summary. The $6,710.24
difference is a reconciliation limit, not a correction to Nvidia receipts.
Those refunds are not included in this receipt-only fixture and have not been
reliably linked to all individual donors. A true net Nvidia total remains
unestablished; a test that only adds these positive receipts cannot prove it.

## Rechecking the evidence

Offline tests can compare normalized records with the committed redacted rows
and verify their redacted hashes. They must not require ignored `outputs/`
files or network access.

A source integrity review additionally downloads each exact official URL (or
uses an existing original-file cache), verifies `source_sha256`, parses the
filing, locates each cited logical record and transaction ID, verifies
`source_row_sha256`, and compares all fields enumerated in `source_fields`.
The original full rows must remain outside the tracked address-free corpus.
That check was performed against the saved official files when these fixtures
were created. It establishes faithful transcription, not universal accounting
correctness.

Useful adversarial checks include removing a required parent/original,
altering a pinned amount, breaking an explicit link, supplying only half a
balanced adjustment, keeping two equal legitimate originals, mixing report
versions, and retaining an unexplained memo. Failures should produce an
unresolved result or explicit error, never an invented relationship or a
confident zero.

## Additional September 19 cases

Two further fixtures retain address-free normalized records and full original
file/row hashes. They do not use the older `source_fields` and redacted-excerpt
schema described above:

- `rounds_cross_source_cases.json`: 13 records covering four JFC allocations,
  their three parent transfers, two exact bulk/source cents differences, two
  unresolved negative Google corrections, and a reverse-link WinRed conduit
  example. The allocations are $7,000 each for Anduril and Palantir; this is
  not a complete Rounds total.
- `musk_partnership_in_kind.json`: 28 records in 14 parent/individual-memo
  families. Count $30,209,514.37 once as filed in-kind attribution. Eight exact
  source/bulk differences explain $5.37. The retained changed-period amendment
  example must continue to fail generic report-chain selection; its reviewed
  unchanged transaction rows do not justify a universal exception.

The combined corpus contains 104 normalized source records. The original six
fixtures still have their 11 redacted excerpts; the additional 41 records can
be checked independently against their original files using:

```powershell
python -m scripts.verify_fec_case_sources --case tests/fixtures/fec_attribution/rounds_cross_source_cases.json --source-dir PATH_TO_ROUNDS_ORIGINALS
python -m scripts.verify_fec_case_sources --case tests/fixtures/fec_attribution/musk_partnership_in_kind.json --source-dir PATH_TO_AMERICA_PAC_ORIGINALS
```

This checker imports no production parser or accounting engine. Full files and
source identity comparisons stay local; contributor addresses are not exported.
