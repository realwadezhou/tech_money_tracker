# FEC methodology validation — September 19, 2026

This review compares the old selected bulk records with original campaign
filings. Its purpose is to explain changed numbers, including changes caused
by uncertainty. A larger component subtotal is not automatically evidence of
more giving.

## The question we can answer

Use: **How much did this campaign report receiving and attributing to people
whose reported employer exactly matches our reviewed company aliases, within
these specified reports, before refunds?** State the committee, report coverage,
source snapshot and unresolved relationships alongside the answer.

This is narrower than all employee giving. It does not verify employment,
recover unitemized gifts, infer employers from names, or assign a donor's gift
to every candidate supported by a PAC. Net giving additionally requires a
supported refund review. The broader site remains a discovery tool until each
question meets these conditions.

## How the comparison works

The audit scanned all **31,894,597** records in the installed 2026 individual
bulk file. It found **126,663** exact curated-employer matches and **117,595**
records selected by the existing rules. Their **$249,074,477** selected sum
reproduces the existing 2026 headline. This is a reproduction check, not a
certification of the headline's underlying accounting.

The input file is 5,903,956,017 bytes, with SHA-256
`8d2c57f932a354a65f42d7b7f6031e3006cb34473eeb4ca882d0e7e70ba13f71`.
The scan preserves the employer lookup, selector and reviewed-exclusion hashes,
and verifies all three observed 2026 reviewed exclusions and their retained
original dependencies. Address-bearing raw extracts remain local audit files.

We acquired complete processed report inventories for El-Sayed, DelBene,
Rounds and Chaudhry, downloaded the originals and amendments, and compared the
latest complete report versions. The first three comparisons search all 37
curated companies, not just Nvidia. Exact committee, filing and transaction IDs
join bulk records to source records; names and amounts never create the join.
The comparison checks source facts and row membership as well as totals.

The reproducible tools are:

- `scripts/fetch_campaign_report_set.py`: acquire a dated report catalog and
  original files; record source hashes and independently selected replacements.
- `scripts/reconcile_campaign_sources.py`: scan bulk once and compare each
  reviewed report set, retaining every difference and unresolved record.
- `scripts/verify_fec_case_sources.py`: verify frozen source facts directly
  against original bytes without importing the production parser or engine.

The detailed working evidence is in `outputs/audit_20260919/methodology/`.
Tracked source cases live in `tests/fixtures/fec_attribution/`; pinned campaign
manifests live in `data/reference/attribution/`.

## Differences already traced to their records

| Scope | Old selected bulk amount | Source evidence | Explanation |
|---|---:|---:|---|
| Nvidia / El-Sayed | $33,500 | $33,500 direct receipts | The same 23 receipts remain. This does not establish refund-adjusted giving. |
| Meta / DelBene | $4,800 | $4,900 direct plus $25 JFC allocation | A $100 source receipt is absent from bulk; the $25 JFC allocation is absent from the old campaign selection. The signed election changes balance to zero. |
| Amazon / DelBene | $5,000 | $24,000 before refunds | Six JFC allocations add $19,000. |
| Microsoft / DelBene | $14,550 | $32,450 before refunds | Four JFC allocations add $17,700 and three direct receipts absent from bulk add $200. Reviewed signed changes balance to zero. |
| Google / DelBene | $3,750 | $3,750 before refunds | No difference. |
| Anduril / Rounds | $0 | $7,000 JFC allocations | Two $3,500 campaign donor allocations appear in originals, outside the old bulk campaign selection. |
| Palantir / Rounds | $0 | $7,000 JFC allocations | Two $3,500 campaign donor allocations appear in originals, outside the old bulk campaign selection. |
| Coinbase / Rounds, one exact record | $143 | $143.56 | Original and bulk amounts genuinely differ by 56 cents; the application did not lose the cents during conversion. |
| Meta / Rounds, one exact record | $2,082 | $2,082.03 | Original and bulk amounts genuinely differ by 3 cents. |
| Google / Rounds | $49,400 | $56,400 positives and $7,000 negative corrections | The correction origins are unresolved. Dropping them would create an apparent $7,000 increase; that is not an established increase in giving. |

El-Sayed also has four Palantir source receipts totaling $185 that are absent
from bulk. Their filed aggregates are $0, $0, $125 and $125, consistent with
the bulk file's narrower coverage. Three exact-ID records show another $1.46
of source-versus-bulk precision differences: Oracle $500 versus $500.99,
and Meta $33 versus $33.27 and $107 versus $107.20.

The [FEC bulk description](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/)
describes its selected itemized universe and a decimal amount field. It does
not establish a universal rounding rule. We therefore report the observed
source differences without inventing one. The itemization threshold concerns
the contributor's aggregate giving; it is not a filter that removes every
individual transaction of $200 or less.

Across DelBene's four matched employers, the old $28,100 becomes **$65,125
before refunds** in the specified full reports. The entire $37,025 bridge is
$300 in direct receipts absent from bulk plus $36,725 in JFC allocations.
All 188 source review annotations and all 61 negative Schedule A rows were
checked, including organizational memo offsets and a PAC election change that
does not belong in the individual employer calculation. The source oracle and
engine agree for each employer. The comparison establishes which records are
absent from bulk; the threshold is a plausible explanation for the small direct
receipts, not proof of why the FEC omitted each one.

DelBene's full-report coverage ends July 15. Six later F6 notices were found;
they remain outside this calculation. Itemized individual refunds total
$20,476, versus $26,705 on the report summaries. The $6,229 gap and unsupported
donor links keep every net amount unknown.

Nvidia's unchanged $33,500 was rechecked after reviewing all 57 negative memo
records in 55 source families, their 69 destination memos, and four returned
items. The reviewed before-refund amount survives those checks. Refund coverage
and employer linkage still prevent a net result.

JFC allocations answer a campaign-attribution question. Adding them to a
global donor-origin total without reconciling the original gift can count money
twice. Likewise, the aggregate campaign transfer must not be added to its donor
allocations; allocations can be gross while the transfer is net of fundraising
expenses. [FEC joint-fundraising guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/joint-fundraising-transfers/).

## The largest memo groups are not a reason to delete all memos

The 2026 bulk selection retains 476 employer-matched memo records, totaling
$81,451,521 net. We examined the two largest groups against their originals:

- **a16z / Leading the Future:** four $12.5 million donor shares describe two
  $25 million organizational receipts. The shares total $50 million once.
- **Musk / America PAC:** 14 individual attribution memos each point to one
  organizational receipt of the same amount. All describe in-kind support.
  Their original total is $30,209,514.37, versus $30,209,509 in bulk. Eight
  exact-ID precision differences explain the $5.37 difference.

The parent employers are blank; these parents are not also in the site's
employer-matched numerator. Adding parent plus child would double-count the
receipt; deleting the child would lose donor attribution. These are filed
attributions, not a ledger of ordinary employee cash checks. The remaining
memo groups are not universally certified by these two reviews.

America PAC also supplies an actual amendment that changes the report's start
date from October 1 to July 1. All 46 transaction rows are unchanged, and the
API identifies the replacement as latest. That source-specific observation is
preserved, while the generic report loader still refuses changed-period chains.
It does not silently generalize this exception to other amendments.

## Directory freshness can change the scope without changing the receipts

The final September 19 check found newer committee/candidate directory releases.
The three 2024 files were byte-identical to their installed predecessors.
The 2026 files changed, so their exact differences were saved and the 2026
legacy exports rebuilt. The individual contribution source used above did not
change. Both cycles' six source files then passed the current-release check.

One important directory change is newly linked authorized account
`C00961805`, Independents for Abdul, first filed September 17. The API returned
registration `2012835` and no financial reports. Our Nvidia result concerns
`C00902668` through July 15; it is not an all-current-accounts result. Missing
reports never establish zero giving. The 31 historical campaign-to-PAC mappings
were refreshed and their candidate relationships remained unchanged.

## Unexplained source records stay unexplained

The latest Chaudhry Q3 amendment, file `1860151`, contains 41 individual
memo/nonmemo pairs that match in every filed field except transaction ID and
memo flag. Their memo side totals $41,631.27. Twelve pairs match tracked tech
employers: $18,944.48 in the original file, versus $18,944 in bulk.

Mansuri's $3,300 example was a single nonmemo receipt in original `1823042`.
The amendment changes its transaction ID and name spelling and includes the
additional unexplained memo. This is stronger evidence of a possible repeated
original, but neither row explains the relationship. The amount is an exposure
to investigate, not a proved overcount or a justified automatic deduction.
The conservative workflow withholds an affected total. A green test cannot
supply information absent from the filing.

## What would justify publishing a number

Before using a reviewed result in a story, all of these must be true:

1. The question states one accounting basis: original giving, campaign
   attribution, receipts, refunds, or outside spending. It does not add them.
2. Account ownership and report coverage are reviewed. All applicable latest
   electronic versions are used once; unsupported paper amendments and changed
   report periods are not silently treated as ordinary replacements.
3. Every matched receipt and consequential correction has a supported role.
   Blank or different employers on corrections do not prove they are irrelevant.
4. Donor allocations retain their parents as context. Signed changes retain
   both sides. Different-donor reattribution never inherits the old donor's
   employer merely because the records are related.
5. Every change from the old number has a record-level explanation. Unresolved
   corrections appear as unresolved, not as a new higher or lower total.
6. Refund-adjusted amounts are withheld until refund linkage and coverage are
   supported. No linked refund records does not establish zero refunds.
7. The article states reported-employer matching, reporting cutoff and remaining
   limitations. A dated API check or a recent download is not all giving through
   today. Later 48-hour notices may exist beyond the latest full reports.

Passing these checks supports a reproducible, bounded interpretation of the
reported data. It cannot verify the truth of a donor's employer, the campaign's
compliance, or gifts that were never itemized.

## Verification and reproducibility

The final implementation passed **236 Python tests and 12 JavaScript tests**.
The rebuilt local site passed validation of **1,197 HTML pages and 166 JSON
files**, including the three attribution reports. Independent original-byte
checks verified the expanded source fixtures and complete Nvidia and DelBene
correction inventories. These checks establish the contracts and reviewed
answers above, not a complete historical donor ledger.

Compact comparisons and reproduction commands are tracked in
[`data/reference/attribution/validation/`](../data/reference/attribution/validation/README.md).
The larger source files, local raw extracts, independent checking scripts and
logs are preserved in `outputs/audit_20260919/methodology/`. Its reproduction
snapshot preserves the code, references and original electronic filings used
at completion; the pre-existing September 18 evidence remains intact.

The local site was rebuilt. No public deployment was performed.
