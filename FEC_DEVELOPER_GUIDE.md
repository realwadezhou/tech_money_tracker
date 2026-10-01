# A developer's guide to the FEC data in Tech Money

Reviewed September 19, 2026. This guide describes what the code does, which
questions it can answer, and where a reporter still needs to inspect filings.
To reproduce a build, preserve its raw inputs, reviewed reference tables,
committee mappings, and code revision together with the source manifests.
Release timestamps alone do not freeze an input dataset.

**Current status:** this is a guide to the implemented rules, not certification
that those rules completely answer an arbitrary employee-to-candidate question.
Read the [confidence review](FEC_CONFIDENCE_REVIEW.md) first. Passing regression
tests establishes specified behavior; bulk/API agreement establishes consistency
between two processed FEC products. Neither establishes a complete accounting
of underlying gifts. Source-filing checks support only the cases actually
reviewed.

The [September 19 methodology validation](FEC_METHODOLOGY_VALIDATION.md) records
the broader campaign comparisons, explains old-versus-new differences, and
sets out the checks required before quoting a number.

There are now two calculation paths. The site's broad company and candidate
totals still use the legacy bulk selected-record rules described below. A new
**reviewed campaign-attribution report** reads original electronic filings,
preserves their relationships, and answers a deliberately bounded question.
It supports pinned campaign report sets and explicitly limited source examples.
It does not replace or certify every site total.

## Start by defining the question

“How much did Nvidia employees give to candidate X?” has several possible
meanings. Before writing a query, agree on these choices:

| Choice | What this project can measure | What that does not establish |
|---|---|---|
| Who gave? | Records whose reported employer exactly matches a reviewed Nvidia alias | That every contributor is a verified employee, or that all employees were found |
| Who received it? | Specified campaign committee IDs | Every committee associated with, supporting, or benefiting the candidate |
| When? | A declared cycle and report set; the legacy extract can also be narrowed by transaction date | The election for which a gift was designated, or when the FEC first received the report |
| Which money? | Legacy selected records, or a separately reviewed campaign-attribution report with explicit components | All campaign receipts, unitemized gifts, loans, or outside spending |
| Gross or net? | Supported attribution before refunds; a net figure only where refund coverage and donor relationships support it | A fully identity-resolved refund-adjusted total merely because selected refunds sum to zero |
| How current? | An identified source release checked at a stated time | Complete reporting through the latest transaction date |

A careful sentence is: “In the specified FEC snapshot, records reporting an
employer matched to Nvidia show $X in selected itemized receipts at these
campaign accounts, under the project's counting rules.” Name the accounts,
period, source date, and material exclusions alongside the number.

Do not shorten this to “Nvidia gave $X.” The employer is not the contributor,
and this measure is not company treasury or company PAC spending. Some source
records describe candidates, organizations, partnerships, or trusts rather than
ordinary employees. If a story requires natural persons only, that is an
additional documented filter; reconcile the change against the site's broader
employer-matched measure.

## Use the reviewed campaign workflow for a specific reporting question

The new reports show direct receipts, joint-fundraising allocations, partnership
attributions, signed adjustments, and refunds separately. Their combined amount
is supported **attribution before refunds within the stated scope**. It is not
all original giving across politics or necessarily cash: receipt lines can
include in-kind contributions. A parent transfer or partnership receipt is
context for the donor allocations, not another amount to add to them.

Three reviewed manifests are included:

| Manifest | Supported result | Remaining limit |
|---|---|---|
| [`nvidia_el_sayed.json`](data/reference/attribution/nvidia_el_sayed.json) | 23 reported-employer receipts, $33,500, in six latest campaign reports covering January 1, 2025–July 15, 2026 | Refunds cannot be fully attributed; net is `null` |
| [`meta_delbene_full_reports.json`](data/reference/attribution/meta_delbene_full_reports.json) | $4,925 before refunds: $4,900 direct receipts plus $25 JFC allocation, in seven full reports through July 15, 2026 | Later F6 notices are outside scope; refunds and net are `null` |
| [`meta_delbene_reviewed_allocation.json`](data/reference/attribution/meta_delbene_reviewed_allocation.json) | $25 for the specified Thayer allocation and its balanced adjustments | Four source records only, not all Meta giving to DelBene; refunds and net are `null` |

From the project root, reproduce the reports with:

```powershell
python -m scripts.build_candidate_attribution --manifest data/reference/attribution/nvidia_el_sayed.json --check-current
python -m scripts.build_candidate_attribution --manifest data/reference/attribution/meta_delbene_full_reports.json --check-current
python -m scripts.build_candidate_attribution --manifest data/reference/attribution/meta_delbene_reviewed_allocation.json
```

`--check-current` uses the locally configured `OPENFEC_API_KEY`. It compares the
complete processed report inventory and latest-version IDs with the reviewed
manifest. A new report or amendment stops the build for review; it is not
silently added. This option is available for `campaign_reports` scope only.
A `reviewed_record_group`, such as Thayer, cannot establish whole-account
coverage and must not be labeled a complete current candidate total.
The $25 allocation example is contained in the $4,925 full-report result; do not
add the two reports together.

Without that option, the command reproduces the pinned source versions without
claiming a fresh API check. It reads cached originals or downloads their exact
filing IDs, verifies their hashes, and writes
`exports/site/2026/attribution/<report_id>.json` only after validation succeeds.
Use `--source-dir PATH` for an existing original-file directory, `--cache PATH`
for a different cache, or `--output PATH` for another JSON destination.
`python -m frontend.build_site` renders the available reports in the local
site. These commands do not deploy the public site.

Read `combined_amount`, `net_amount`, their statuses, the component table,
`unresolved`, and `scope` together. `null` means the claim is not established;
it is not zero. A component can be supported while the combined amount is
withheld because another relevant receipt is unresolved. A supported combined
amount can coexist with an unknown net because refunds remain unlinked or
itemized refunds do not reconcile to the report summary.
The current workflow also withholds a combined amount for gaps in full-report
coverage or more than one campaign account. Reviewing ownership does not, by
itself, reconcile the same donor money across multiple accounts.

The Nvidia inventory was checked on **September 19, 2026 at 10:12:59 UTC**:
OpenFEC still returned the same 18 report versions and six latest reports.
The repeat check at **10:15:00 UTC** was also unchanged. Their coverage still
ends July 15. The September 18 saved catalog date, the September 19 API checks,
report coverage, transaction dates, and JSON generation time are different
facts. The processed API can lag newly filed documents, so
an unchanged inventory is not proof that no newer unprocessed filing exists.
The FEC distinguishes these [processing stages](https://www.fec.gov/data/filings/).

To add another case, start with a new manifest under
`data/reference/attribution/` and an independently reviewed fixture under
[`tests/fixtures/fec_attribution/`](tests/fixtures/fec_attribution/README.md):

1. Define the candidate, account IDs and ownership evidence, employer aliases,
   report coverage, and exact question. Choose a complete supplied report set
   (`campaign_reports`) or an explicitly listed subset (`reviewed_record_group`
   with `selected_record_ids`). Do not turn an example into a campaign total.
2. Preserve all known originals and amendments for the chosen chains. Pin each
   full source hash in `sources` and record `latest_file_numbers`. The loader
   selects replacement versions by original report ID and amendment sequence;
   it refuses missing sequences, changed report periods, overlapping reports,
   or unsupported forms rather than guessing.
3. Keep filed facts separate from interpretations. An annotation in `reviews`
   names an exact record, its original full-row hash, role, related record,
   source evidence, and rationale. It cannot overwrite filed amounts, employers,
   or dates. Every reviewed row and required parent must remain present and
   unchanged; this preserves all children needed by a reviewed adjustment.
4. Check memo relationships, receipt-summary reconciliation and refunds.
   Joint-fundraising classification requires an explicit reviewed role; the
   existence of an individual memo under a transfer is insufficient. The code
   recognizes other supported explicit links, but unexplained adjustments and
   unmatched refund identities still need review. Equal names or amounts do
   not establish a duplicate gift or a donor identity.
5. Compare the report with an expected result established from the source
   records, including a nearby counterexample. Preserve the manifest, original
   inputs, aliases, code and output together. New records, amended contents or
   changed aliases require a new review and an explained comparison.

The tracked source cases cover JFC allocations, partner attributions,
legitimate equal-value receipts, a repeated original, an unexplained memo, and
Nvidia receipts with conduit context. Additional Rounds and Musk cases preserve
real differences in conduit link direction, original-file precision, in-kind
attribution, unresolved negative corrections and changed report periods.
Their invariants are deliberately narrow:
context must not add a second gift; signed redesignations preserve the reviewed
amount; two legitimate originals remain two; a missing or changed dependency
cannot silently preserve an old conclusion. These tests do not settle unknown
relationships elsewhere.

## Corrections need a donor origin, not just an employer filter

A correction can omit the employer or use a different employer from the
original gift. Filtering those rows out before resolving their relationships
can produce a confident but inflated number. The engine now keeps consequential
unresolved negative receipt and allocation corrections visible even when their
own employer does not match the company being investigated.

There are three different operations:

- An election redesignation moves the same donor's money between elections.
  Keep the negative and positive entries; the total across elections normally
  stays the same.
- A reattribution moves attribution to another donor. A reviewed positive
  `reattribution` uses the new donor's own reported employer. It must never
  inherit the old donor's employer merely because it references the old gift.
  The engine requires distinct reviewed identities, a balanced negative family
  and adequate original amount. A missing or contradictory family stays
  unresolved.
- A returned item reverses a receipt that did not clear. It may appear as a
  negative Schedule A receipt, not a Schedule B refund. Preserve its sign and
  trace its original. Later replacement receipts remain separate receipts.

See the FEC's [redesignation and reattribution examples](https://www.fec.gov/help-candidates-and-committees/filing-reports/redesignating-and-reattributing-contributions/)
and [bounced-check guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/bounced-checks/).

A filed transaction reference is evidence of a relationship, not automatically
evidence of its accounting meaning. The engine follows unclassified references
to propagate uncertainty. It does not merge all donors sharing an ActBlue,
WinRed or JFC context record into one person. An unresolved parent also keeps a
dependent allocation unresolved.

Some reviewed negative records have no filed back-reference. In those cases,
the manual source review examines the full family, the positive child's filed
reference, exact reported identity, dates, election and signed amounts. Each
adopted decision pins every required row's original hash and explains the
inference. These are frozen, case-specific decisions; there is no general
name/amount deduplication rule. Exact identity fingerprints record what was
compared without publishing contributor addresses. They do not verify a
person's real-world identity or employment.

Unexplained organizational corrections need review too: an organizational
receipt can have individual partnership attributions. A generic context label
must not conceal a correction that could reverse those attributions. Only the
explicitly reviewed, balanced organizational memo families supported by the
engine can be treated as organizational context without adding employee money.

Conduit links also run in both directions. An individual receipt can reference
its ActBlue context, or a WinRed memo can reference the individual receipt.
The latter requires explicit filed conduit language and matching source
structure. An arbitrary organizational memo is not automatically a conduit.

## Where the records come from

The [FEC bulk download page](https://www.fec.gov/data/browse-data/?tab=bulk-data)
publishes files on schedules that vary from daily to weekly. Our ingest downloads
the cycle's files, checks the archives and expected members, and records source
release timestamps. It does not scrape the displayed contribution tables.

| File | Main use here | Join or interpretation to get right |
|---|---|---|
| `indivYY/itcont.txt` | Employer-matched contribution records | The recipient is `cmte_id`, not a candidate ID |
| `cmYY/cm.txt` | Committee directory | One committee ID must not multiply a transaction during a join |
| `cnYY/cn.txt` | Candidate directory | Use candidate ID and election year, not a name substring |
| `cclYY/ccl.txt` | Candidate-to-committee relationships | A relationship does not automatically mean campaign receipts |
| `othYY/itoth.txt` | Committee transactions and outside spending | Different money flows; never append wholesale to donor receipts |
| `pas2YY/itpas2.txt` | Candidate-facing committee spending evidence | Overlaps `itoth`; adding both can duplicate the same spending |

These are pipe-delimited files without headers. Their field layouts differ;
in particular `itpas2` has a separate candidate-ID field. Keep IDs as strings.
Never infer a field position from a different file's layout.

The individual-contribution file is a selected subset of itemized records,
not a census of all gifts. The FEC describes its modern inclusion thresholds
in the [individual file documentation](https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/).
Do not apply a new `amount > 200` filter: smaller transactions can be present
because a contributor's aggregate exceeds the reporting threshold.

## Candidate IDs and committee IDs solve different problems

Resolve the person and office first, then the accounts. The
[candidate–committee linkage specification](https://www.fec.gov/campaign-finance-data/candidate-committee-linkage-file-description/)
distinguishes the candidate's election year from the active two-year FEC period.
It also distinguishes principal and authorized campaign accounts from joint
fundraisers, leadership PACs, and unauthorized committees.

Our campaign-account selector uses committee types `H`, `S`, or `P` and
designations `P` or `A`. It consults the committee master to resolve shared
links, removes repeated links, and leaves unresolved multiple ownership
unattributed. A naive transaction-to-linkage merge can duplicate every dollar
when one committee is linked to multiple candidates.

This resolves duplicate allocation, not historical ownership on each receipt
date. If a campaign account changes candidates, this code assigns its selected
cycle receipts using the committee master's candidate association; it does not
split them at the handover date. The rebuilt 2024 presidential page illustrates
the consequence: the shared account is attributed to Harris, while Biden has
zero assigned account receipts. That zero is not evidence that nobody gave to
Biden's campaign. A story about giving before or after a candidate change needs
dated account history and a separate transaction-level allocation.

The site also preserves FEC-confirmed **former campaign accounts**. A committee
can later become a PAC while keeping its ID, and the current directory can show
that later status even for an older cycle. The supplement under
`data/reference/committees/` establishes historical association. It does **not**
provide a transaction-level conversion-date cutoff.

Consequently, combined associated-account totals may contain post-conversion
PAC activity. They are account receipts, not a proven amount received by the
campaign before conversion. Keep current campaign-account and former-account
components separate. Neither dropping every former account nor treating every
former-account receipt as campaign money solves the historical question.

The candidate pages select candidates whose candidate-master election year
equals the displayed cycle. A person running for a later Senate election can
still receive money during the current two-year period. Their absence from a
cycle's candidate directory view does not prove that their committee received
zero. Use an explicit candidate/account investigation for that question.

The candidate CSV's `tech_itemized_receipts` combines **all** tracked companies.
`tech_company_tags` only says which companies occur; filtering a candidate row
for the word Nvidia does not isolate Nvidia dollars. Company JSON contains
only the top 50 recipient committees and top 50 donor names. Absence from those
lists does not mean zero receipts.

For a legacy company-to-candidate extract, work from the retained transaction
rows below. This is evidence for investigation; use the reviewed campaign
workflow above when establishing a source-backed attribution claim.

1. Select the canonical company (`nvidia`) using the reviewed alias lookup.
2. Resolve the candidate's exact current campaign committee IDs; retain former
   accounts as a separate component and explain the historical-date limitation.
3. Filter by those recipient IDs without multiplying rows through linkage.
4. Apply the declared date scope, retaining an explicit count/amount for
   unknown dates rather than silently treating them as zero.
5. Sum signed `net_amt`, and separately inspect included refunds, excluded
   memos, nonindividual rows, and evidence of routed gifts.
6. Save the contributing rows and their source IDs with the answer, then
   compare representative rows against the underlying official filings.

## Employer matching and donor identity

`data/reference/companies/curated.csv` is the legacy production employer lookup.
The loader keeps rows marked `TRUE`, trims and uppercases employer text, then
matches exactly. Candidate search patterns and fuzzy review queues do not
automatically admit donors into totals.
The new campaign manifests freeze their own explicit `employer_aliases` map;
changing the shared company table does not silently alter a reviewed report.

This is a per-record rule. If a contributor reports Nvidia on one gift and
“retired” on another, only the matched record enters the Nvidia amount.
It is not a rule assigning all of that person's lifetime giving to Nvidia.
Blank employers, misspellings, job changes, and missing aliases affect coverage.
Ambiguous aliases can also create false positives, so the result is not a
mathematically guaranteed lower bound.

Donor counts use exact reported name strings. Two people with the same name can
be combined, and one person with several spellings can be split. The separate
individual identity table is not wired into the production pipeline. Do not
call these counts verified unique people. Within a candidate, a repeated name
across included accounts is counted once; sums across candidates are
donor–candidate pairs, not unique people across a state or district. Counts can
include names with zero or negative net receipts. Contribution counts count
retained records, including adjustments, rather than verified distinct gifts.

## Contributions, refunds, memos, and intermediaries

There is no single contribution filter that answers every money question.
Distinguish three measures before changing an allowlist:

- **A donor's original giving across politics:** count an originating gift once,
  without adding subsequent transfers or allocations of that gift.
- **Giving attributed to a particular campaign:** include the donor's supported
  share of joint fundraising and partnership gifts where applicable. Those
  attribution records can be needed even though they are not new cash receipts.
- **Cash received by an account:** follow reported receipts and transfers;
  allocation memos explain the source of that money rather than add more cash.

Our legacy common selected-record pool does not fully implement these as separate,
reconciled measures. In particular, excluding joint-fundraising allocations
from the overall pool cannot also establish complete candidate attribution.
Do not repair that by appending every allocation to every total. Preserve the
source relationship and state which perspective each output represents.

The authoritative meanings are in the
[FEC transaction codebook](https://www.fec.gov/campaign-finance-data/transaction-type-code-descriptions/).
Our actual allowlists live in `pipeline/fec/load.py`. Read those constants
before changing a filter; the exploratory notebook is not the rulebook.

The legacy receipt allowlist is `10`, `11`, `15`, `15C`, `15E`,
`30`, `31`, `32`, `30E`, `31E`, `32E`, `30T`, `31T`, and `32T`.
The special-account `T` suffix here identifies tribal receipts, not a generic
conduit transfer. Included refund types are `21Y`, `22Y`, `40Y`, `40T`,
`41Y`, `41T`, `42Y`, and `42T`.

For an ordinary receipt, retain the signed amount. For a refund type, negate
the filed amount: a positive refund reduces net receipts; a negative refund
reversal increases them. Do not take absolute values. Do not throw away all
negative rows. Negative adjustments can be necessary to offset an earlier
attribution or designation.

An internal field called `gross_positive` sums positive **net** amounts. It
includes refund reversals, so it is not conventional gross new donations.
For example, a $100 receipt, $30 refund, and $10 refund reversal produce $80 net
and $110 `gross_positive`. Use a separately defined measure if a story asks for
gross donations before refunds.

The September audit corrected `41Y` and `42Y`, which had incorrectly been
treated as incoming money, and added the missing special-account refund types.
This affected committee receipt denominators. The affected snapshots' refund
rows had no matched employers, so the correction did not reduce company
numerators. That difference exposes a real limitation: a blank-employer refund
cannot be assigned to Nvidia merely because a same-name contribution reported
Nvidia. A reliable assignment requires transaction or identity evidence.

`MEMO_CD = X` is not a universal “ignore this row” instruction. The FEC bulk
file guidance explicitly warns that memo items can belong in analysis.
The project generally retains memos for included receipt types except `15E`,
subject to separately documented source-reviewed duplicate exclusions.
The `15E` exception is a conservative, unresolved routing assumption.

Why unresolved? An earmarked gift may appear at both an intermediary and the
destination. Memo rows also include summaries, redesignations, attributions,
and in-kind activity. In the September audit, some individual `15E` memos
matched money already represented upstream. Adding all of them would
double-count; excluding all may omit destination-specific evidence. The
excluded amount is therefore an **exposure to investigate**, not an estimate
of missing donations.

For a report about a specific candidate, inspect excluded memo rows and the
corresponding intermediary and destination filings. An intermediary's receipt
is not automatically a direct campaign receipt. Joint fundraising allocations
need their own reconciliation; the project does not fully trace each original
donor dollar through every transfer. Never add the donor's original gift,
the joint fundraiser's transfer, and the destination's attribution as three
independent gifts.

This limitation has a concrete source-backed counterexample. DelBene's filing
1920909 contains a $25 Meta-employer donor allocation (type `15J`) followed by
-$25 primary / +$25 general adjustments. The selected `itcont` adjustments
sum to zero, while the filed candidate attribution for that chain is $25.
The legacy common selector therefore cannot be treated as complete candidate
attribution. The new reviewed campaign report correctly preserves $25 for this
four-record chain, while leaving the legacy selector unchanged. It does not
add $25 to global origin-giving totals. See the tracked
[source fixture](tests/fixtures/fec_attribution/thayer_jfc_allocation.json) and
the earlier [offline comparison](outputs/audit_20260918/fec_confidence_followup/README.md).

There is a separate partnership pitfall. A committee can report one receipt
from a partnership and memo records allocating that same money to partners.
In the September 2026 source review, two $25 million partnership gifts to
`C00916114` each had two $12.5 million partner memos. The partner records were
the employer-matched attribution; the partnership parent had no matched
employer. The tech attribution was $50 million across both gifts, but summing
parents and memos produced $100 million in that part of the all-records pool.

Consequently, a raw selected-record sum is not always unique money received.
We retain a separate nonmemo diagnostic sum and withhold Tech Share where
retained nonzero memo activity needs reconciliation. Do not treat the diagnostic
as a universally repaired denominator: dropping memos would also discard valid
partner attribution. The
[FEC partnership reporting example](https://www.fec.gov/help-candidates-and-committees/filing-reports/partnership-contributions/)
explains the parent/partner reporting structure.

Read these diagnostic fields together:

| Field | Meaning |
|---|---|
| `selected_record_net_total` | Signed sum of selected records; compatibility fields `total_receipts` and `total_itemized_receipts` carry the same sum. It can include overlapping parent receipts and memo attributions. |
| `nonmemo_receipt_net_total` | Signed sum of selected nonmemo records. This is a diagnostic, not an independently reconciled or complete receipt total. |
| `memo_receipt_net_total` | Signed sum of retained memo records; an offsetting pair can make this zero without resolving the underlying events. |
| `memo_receipt_record_count` | Number of retained memo records, including zero-amount records. |
| `has_unreconciled_memo_attributions` | True when any retained memo record has a nonzero amount. This triggers conservative suppression. |
| `tech_share_unavailable_reason` | Committee-export reason: `unreconciled_memo_attributions` or `invalid_net_receipts`; empty for an available share. |

Committee `tech_pct` and candidate `tech_pct_itemized_receipts` are unavailable
when the memo-risk flag is true. Current and former campaign components have
their own prefixed flags, and state, House-district, and Senate summaries carry
the risk from any constituent candidate. The site displays affected all-record
totals as **Unreconciled**, while downloadable diagnostics retain the record
sums. Tech-attributed amounts remain visible and require their own source
review; withholding a denominator does not certify every numerator record.
The tech-dominated flag requires an available share strictly greater than 50%.

## Amendments and duplicate detection

Use one coherent processed source snapshot as the base. Do not sum original
reports and every amendment, and do not append a second download to the first.
Do not assume `AMNDT_IND = A` means “discard”: the amended version can contain
the operative record.

The legacy production loader relies on the supplied FEC bulk snapshot's processing;
it does not independently reconstruct amendment chains or deduplicate `SUB_ID`.
The investigation script and source-level spot checks are additional evidence,
not a claim that the entire historical dataset has been independently adjudicated.
The new source-report loader reconstructs replacement chains within its pinned
input inventory. It does not establish that the inventory contains every report
ever filed, and it does not support paper amendments or arbitrary older formats.

`SUB_ID` identifies a source row. `TRAN_ID` is only meaningful in its reporting
context; it is not a universal ID for a real-world gift. Two rows with the same
name, date, and amount may be separate legitimate gifts. Conversely, different
row IDs may represent the same money at different stages of routing. A
`drop_duplicates(name, date, amount)` operation cannot resolve either problem.

A later memo can also restate the original contribution before showing its
redesignation children. Source-reviewed exclusions must identify that precise
record and preserve the offsetting children. Keep the original source IDs,
expected record contents, filing evidence, and reason for each reviewed
exclusion. If later source data change those contents, fail for review rather
than applying a stale exception to a different record.

The exact reviewed decisions are in
[`data/reference/transactions/reviewed_exclusions.json`](data/reference/transactions/reviewed_exclusions.json),
with the contract explained in its
[README](data/reference/transactions/README.md). The shared
[`pipeline/fec/transaction_reviews.py`](pipeline/fec/transaction_reviews.py)
helper is used by the core loader, narrow candidate rebuild, and candidate
audit. It checks cycle, source filename, source-row ID, reviewed fields, and a
hash of all 21 source fields. Each complete source scan also verifies that an
observed excluded record's retained original occurs exactly once and matches
its own full source signature and hash. Chunked paths accumulate this check
across the file before returning data or writing outputs; an original can
precede or follow its repeated memo. Missing, duplicated, or changed originals
stop the run for review. These checks do not discover duplicates or
automatically follow a reviewed event to a new row ID.

The September 18 `v3` reference contains ten reviewed repeats: seven in 2024
remove $39,736 from selected tech totals, and three in 2026 remove $15,500.
Their original receipts and signed adjustment children remain. These are
specific reviewed corrections, not evidence that every other memo is correct
or incorrect.

One unresolved example is committee `C00876383`: 12 same-filing memo/nonmemo
pairs have identical reported details apart from transaction ID and memo flag.
The memo side totals $18,944 in bulk ($18,944.48 in the original filing), but
the filing does not explain the relationships.
Those records remain selected; the amount is an exposure to investigate, not
a proven overcount. Do not quote this committee's employer totals as distinct
gifts without further evidence. See the September audit for the bounded review
and the examples where signed adjustments correctly cancel.

Preserve the source's precision when comparing records. In one reviewed case,
the electronic filing reports $3,436.02 and the bulk record reports $3,436.
The site's correction uses the bulk amount. A difference of cents across
products does not by itself prove an extra or missing contribution.

Keep committee, filing, transaction, memo, and source-row identifiers in audit
extracts. For a suspicious group, read the underlying filing and its amendment
chain. Reconciliation to a headline total alone cannot reveal a duplicated
record if every downstream view inherited the same duplicate.

## Receipts are different from support and party lean

Independent expenditures are spent by an outside committee, not received by
the candidate. Support and opposition are separate flows. Do not add them to
campaign receipts or turn opposition into a negative campaign contribution.

“Spending by a committee that received tech money” means the committee had
positive net matched tech receipts across the cycle. It does not establish that
those receipts preceded the expenditure, or that tech donors funded
all of its spending, or that a particular employee's dollars paid for an ad.
The project's `tech_funded_ie_*` fields are this broad committee-level measure,
not a traced allocation of tech dollars.

Party lean is an analytical label. Candidate/party affiliation and inferred
PAC behavior are different evidence. The donor classifier uses the retained
records for an exact name, including records without a matched tech employer;
the company amount uses only employer-matched records. Thus a donor's displayed
party dollars and tech-only total can have different scopes. A donor-based
company percentage also differs from a recipient-based percentage.

Read each denominator. “Tech share” here uses the project's selected itemized
receipt pool; it is not a share of the committee's total reported receipts.
Unitemized gifts, loans, and other receipts are outside that denominator.
Refunds can make ratios invalid; missing percentages are preferable to claims
above 100%. Signed amounts remain available in downloadable diagnostics when
unreconciled totals are withheld on the page.

## Dates and source freshness

Keep these dates separate: transaction date, election designation, report
coverage dates, filing/processing dates, source release date, and local build
date. A new amendment can change a contribution with an old transaction date.
Fetching only transactions dated after the last download misses that change.

`latest_local_bulk_release_utc` describes the newest installed bulk file.
`latest_bulk_release_utc` describes the newest remote release observed.
`source_check_status` distinguishes current, stale, missing, and unknown.
An unavailable network check must not be read as current. Different files can
have different release dates; inspect all six entries in `source_manifest.json`.
Its `reviewed_reference_inputs` also records the paths and SHA-256 values of
the included employer lookup, cycle committee-conversion reference, and
reviewed transaction exclusions. Preserve those actual files with the raw
inputs; their hashes identify versions but cannot recover missing files.

The maximum matched transaction date is not a completeness certificate.
Invalid or absent dates still contribute signed money to totals, while weekly
charts omit those records and record their separate amount. Some valid dates
fall outside the chart's default display window. The headline and cumulative
series must reconcile with that distinction intact.

## The API can be fresher, but it is not an interchangeable extra file

[OpenFEC](https://api.open.fec.gov/developers/) exposes processed itemized
receipts through `/schedules/schedule_a/`, as well as reports and other data.
It can contain changes between bulk releases. Freshness also depends on when
the committee filed and when processing finished: the
[FEC explains its processing stages](https://www.fec.gov/data/filings/), and
its [data overview](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/)
distinguishes summary availability from the underlying reporting process.
“API” does not mean real time or that every donor has already been reported.

The [receipt coverage documentation](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/about-receipts-data/)
describes a broader Schedule A record universe than the selected individual
bulk file. The API's `is_individual=true` flag is an FEC analytical definition;
it includes certain joint-fundraising allocation types and is not the same as
our contribution/refund allowlist. See the
[FEC aggregation methodology](https://www.fec.gov/campaign-finance-data/about-campaign-finance-data/methodology/).
Do not substitute it as a magic “all employees, net of refunds” filter.

For a scoped API comparison:

1. Pin the committee IDs and `two_year_transaction_period`; declare any
   additional transaction-date or election-designation restriction.
2. Retrieve processed Schedule A rows and then apply the reviewed employer
   aliases locally. A server-side employer search can be broader than our exact
   matching rule. One keyword can also miss an alias such as a ticker or
   misspelling. Search the necessary aliases or retrieve the complete scoped
   committee records; union overlapping query results by `sub_id` before the
   local filters. Save the exact queries separately from those filters.
3. Follow every `pagination.last_indexes` cursor, including all returned fields
   and any null-date transition. `/schedules/schedule_a/` does not use the
   ordinary `page=1,2,3` pattern. The local `OpenFECClient` now handles this and
   fails on a repeated or missing cursor. A deliberately bounded query is
   incomplete and must be labeled as such.
4. Compare row IDs, amounts, dates, employer text, and filing IDs against the
   bulk extract. Inspect differing records and the reports' coverage/amendment
   chain. API and bulk amendment fields do not necessarily use the same codes.
5. Keep the API comparison separate from the production bulk ledger. Never
   concatenate the two and sum. A future incremental ingester needs replacement
   and amendment semantics, source provenance, and tests for old-dated changes.

The broad company and candidate views remain bulk-based. The separate reviewed
reports read original electronic filings and can use the API to check their
report inventory. Neither path claims a complete API-to-bulk reconciliation of
every record, and the new reports do not change the legacy counting universe.

For the worked example below, with `OPENFEC_API_KEY` configured locally:

```python
from pipeline.fec.openfec import OpenFECClient

records = list(OpenFECClient().iter_results(
    "schedules/schedule_a/",
    committee_id="C00902668",
    two_year_transaction_period=2026,
    contributor_employer="NVIDIA",
    per_page=100,
))
# Inspect and exact-match aliases locally; do not add these to bulk totals.
```

For a net-receipts investigation, inspect refunds too. For this Senate
committee, Schedule B's `F3-20A` line contains individual contribution refunds;
other filer forms use other lines. Refunds often have no employer, so they
require separate evidence. Do not invent Schedule A query parameters such as
`most_recent=true`, `amendment_indicator=N`, or `memoed_subtotal=false`:
those are not supported filters in its current official argument definitions.
Review returned fields and report history instead. See the
[official API argument definitions](https://github.com/fecgov/openFEC/blob/develop/webservices/args.py).

## Worked example: Nvidia and Abdul El-Sayed, 2026

The September 18 investigation selected candidate `S6MI00418` and specified
campaign committee `C00902668`. The independently scanned fresh bulk records
and live processed API agreed on **23 rows totaling $33,500**, including the
same source-row IDs, amounts, dates, employers, and filing IDs. There were nine
exact reported donor-name strings. This is a scoped, reproducible agreement,
not independent verification of employment or every employee's giving.

The confidence follow-up also checked 18 original electronic filings in six
known amendment chains. The latest versions contain those same 23 nonmemo
individual receipts; all link to same-report ActBlue memos. Each latest report's
nonmemo itemized-individual subtotal reconciles exactly to its summary line.
The standalone [source checker](outputs/audit_20260918/nvidia_el_sayed/validate_source_oracle.py)
uses original bytes and frozen expectations, without production selection
constants. A $750 record changes its reported employer from `Nvda` to `Nurse`
in the latest Q1 amendment, providing a real test of replacement-version
selection. This supports reported receipts, not fully refund-adjusted giving.
The September 19 campaign-report workflow reproduces that $33,500 from the
original files and leaves refunds and net unknown. Its processed inventory
checks at 10:12:59 and 10:15:00 UTC found the same 18 versions; coverage remains
July 15.
The durable [fixture and amendment example](tests/fixtures/fec_attribution/nvidia_el_sayed_direct_receipts.json)
preserve the expected result without depending on the ignored audit directory.

The broader September 19 review also adjudicated all 57 negative memo records
in 55 complete reattribution families, plus four returned-item corrections.
The Nvidia attribution remains $33,500 before refunds after those checks.
The newer committee directory lists another authorized account, Independents
for Abdul (`C00961805`), first filed September 17. The API has its registration
but no financial reports at the September 19 check. It is outside this
`C00902668` report scope. Do not describe the worked example as all current
campaign accounts or interpret an absent report as zero giving.

A second live check queried all 15 tracked Nvidia employer aliases, followed
each query's pagination, combined overlapping results by source-row ID, and
then applied the exact aliases locally. It still returned the same 23 records
and $33,500, with no API-only rows. That wider check matters: matching all known
bulk records with one keyword would not rule out newer API records using a
different spelling. The saved query coverage is
`outputs/audit_20260918/nvidia_el_sayed/api_alias_coverage.json`.

The records split into $26,000 designated for `P2026` and $7,500 for `G2026`.
The latest campaign reporting coverage was July 15, 2026. All 23 bulk records
carried amendment marker `A`; dropping amended records would erase the whole
answer. The processed API described those same rows as 22 unchanged records
and one changed record, illustrating why those code systems cannot be equated.

Separately, 28 Nvidia-alias ActBlue forwarding rows to this committee totaled
$40,000. They are not another $40,000 to add. The forwarding evidence included
$6,000 dated after the campaign's coverage, a $750 employer disagreement between
stages, and different matching coverage for another $250. Those differences
need explanation before asserting one exhaustive employee-giving figure.

Re-run the local evidence extraction with:

```bash
python -m scripts.audit_candidate_receipts --cycle 2026 --candidate S6MI00418 --company nvidia --output outputs/nvidia_el_sayed
```

The investigation preserves selected records and reports excluded/routing
evidence separately. The command does not automatically turn intermediary
records into campaign receipts or change the published total. The detailed
September comparison is saved under `outputs/audit_20260918/`.

The command writes two files, with deliberately different roles:

- `evidence.csv` retains the scoped records, including reviewed exclusions.
  A reviewed repeat has `core_included=False` and
  `exclusion_reason=source_reviewed_repeated_original`. Other reasons identify
  intermediary routing, excluded JFC allocation types, and unresolved `15E`
  memos. Retained partnership or redesignation memos can still be included;
  this is not a blanket memo-exclusion rule.
- `summary.json` computes `current_authorized_net` from exact-employer,
  core-included `itcont` records at the selected current campaign accounts. Its
  `breakdown` groups by source, account scope, transaction type, memo code, and
  inclusion state. It does **not** provide a dedicated reviewed-exclusion count,
  amount, or reference-table version. Use the evidence row reasons for those
  checks and preserve the reviewed-exclusion reference with the extract.
  New scans include `review_dependencies` within each `source_files` entry:
  the observed reviewed-row count and verified-original count apply to the
  complete scanned source, not just this company's selected evidence rows.

Neither file is a live API snapshot. The saved September API comparison is a
separate retrieval described in
[`fec_journalistic_case.md`](outputs/audit_20260918/fec_journalistic_case.md).
The extract's `evidence_sha256` protects the output contents; it does not by
itself preserve every raw input, alias table, committee directory, or reviewed
exception needed to reproduce the result.

## Safe workflow for changing the analysis

1. Save the existing outputs and record the source manifests, alias version,
   committee mappings, and exact question.
2. Reproduce the suspected issue with a small source-backed example. State
   which money flow should be counted once and which should be excluded.
3. Add a regression test that would fail under the old rule. Include a nearby
   counterexample: a same-amount legitimate gift, a shared committee, a refund
   reversal, an amendment, or an intermediary's duplicate as appropriate.
4. For a scoped question, update its reviewed manifest and attribution rule.
   If changing the legacy shared selector instead, verify the narrow candidate
   rebuild uses the same rule as the full pipeline. Do not silently transfer
   a campaign-specific interpretation into global origin-giving totals.
5. Rebuild from a consistent input snapshot. Compare company, donor, committee,
   candidate, and weekly totals; explain every material change.
6. Review representative rows against official filings. Tests and matching
   aggregate totals do not prove that every underlying definition is correct.

The fixture tests establish specified arithmetic and join behavior. They do not
provide an independent answer for every real-world gift. Earmark routing,
blank-employer refunds, donor identity, and PAC conversion timing still require
source-level evidence. Record uncertainty rather than declaring a journalistic
number fully verified because the test suite is green.

Useful entry points:

| Code | Responsibility |
|---|---|
| `pipeline/fec/update_bulk.py` | Source archive refresh |
| `pipeline/fec/sources.py` | Installed/remote release and verification state |
| `pipeline/fec/load.py` | Field layouts, receipt selection, refund signs, employer tags |
| `pipeline/fec/transaction_reviews.py` | Exact source-reviewed repeat exclusions, shared across loaders and the audit |
| `pipeline/fec/filings.py` | Original electronic filing parsing, source hashes, report summaries and replacement-version selection |
| `pipeline/fec/attribution.py` | Separate attributed-receipt components and unresolved relationships within supplied records |
| `pipeline/fec/campaign_reports.py` | Reviewed scope, annotation dependencies, refund coverage and report evidence |
| `scripts/build_candidate_attribution.py` | Reproduce a pinned campaign report and optionally check its processed API inventory |
| `pipeline/fec/committee_history.py` | FEC-confirmed former campaign accounts |
| `pipeline/build_summaries.py` | Candidate attribution and analytical aggregates |
| `pipeline/classify_partisan.py` | Party evidence and inferred lean |
| `pipeline/build_frontend_exports.py` | Site data, charts, strict JSON, and source metadata |
| `pipeline/rebuild_candidate_exports.py` | Narrow rebuild after candidate-attribution changes |
| `frontend/build_site.py` | Display labels, methodology, and static pages |
| `scripts/validate_site.py` | Structural and numerical consistency of the built site |
| `scripts/audit_candidate_receipts.py` | Scoped company/current-campaign evidence extraction and comparison |
| `scripts/fetch_campaign_report_set.py` | Dated full-report inventory and original source acquisition |
| `scripts/reconcile_campaign_sources.py` | Exact-ID old-versus-new comparison with every difference and unresolved correction retained |
| `scripts/verify_fec_case_sources.py` | Independent transcription check against original bytes for the expanded source cases |

For these FEC changes, focused regression checks include:

```bash
python -m unittest tests.test_finance_analysis tests.test_candidate_rebuild tests.test_transaction_reviews tests.test_openfec_pagination tests.test_candidate_receipt_audit
python -m unittest tests.test_fec_filings tests.test_fec_attribution tests.test_attribution_frontend
```

The transaction-review tests retain a separate equal-value gift and signed
adjustment children, reject stale signatures, and compare the shared rule
across all three receipt paths. They also verify retained-original dependencies
across chunk boundaries and reject missing, repeated, or changed originals.
The pagination tests cover complete cursors,
repeated or missing cursors, resumed null-date transitions, and ordinary
page-based endpoints. These tests verify the stated contracts, not the entire
historical FEC universe.

Run the checks described in the project README. Source refresh and calculation
are separate from publishing: a locally rebuilt `docs/` tree is not evidence
that GitHub Pages is serving the new snapshot.
