# What we can trust in the FEC analysis

Updated September 19, 2026. This review follows the user's September 18 concern
that the first audit did not establish enough confidence in the underlying
accounting model. The original findings remain relevant; a separate reviewed
campaign workflow now implements three bounded reports. The subsequent
[methodology validation](FEC_METHODOLOGY_VALIDATION.md) expands the original-source
comparison across three campaign accounts and investigates the largest memo groups.

## The present judgment

The site is a research and discovery tool. We do not yet have enough evidence
to treat any arbitrary employer-to-candidate total as a complete, reconciled
answer to "How much did this company's employees give this candidate?"

The first audit corrected real errors and made the implementation more robust.
It did not certify the general model. The remaining uncertainty is not bounded
by the dollar value of the issues already found. A green test suite, matching
site totals, and current downloads do not change that conclusion.

The new workflow improves the evidence for explicitly reviewed questions. It
reads original electronic reports, selects replacement versions, preserves
source relationships, and separates attribution from refunds. It currently
produces these local reports:

| Reviewed scope | Supported attribution before refunds | Net giving |
|---|---|---|
| Nvidia reported-employer receipts at El-Sayed's `C00902668`, six latest reports covering January 1, 2025–July 15, 2026 | $33,500 in 23 receipts | Unknown |
| Meta reported-employer attribution at DelBene's `C00459099`, seven full reports through July 15, 2026 | $4,925: $4,900 direct plus $25 JFC allocation | Unknown |
| Thayer's specified four-record JFC chain at DelBene's `C00459099` | $25 | Unknown |

The final row is one donor chain contained in the full Meta result; do not add
the two. None of these results verifies employment or complete net employee giving. These separate
reports do not replace the site's legacy bulk company/candidate totals or
resolve all remaining source relationships.

The expanded review traced Nvidia's 57 negative memo records and four returned
items without changing its $33,500 before-refund result. DelBene's four matched
employers reconcile from $28,100 under the old selection to $65,125 before
refunds: $300 in direct receipts absent from bulk and $36,725 in JFC allocations
explain the entire difference. The engine and an independent raw-file checker
agree. Rounds still has unresolved negative corrections; its larger supported
positive subtotal is not a certified increase in giving.

The final directory refresh also added another El-Sayed authorized account,
`C00961805`, first filed September 17, with no financial reports in the API yet.
The Nvidia result remains about `C00902668` through July 15. All-current-account
giving and missing financial reports must not be equated with that result.

## Why the question needs its own accounting rules

An original gift, the cash reaching a committee, and the donor's attributed
share at a campaign are different quantities. A single gift can appear in
several records. Some records move or explain that money; others replace
earlier reporting. Counting every record repeats money, but deleting every
memo loses valid attribution.

For joint fundraising, the campaign receives a transfer after allocated
fundraising expenses while donor memos describe allocated contributions.
The transfer and the donor allocations therefore need not have equal totals.
For partnerships, the receipt and partners' memo shares describe the same
money from different perspectives. [FEC joint-fundraising guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/joint-fundraising-transfers/),
[FEC partnership guidance](https://www.fec.gov/help-candidates-and-committees/filing-reports/partnership-contributions/).

The legacy production selector tries to avoid repeated donor-origin money
across committees. Reusing that same selection for every campaign does not
establish complete donor attribution to that campaign. The new workflow uses
separate components and reviewed relationships for its stated campaign scope;
it does not expand the legacy transaction-type allowlist.

## What each existing check establishes

| Check | Evidence it provides | What it cannot prove |
|---|---|---|
| Regression tests | The code follows the specified rules and handles known failures | The rules answer every journalistic question |
| Company/candidate/chart reconciliation | The views agree on the selected records | The underlying gifts are counted exactly once |
| Bulk versus processed API | Agreement across extraction paths and processed products | Independence from shared FEC processing or completeness of reported employment |
| Original filing and amendment review | The filed records and relationships support a particular interpretation | That the filed information is true, or every other case behaves the same way |
| Download freshness | Which release was retrieved and when | Complete reporting through today |

Electronic amendments replace complete reports; paper amendments can be
partial. "Keep only new filings" and "sum every version" are both unsuitable
general rules. The legacy bulk pipeline relies on processed FEC snapshots.
The new electronic-filing loader reconstructs replacement chains within the
supplied, hash-pinned inventory. Missing sequences, overlapping or changed
report periods, unsupported formats and changed source contents require review.
This is not proof that the supplied inventory contains every filed document.
[FEC amendment instructions](https://www.fec.gov/help-candidates-and-committees/filing-amendments/).

## A real counterexample to the general candidate interpretation

DelBene's campaign reported a $25 joint-fundraising donor allocation for Susan
Thayer, whose employer is reported as Meta Platforms. The allocation points
to the related transfer. Two later primary/general adjustments are -$25 and
+$25. The source-supported candidate attribution for this chain is therefore
$25; the legacy common `itcont` selector returns $0 because it retains the
adjustments but does not include the type-15J allocation. The report was the
latest version for its period in the saved September 18 API review, so an
obsolete amendment did not explain that difference.

The new reviewed report returns **$25** for these four source records. It keeps
the $124,357 aggregate JFC transfer as context, includes the $25 donor allocation,
and retains both signed election adjustments. Every annotation is tied to the
original row's hash and its required record IDs. The legacy result remains
unchanged; a separate calculation answers the narrower attribution question.

This proves a scope gap, not its prevalence. It is one chain, not the donor's
entire giving or the campaign's entire Meta total. It does not determine the
donor's original giving across committees or her share of the campaign's net
cash transfer. Adding $25 to every global total would be unjustified.

[Official filing 1920909](https://docquery.fec.gov/dcdev/posted/1920909.fec),
[tracked source fixture](tests/fixtures/fec_attribution/thayer_jfc_allocation.json),
[reviewed report manifest](data/reference/attribution/meta_delbene_reviewed_allocation.json),
[source evidence and independent expected amounts](outputs/audit_20260918/fec_confidence_followup/README.md),
[offline checker](outputs/audit_20260918/fec_confidence_followup/check_thayer_oracle.py).

## A case with stronger supporting evidence

The Nvidia-to-Abdul-El-Sayed example now has an original-filing check beyond
bulk/API agreement. We preserved 18 electronic filings in six amendment chains
and used their headers to select the latest version of each known chain.
Those six reports contain 23 nonmemo individual receipt records with the
reviewed Nvidia employer spellings, totaling $33,500. Each receipt links to
an equal-value ActBlue memo in the same report. All six reports' nonmemo
itemized-individual receipt sums reconcile exactly to their summary lines.

An actual amendment changes a $750 record's employer from `Nvda` to `Nurse`.
The latest version excludes it from the employer match. This demonstrates why
using an old report or adding every amendment gives a different answer.
It does not establish which reported employer was factually correct.

This supports the narrow $33,500 **reported-employer receipt** figure in reports
whose coverage ends July 15, 2026. The new campaign workflow reproduces it from
the originals. It does not verify employment, resolve every donor identity,
or establish fully refund-adjusted giving. The six reports contain $61,682.15
of itemized individual refunds against $68,392.39 in their refund summaries.
Unlinked donors and the $6,710.24 difference prevent a verified Nvidia net;
the report emits `null`, not zero, for refunds and net.

On September 19 at 10:12:59 UTC, the processed OpenFEC report inventory still
contained the same 18 versions and the same six latest reports; a subsequent
check at 10:15 UTC was also unchanged. That check is distinct from the September
18 saved catalog date and the July 15 report-coverage end. It cannot rule out a
new filing that has not reached the processed API. The FEC documents its
[raw and processed filing stages](https://www.fec.gov/data/filings/).

The source-file inventory came from that report catalog. The independent
evidence is the original filing content and reconstruction of its replacement
chains, compared with expectations established before the new engine. This
does not make the API catalog an exhaustive source of all possible evidence.

[Frozen source facts and hashes](outputs/audit_20260918/nvidia_el_sayed/source_oracle.json),
[independent checker](outputs/audit_20260918/nvidia_el_sayed/validate_source_oracle.py),
[tracked source fixture](tests/fixtures/fec_attribution/nvidia_el_sayed_direct_receipts.json),
[reviewed report manifest](data/reference/attribution/nvidia_el_sayed.json).

Reproduce the new reports from the project root:

```powershell
python -m scripts.build_candidate_attribution --manifest data/reference/attribution/nvidia_el_sayed.json --check-current
python -m scripts.build_candidate_attribution --manifest data/reference/attribution/meta_delbene_reviewed_allocation.json
```

The commands verify cached originals or download the pinned filing versions.
The optional current-inventory check requires `OPENFEC_API_KEY` and refuses
changed report inventories. The earlier offline checkers remain useful audit
history: the Thayer checker intentionally demonstrates the legacy selector's
gap. The tracked fixtures and their redacted source excerpts make the new
tests independent of the ignored audit-output directory.

## The definition used by the reviewed campaign workflow

Start with **itemized contributions attributed to named campaign accounts
from contributors reporting a tracked employer**, over an explicit period.
If the question requires natural persons, select and review that scope rather
than assuming every employer-matched record is a person. Report these parts
separately:

1. Direct and earmarked receipts recorded at the campaign.
2. Joint-fundraising donor allocations supported by source records.
3. Partnership or other donor attributions, with their parent relationships.
4. Signed corrections linked by adequate evidence.
5. Refunds linked by adequate evidence, separately from pre-refund attribution.
6. Unresolved records, conflicting employer reports, and reporting gaps.

Only combine the parts after verifying that they are disjoint for the chosen
question. Keep gross attributed contributions separate from reconciled refunds;
do not call the result fully net when blank-employer refunds remain unlinked.
Employer text is a reported attribute, not verification of employment. The
result is not necessarily a lower bound because false matches can overstate it.

## What must be checked before quoting a number

Record the question, account IDs and ownership dates, date/election scope,
source release and report coverage, and exact employer aliases. Preserve every
included record and relevant excluded record with its filing and transaction
identifiers. Review amendment versions and memo/transfer relationships in the
original reports; check refunds separately. Reconcile cash subtotals to report
lines and donor attribution to the related gifts, without expecting those two
totals to be interchangeable.

An unresolved relationship affecting the answer is a reason to withhold the
combined claim or narrow it to the component the evidence supports. The site
can still display a clearly labeled selected-record sum for exploration.

## What the new safeguards establish, and what remains

The source-backed test corpus contains six actual cases: Thayer's allocation,
A16Z partner attributions, Vandiver's legitimate equal receipts and balanced
adjustments, Flores's repeated-original memo, an unexplained Mansuri memo, and
Nvidia receipts with ActBlue context. Original-file and row hashes bind the
reviews to source contents; address-free excerpts support offline tests.
[Corpus scope and expected outcomes](tests/fixtures/fec_attribution/README.md).

The key checks preserve the accounting distinctions: a transfer or conduit
memo must not create a second gift; a reviewed balanced redesignation keeps
its required children; legitimate equal-value originals remain separate; an
unexplained memo stays unresolved. A review cannot overwrite the filed amount,
employer or date. Joint-fundraising classification requires source review:
an individual memo attached to a transfer alone does not establish that role.
These are meaningful protections for the reviewed questions, not a universal
proof that every filing has been interpreted correctly.

A report also needs reviewed account ownership and receipt-summary
reconciliation. The current workflow withholds a combined amount for gaps in
full-report coverage or more than one campaign account: ownership alone does
not resolve overlapping money between accounts. Refunds remain separate;
incomplete coverage or unlinked donor relationships leave net unknown. A
nonmemo-receipt diagnostic can include in-kind contributions and must not be
described as cash flow.

Further cases still require their own report inventory, ownership evidence,
exact employer scope, source relationships and independent expectations. The
largest remaining limitations include blank-employer refunds, unresolved memo
roles, intermediary routing, changes of employer or account ownership, missing
or unitemized reporting, and coverage beyond the known reports. The value of
the errors already found does not bound those risks.

The site's broad totals remain selected-record research views with cautions.
The new reports are separate local outputs; their presence here does not
establish that the public site serves them. Source refresh, local calculation,
and deployment remain separate steps. Earlier audit and validation results
are preserved in the September audit rather than treated as certification of
the expanded model.

The implementation and known exceptions are documented in the
[developer guide](FEC_DEVELOPER_GUIDE.md). The
[September audit](AUDIT_2026-09-18.md) preserves earlier checks and limitations.
