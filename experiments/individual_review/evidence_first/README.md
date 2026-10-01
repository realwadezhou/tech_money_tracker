# Evidence-first donor identity pilot

[Open the review](output/review.html) · [Results](output/README.md) · [Calibration](output/calibration.json) · [Archived experiment](../old/README.md)

This is a fresh implementation for the same 20 prominent people. It retrieves
candidate records, scores interpretable evidence, then saves a compact contextual
review. A score alone never creates an identity assignment.

The first review covers **6,693 records in 475 cases** from the existing 2024 and
2026 local FEC snapshots: **3,529 accepted, 1,308 rejected, 1,856 unresolved**.
These are source-record counts, not counts of donations or unique donors. The
production pipeline does not consume these assignments.

## Review the results

From the repository root, with Python 3.10+ and no additional packages:

```powershell
python experiments/individual_review/evidence_first/run.py build
python experiments/individual_review/evidence_first/run.py serve
```

Open <http://127.0.0.1:18768/>. The HTML also works directly as a file; original
record expansion needs the loopback server. Select a person, compare the
quantitative suggestion with the recorded decision, and expand a case for its
evidence and concise rationale. The server exposes only this review and its
case-record endpoint. It does not expose the project directory or write reviews.

## Structure of the approach

1. **Profile once.** Store known first-name variants, surname retrieval variants,
   middle/suffix evidence, multiple plausible locations, career affiliations,
   compatible roles, contradictory professions and independently documented
   namesakes. Facts retain source IDs. A location list is open-ended; absence
   from it is not a contradiction. Company associations can be historical.
2. **Retrieve broadly, within a surname gate.** Exact or explicitly listed surname
   variants are required. First names allow approved variants, initials and a
   one-edit typo route. A retrieval hypothesis is not an approved identity alias.
3. **Score before judging.** Three evidence families—name, geography, career—have
   separate support, missing and conflict states. Report supportive/observed
   families, missing families and conflict counts alongside a weighted score.
   A documented namesake is an additional negative signal.
4. **Group equivalent review questions.** Repeated cycles, ZIP extensions and
   equivalent known-company descriptions share cases. Different unknown
   employers, roles, ambiguous names and weak locations remain distinguishable.
   Grouping unresolved records does not assert they are the same person.
5. **Contextual pass.** Review one compact packet per person. Keep or override the
   score, save accept/reject/unresolved, a reason code and a short evidentiary
   explanation. A shared explanation can cover several equivalent cases. This
   first pass was performed by Codex in this task; no external model API runner
   is included. Filing images were not individually checked.
6. **Emit auditable links.** Only saved, valid accept decisions produce
   `(cycle:SUB_ID, person_id)` links with record, case, review and evidence hashes.
   Raw source records remain unchanged. Conflicting assignments fail the build.

## Quantitative policy, version 1

| Evidence | Points / treatment |
|---|---|
| Approved first name and surname | +4 |
| Authored name-distinctiveness hypothesis | +0 to +2; not measured population rarity |
| Independently verified full middle name | +1 |
| Initial only / unapproved first-name variant | 0 / −4; requires contextual review |
| Listed surname typo | −1; requires contextual review |
| Conflicting middle / suffix | −4 / −5; prevents automatic acceptance |
| Supported generational suffix | +1 |
| Best plausible city / state | +2 / +1, never both |
| Specific known company affiliation | +5 |
| Swapped fields / company typo | +4 / +3; requires contextual review |
| Compatible generic role | +1 |
| Blank, retired, unemployed or self-employed alone | 0; neutral |
| Explicit contradictory profession | −8; suggests rejection |
| Documented namesake employer + profession | −12; suggests rejection |

The starting acceptance threshold is 8, with no conflict or review flag. Weights
and threshold are hypotheses, not fitted likelihoods. **2/2 observed with one
missing family does not mean 100% confidence.** A broad state match is weaker
than a specific affiliation; city, state and postal code are not independent
votes. An engineer may fit Musk; a lawyer does not. Google employment does not
identify Eric Schmidt when the specific job is staff software engineering.

An expanded middle name sharing a known initial remains flagged unless the full
form is independently recorded. The contextual reviewer may accept a particular
company-supported case without claiming the expanded middle name was verified.

The optional `cohort_context` records direct postal overlap with cases supported
by specific profile affiliations. It does not add points or propagate identity.
It can corroborate a sparse case, but an incompatible profession still matters:
the Marc Andreessen attorney and Ron Conway art-director cases were rejected
despite nearby or matching postal information. Postal context is not a residence
claim, and a shared mailing service can limit its specificity.

## Concise review and refresh workflow

```powershell
# Show just one compact model packet.
python experiments/individual_review/evidence_first/run.py packet --person p0001

# Show new or stale cases only after rebuilding.
python experiments/individual_review/evidence_first/run.py packet --pending

# Scan the local raw bulk snapshots again after input/retrieval changes.
python experiments/individual_review/evidence_first/run.py scan --cycle 2024 --cycle 2026
python experiments/individual_review/evidence_first/run.py build

# Save explicitly authored review objects, then rebuild.
python experiments/individual_review/evidence_first/run.py apply-reviews --file path/to/reviews.json
```

`packet` exports unique cases, not every transaction. Short packet references map
to stable IDs in `output/review_packets.json`. Sources and raw records are loaded
only when needed. Unchanged unresolved decisions are not repeatedly sent by
`--pending`; re-review them when new evidence warrants it.

Review objects contain `person_id`, reviewer/date, `person_note`, profile/model
hashes, case context/evidence hashes, scope, and judgments with case IDs,
decision, reason and note. See `data/reviews.jsonl` for exact examples. Apply
rejects stale inputs and freezes the referenced profiles and scoring source in
`data/versions/`. Rebuild never invents a new review or overwrites the journal.

All first-round reviews are **snapshot** decisions, including the person's
cohort hash because the reviewer considered neighboring cases. A changed cohort
requires review again. The implementation also supports explicit
`context_policy` decisions that can cover later records with the same scored
context; the first round deliberately does not grant those broader rules.
Profile/scoring changes invalidate old reviews. New contexts remain unreviewed.

## Calibration and remaining limitations

The development anchors are the user's reviewed Musk cohort (140 records in
seven cases) and the Venable attorney (18 records in one case). The initial
scorer and contextual review accept all 140 Musk records and reject all 18
attorney records. Both anchors informed the policy. Repeated records are not
independent evaluation examples, and there is **no independent holdout yet**.

`output/calibration.json` preserves before/after outcomes, distinct case counts,
missing retrieved examples and a threshold sweep. The 20-person review journal
is not ground truth for measuring its own accuracy. Next, label a separate set
of cases across score bands, common/rare names, sparse careers, typos and hard
conflicts; split by person/case rather than randomly splitting repeated records.
Measure false merges, missed matches and abstention, and audit retrieval misses
separately. Broader location profiles and independently verified affiliations
can then reduce the unresolved queue without changing the meaning of missing data.

The surname list can miss unlisted spelling errors, compound-name parsing cases,
or historical surnames. Distinctiveness bonuses are authored assumptions.
Role semantics and name parsing are intentionally small and require calibration;
for example a terminal `V` is currently parsed as a generational suffix and is
flagged if unsupported. Career/location facts are mostly undated sets, so this
is not a timeline model. A review can be wrong: source citations and explicit
overrides make it inspectable, not statistically validated.

## Files and provenance

| Path | Purpose |
|---|---|
| `data/profiles.json`, `data/sources.json` | Public profile facts, authored assumptions and source references |
| `data/records.jsonl`, `data/provenance.json` | One raw candidate snapshot with field hashes and extract provenance |
| `data/reviews.jsonl`, `data/versions/` | Append-only review decisions and their frozen inputs |
| `data/calibration_labels.json` | Explicitly identified development labels |
| `model.py` | Deterministic retrieval, scoring and case signatures |
| `run.py`, `review.html` | Build, review import, compact packets and local viewer |
| `output/` | Rebuildable cases, membership, assignments, calibration and review page |
| `state/` | Ignored raw extracts and working scratch files |

The first input reuses checksum-verified raw surname extracts from
`../old/state/pilot/`. `seed_profiles.py` reused cited public biographical source
observations and authored a new schema; no former review decisions were imported.
Four former ERIKA/SCHMIDT candidates fall outside the revised first-name retrieval
route, explaining 6,693 versus the archived pilot's 6,697. This difference is a
retrieval policy change, not evidence that those records were duplicates.

Validation:

```powershell
python -m unittest discover -s tests -p test_individual_evidence_first.py
```

Checks cover neutral missing fields, contradictory careers, nickname/surname
routes, uncertainty in middle names, changed profiles/cohorts, conflicting
assignments and preservation of raw rows—including repeated, negative and memo
records. The journal is an identity layer; contribution deduplication and money
accounting remain separate.

First-round validation passed 16 identity tests and seven repository-layout
tests. Browser QA used wholly fictional records to check pagination, search,
decision filters, person navigation, case expansion, original-record loading
and HTML escaping; it did not expose the donor snapshot to the browser connector.
