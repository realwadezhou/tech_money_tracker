# Prominent-person identity review

The September 20, 2026 pilot reviews 20 named technology figures against the
installed 2024 and 2026 FEC individual-contribution bulk files. It produces
reviewable identity annotations. It does **not** change production financial
filters, reported employer attribution, donor counts, or public exports.

## What a decision means

- **Accepted:** the specified person is the best-supported identity for this
  exact evidence group under the documented criteria. This is a review judgment,
  not an FEC-issued person identifier or a guarantee the filer made no error.
- **Rejected:** this proposed person/group mapping fails the reviewed criteria
  (for example a different given name, substantive professional conflict,
  independently corroborated namesake, or non-individual entity). Rejection
  does not identify the actual person or prove that a filing typo is impossible.
- **Unresolved:** the evidence is compatible but insufficient, contradictory,
  or requires clarification. These records are not linked to the target person.
- **Stale:** the record set, record contents, public-person context, or supporting
  anchor decision changed. No mapping is emitted until it is reviewed again.

Initial decisions identify the reviewer as **Codex — agent-assisted initial
review**. The assistant inspected per-person name/employer/occupation inventories,
checked cited public identity sources, documented person-specific criteria in
`pilot/review_profiles.json`, and expanded those criteria over the evidence
groups. This is **not an independent human review of every filing image**.
The dossiers give concise evidence-based explanations, competing evidence,
and decision scope.

Professional conflicts are evaluated using judgment, not merely an absence of
matching keywords. For example, the Venable attorney is rejected as the a16z
founder: Benjamin E., the legal employer, attorney role and DC location converge
on Venable's independently documented litigation partner. A staff engineer at
Workday is also rejected for the conflicting professional context, with that
weaker basis labeled as a judgment. Generic retirement, missing fields or an
unfamiliar investment vehicle remain unresolved when they do not supply a clear
contradiction. No person's giving pattern or presumed party preference supplies
positive or negative identity evidence.

## Retrieval and evidence preservation

1. Start from the explicit watchlist in `pilot/people.json`. Stable `person_id`
   values are independent of spelling, employer and residence.
2. Scan both requested raw files for listed surname tokens and selected surname
   misspellings. The scan has no amount, employer, transaction-type, or memo filter.
3. Parse names while retaining middle names and generational suffixes. Retrieve
   listed given-name hypotheses, initial-only names, and single-edit given names.
   These are search hypotheses, **not approved name aliases**. Honorifics are
   removed only from parsed helper fields; raw strings remain unchanged.
4. Group by raw name, city, state, full reported ZIP, employer, occupation,
   entity type and reporting cycle. Keep every source record's transaction date,
   amount, committee, type, memo, filing number, transaction ID, image number,
   SUB_ID, and SHA-256 of the original field values.
5. Record the source path/size/modification time, extraction timestamp, queried
   surnames and SHA-256 of each bounded extract. Scans fail if a requested source
   cannot be read or changes while being read; they never silently omit a cycle.

An evidence group is a review convenience, not a proven unique identity. A
single raw alias can be accepted in one group and unresolved/rejected in another.
The current pilot supports group decisions; a group requiring a split within its
otherwise identical context should remain unresolved until row-level exception
support is added. A reporting cycle is not a transaction-date filter: older
transactions reported in a later cycle remain visible.

## Acceptance criteria

| Code | Required evidence | How it is applied |
|---|---|---|
| `N1_E1_O1` | Compatible reviewed name **and** specifically sourced affiliation **and** compatible reported role | Person-specific employer and role lists are reviewed in context. Name normalization alone never accepts a group. |
| `N1_L1_T1` | Exact normalized full reported name, city, state and **full nine-digit ZIP** matching a directly accepted group; each record within **90 days** of an anchor record in the **same reporting cycle** | Limited to missing/generic employer fields and non-conflicting roles, plus the explicitly documented OAI hypothesis. Each decision cites the direct anchor's group hash and decision ID. |
| `R1` | Unsupported given-name variant | Reject this proposed match; do not infer another identity. |
| `R2` | Source entity type is not explicitly `IND` | Exclude from this pilot's person mapping. A blank entity type is not assumed to be an individual. |
| `R3` | Substantive professional conflict with the public figure's documented career | An explicit person-specific judgment over observed employer/occupation pairs. Merely missing from a biography is insufficient. |
| `R4` | An independently documented professional namesake corroborates the conflicting context | Cite the alternative professional source and explain the matching fields. Reject the target-person merge without automatically creating another donor identity. |
| `U1` | Insufficient positive corroboration | Leave unresolved; name/state/amount/fame are insufficient. |
| `U2` | Conflicting or unsupported middle/suffix, role, employer or time context | Leave unresolved; do not erase conflicts to improve coverage. |

The initial secondary rule is deliberately stricter than ZIP5 matching. Shared
ZIP5, household, employer, recipient, party, amount or timing alone never
establishes a person. Anchor matches cannot serve as anchors for further matches:
there is no transitive expansion through weak evidence.

Middle initials are checked against the reviewed variants, with the exact full
middle text retained for inspection. Compatibility is not independent proof of
the legal middle name. Generational suffixes are preserved; Gates III is
specifically supported by Microsoft's annual report. Unexpected suffixes stay
unresolved. Nicknames such as Ben/Benjamin are scoped to one person's supported
contexts, not globally substituted.

## Source use and chronology

Sources establish the public person's name, role and affiliations; they do not
by themselves identify an arbitrary same-name FEC row. `people.json` includes
source URLs, titles, access dates, bounded factual observations and known date
bounds. Null date bounds mean **unknown**, not perpetual employment. A current
biography and a historical filing may describe different roles.

The review specifically records historical-role problems, including Ballmer's
2014 Microsoft board departure, Sandberg's 2022 COO departure, and Schmidt's
Google/Alphabet chronology. Acceptance of an identity never validates a current
employment label. Public-person affiliations remain separate from the existing
per-record employer-based company attribution.

The available original filing links are supplied for follow-up. Initial review
used bulk-record fields and cited public biographies/issuer filings, not a claim
that every linked FEC page was independently opened. DIME remains a possible
independent benchmark; this pilot has not imported DIME identities or measured
recall against it.

## Audit trail and safe refreshes

`pilot/decisions.jsonl` is append-only through the review server/CLI. Each entry
stores person, group, evidence hash, public-context hash, criteria, source IDs,
reviewer, time, support, rationale, limitations and any direct anchor references.
The initial entries also carry the reviewed profile hash. Revisions retain older
entries; the latest entry for each person/group is active.
`pilot/context_history.json` retains the actual person/source contexts and
authored review profiles behind those historical hashes. The second pass adds
explicit professional-conflict judgments; earlier unresolved decisions remain
in the journal.

Group IDs hash the raw context. Evidence hashes bind the **entire record set**:
even a newly added record in an existing group triggers a fresh review. A revised
or invalidated direct anchor also invalidates dependent mappings. A single source
record cannot be accepted into two people; conflicting acceptances fail before
writing. `record_person_links.csv` has one accepted link per cycle/SUB_ID and
retains the original source-row hash.

Source records with the same SUB_ID and identical contents produce one identity
link, with repeat occurrences reported. Different contents under the same ID
fail. Separate SUB_IDs with identical amounts/names/dates remain separate records.
The annotation function preserves every supplied input row, field and order,
including repeat occurrences, refunds and memo rows. It is not a deduplication or
accounting function. Raw signed amount checks are arithmetic invariants, **not
publishable contribution totals**.

## Reproduce and continue

```powershell
# Rescan the installed inputs. This may require read access to the bulk files.
python experiments/individual_review/watchlist.py scan --cycle 2024 --cycle 2026
python experiments/individual_review/watchlist.py build

# Inspect/revise existing decisions through the local app.
python experiments/individual_review/app.py --watchlist --port 8766
# Open http://127.0.0.1:8766/

# Re-render from the frozen evidence and durable decisions, without a new scan.
python experiments/individual_review/watchlist.py export

# Run focused regression checks.
python -m unittest tests.test_individual_identity_review -v
```

The HTML can also be opened directly for read-only review. Saving requires the
local server. Revisions require a reviewer, rationale, support, limitations,
criteria and valid source references. The server listens only on localhost.

`prepare_pilot_review.py` reproduces an **unapplied draft** from the authored
profiles. It is not invoked by the normal build/refresh. Do not use it as an
automatic future approval engine. Inspect new evidence before applying an
explicit decision batch with `watchlist.py decide --file <reviewed-json>`.
Unreviewed and stale groups never enter the mapping.

## Known limits and next review priorities

- This is a named watchlist, not a census of prominent donors or a complete-recall
  entity-resolution system. Surname changes, unlisted typos, names missing the
  surname, non-ASCII cases and unitemized giving may be missed.
- Blank or generic employers, employer/occupation swaps, inconsistent titles,
  unsupported abbreviations, and different locations remain common unresolved
  cases. Absence of accepted links for a person does not establish no giving.
- Current decisions are agent-assisted and should receive a second independent
  review before production integration or quoted person totals. No numerical
  precision/recall claim is made from the review counts.
- Prioritize same-name conflicts (Schmidt, Sacks, Conway, Parker, Gates), then
  unresolved salient variants (Musk's fuller name, OAI, Ballmer, Sandberg's
  post-COO records), and compare a sample with independently retrieved filings.
- Add further people through the registry, preserve their provenance, and
  review evidence groups. Do not overwrite raw names or reuse display names as IDs.
