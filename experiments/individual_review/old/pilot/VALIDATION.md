# Pilot validation — September 20, 2026

The two installed source snapshots yielded 1,419 candidate evidence groups and
6,697 unique source records. Current decisions contain 848 accepted, 229 rejected
and 342 unresolved groups, with no stale or unreviewed groups. The export contains
3,067 unique record-to-person links across 19 people. Steve Ballmer remains
unresolved; the review does not force a match for every person.

## Evidence checks

- All 19 focused regression tests pass:
  `python -m unittest tests.test_individual_identity_review -v`.
- Rebuilding groups from the frozen evidence reproduces the record hashes and
  group contents. Historical profile and public-source hashes resolve to retained
  contexts. Every latest decision matches the current reviewed context.
- One source record cannot map to two people. New/changed source records and
  revised direct anchors invalidate old acceptances. Anchor validation requires
  exact full name/location/ZIP+4, the same cycle, and every date within 90 days.
- Annotation preserves all original fields, row order, record count and raw
  signed amounts. These arithmetic checks are not contribution accounting.
- Specific regressions exclude the 18 Venable attorney records from a16z's Ben
  Horowitz, reject Workday/Google staff-engineer collisions, and leave unfamiliar
  investment vehicles unresolved where there is no clear professional conflict.

The professional-conflict second pass revised 100 groups from unresolved to
rejected; no accepted links changed. The earlier decisions remain in the journal.

## Interface checks

JavaScript syntax validation passed. Browser testing used a wholly fictional
fixture and verified search, evidence expansion, status filtering, and an
end-to-end saved decision appearing in the review and mapping. Visual inspection
confirmed that the evidence, rationale, limitations and sources are readable.

Automatic approval review blocked browser inspection of the real donor dataset,
citing possible disclosure through the browser connector. Real data was checked
locally; it was not substituted into the fictional browser fixture. The real-data
page's complete visual appearance has not been inspected through that connector.

These checks validate audit behavior and the scoped implementation. They do not
establish measured identity precision/recall or independent human sign-off on
each filing. The pilot has not been integrated into production donor totals.
