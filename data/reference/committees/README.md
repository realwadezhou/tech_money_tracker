# Historical campaign committee conversions

FEC committee and candidate-linkage files can reflect a committee's later
conversion into a PAC, even in an earlier cycle's bulk download. Filtering only
current committee types would therefore erase some historical campaign accounts.

`converted_campaigns_<cycle>.json` restores an account's attribution only when an
official FEC historical conversion notice identifies its former candidate and
that candidate appears in the cycle's candidate master and committee linkage.
Current shared campaign links still use the committee master's candidate ID;
unresolved shared links remain unattributed.

Refresh the supplement after downloading new bulk files:

```bash
python -m pipeline.fec.committee_history 2024 2026
```

The default source is the public FEC committee overview page. The reader verifies
the page's rendered cycle and rejects pages that fall back to another cycle.
Each record saves its source URL and the supplement records the check time.
`former_candidate_election_year` is a compatibility field derived from the
matching candidate master and cycle linkage for public-page records; it is not
an API field extracted from HTML. `election_year_source`, `history_cycle`, and
`conversion_source` make this distinction explicit.

The alternative `--source api` uses the configured OpenFEC key and the official
`convert_to_pac_flag`, `former_candidate_id`, and
`former_candidate_election_year` fields. API keys are never saved in the
supplement. A failed refresh preserves the previous complete file.

Normal builds read the saved supplement offline. This restores historical
account attribution, not a converted PAC's partisan label. Cycle totals may
include activity after conversion; this supplement does not divide a committee's
receipts into amounts before and after its conversion date.

Candidate exports keep the legacy combined `total_itemized_receipts` and
`tech_itemized_receipts` amounts for compatibility. Their components are now
explicitly separated under `current_campaign_account_` and
`former_campaign_account_` prefixes, each with `total_itemized_receipts`,
`tech_itemized_receipts`, `tech_itemized_contributions`, `committee_count`, and
`committee_ids`. Component amounts and row counts reconcile to the combined
figures. Former-account receipts must not be quoted as campaign donations:
the whole account is placed in the former-account component, and this is not
a before/after-conversion allocation. Current-account components describe the
installed account classification, not a verified election-designation split.

The FEC's [Trump committee overview for 2024](https://www.fec.gov/data/committee/C00828541/?cycle=2024)
provides one example of its historical conversion notice.
