# Committees

Two things live here: the **committee registry** (which super PACs and PACs
count as tech, AI or crypto vehicles) and, further down, the historical
campaign-committee conversion files.

## Committee registry

`registry.csv` is the reviewed list of committees the project tracks by name.
It exists because employer matching only sees one kind of money: a person
with a tracked employer giving to a committee. It cannot see a company
writing a check, one super PAC funding another, or what the money is then
spent on. For committees on this list, the project reads all of that.

| File | What it is | Who writes it |
|---|---|---|
| `registry.csv` | **Reviewed decisions.** Source of truth. | By hand (or by Claude, logged in DECISIONS.md). Never by a script. |
| `candidates.csv` | Every committee the sweeps surfaced, with evidence. | `python -m pipeline.tagging.committees` (overwritten) |
| `review_queue.csv` | Candidates not yet in `registry.csv`. | Same command (overwritten) |

### Columns in `registry.csv`

| Column | Meaning |
|---|---|
| `cmte_id` | FEC committee ID. |
| `name` | Name in the FEC committee file when the row was added. |
| `kind` | `issue_vehicle` (exists to push AI, crypto or tech policy), `tech_funded` (general politics, but most of its money is from tech people), `corporate_pac` (a tracked company's employee-funded PAC), `trade_association`, or `general` (reviewed and rejected: receives tech money but is an ordinary party or candidate vehicle). |
| `topic` | `ai`, `crypto`, `tech` or `general`. **Crypto is always labelled** so it can be shown separately. |
| `network` | Committees that fund each other share a network name (for example `leading_the_future`, `fairshake`, `public_first`). |
| `company` | For corporate PACs, the company's `canonical_name` in `../companies/companies.csv`. |
| `include` | `TRUE` to track, `FALSE` for reviewed-and-rejected (so it stops reappearing in the queue). |
| `how_found` | Which sweep surfaced it. |
| `notes` | Why, in a sentence. |

### How committees are found

`python -m pipeline.tagging.committees` sweeps the FEC files five ways, so the
list does not depend on any one method:

1. **Name:** AI, crypto or tech words in the committee's name.
2. **Employer money:** at least $250,000 from donors whose employer is a tracked company.
3. **Organization money:** at least $250,000 given directly by tech or crypto organizations (Ripple, Coinbase, a16z and so on).
4. **Transfers:** at least $100,000 passed to or from a committee already in the registry. This is how affiliates are found, so rerun the sweep after adding committees.
5. **Connected organization:** a PAC whose sponsor or name matches a tracked company.

Candidate, party and joint-fundraising committees are left out of the sweep:
they are where tech money goes, not vehicles for it.

A sixth source is outside knowledge (news coverage of a new super PAC). Add
those by hand and say so in `how_found`.

### What gets built from it

```bash
python -m pipeline.fec.committee_profiles 2024 2026
```

writes `exports/committees/`: for each registry committee and cycle, money in
by kind of source (people, organizations, other committees), the top named
contributors, who stands behind organization gifts where the filing says,
transfers to and from other committees, and spending for or against each
candidate. It takes about three minutes.

Known limits, also noted in the output:

- Nonprofits that only report spending (Public First Action Inc.) have no
  donors in FEC contribution files. Their gifts to registry committees do appear,
  as organization money.
- A few spending rows carry no candidate ID in the bulk file; they are labelled
  as not identified rather than guessed.
- Spending totals have not been checked against each committee's own summary
  filings.

The rules behind individual decisions are in
[`../companies/DECISIONS.md`](../companies/DECISIONS.md) (Decision 12).

## Historical campaign committee conversions


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
