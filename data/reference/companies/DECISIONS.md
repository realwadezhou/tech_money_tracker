# Company tagging decisions log

Judgement calls about which names count as which company, and why. Newest
entries go at the bottom. Add an entry whenever a rule is made or changed, so
a later reader can tell a deliberate choice from an accident.

Each entry says **who decided**. "Claude, approved by Wade" means the AI
proposed it and Wade agreed. "Claude" alone means the AI made the call under
Wade's standing permission (2026-09-30) to use judgement as long as it is
written down here.

Per-row reasons live in the `notes` column of `lda_clients.csv` and
`curated.csv`. This file holds the rules behind them.

---

## 2026-09-30: first lobbying name review

Context: `lda_clients.csv` was created by reviewing the 263 lobbying client
names that the broad searches matched for tracked companies. Result: 168
included, 95 rejected. No person checked individual rows; they are marked
"AI first pass 2026-09-30" or cite a decision number below.

### Decision 1: subcontractor reports are left out

*Decided by: Claude, approved by Wade.*

Names like "WILMERHALE ON BEHALF OF APPLE INC." or "CORNERSTONE GOVERNMENT
AFFAIRS OBO MICROSOFT CORPORATION" are a lobbying firm reporting work it did as
a subcontractor to another firm. They are marked `include=FALSE` (26 names).

- **Why:** the subcontractor is paid by the main firm, whose own report already
  includes what the company paid. Counting both double counts dollars.
- **Cost:** the issue text in those reports does not show up when you search
  within that company. If that matters later, the fix is a separate
  "count text but not dollars" flag, not flipping these rows to TRUE.

### Decision 2: Cerner is not Oracle

*Decided by: Claude, approved by Wade.*

Oracle bought Cerner in June 2022. "CERNER CORPORATION", "ORACLE CERNER
(FORMERLY CERNER CORPORATION)" and one subcontractor name are `include=FALSE`.

- **Why:** most of those reports cover lobbying Cerner did as an independent
  company. The lookup has no dates, so a name is either in or out for all years.
- **Cost:** a few post-purchase Cerner-branded reports (late 2022) are missing
  from Oracle. Oracle's own reports from 2022 onward cover health-records
  lobbying under the Oracle name.

### Decision 3: TikTok USDS Joint Venture is tracked under ByteDance / TikTok

*Decided by: Claude, approved by Wade.*

"TIKTOK USDS JOINT VENTURE LLC" and "TIK TOK USDS JOINT VENTURE" (reports start
2026 Q1) are `include=TRUE`, company `bytedance`.

- **Why:** a reader looking for TikTok's lobbying will look under this company.
- **Caveat:** the joint venture is a separate US entity and is not simply
  ByteDance. The company's display name is "ByteDance / TikTok" for this reason.
  Do not describe the combined total as "ByteDance's lobbying".

### Decision 4: subsidiaries roll up to the parent

*Decided by: Claude, following the existing rule in `curated.csv`.*

Waymo, Verily, DeepMind, Wing → `google`. LinkedIn → `microsoft`. Zoox, AWS →
`amazon`. Facebook → `meta`. Twitter, X Corp, SpaceX → `x_twitter_spacex`
(Tesla stays separate, as on the FEC side).

- **Why:** the FEC employer lookup already works this way, and the two lookups
  need to mean the same thing by "Google".
- **Caveat:** as with Cerner, there are no dates. A subsidiary bought or sold
  mid-period is attributed to the parent for all years.

### Decision 5: bare names are accepted when the filing text fits

*Decided by: Claude.*

Short names such as "APPLE", "INTEL", "ORACLE", "AMAZON", "TESLA" and
"PALANTIR" are included. Each was checked against its lobbying firm and issue
text (for example, "APPLE" is Capitol Tax Partners on corporate and
international tax).

### Decision 6: look-alikes rejected

*Decided by: Claude.*

Marked `include=FALSE`, with the reason on each row:

| Name | Why it is not the tech company |
|---|---|
| NATIONAL PHILANTHROPIC TRUST | "anthropic" appears inside "philanthropic" |
| U.S. APPLE ASSOCIATION / US APPLE ASSOCIATION | fruit growers |
| APPLE HOMECARE MEDICAL SUPPLY, INC. | unrelated company |
| O.N.E. AMAZON | rainforest protection group |
| ACCEL | lobbies on special-education funding; not the venture firm |
| GREYLOCK CAPITAL MANAGEMENT, LLC | a hedge fund; not Greylock Partners |
| MODERN INTEL, RIPPLE AND FLOW, EDGESCALE AI, PENDULLUM POWERE AMD WATER | unrelated companies |
| TIKTOK COALITION | an outside coalition, not the company |
| FedEx, RTX, Fox, CSX, Xerox and others (54 names in total) | the "X CORP" search matched the end of another name |

### Decision 7: four companies added to the shared list

*Decided by: Claude, approved by Wade (as part of "expand the company list").*

`intel`, `bytedance`, `cohere`, `scale_ai` were added to `companies.csv`. They
have lobbying names but no FEC employer names yet, so they do not appear on the
public campaign-finance site.

Sectors were filled in for companies whose `curated.csv` rows disagreed or were
blank: `palantir` = defense, `salesforce` = software, `uber` = rideshare,
`x_twitter_spacex` = elon_empire (the most common existing value).

### Not decided

260 candidate names remain in `lda_review_queue.csv`. All belong to companies
that are not in `companies.csv` (Comcast, Verizon, AT&T, T-Mobile, Cisco,
Adobe, Intuit and others). They stay undecided until someone chooses to track
those companies.

---

## 2026-09-30: AI explorer switched to the shared list

### Decision 8: the explorer uses the shared company list

*Decided by: Claude, approved by Wade.*

`pipeline/lda/build_explorer.py` now takes companies and name spellings from
`companies.csv` and `lda_clients.csv`. Its old 10-company
`data/reference/lobbying/organizations.json` was replaced by `watchlists.json`,
which only groups company IDs.

- **Effect, checked by rebuilding all seven years:** the same 19,621 AI issue
  entries are indexed. Entries mapped to a tracked company rose from 1,301 to
  2,589, because 35 companies are now recognized instead of 10 (Oracle, IBM,
  a16z, Qualcomm, Intel and others are new) and more spellings are covered.
- **Kept as they were:** the "Big Tech" (Apple, Amazon, Google, Meta,
  Microsoft) and "AI firms and infrastructure" (OpenAI, Anthropic, Cohere,
  Scale AI, NVIDIA) lists. They are hand-picked, not derived from `sector`,
  because whether Netflix or IBM counts as "Big Tech" is a judgement call that
  should be made on purpose.
- **Changed:** the default list, formerly "All selected tech / AI firms" (10
  companies), is now "All tracked companies" (every company with a reviewed
  lobbying name, 35 today). Scale AI's ID changed from `scale-ai` to `scale_ai`.
- **Still labelled "name seed":** the explorer's per-entry status text still
  says the mapping is an exact-name match awaiting source-ID review. That
  remains true: names were reviewed, individual registrant/client IDs were not.

---

## 2026-09-30: quarterly lobbying spending page

### Decision 9: how one spending number is made per company and quarter

*Decided by: Claude.*

A lobbying report carries one of two kinds of amount, never both: **in-house
expenses** (a company reporting its own lobbying) or **outside-firm income** (a
hired firm reporting what the company paid it). For each company and quarter,
`pipeline/lda/build_spending.py` adds up each kind and uses **the larger of
the two sums**. It never adds the two together.

- **Why not add them:** a company's in-house expense figure normally already
  includes what it paid outside firms. Adding both double counts.
- **Why not always use in-house when it exists** (the common convention): in 13
  of 656 company-quarters with an in-house report, the outside firms together
  reported more than the company did (Zoom reported $50,000 in-house while its
  firms reported $230,000). Using the larger sum avoids a figure that is
  plainly too low. In the other 98% of cases the result is identical.
- **Separate in-house filers add up.** Google and Waymo, Microsoft and
  LinkedIn, X and SpaceX each file their own expense reports; these are summed.
- **Known cost:** the result is a floor, not an exact total. Where in-house
  expenses do not include outside firms, the true figure is higher. When a
  parent's in-house figure is used, outside firms hired by a subsidiary are
  assumed to be inside it.
- **Check:** Meta's 2024 total comes out at $24.43 million, in line with the
  figure widely reported for Meta that year.
- **Other rules:** only quarters whose filing deadline (20 days after quarter
  end) had passed at the newest posting in the data are shown. Blank amounts
  (filers may leave amounts under $5,000 blank) count as zero. One outside firm
  (Hilltop Advocacy for Microsoft, 2020 Q3, $10,000) filed its amount as
  expenses; it is treated like any in-house report because the amount is too
  small to matter and special-casing it would hide the rule.

The public download lists which sum was used for every company and quarter
(`basis` column), and every report behind each number.
