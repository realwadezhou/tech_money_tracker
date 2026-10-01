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
