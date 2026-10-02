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

### Decision 10: the AI explorer stays unpublished; spending is the lobbying landing page

*Decided by: Claude, at Wade's request to "do what you think is best".*

The public site's lobbying section is the spending page (`/lobbying/spending/`).
The AI lobbying explorer is still built and validated locally on every
lobbying refresh, but it is not copied into `docs/` and nothing links to it.
`/lobbying/` redirects to the spending page.

- **Why:** the explorer's topic matches are keyword hits that no person has
  reviewed (`data/reference/lobbying/topic_reviews.csv` is empty), and it adds
  118 MB to the published site. The spending page rests on reported dollar
  amounts and a reviewed name list.
- **How to reverse:** set `PUBLISH_AI_EXPLORER = True` in
  `frontend/lobbying.py`, then run `python -m frontend.build_site` and
  `python scripts/publish_site_to_docs.py`. The explorer returns at
  `/lobbying/` and the spending and cycle pages link to it again.
- **Before reversing:** review a sample of matches (the worksheet is
  `exports/lobbying/topic_review_queue.csv`), and decide whether 118 MB of
  page data is acceptable or the explorer should load a smaller index.

---

## 2026-09-30: second batch of lobbying companies

### Decision 11: 49 more tech companies tracked for lobbying; telecoms left out

*Decided by: Wade (scope), Claude (individual names).*

Wade decided: add the clearly-tech companies waiting in the lobbying review
queue, and leave out the four telecom and cable companies (Comcast, Verizon,
T-Mobile, AT&T).

- **Why telecoms are out:** together they reported about $419 million over
  2020-2026, more than a third of everything already tracked. Including them
  would change what "tech lobbying" means on the page. Their 41 names stay
  undecided in `lda_review_queue.csv`. To add them later, give them a
  `telecom` sector so they can be shown separately.
- **Added (49):** Adobe, Airbnb, Arm, Atlassian, Block, Broadcom, C3.ai,
  Character.AI, Cisco, Cloudflare, CrowdStrike, Databricks, Discord, DoorDash,
  Dropbox, ElevenLabs, Figma, Fortinet, GitLab, HP, Hewlett Packard Enterprise,
  Hugging Face, Instacart, Intuit, Lyft, Marvell, Micron, Neuralink, Okta, Palo
  Alto Networks, PayPal, Pinterest, Plaid, Reddit, Robinhood, SambaNova, SAP,
  ServiceNow, SentinelOne, Snap, Snowflake, Spotify, Stability AI, Tempus AI,
  TSMC, UiPath, VMware, Workday, Zscaler. 122 names included, 93 rejected.
- **Lobbying only.** These companies have no FEC employer names in
  `curated.csv`, so they do not appear on the campaign-finance pages. Adding
  them there is a separate review of the FEC queue (about 4,000 names).
- **Effect on the public page:** tracked companies went from 35 to 84. The
  "all tracked companies" total for 2026 Q2 went from $46.7 million to $61.1
  million because the list grew, not because spending did. The page's sector
  selector and Sector column exist so totals can be read for a stable group.

Judgement calls on names, following the earlier decisions:

- **Acquisitions (Decision 2 precedent).** VMware is tracked as its own
  company; Broadcom bought it in November 2023 and its reports end in 2024.
  Moveworks (bought by ServiceNow, 2025) and Afterpay (bought by Block, 2022)
  are rejected because their reports are from before the purchases. "VMWARE,
  INC. OBO BROADCOM INC." is one outside firm's client record spanning the
  purchase; it cannot be split, so it counts for neither.
- **Credit Karma is counted as Intuit.** The filer names it "an Intuit
  affiliate". Intuit completed the purchase in December 2020, so that name's
  2020 reports slightly predate it.
- **Block.** "Block, Inc." and "Square, Inc." are the payments company. H&R
  Block and the other names caught by the broad "Block/Square/Tidal/Proto"
  search (45 in all) are unrelated and rejected. Proton AG (the email company) is
  rejected here because it is not Block; it is not otherwise tracked.
- **Atlassian.** Only "ATLASSIAN" counts. The search also caught Bloom
  Energy, Bloomberg, and several cities named Bloomington through its "Loom"
  and "Confluence" product names; all rejected.
- **Neuralink** is tracked on its own, in the same sector as X / Twitter /
  SpaceX, not merged into it.
- **Tempus AI** is included as an AI company although its lobbying is about
  healthcare; it was already in the project's search list.
- **Sectors** for the new companies were assigned by Claude. New sector tags:
  `cybersecurity`, `delivery`, `marketplace`, `media`. The reader-facing sector
  names are in `frontend/lobbying_spending.py` (`SECTOR_LABELS`). The existing
  `tech_giant` tag (Amazon, Apple, Google, IBM, Meta, Microsoft, Netflix) is
  shown as "Large tech companies", not "Big Tech", because it includes IBM
  and Netflix.

---

## 2026-09-30: committee registry

### Decision 12: which committees are tracked by name, and how they are labelled

*Decided by: Wade (build it; include crypto but label it), Claude (entries).*

`data/reference/committees/registry.csv` lists 71 tracked committees and 31
reviewed-and-rejected ones. Method and columns are in
`data/reference/committees/README.md`.

- **Four kinds are kept apart** because they mean different things:
  issue vehicles (Leading the Future, Fairshake, Public First), general
  vehicles that are mostly tech-funded (America PAC), corporate PACs, and
  trade association PACs. Adding them together would be meaningless.
- **Crypto is in, and always labelled** (`topic=crypto`). Fairshake and its
  affiliates are the largest vehicles in the data.
- **Networks.** Committees that fund each other share a `network`. Leading the
  Future gave $20 million each to Think Big and American Mission; Fairshake
  funds Defend American Jobs and Protect Progress; Public First funds Jobs and
  Democracy PAC and Defending Our Values PAC. Money moving inside a network is
  not new money and must not be added to the network's outside receipts.
- **"Tech-funded" rule.** A general-purpose committee is included as
  `tech_funded` when tracked-employer donors gave more than half of its
  receipts and at least $500,000. Mainstream Democrats PAC and Republican
  Accountability PAC have large tech donors but fall under half, so they are
  rejected.
- **Ordinary party and candidate super PACs are rejected, on purpose,** even
  when they receive tens of millions in tech and crypto money (MAGA Inc., SLF,
  CLF, SMP, HMP, Future Forward). They are where the money goes. They show up
  as recipients; they are not tech vehicles.
- **Corporate PACs** are included only for companies in `companies.csv`.
  Telecom PACs are out, matching Decision 11.
- **AI committees with no money yet** (Stop AI, AI Accountability Super PAC
  and others) are included so that money shows up as soon as it is reported.
- **No stance labels yet.** The registry says a committee is about AI. It does
  not say which side it is on. That needs agreed policy questions and sourced
  statements (see `notes/OPEN_QUESTIONS.md`, step 4).
- **Not checked:** the affiliations in `notes` for rejected committees come
  from general knowledge of who they support, not from this data. The
  unexplained items are the a16z partner memo rows on Fairshake's 2026 filing
  (no matching firm gift in the same file) and gifts labelled "Coinbase" to a
  climate PAC.

---

## 2026-10-01: public topics page

### Decision 13: publish phrase counts, but not the AI explorer

*Decided by: Wade (wanted a quick public page with AI and data-center charts
by company), Claude (design and topic list).*

`/lobbying/topics/` shows, for 15 topics, how many tracked companies name the
topic in their lobbying reports each quarter, a company-by-quarter grid, and
each company's most recent passage with a link to the filing.

- **Why this is published when the AI explorer is not (Decision 10):** the
  explorer presents keyword hits as classified "AI topic" matches with a
  review workflow that nobody has run. This page claims less: it says a
  phrase appears, shows the phrase list, and shows the filer's own words.
- **The 15 topics and their phrases** were chosen by Claude
  (`data/reference/lobbying/phrase_topics.json`). Wade asked for AI and data
  centers "and so on"; the rest are subjects that come up in tech lobbying:
  preemption of state laws, export controls, semiconductors, energy, privacy,
  copyright, children's online safety, Section 230, antitrust, crypto,
  tariffs, immigration, quantum computing.
- **"AI" is matched as a capitalized whole word** ("AI" or "A.I."), so that
  "said", "chain" and "FAIR Act" do not count. "Artificial intelligence"
  ignores capitalization.
- **Known loose matches,** stated on the page: "energy" also matches the
  Energy and Commerce Committee; "chips" covers semiconductors and the CHIPS
  Act; "competition" and "children" are broad. Counts are a floor, since a
  report can raise a subject without these words.
- **What is counted.** A company counts in a quarter when at least one of its
  reports (latest version, subcontractor reports excluded, as elsewhere) has
  an issue entry containing a phrase. "All lobbying clients" counts reported
  client names in every industry.
- **Numbers are baked in at build time.** The page does no searching, so the
  public site stays static files.
