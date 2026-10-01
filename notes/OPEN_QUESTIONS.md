# Open questions: what still needs to be nailed down

Written 2026-09-30 after the lobbying work, at Wade's request for a
first-principles look. Numbers are from the 2026-cycle FEC build of
2026-09-19 and the LDA data through 2026-09-18.

## The underlying problem

The site answers "what did people who typed a tracked employer name give?"
The question Wade wants answered is closer to "how are tech's money and
people trying to shape politics, especially AI policy, and who is on which
side?" Those are different, and the gap is in four places.

## 1. Who counts is only defined for companies

Companies are a reviewed list. People and committees are not lists at all:
a person appears only if their employer field matches a tracked company, and
a committee appears only if such a person gave to it.

Evidence:

- Peter Thiel, David Sacks, Joe Lonsdale, Dustin Moskovitz and Ron Conway do
  not appear in either cycle. The likely reason is that their employer fields
  name a fund or say something like "self-employed" or "investor"; this has
  not been checked record by record.
- Fairshake (crypto super PAC, $107.3M in receipts) shows $0 of tech money.
  Think Big ($20.5M) and American Mission ($20.25M) also show $0.

Needed: three explicit registries (companies, people, committees), each with
a written method for who gets on it.

## 2. Only one money channel is counted

Counted today: an individual, with a matched employer, giving to a committee.

Not counted:

- **Organizations giving directly.** Leading the Future has $125.1M in
  receipts; the site attributes $62.5M, from three donor names. The other half is not individual-with-employer money. Fairshake
  is funded almost entirely this way.
- **Committee-to-committee transfers.** Money a super PAC passes to an
  affiliated super PAC.
- **What the committees then spend**, for or against which candidates
  (independent expenditures). Partly present on candidate pages, not
  organized by committee network.
- **Nonprofits.** "Public First Action Inc." files only as an independent
  spender; its donors are not in FEC contribution files at all. This channel
  can be described but never fully measured.

Needed: for registry committees, the full picture: all receipts by source
type, transfers in and out, and spending by candidate.

## 3. Totals are a handful of very rich people

2026 cycle, employer-matched giving ($249M, 15,419 donor names):

| Group | Donor names | Share of dollars |
|---|---:|---:|
| Gave $100,000 or more | 79 | 91% |
| Gave under $5,000 | 14,292 | 4% |
| Top 10 names alone | 10 | 80% |

So any company total or party lean is, in practice, its richest person.
"Rank and file" needs its own view or it is invisible.

Two blockers:

- **Names are not people.** Alex Karp appears under eight spellings, Larry
  Ellison four, Marc Andreessen and Keith Rabois two each. Tiers computed on
  names are wrong at exactly the top, where it matters.
- **Giving does not reveal an AI position.** FEC data shows which party or
  candidate received money. It shows an AI stance only when the recipient is
  an AI-specific committee (Leading the Future, Public First and a few
  others). A small donor giving $50 to a Democrat through ActBlue tells us
  party, not "pro safety".

Needed: resolve identities for the top few hundred people (not all 15,000);
define tiers after that; and replace "pro safety" with a few concrete policy
questions (for example: federal preemption of state AI laws, liability for
developers, export controls) on which committees and organizations can be
classified from their own public statements, with sources.

## 4. Company matching has never been measured

There are 447 employer spellings for 37 companies on the FEC side, and 4,080
candidate spellings never reviewed. Nobody has measured how much is missed
(recall) or wrongly included (precision). 49 companies are tracked for
lobbying but not for campaign money, and some obviously salient ones (xAI,
Scale AI, Founders Fund, other venture firms) need a deliberate in-or-out
decision.

Needed: a written rule for which companies are in scope, the FEC queue
reviewed for the top spellings by dollars, and a simple recall check (for
each company, how much money sits in near-miss employer strings).

## Things not on Wade's list that matter as much

- **Scope: "tech money" or "AI money"?** The four points above are mostly
  about AI politics. Crypto (Fairshake) is the largest tech-adjacent money in
  the data. Decide whether it is in, out, or a separate section.
- **Connecting lobbying to giving.** Both now share one company list, but no
  page shows a company's lobbying next to its people's giving.
- **What counts as quotable.** The FEC audits in this folder set a high bar
  for individual numbers. The same discipline is needed for any claim about
  "who is on which side".
- **Keeping it current.** Refreshes are manual. Q3 lobbying reports are due
  2026-10-20 and the election is 2026-11-03.

## Suggested order

1. **Committee registry** (about 20 committees: AI, tech and crypto super
   PACs and their affiliates). Small, fast, and produces the Leading the
   Future picture immediately, including organization money and spending.
2. **People registry and method.** Build the candidate list three ways and
   merge: (a) everyone giving $100,000+ to registry committees; (b) everyone
   giving $250,000+ anywhere whose employer or occupation text looks
   tech-related; (c) outside lists (tracked-company leadership, AI lab
   founders, major venture investors). Then resolve identities for those
   people only.
3. **Tier split** on every total: named megadonors versus everyone else.
4. **Stance questions.** Agree the concrete policy questions, then classify
   registry committees and organizations.
5. **Company alias audit** on the FEC side.

Steps 1 and 2 answer most of what is being asked in Washington right now.
Step 5 improves numbers that are, by dollars, already dominated by people
steps 1 and 2 will cover by name.
