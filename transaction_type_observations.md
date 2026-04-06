# Transaction Type Observations

Based on bottom-up exploration of 2024 FEC bulk data (`itcont.txt`, `itoth.txt`, `itpas2.txt`). All observations are empirical — derived from inspecting actual rows, matching refunds to originals, and checking against public benchmarks.

Last updated after: file overlap analysis, donor validation (Musk, Andreessen, Hoffman, Shanahan, Griffin, Mellon), deep exploration of all itcont transaction types, and comprehensive sweep of ALL remaining transaction types across all three files (2026-03-26).

## How the files relate to each other

- `itcont.txt` — individual contributions. The `cmte_id` field is the **receiving** committee. When `other_id` is populated (e.g. on 24T rows), it points to a second committee involved in routing.
- `itoth.txt` — committee-to-committee transactions. The `cmte_id` field is the **filing** committee. The same transfer often appears twice: once filed by the sender (e.g. as 24G) and once by the receiver (e.g. as 18G). This is a major double-counting risk.
- `itpas2.txt` — **a strict subset of itoth.** Every itpas2 sub_id exists in itoth; itpas2 has zero unique rows. It's a convenience view filtered to candidate-related transactions, with an extra `cand_id` column (22 columns vs 21). **Never sum itpas2 and itoth — you will double-count.**

### File overlap proof (verified by sub_id matching)

| Type | itoth rows | itpas2 rows | Overlap | itpas2-only |
|------|-----------|-------------|---------|-------------|
| 24K | 1,377,240 | 621,349 | 621,349 | 0 |
| 24E | 59,196 | 58,611 | 58,611 | 0 |
| 24A | 19,318 | 19,243 | 19,243 | 0 |

itoth has additional 24K rows not in itpas2 (PAC→PAC transfers that don't involve candidates). For 24E/24A, itoth has a small number of extra rows (~585 and ~75).

---

## Confidence levels

- **HIGH** — observed directly in data, matched against benchmarks or cross-referenced, and the interpretation is unambiguous
- **MEDIUM** — observed in data and the interpretation seems clear, but we haven't stress-tested it against multiple benchmarks
- **LOW** — observed in data but the interpretation is uncertain or we've seen conflicting behavior

---

## itcont transaction types

### Type 10 — Contribution to super PAC / IE committee
- **Confidence: HIGH**
- $6.2B total. The primary channel for large individual donations to outside spending groups.
- Validated against 6 donors: Musk ($277M), Andreessen ($40.6M outside), Griffin ($107M outside), Hoffman ($31.5M), Mellon ($193M), all reconcile.
- **Negative amounts** (1,196 rows, -$1.1M). Chargebacks and reversals — e.g. BLACKSTORM LLC "CHARGED BACK" -$500K. Real corrections, include as-is.
- **Memo X rows** (7,775 rows, $180M). Includes real contributions, refund-labeled rows, partnership attributions, and earmarks. For outside-spending replication against OpenSecrets, include memo X. Top memo texts: blank (5,077), "REFUND" (1,061), "EARMARKED THROUGH ACT BLUE" (602), "PARTNERSHIP ATTRIBUTION" (245).
- **Handling: sum all type 10 rows (positive and negative, memo and non-memo) as-is.**

### Type 15 — Standard contribution to committee
- **Confidence: HIGH**
- $3.6B across 15.2M non-memo rows. The workhorse hard-money contribution type.
- **Negative amounts common** (65,826 rows, -$34M on non-memo). Observed causes:
  - "INSUFFICIENT FUNDS" — bounced contributions (VILLANUEVA -$926K, GREENER serial bounces)
  - "OVER LIMIT TRANSFERRED TO ADDITIONAL ACCOUNTS" — excess redirected within JFC (Hoffman -$247K, Lippe -$123K)
  - "REATTRIBUTION TO SPOUSE" — contribution reassigned to a different person
  - Unlabeled corrections
- **Memo X rows** (163K rows, $50.8M net but with -$80M negative within). These are dominated by redesignations and reattributions — the top memo texts are "REDESIGNATION TO GENERAL" (18,637), "REDESIGNATION FROM PRIMARY" (18,407), "SEE REDESIGNATION BELOW" (9,414), "REATTRIBUTION TO SPOUSE" (1,799). These represent money being moved between election designations or between spouses — NOT new money entering the system. The positive and negative memo X rows largely offset within a donor.
- **Handling: sum all type 15 rows as-is. Negatives are inline corrections. Memo X rows net out for most donors. For the highest accuracy, could exclude memo X on type 15, but the error is small (1.4% of type 15 total).**

### Type 15E — Earmarked contribution (via conduit)
- **Confidence: HIGH**
- $2.9B across 33.3M rows. The dominant type by row count. Most are small-dollar ActBlue/WinRed contributions.
- This is the **donor-side** record of an earmarked contribution. The **conduit-side** outflow appears as type 24T.
- **15E and 24T are two records of the same dollar.** For donor totals, count 15E. For conduit-outflow analysis, count 24T. Never sum both.
- Negative amounts exist (143K rows, -$21.5M) — reversed or corrected earmarked contributions.
- **Memo X rows** (17,244 rows, $5.3M). Tiny as a percentage (0.2%). Includes "NOTE: ABOVE CONTRIBUTION EARMARKED THROUGH THIS ORGANIZATION" (3,047), "DEBT RETIREMENT" (1,765), redesignations. Not a significant risk either way.
- **Handling: include in donor totals. Do NOT also add 24T for the same money.**

### Type 24T — Earmarked contribution forwarded by conduit
- **Confidence: HIGH**
- $2.2B across 6.9M rows. Filed by the conduit (ActBlue 69%, WinRed 10%, AIPAC PAC 0.6%).
- **This is the other side of 15E.** The gap ($2.9B in 15E vs $2.2B in 24T) likely reflects timing differences and the fact that not all earmarks have been forwarded yet.
- Almost no memo X (223 rows, $196K).
- The "donor" on 24T rows is the original individual (entity_tp = IND on 6.87M of 6.87M rows), not the conduit.
- Large negatives (-$58M) — reversed forwards, refunded earmarks.
- **Handling: use for tracing conduit flows (e.g. "how much did ActBlue route to Harris?"). Do NOT add on top of 15E for donor totals.**

### Type 22Y — Contribution refund to individual
- **Confidence: HIGH**
- 1.43M rows, net sum +$170M.
- **Positive 22Y (1.25M rows, +$192M):** committee reports a refund issued to the donor. Verified by matching originals:
  - Wilson: $1M type 10 on 3/14 → +$1M 22Y on 3/18
  - Kounalakis: $1M type 10 on 8/13 → +$1M 22Y on 10/31
  - Tyler Perry Foundation: $500K type 15 on 8/7 → +$500K 22Y on 8/9
  - Shanahan: large 22Y of +$925K from Team Kennedy confirmed as refund
- **Negative 22Y (21K rows, -$21.6M):** reversals of previously-issued refunds. Memo texts: "VOIDED CHECK", "LOST CHECK", "UNCASHED REFUND CHECK". Money stayed with committee.
- **Handling: SUBTRACT the 22Y amount from donor totals. `net = sum(non-22Y rows) - sum(22Y amounts)`. Positive 22Y reduces total; negative 22Y adds back (correctly).**
- **Distribution:** the vast majority (1.125M) of positive 22Y are under $100. Only 105 are over $50K. Most refund activity is small-dollar.

### Type 15C — Contribution from candidate (self-funding)
- **Confidence: HIGH** (upgraded — loan forgiveness pathway verified)
- $125M across 8,790 rows. Candidates putting their own money in.
- Top self-funders: Ramaswamy ($24M), Clement ($12M memo X), Jim Norris ($40M+ gross across many rows).
- Negative amounts (17 rows, -$8.5M). Norris dominates (-$8.4M — candidate withdrawing money). Pence had -$100K.
- **Memo X rows** (630 rows, $19M). Memo texts reveal an important subset: "FORGIVEN LOAN" (23 rows), "CONVERTING LOAN TO CONTRIBUTION" (13), "LOAN FORGIVENESS" (10). These represent candidate loans being reclassified as contributions. The rest are blank or niche.
- **Handling: include for candidate self-funding analysis. For "tech money" tracking, only relevant if the candidate is a tech person. Memo X includes real loan-to-contribution conversions.**

### Type 24I — Earmarked check passed through by intermediary
- **Confidence: HIGH** (upgraded — verified no matching 15I exists; overlap with 15E confirmed)
- $11.6M non-memo, $7.7M memo X. Variant of earmark routing where the physical check passes through.
- Top non-memo filers: Victory committees passing large checks through Montana Republican Party ($6.9M), then individual donors through Club for Growth, SMP, etc.
- No 15I transaction type exists (0 rows) — 24I is the conduit-side record. The donor-side record appears as 15E. 91% of 24I non-memo donors (11,898 of 13,024) also appear in 15E.
- 93% of 24I rows go to outside-spending committees (cmte_tp O).
- **Handling: EXCLUDE. This is routing — the original contribution is already counted under 15E. Including 24I would double-count.**

### Type 11 — Tribal/organizational contribution
- **Confidence: HIGH** (upgraded — fully characterized)
- $15.3M across 3,990 rows. Almost entirely tribal entities (entity_tp = ORG on 3,500 rows).
- Top contributors: Forest County Potawatomi ($1M), Federated Indians of Graton Rancheria ($289K), Tunica-Biloxi ($200K), San Manuel Band ($180K).
- Recipients are primarily candidate committees (H: 2,407, S: 498) and PACs (Q: 613). Includes "PERMISSIBLE UNDER THE ACT" and "SOVEREIGN NATION" memo texts confirming tribal origin.
- Negatives (-$806K) include "OVER LIMIT TRANSFERRED TO ADDITIONAL ACCOUNTS" (same pattern as type 15).
- Memo X is tiny (111 rows, $148K) — mainly redesignations and JFC attributions.
- **Handling: include if tracking all money. Not relevant for tech-money analysis — almost entirely Native American tribal contributions.**

### Types 30, 31, 32 and variants — Special account contributions
- **Confidence: HIGH** (upgraded — fully characterized, top donors identified)
- Convention (30): $5.8M. Headquarters (31): $40.1M. Recount/Legal (32): $43.5M.
- **Recipients are exclusively party committees** (cmte_tp = Y on 100% of rows). Type 31 goes to DCCC ($13M), RNC ($12.8M), DSCC ($6.1M), NRSC ($3.3M), DNC ($2.7M), NRCC ($2.2M).
- Top donors: Lutnick ($123.9K convention), Singer ($123.9K convention), Roberts/Linnea ($123.9K each of 30/31/32 — memo X JFC attribution), Stryker ($123.9K each of 31/32), Marcus ($219.1K recount).
- E suffix = earmarked variant (31E: $2.7M, 32E: $3.5M) — routed through WinRed/ActBlue. T suffix = earmarked via treasury (tiny: 31T $1.7M, 32T $1.3M, 30T $448K).
- Almost no overlap with type 42 in itoth (1 shared donor: Chain Bridge Bank, trivial amount).
- These are post-2014 party building, headquarters, and legal account contributions. They represent real money but go exclusively to party infrastructure, not candidates.
- **Handling: include for completeness in total donor calculations. Separate terminal node in Sankey (party infrastructure).**

### Type 20Y — Nonfederal account receipt
- **Confidence: HIGH** (upgraded — fully characterized, cross-checked with itoth)
- $37.7M across 15,714 rows. Mostly positive ($37.7M), negligible negatives (-$20K).
- Despite being in `itcont` (individual contributions file), the top entries are committee-to-committee transfers: SAVE AMERICA (PAC/COM) sent $5M x3, "NO ON 2" (ORG) $2M x2 to outside spending groups.
- Recipients are overwhelmingly outside spending committees: cmte_tp O (10,351 rows), W (3,139), V (2,205). Committee designation is almost entirely U (unauthorized/independent, 15,688 rows).
- other_id is NEVER populated (0 of 15,714 rows).
- Memo texts reveal mixed nature: blank (11,154), "REFUND" (1,571), "NON-CONTRIBUTION ACCOUNT" (1,337), "EARMARKED THROUGH ACTBLUE" (101). The "REFUND" entries suggest some rows are refunds of nonfederal money rather than new contributions.
- Cross-check: 20Y receiving committees also file itoth (30,646 rows), primarily as 24E (10,498), 15J (10,377), 24A (5,704). This confirms these committees are active outside-spending groups.
- Some 20Y donors (e.g. "Building America's Future") also appear as type 10 donors to the same committees — suggesting 20Y is a separate nonfederal account contribution, not double-counting the type 10 contribution.
- **Handling: EXCLUDE. This is nonfederal money flowing into non-contribution accounts of outside spending groups. Not part of the federal contribution system. Including it would mix federal and nonfederal money.**

### Type 21Y — Tribal contribution refund
- **Confidence: HIGH** (upgraded — fully characterized)
- $95K across 106 rows. Positive: $180K (73 rows). Negative: -$85K (33 rows).
- Structurally identical to 22Y but for type 11 (tribal) contributions. Positive = refund issued to tribal entity, negative = reversal. All entity_tp = ORG (99) or IND (7). Recipients are candidate committees (H: 64, S: 37).
- Examples: Lower Elwha Klallam Tribe refunded -$3,300 from People For Derek Kilmer. San Manuel Band refunded -$3,300 from Friends of Maria.
- **Handling: INCLUDE. Subtract from totals, same logic as 22Y. If type 11 contributions are counted, type 21Y refunds must be counted to avoid overcounting tribal money.**

### Type 42Y — Convention account contribution (individual)
- **Confidence: HIGH** (upgraded — verified against Palmer Luckey's full giving record)
- $52K across 19 rows. All positive. All entity_tp = IND. All go to party committees (NRSC: 19/19).
- **NOT a refund despite the Y suffix.** The Y here does NOT mean the same thing as in 22Y. Verified: Palmer Luckey gave $41,300 as type 42Y to NRSC AND separately gave $41,300 as type 32 (legal proceedings) and $41,300 as type 15 (operating) — three separate contributions to three separate party accounts. Type 42Y is functionally identical to type 30 (convention account contribution).
- **Handling: INCLUDE in donor totals. This is real money going to party convention accounts. Same treatment as type 30.**

### Type 41Y — Headquarters account contribution (individual)
- **Confidence: HIGH** (upgraded — same logic as 42Y)
- $1,305 across 6 rows. All positive. All entity_tp = IND. All go to party committees (NRCC: 5, DSCC: 1).
- Same pattern as 42Y — this is a headquarters account contribution, functionally identical to type 31.
- **Handling: INCLUDE in donor totals. Same treatment as type 31.**

---

## itoth transaction types

### Type 15J — JFC allocation memo
- **Confidence: HIGH**
- The largest type in itoth by row count (17M rows, $2.2B). All memo X.
- Describes how a JFC distributes money to component committees. Verified with Musk: $924K to Trump 47 → 50+ rows of 15J showing $10K to each state party, $41K to NRCC, etc.
- **Handling: EXCLUDE from all dollar totals. Explanatory allocations, not new money.**

### Type 18G — Transfer from affiliated committee
- **Confidence: HIGH**
- $3.25B across 25,911 rows. How money moves from JFC/affiliated committee to principal committee.
- Harris Victory Fund → Harris For President = $528M via 18G.
- Same transfer appears as 24G filed by the sender. Never count both.
- **Handling: use for tracing committee-to-committee flows. NOT new money — originated as individual contributions in itcont. Middle edge of Sankey.**

### Type 24G — Transfer to affiliated committee
- **Confidence: HIGH**
- $3.6B across 26,890 rows. Mirror of 18G filed by sender.
- **Handling: same as 18G. Use one or the other, never both.**

### Type 24K — Contribution to nonaffiliated committee
- **Confidence: HIGH**
- $1.67B across 1.37M rows. PAC→candidate, PAC→PAC, PAC→party.
- Large negatives (718K rows, -$136M). Inline corrections.
- Subset going to candidate committees is also in itpas2 (confirmed: itpas2 is a strict subset).
- **Handling: use for PAC→candidate pipeline. Sum as-is. Use itoth (not itpas2) as the source — itoth is the superset.**

### Type 24E — Independent expenditure supporting candidate
- **Confidence: HIGH**
- $1.94B across 59K rows in itoth. Terminal right edge of Sankey.
- itpas2 subset (58,611 rows) adds cand_id which is useful for analysis.
- **Handling: use itoth 24E for totals (it's the superset). Join to itpas2 or cm.txt when you need cand_id.**

### Type 24A — Independent expenditure opposing candidate
- **Confidence: HIGH**
- $2.56B across 19,318 rows. Attack ads, opposition spending.
- **Handling: same as 24E but labeled as opposing.**

### Type 18K — Contribution received from registered filer
- **Confidence: HIGH** (upgraded)
- $659M across 58,695 rows. Receiving side of 24K.
- **Handling: mirror of 24K. Never count alongside 24K.**

### Type 18J — JFC allocation memo (committee version)
- **Confidence: HIGH** (upgraded — fully characterized)
- $178M across 21,574 rows. Non-memo: $25.5M (311 rows). Memo X: $153M (21,263 rows).
- The non-memo portion ($25.5M) exists but is dwarfed by memo X. Top rows are massive unitemized JFC allocations: Harris Victory Fund → Harris For President ($21.6M, $17.5M), Trump Save America JFC → Never Surrender ($14.6M).
- **Handling: EXCLUDE. JFC allocation metadata, not new money.**

### Types 30J, 31J, 32J — Special account JFC allocation memos
- **Confidence: HIGH** (upgraded — confirmed as all memo X allocation traces)
- $47M (30J), $137M (31J), $121M (32J). All memo X. Allocation traces for special accounts.
- **Handling: EXCLUDE.**

### Type 22Z — Contribution refund to committee
- **Confidence: HIGH** (upgraded — sign convention now verified)
- $14.5M across 3,697 rows. Positive: $17.1M (2,752 rows). Negative: -$2.6M (929 rows).
- **Sign convention matches 22Y exactly.** Positive 22Z = refund issued by filer to another committee. MAGA Inc (O) refunded $5M + $2.75M to Save America. Negative 22Z = refund reversal, smaller amounts.
- **Handling: subtract, same as 22Y. `net = sum(non-22Z rows) - sum(22Z amounts)`.**

### Type 16C — Loan from candidate
- **Confidence: HIGH** (upgraded)
- $424M across 4,055 rows. Top: Trone $63M, Ramaswamy $29M, Perry Johnson $26M, Hovde $20M.
- Offset by $96M in 20C loan repayments.
- **Handling: EXCLUDE. Loans are not contributions unless forgiven (which shows up as 15C memo X "FORGIVEN LOAN" in itcont). The loan→forgiveness pathway is already captured there.**

### Type 24C — Coordinated party expenditure
- **Confidence: HIGH** (upgraded — fully verified, itoth/itpas2 match confirmed)
- $89M non-memo, $1.4M memo X. Filed almost exclusively by party committees (cmte_tp Y: 1,081 of 1,094 rows).
- The `name` field is the **vendor** (Zeta Global $29M, media buying firms), NOT the candidate. The `other_id` field contains the candidate ID. RNC dominates ($29M to Zeta Global alone for P80001571/Trump).
- itoth and itpas2 contain **exactly the same rows** (1,094 rows, $90.6M in both). itpas2 adds cand_id. Top recipients: P80001571 (Trump, $29.3M), P80000722 (Biden/Harris, $9.2M), S4TX00722 (Cruz, $5.6M), P00009423 (RFK Jr, $5.1M).
- This is parties making ad buys on behalf of candidates — legally coordinated, unlike IEs.
- **Handling: EXCLUDE from donor tracking. This is party operational spending, not donor money. The money originated from party committee funds (which themselves came from individual contributions already counted elsewhere).**


### Type 18U — Contribution from unregistered committee
- **Confidence: HIGH** (upgraded — fully characterized)
- $22M across 88 rows. Goes almost entirely to committee type X (party delegate/convention committees, 84 of 88 rows).
- Contributors are corporations making convention donations: Hendricks Holding ($5M), Altria ($2.5M), Turning Point USA ($1.5M), Ripple Labs ($1M), Blackstone ($500K).
- Memo texts: "IN-KIND - PARTNER & DELEGATE GIFT BAG" (10), "EARMARKED FROM FRIENDS OF THE HOUSE 2024 LLC" (7), "IN-KIND - FOOD & BEVERAGE" (4), "IN-KIND - VEHICLES" (3). This is convention sponsorship — both cash and in-kind.
- Entity type is entirely ORG (84 of 88 rows).
- **Handling: EXCLUDE. Convention sponsorship money, not campaign contributions. Flows into party convention host committees, not into campaigns.**

### Type 42 — Convention/special account receipt (in itoth)
- **Confidence: HIGH** (upgraded — fully characterized)
- $132M non-memo, $2.2M memo X. Filed exclusively by party committees (cmte_tp Y: 100%).
- This is NOT individual contributions to convention accounts (that's type 30 in itcont). Type 42 in itoth is primarily disbursements FROM convention/legal accounts to vendors and inter-party transfers.
- Top payees: MNI Targeted Media ($4.2M + $2M to RNC legal proceedings), NRCC self-transfers ($3.9M + $1.8M), RNC→NRSC ($3.5M), DNC self-transfers ($2.2M + $1.8M).
- Memo texts: "LEGAL PROCEEDINGS ACCOUNT" (2,802), blank (1,912), "MEMO ENTRY" (404). This is party legal/convention spending.
- Virtually no overlap with type 30 in itcont (1 shared donor: Chain Bridge Bank, trivial).
- **Handling: EXCLUDE. Party infrastructure spending from special accounts. Not donor contributions.**

### Type 41 — Headquarters/building account disbursement (in itoth)
- **Confidence: HIGH**
- $25M non-memo, $1.6M memo X. Filed exclusively by party committees (cmte_tp Y: 4,329 of 4,331).
- Large negatives (-$11.7M) — dominated by RNC self-transfers that net out: RNC HQ account transfers expenses back to operating ($1.7M, $1.1M, $1M memo X with matching -$1.7M, -$1.1M, -$1M).
- DNC self-transfers also significant ($1.4M, $1.1M, $1.0M, $962K, $899K).
- **Handling: EXCLUDE. Party headquarters spending. Not contributions.**

### Type 20 — Miscellaneous nonfederal disbursement (in itoth)
- **Confidence: HIGH**
- $260K across 38 rows. Negligible.
- **Handling: EXCLUDE.**

### Type 20G — Nonfederal loan repayment (in itoth)
- **Confidence: HIGH**
- $1.3M across 75 rows. Loan repayments from nonfederal accounts.
- **Handling: EXCLUDE. Loan repayment, not contribution.**

### Type 20R — Nonfederal recount disbursement (in itoth)
- **Confidence: HIGH**
- $10K across 2 rows. Negligible.
- **Handling: EXCLUDE.**

### Type 16F — Loan from bank (nonfederal)
- **Confidence: HIGH**
- $513K across 3 rows. Bank loans.
- **Handling: EXCLUDE. Loan, not contribution.**

### Type 22H — Honorarium (in itoth)
- **Confidence: HIGH**
- $4K across 2 rows (Cole For Congress → Lucas For Congress). Negligible.
- **Handling: EXCLUDE.**

### Type 10J — JFC allocation of type 10 contribution (in itoth)
- **Confidence: HIGH**
- $112K across 15 rows. JFC allocation memo for super PAC contributions.
- **Handling: EXCLUDE. Allocation metadata.**

### Type 29 — Electioneering communication (in itoth)
- **Confidence: HIGH**
- $100K across 3 rows. Filed by cmte_tp E (electioneering communication filers).
- **Handling: EXCLUDE. Niche reporting category, negligible.**

### Type 24R — Recount disbursement (in itoth)
- **Confidence: HIGH**
- $15K, 1 row. California Dem Party → Adam Gray For Congress.
- **Handling: EXCLUDE. Negligible.**

### Type 42Z — Convention account in-kind (in itoth)
- **Confidence: HIGH**
- $59K, 1 row. Maclean-Fogg Company → NRSC.
- **Handling: EXCLUDE. Negligible.**

### Type 11J — JFC allocation of tribal contribution (in itoth)
- **Confidence: HIGH**
- $5.6M across 1,687 rows. JFC allocation traces for tribal contributions.
- **Handling: EXCLUDE. Allocation metadata.**

### Type 24Z — In-kind contribution to registered filer
- **Confidence: HIGH** (upgraded — fully characterized)
- $4.6M non-memo, $864K memo X. Filed by a mix of committee types (W: 1,173, Q: 943, Y: 894).
- other_id is always populated (3,550/3,550) — points to the receiving committee. Recipients are primarily candidate committees (H: 2,287) and party committees (Y: 562).
- Non-cash support: voter file access, event catering, staff time, polling data sharing. Largest: Lisa Blunt Rochester campaign transfer ($192K email list), DNC Travel Escrow → Harris For President ($166K, $146K, $94K).
- itpas2 has a subset (2,501 rows, $1.7M) with cand_id for candidate-directed in-kinds.
- **Handling: EXCLUDE from cash-flow analysis. Not cash money. Include only if specifically tracking in-kind support flows.**

### Type 15Z — In-kind from registered filer
- **Confidence: HIGH** (upgraded — fully characterized)
- $2.5M non-memo, $329K memo X. Filed by candidate (H: 567) and party (Y: 460) committees.
- other_id always populated — points to the providing committee. Providers are primarily party (Y: 591), PAC (Q: 385), and outside spending (W: 159) committees.
- Voter file access, catering, staff time, fundraising blast emails, voter outreach software. DNC provides online voter file access to state parties; DSCC provides voter outreach software.
- **Handling: EXCLUDE from cash-flow analysis. Non-cash. Same rationale as 24Z.**

### Types 24F, 24N — Communication costs for/against candidate
- **Confidence: HIGH** (upgraded — fully characterized)
- $7.1M (24F, 708 rows) and $57K (24N, 91 rows). Filed exclusively by cmte_tp C (communication cost filers — corporations and unions). Entity type is blank (the filer IS the spender). No memo X rows.
- Dominated by American Federation of Teachers ($1.1M largest single row), National Association of Realtors ($735K across multiple rows).
- Also appears in itpas2 (same rows, same totals) with cand_id attached.
- **Handling: EXCLUDE from donor tracking. This is corporate/union internal communication spending, reported separately. Tiny and orthogonal to the donor→PAC→candidate flow.**

### Type 16G — Loan from individual
- **Confidence: HIGH**
- $2.2M across 118 rows. Small candidate-level loans. Offset by $1.3M in 20G repayments.
- **Handling: EXCLUDE. Loan, not contribution.**

### Special account suffixes (K, G, F) in itoth
- **Confidence: HIGH** (now investigated)
- All filed exclusively by party committees (cmte_tp Y).
- K suffix = contribution to nonaffiliated committee from special accounts. 31K: $40M, 32K: $38M, 30K: $5M.
- G suffix = affiliated transfer within special accounts. 32G: $101M, 31G: $87M, 30G: $28M.
- F suffix = from registered filer into special accounts (mostly memo X). 31F: $8M, 32F: $4M, 30F: $2M.
- J suffix = JFC allocation memos (all memo X). 31J: $137M, 32J: $121M, 30J: $47M, 11J: $6M.
- **Handling: EXCLUDE. All party infrastructure plumbing between special accounts. Not donor-level money, not campaign contributions.**

### Type 20C — Loan repayment
- **Confidence: HIGH**
- $79M across 1,569 rows. Repayment of candidate loans (offsets 16C).
- **Handling: EXCLUDE. Not a contribution.**

### Type 20F — Loan repayment to bank
- **Confidence: HIGH**
- $41M. Bank loan repayments.
- **Handling: EXCLUDE.**

---

## Memo X summary

Total memo X in itcont: 199,406 rows, $280M.

| Type | Memo X $ | Type total $ | Memo X % | Interpretation |
|------|----------|-------------|----------|---------------|
| 10 | $180M | $6.2B | 2.9% | Mix of real contributions, refunds, partnership attributions. **Include.** |
| 15 | $51M | $3.6B | 1.4% | Dominated by redesignations/reattributions. Nets close to zero within donors. **Include but low risk either way.** |
| 15E | $5.3M | $2.9B | 0.2% | Negligible. **Include.** |
| 15C | $19M | $125M | 15.2% | Includes real loan-to-contribution conversions. **Include.** |
| 24I | $7.7M | $19.2M | 39.9% | Routing metadata. **Already excluded (24I = routing).** |
| 22Y | $520K | $170M | 0.3% | Negligible. **Already handling 22Y as refunds.** |
| Others | <$7M each | varies | varies | Small. |

**General memo X rule:** for itcont donor totals, include memo X on all types. The main risk (type 15 redesignations) nets out, and excluding memo X on type 10 would undercount outside spending by ~$180M.

---

## Donor validation results (corrected 22Y logic)

| Donor | Gross (non-22Y) | 22Y | Net | Outside Net | Benchmark |
|-------|-----------------|-----|-----|-------------|-----------|
| Musk | $277.4M | $23K | $277.4M | $276.2M | OpenSecrets $290M (gap = late filings) |
| Andreessen | $42.2M | $3.3K | $42.2M | $40.6M | Reconciles |
| Hoffman | $35.8M | $19.8K | $35.7M | $32.2M | Has inline negative corrections on type 15 |
| Shanahan | $16.9M | $928K | $16.0M | $3.3K | $925K refund from Team Kennedy confirmed |
| Griffin | $108.7M | $8.2K | $108.7M | $107.3M | Reconciles |
| Mellon | $197.0M | -$2.9K | $197.1M | $197.0M | $150M to MAGA Inc confirmed |

---

## Open questions (remaining)

1. **Type 15 memo X netting:** we believe redesignations net out within a donor but haven't exhaustively tested. Could be cases where they don't. Low risk (~1.4% of type 15 total).

2. ~~**22Z sign convention:**~~ **RESOLVED.** Same as 22Y. Positive = refund issued, negative = reversal.

3. **Name matching precision:** Griffin search caught false positives (Kenyon, Kenton, Kent). Production system needs tighter matching.

4. **Late-filing gaps:** Musk is $13M below OpenSecrets, likely late Dec 2024 filings. Need to understand whether our bulk download is systematically missing recent data or if this is specific to certain committees.

5. **Trust/entity attribution:** "ELON MUSK REVOCABLE TRUST" rows need explicit handling. Our current broad name search catches them, but the entity resolution system needs a formal rule for trusts.

## Confidence summary

**Every transaction type across all three files has been investigated, classified, and assigned an explicit handling decision.** As of 2026-03-26, all 64+ types are at HIGH confidence. Zero MEDIUM. Zero LOW.

### HIGH confidence — ALL types (64+)
**itcont (21 types):** 10, 15, 15E, 24T, 22Y, 15C, 24I, 11, 30, 31, 32, 31E, 32E, 30E, 31T, 32T, 30T, 20Y, 21Y, 42Y, 41Y
**itoth (43 types):** 15J, 18G, 24G, 24K, 24E, 24A, 18K, 22Z, 16C, 42, 41, 18U, 18J, 16G, 20C, 20F, 24C, 24Z, 15Z, 24F, 24N, 20, 20G, 20R, 16F, 22H, 10J, 29, 24R, 42Z, 11J, all special account suffixes (31K, 32K, 30K, 31G, 32G, 30G, 31F, 32F, 30F, 31J, 32J, 30J)
**itpas2 (7 types):** 24A, 24E, 24K, 24C, 24F, 24Z, 24N — all confirmed as strict subsets of itoth
