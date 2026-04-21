# Frontend Plan

## Goal

Build a low-bloat election-finance website that feels like a plain 1990s hand-made HTML site, while still supporting charts and many generated pages.

The site should be:

- mostly static HTML
- text-first
- fast to load
- free or nearly free to host
- free of trackers, analytics junk, ad tech, and unnecessary JavaScript
- generated from our existing data pipeline, with a small amount of frontend-specific derived data

## Style Rules

These are not suggestions. They are the design constraints.

- `Times New Roman` is the default body font
- tables use a monospace font, with bold header text
- background is `#FCF9EA`
- text is `#1A1A1A`
- links use `#6f4a86`, with visited links at `#50365f`
- Democratic and Republican color accents use `#5EABD6` and `#E14434`
- table headers use `#e3d8bd`
- no card UI
- no gradients
- no rounded-corner component chrome
- no icon packs
- minimal images
- minimal CSS
- JavaScript only where necessary for charts, table sorting, table filtering, or very light page behavior

The target is not "retro-themed." The target is "plain serious web document."

## Technical Direction

Preferred direction:

1. Generate static pages at build time
2. Host as a static site
3. Use minimal JavaScript for charts only
4. Keep page navigation as normal links to normal HTML pages

Likely stack:

- static site generation with Astro, used quietly as a build tool
- or, if Astro feels like overkill, a tiny custom generator later
- charting with Observable Plot or a similarly small self-hosted JS approach
- hosting on GitHub Pages or Cloudflare Pages

Important principle:

- users should click links and receive prebuilt HTML pages
- we should not build a JS-heavy single-page app

## Why Static Pages

Generating many pages is normal for this kind of site.

Examples:

- one page per company
- one page per candidate
- one page per district
- one page per state
- one page per major committee

This is preferable to fetching whole page views through client-side JavaScript.

Benefits:

- fast loads
- cheap hosting
- direct linking
- easy caching
- easy archival
- simpler code

## Proposed Site Structure

Top-level pages:

- `/`
- `/about/`
- `/methodology/`
- `/data/`
- `/companies/`
- `/candidates/`
- `/districts/`
- `/states/`
- `/committees/`

Generated pages:

- `/companies/[slug]/`
- `/candidates/[slug]/`
- `/districts/[slug]/`
- `/states/[slug]/`
- `/committees/[slug]/`

Possible homepage sections:

- last updated timestamp
- headline numbers
- top companies
- top recipients
- top candidates
- key charts
- methodology notes

## Page Templates

### Home

Plain document page with links, key numbers, and a few embedded charts.

### Candidate Page

Should answer:

- how much tech-linked money reached this candidate directly?
- which companies' employees gave?
- which major individual donors gave?
- which committees and PACs supported or opposed them?
- what caveats apply?

### Company Page

Should answer:

- how much did employees of this company give?
- how many donors?
- where did the money go?
- what is the donor partisan pattern?
- which committees received the most?

### District or State Page

Should answer:

- which candidates matter here?
- how much tech-linked money is flowing into the race?
- who are the main funders?
- how has that changed over time?

## Performance Philosophy

We should behave as if every kilobyte matters.

Rules:

- no third-party tracking scripts
- no external font loading
- no client-side framework runtime unless truly necessary
- no loading chart data for pages the user is not visiting
- no giant images
- no decorative media
- no generic UI libraries

Practical page-weight target:

- ordinary content pages should be very small
- chart pages may be larger, but should still remain modest and intentional

## Time-Series Display Rule

We keep validated aggregates intact, even if a small number of rows in the
2024 files fall before calendar year 2023.

Rule:

- aggregate totals should continue to use the full validated export
- time-series charts shown to users should default to a display window that
  starts on `2023-01-01`

This avoids silently discarding data while keeping the visible timeline
aligned with the practical start of the 2024 cycle.

## What Data We Already Have

Current outputs already support a first version of the frontend:

- `data/fec/derived/<cycle>/tech_company_summary.csv`
- `data/fec/derived/<cycle>/tech_donor_summary.csv`
- `data/fec/derived/<cycle>/committee_tech_receipts.csv`
- `data/fec/derived/<cycle>/tech_company_partisan.csv`
- `data/fec/derived/<cycle>/committee_party_classification.csv`

These are enough for:

- homepage headline numbers
- company pages
- top donors lists
- committee pages
- simple recipient charts

## What Data Is Not Yet Frontend-Ready

Some current outputs are either too broad or not shaped correctly for public pages:

- `committee_outbound_spending.csv` is currently too broad
- `tech_sankey_edges.csv` includes committee-to-candidate edges that are not yet clean enough for a public-facing core feature

We should not treat those as stable frontend primitives yet.

## Additional Pipeline Work We Probably Need

Yes, we will probably need more derived-data work, but not a whole new raw-data ingestion system.

The likely need is:

1. extend the existing pipeline to export frontend-ready JSON or smaller CSV files
2. add candidate-, district-, and state-level summary tables
3. create cleaner page-specific aggregates so pages do not have to compute large joins in the browser

This should be a thin presentation layer on top of the validated counting logic, not a second independent analysis pipeline.

## Recommended New Derived Outputs

### Frontend manifest / metadata

- site-wide `last_updated`
- cycle
- notes on data coverage

### Candidate summary

One row per candidate:

- candidate name
- office
- state
- district if applicable
- committee IDs
- direct receipts from tech-linked donors
- top tech companies
- top tech donors
- total tech donor count

### Candidate company breakdown

One row per candidate x company:

- candidate
- company
- amount
- donor count

### Candidate donor breakdown

One row per candidate x donor for major donors only:

- candidate
- donor
- amount
- employer / company tags

### District summary

One row per district:

- district slug
- state
- office
- major candidates
- total tech-linked direct receipts
- top companies
- top donors

### State summary

One row per state:

- total tech-linked giving into races in state
- top recipients
- top companies

### Time-series outputs

If we want trend charts:

- weekly receipts by candidate
- weekly receipts by company
- weekly receipts by state

## Build Strategy

Phase 1:

- create frontend-ready exports from current summary data
- build home page
- build companies index
- build company pages
- build methodology page

Phase 2:

- add candidate summary pipeline
- build candidate pages
- build district pages
- add time-series charts

Phase 3:

- refine committee spending logic
- add cleaner committee-to-candidate or race-level flow views

## Open Questions

- what exact entity should candidate pages key off of: candidate ID, principal committee, or merged race entity?
- what threshold defines a "major donor" for page display?
- what threshold defines a "tech-funded committee" for public presentation?
- should candidate pages include candidate photos at all?

Current instinct:

- key candidate pages off candidate ID where possible
- keep photos optional and probably omit them
- prefer text and charts over imagery

## Working Rule

Every line of CSS and JavaScript must justify its existence.
