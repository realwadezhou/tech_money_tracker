# Tech Employer Reference

This directory holds the curated employer-tagging files used to identify tech-linked donors.

Important caveat:

- This is a working classification layer, not a complete or authoritative universe of all tech companies, brands, subsidiaries, or aliases.
- Inclusion here means "currently recognized by this project," not "definitively a tech entity in every context."
- Exclusion here does not mean a company is non-tech; it may simply be missing, unmatched, or not yet reviewed.
- Any downstream analysis using these files should describe them as project-curated and incomplete.

Key files:

- `employer_matches_for_review.csv`
  Broad-search employer matches prepared for manual review
- `employer_matches_for_review_manual_mar31temp.csv`
  The current manually curated lookup consumed by the pipeline

These are small project reference assets, so they live under `data/reference/` rather than with the large FEC bulk files.
