# Reviewed transaction exclusions

`reviewed_exclusions.json` records individually verified duplicate receipt rows.
It is not a general rule that memo entries or equal-value gifts are duplicates.

Each decision records the cycle, source filename, non-address source identifiers
and a SHA-256 of all 21 source fields for both the excluded and retained original
row, official electronic filing URLs and hashes, review attribution, and
rationale. Version `2026-09-18-v3` excludes ten source-proven repeated-original
memos: seven totaling $39,736 in 2024 and three totaling $15,500 in 2026.
The underlying original receipts and signed adjustment children remain counted.

The core loader, chunked candidate rebuild, and candidate audit use the same
helper in `pipeline/fec/transaction_reviews.py`. It removes only the specified
source row in the specified cycle. A known row ID whose source fields changed
raises an error instead of silently reusing the review. Decimal formatting of
the same amount (for example `6600` versus `6600.0`) is equivalent. A new source
row ID is never automatically excluded, even if its amount or name matches.
Location fields are covered by the hash but are not reproduced in this reference.

Before any output is written, each complete source scan also verifies that an
observed excluded row's retained original is present exactly once and matches
its own signature and full-row hash. The dependency tracker observes raw rows
before employer or candidate filtering. Originals can occur before or after
their repeated memo, including in different chunks. A missing, repeated, or
changed original stops the run. An absent reviewed memo does not require its
original to be present.

The audit extract retains excluded records with their exclusion reason. Raw
source files are never edited. Re-audit these decisions after source revisions;
the helper does not implement amendment chains or discover other duplicates.

See `outputs/audit_20260918/attribution_review.md` for the review evidence and
unresolved candidate groups. Partnership memo attribution and an organization's
underlying receipt are a separate denominator-accounting problem; these
reviewed exclusions do not remove partnership attribution.
