# Reviewed campaign questions

Each top-level JSON file pins one question to specific original FEC reports,
employer aliases and source-reviewed relationships. It is not a universal donor
lookup. Start with the [developer guide](../../../FEC_DEVELOPER_GUIDE.md) and
[old/new validation](../../../FEC_METHODOLOGY_VALIDATION.md).

- `nvidia_el_sayed.json`: one campaign account, six latest reports through
  July 15, 2026; $33,500 before refunds.
- `meta_delbene_full_reports.json`: one campaign account, seven full reports
  through July 15, 2026; $4,925 before refunds.
- `meta_delbene_reviewed_allocation.json`: one four-record example from the
  2025 third-quarter report; $25 already contained in the full Meta result.

All three net amounts are unknown. Do not add overlapping reports together.
The broader site's bulk totals use a different counting basis.

`sources` pins complete file hashes. `latest_file_numbers` pins replacement
selection. Every `reviews` entry pins its exact source-row hash and states a
role, relationship, evidence and rationale. Reviewed fields cannot rewrite
filed amounts, employers or dates. Missing or changed dependencies stop the
build. The cycle must be an even integer year matching report-header coverage;
an older transaction date or election designation alone does not invalidate a
record in that report.

`review_evidence/` preserves address-free family checks supporting the manual
decisions. `validation/` preserves exact-ID old/new comparisons and reproduction
commands. Neither folder supplies automatic donor matching rules.

Build from the project root with:

```powershell
python -m scripts.build_candidate_attribution --manifest data/reference/attribution/nvidia_el_sayed.json --check-current
```

`--check-current` compares the processed report inventory and latest IDs; it
does not certify all current accounts, unprocessed filings or complete giving
through today. A new filing requires source review before updating a manifest.
Report builds and static-site generation do not publish the site.
