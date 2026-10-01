from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from frontend import build_site
from scripts.validate_site import validate_candidate_components, validate_fec_freshness


class CandidateDisplayTests(unittest.TestCase):
    def test_explicitly_excluded_candidates_stay_hidden(self):
        rows = [
            {"cand_name": "Inactive candidate", "is_display_candidate": "False"},
            {"cand_name": "Another inactive candidate", "is_display_candidate": False},
        ]
        self.assertEqual(build_site.display_candidate_rows(rows), [])
        page = build_site.page_president({"cycle": 2026, "data_as_of": "2026-09-01"}, rows)
        self.assertNotIn("Inactive candidate", page)
        self.assertNotIn("Another inactive candidate", page)

    def test_mixed_flags_only_show_eligible_candidates(self):
        eligible = {"cand_name": "Active candidate", "is_display_candidate": "True"}
        rows = [eligible, {"is_display_candidate": "False"}]
        self.assertEqual(build_site.display_candidate_rows(rows), [eligible])

    def test_legacy_exports_without_display_flags_still_render(self):
        rows = [{"cand_name": "Legacy candidate"}]
        self.assertEqual(build_site.display_candidate_rows(rows), rows)


class ChartDisclosureTests(unittest.TestCase):
    def test_undated_contributions_are_disclosed(self):
        note = build_site.undated_contributions_note({
            "undated_tech_contribution_count": 3,
            "undated_tech_net_total": -125,
        })
        self.assertIn("3 tech-linked contribution rows ($-125 net)", note)
        self.assertIn("included in contribution totals but excluded from weekly charts", note)

    def test_no_disclosure_for_no_undated_rows(self):
        self.assertEqual(build_site.undated_contributions_note({}), "")


class DefinitionDisclosureTests(unittest.TestCase):
    def test_unreconciled_memo_sums_are_not_shown_as_receipt_totals(self):
        affected = {"total_itemized_receipts": "200", "has_unreconciled_memo_attributions": "True",
                    "former_campaign_account_total_itemized_receipts": "150",
                    "former_campaign_account_has_unreconciled_memo_attributions": True}
        clean = {"total_itemized_receipts": "50", "has_unreconciled_memo_attributions": "False"}
        self.assertEqual(build_site.account_receipts_value(affected), "Unreconciled")
        self.assertEqual(build_site.account_receipts_value(affected, "former_campaign_account_"), "Unreconciled")
        self.assertEqual(build_site.account_receipts_value(clean), "$50")
        self.assertEqual(build_site.account_receipts_total([affected, clean]), "Unreconciled")
        self.assertEqual(build_site.account_receipts_total([clean, clean]), "$100")
        note = build_site.candidate_money_note()
        self.assertIn("partnership and partner records", note)
        self.assertIn("adding parent and attribution rows", note)

    def test_former_account_breakdown_never_calls_its_total_direct_campaign_receipts(self):
        page = build_site.page_president({"cycle": 2026, "data_as_of": "2026-09-01"}, [{
            "cand_name": "Example", "total_itemized_receipts": 1100, "tech_itemized_receipts": 1100,
            "former_campaign_account_committee_count": 1, "former_campaign_account_committee_ids": "C12345678",
            "current_campaign_account_tech_itemized_receipts": 100,
            "former_campaign_account_tech_itemized_receipts": 1000,
            "former_campaign_account_total_itemized_receipts": 1000,
        }])
        self.assertIn("Former Campaign Accounts", page)
        self.assertIn("Former-Account Tech", page)
        self.assertIn("C12345678", page)
        self.assertIn("must not be described as donations received by the candidate", page)
        self.assertIn("omits odd-year special-election candidates", page)
        self.assertNotIn('class="sort-button">Total Receipts', page)

    def test_transaction_date_does_not_claim_source_completeness(self):
        note = build_site.note({"data_as_of": "2026-08-27", "latest_bulk_release_utc": "2026-09-18T00:00:00Z",
                                "source_check_status": "unknown"})
        self.assertIn("Latest tech-matched transaction: 2026-08-27", note)
        self.assertIn("not filing-completeness dates", note)
        self.assertIn("could not be verified", note)
        self.assertNotIn("2026-09-18", note)
        installed = build_site.note({"data_as_of": "2026-08-27", "latest_local_bulk_release_utc": "2026-09-17T00:00:00Z"})
        self.assertIn("Latest installed FEC bulk release: 2026-09-17", installed)

    def test_donor_table_keeps_both_denominators_explicit(self):
        page = build_site.page_donors({"cycle": 2026, "data_as_of": "2026-09-01"}, [])
        self.assertIn("including records with unmatched employers", page)
        self.assertIn("Mixed and Unknown committees are excluded", page)
        self.assertIn("Total includes only tech-matched records", page)

    def test_methodology_discloses_nonindividual_records_and_unmatched_refunds(self):
        page = build_site.page_methodology({"cycle": 2026, "data_as_of": "2026-09-01"})
        self.assertIn("partnerships, organizations, and other nonindividual sources", page)
        self.assertIn("refund with a blank or unmatched employer", page)
        self.assertIn("not a fully reconciled donor ledger", page)


class SourceFreshnessValidationTests(unittest.TestCase):
    def manifest(self, **changes):
        return {"all_local_bulk_files_present": True, "source_check_status": "current",
                "latest_local_bulk_release_utc": "2026-09-17T00:00:00Z", "bulk_sources": [], **changes}

    def test_unknown_status_cannot_pass_as_current_without_per_file_errors(self):
        errors = []
        validate_fec_freshness({"cycle": 2026}, self.manifest(source_check_status="unknown"), errors)
        self.assertEqual(errors, ["2026 source freshness is not current: unknown"])

    def test_metadata_must_agree_with_installed_sources(self):
        errors = []
        validate_fec_freshness({"cycle": 2026, "source_check_status": "current",
                                "latest_local_bulk_release_utc": "2026-09-18T00:00:00Z"}, self.manifest(), errors)
        self.assertEqual(errors, ["2026 metadata latest_local_bulk_release_utc disagrees with source manifest"])

    def test_legacy_metadata_can_omit_new_fields(self):
        errors = []
        validate_fec_freshness({"cycle": 2026}, self.manifest(), errors)
        self.assertEqual(errors, [])


class CandidateComponentValidationTests(unittest.TestCase):
    def row(self):
        row = {"cand_id": "candidate"}
        for metric in ("total_itemized_receipts", "tech_itemized_receipts", "tech_itemized_contributions"):
            row.update({metric: "10", f"current_campaign_account_{metric}": "13",
                        f"former_campaign_account_{metric}": "-3"})
        return row

    def test_components_reconcile_even_with_signed_refunds(self):
        errors = []
        validate_candidate_components(2026, self.row(), errors)
        self.assertEqual(errors, [])

    def test_changed_missing_or_nan_components_fail(self):
        for value in ("5", "", "NaN"):
            row = self.row()
            row["former_campaign_account_tech_itemized_receipts"] = value
            errors = []
            validate_candidate_components(2026, row, errors)
            self.assertEqual(len(errors), 1)
            self.assertIn("tech_itemized_receipts", errors[0])


class AssetCacheTests(unittest.TestCase):
    def test_asset_urls_change_only_when_their_content_changes(self):
        def urls(page):
            return dict(re.findall(r'(?:src|href)="([^\"]*static/([^?\"]+)\?v=[a-f0-9]{12})"', page))

        with tempfile.TemporaryDirectory() as directory:
            assets = Path(directory)
            for name in ("site.css", "tables.js", "charts.js"):
                (assets / name).write_text("original " + name)
            with patch.object(build_site, "ASSET_ROOT", assets):
                render = lambda: build_site.shell("Test", "", prefix="../../../", include_charts=True)
                first = render()
                self.assertEqual(first, render())
                first_urls = {name: url for url, name in urls(first).items()}
                self.assertEqual(set(first_urls), {"site.css", "tables.js", "charts.js"})
                self.assertTrue(all(url.startswith("../../../static/") for url in first_urls.values()))
                for changed_name in ("tables.js", "charts.js", "site.css"):
                    (assets / changed_name).write_text("updated " + changed_name)
                    next_urls = {name: url for url, name in urls(render()).items()}
                    self.assertNotEqual(first_urls[changed_name], next_urls[changed_name])
                    for unchanged_name in set(first_urls) - {changed_name}:
                        self.assertEqual(first_urls[unchanged_name], next_urls[unchanged_name])
                    first_urls = next_urls

                index = build_site.page_site_index({2026: {"metadata": {}}})
                index_urls = {name: url for url, name in urls(index).items()}
                self.assertEqual(set(index_urls), {"site.css", "tables.js"})
                for name, url in index_urls.items():
                    self.assertEqual(url, first_urls[name].replace("../../../", "2026/"))


if __name__ == "__main__":
    unittest.main()
