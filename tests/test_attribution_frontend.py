from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from frontend import attribution, build_site


def report_fixture():
    return {
        "schema_version": 1,
        "report_id": "S6MI00418--nvidia",
        "_slug": "S6MI00418--nvidia",
        "cycle": 2026,
        "candidate": {"id": "S6MI00418", "name": "Example Candidate"},
        "company": {"key": "nvidia", "label": "NVIDIA"},
        "scope": {
            "description": "Employer-matched records in the listed campaign reports.",
            "coverage_start": "2025-01-01", "coverage_end": "2026-06-30",
            "committee_ids": ["C00902668"],
            "account_ownership": "Named campaign account only.",
            "source_completeness": "Two reports reviewed; other periods are not covered.",
        },
        "generated_at": "2026-09-19T12:00:00Z",
        "source_catalog_as_of": "2026-09-18",
        "sources": [{"file_number": "1234567", "url": "https://docquery.fec.gov/dcdev/posted/1234567.fec",
                     "sha256": "a" * 64, "coverage_start": "2026-04-01", "coverage_end": "2026-06-30"}],
        "components": {
            key: {"amount": value, "count": 1, "status": "supported_within_scope", "description": key}
            for key, value in {"direct_receipts": "100.25", "jfc_allocations": "200",
                               "partnership_attributions": "0", "signed_adjustments": "-25.25",
                               "refunds": "-50"}.items()
        },
        "combined_amount": "275", "combined_status": "resolved_for_scope",
        "combined_reason": "Distinct supported receipts and adjustments in the reviewed sources.",
        "net_amount": None, "net_status": "unresolved",
        "net_reason": "An additional possible refund has no resolved donor match.",
        "unresolved": [{"record_id": "R2", "reason": "Donor identity unresolved.", "amount": "-5", "scope": "refunds"}],
        "limitations": ["Employer strings are not verified employment."],
        "evidence": [{"record_id": "R1", "date": "2026-05-01", "amount": "-25.25",
                      "component": "signed_adjustments", "disposition": "included", "reason": "Signed adjustment.",
                      "source_url": "https://docquery.fec.gov/cgi-bin/fecimg/?1234567"}],
    }


class AttributionReportRenderingTests(unittest.TestCase):
    def test_catalog_snapshot_and_live_comparison_do_not_imply_complete_current_giving(self):
        report = report_fixture()
        page = build_site.page_attribution({"cycle": 2026}, report)
        self.assertIn("Saved source catalog as of:</strong> 2026-09-18", page)
        self.assertIn("No live catalog comparison was recorded", page)
        report["current_catalog_check"] = {"checked_at": "2026-09-19T13:00:00Z", "reports": 18, "status": "unchanged"}
        page = build_site.page_attribution({"cycle": 2026}, report)
        self.assertIn("Latest catalog comparison:</strong> 2026-09-19T13:00:00Z; 18 report records; unchanged", page)
        self.assertIn("API-processed reports", page)
        self.assertIn("does not establish complete current giving", page)
        self.assertNotIn("No live catalog comparison was recorded", page)

    def test_null_and_zero_are_distinct_and_signed_cents_survive(self):
        report = report_fixture()
        page = build_site.page_attribution({"cycle": 2026}, report)
        self.assertIn("Attributed receipts before refunds:</strong> $275", page)
        self.assertIn("Net amount after refunds:</strong> Unavailable", page)
        self.assertIn("$100.25", page)
        self.assertIn("-$25.25", page)
        self.assertIn('class="number">$0</td>', page)
        self.assertIn("Refunds:</strong> -$50", page)
        self.assertIn("1 linked records", page)
        self.assertIn("Donor identity unresolved.", page)
        self.assertIn("not a complete-cycle total", page)
        self.assertIn("Two reports reviewed; other periods are not covered.", page)
        report.update(net_amount="0", net_status="resolved_for_scope")
        self.assertIn("Net amount after refunds:</strong> $0", build_site.page_attribution({"cycle": 2026}, report))

    def test_unresolved_status_never_presents_component_sum_as_combined_amount(self):
        report = report_fixture()
        report.update(combined_amount="999999", combined_status="unresolved", combined_reason="Possible overlap.")
        report.update(net_amount="999949", net_status="unresolved")
        page = build_site.page_attribution({"cycle": 2026}, report)
        self.assertIn("Attributed receipts before refunds:</strong> Unavailable", page)
        self.assertIn("Possible overlap.", page)
        self.assertNotIn("$999,999", page)
        self.assertNotIn("$999,949", page)
        self.assertIn("$200", page)  # Supported components remain visible.

    def test_positive_and_negative_adjustments_are_explicit(self):
        self.assertEqual(attribution.amount_text("1.25", signed=True), "+$1.25")
        self.assertEqual(attribution.amount_text("-1.25", signed=True), "-$1.25")
        for unavailable in [None, "", "NaN", "Infinity", "not a number"]:
            self.assertEqual(attribution.amount_text(unavailable), "Unavailable")

    def test_source_and_download_links_preserve_proof_without_unsafe_anchors(self):
        report = report_fixture()
        page = build_site.page_attribution({"cycle": 2026}, report)
        self.assertIn('href="https://docquery.fec.gov/dcdev/posted/1234567.fec"', page)
        self.assertIn('href="../../data/attribution/S6MI00418--nvidia.json"', page)
        self.assertIn("a" * 64, page)
        report["candidate"]["name"] = '<script>alert("x")</script>'
        report["evidence"][0]["record_id"] = '<img src=x onerror=alert(1)>'
        for unsafe in ["javascript:alert(1)", "https://fec.gov.evil.test/source", "http://fec.gov/source",
                       "https://user@fec.gov/source", "https://fec.gov:444/source"]:
            report["sources"][0]["url"] = unsafe
            report["evidence"][0]["source_url"] = unsafe
            page = build_site.page_attribution({"cycle": 2026}, report)
            self.assertNotIn(f'href="{unsafe}"', page)
            self.assertIn("source link unavailable", page)
            self.assertNotIn('<script>alert', page)
            self.assertNotIn('<img src=x', page)
            self.assertIn("&lt;script&gt;", page)

    def test_refund_count_describes_links_and_context_rows_do_not_print_none(self):
        report = report_fixture()
        report["components"]["refunds"].update(amount=None, count=0, status="unresolved")
        report["evidence"][0].update(component=None, disposition="context")
        page = build_site.page_attribution({"cycle": 2026}, report)
        self.assertIn("Refunds:</strong> Unavailable (0 linked records; unresolved)", page)
        self.assertIn("<td>Context</td>", page)
        self.assertNotIn("<td>None</td>", page)

    def test_campaign_refunds_are_not_company_deductions_and_long_unresolved_lists_collapse(self):
        report = report_fixture()
        report["unresolved"] = [{"record_id": "", "scope": "refunds", "reason": "Refund coverage unknown."}] + [
            {"record_id": f"R{index}", "scope": "refunds", "amount": "100.00", "reason": "Donor relationship unknown."}
            for index in range(20)
        ]
        page = build_site.page_attribution({"cycle": 2026}, report)
        explanation = "They are not evidence that donors matched to NVIDIA received refunds."
        collapsed = "<details><summary>Inspect unresolved source records (21)</summary>"
        self.assertIn(explanation, page)
        self.assertIn("not deductions from this employer's attributed receipts", page)
        self.assertLess(page.index(explanation), page.index(collapsed))
        self.assertIn("<td>Coverage</td>", page)
        self.assertIn('<td class="number">Unavailable</td>', page)
        self.assertNotIn("<details open", page)
        report["unresolved"] = report["unresolved"][:20]
        self.assertNotIn("Inspect unresolved source records", build_site.page_attribution({"cycle": 2026}, report))


class AttributionNavigationTests(unittest.TestCase):
    def test_company_links_are_filtered_and_absent_cases_do_not_create_links(self):
        nvidia = report_fixture()
        other = deepcopy(nvidia)
        other.update(_slug="H0XX00000--a16z", company={"key": "a16z", "label": "a16z"})
        links = attribution.report_links([nvidia, other], "../../", "nvidia")
        self.assertIn('../../attribution/S6MI00418--nvidia/', links)
        self.assertNotIn("H0XX00000--a16z", links)
        self.assertEqual(attribution.report_links([nvidia], company_key="openai"), "")
        self.assertEqual(attribution.report_links(None), "")
        home_links = attribution.report_links([nvidia, other])
        self.assertIn('href="attribution/S6MI00418--nvidia/"', home_links)
        self.assertIn('href="attribution/H0XX00000--a16z/"', home_links)

    def test_home_company_and_data_pages_surface_available_reports(self):
        report = report_fixture()
        metadata = {"cycle": 2026, "data_as_of": "2026-06-30", "total_tech_linked_giving": 1234,
                    "tech_donor_count": 3, "tracked_company_count": 1, "committees_receiving_tech_money": 2}
        homepage = {"top_companies": [], "top_candidates": [], "top_political_bodies": [], "top_donors": []}
        home = build_site.page_home(metadata, homepage, [report])
        self.assertIn('href="attribution/S6MI00418--nvidia/"', home)
        self.assertIn("<dt>Employer-matched giving</dt><dd>$1,234</dd>", home)
        company = {"company": "nvidia", "slug": "nvidia", "top_donors": [], "top_committees": [],
                   "summary": {"net_total": 1234, "n_donors": 3, "n_contributions": 4, "n_committees": 2}}
        page = build_site.page_company(metadata, company, [report])
        self.assertIn('href="../../attribution/S6MI00418--nvidia/"', page)
        company["company"] = "openai"
        self.assertNotIn("S6MI00418--nvidia", build_site.page_company(metadata, company, [report]))
        self.assertIn('href="../attribution/"', build_site.page_data(metadata, [report]))
        self.assertNotIn('href="../attribution/"', build_site.page_data(metadata))

    def test_cycle_switch_keeps_existing_case_or_falls_back_to_available_section(self):
        report = report_fixture()
        bundle = {"companies": [], "candidate_state": [], "candidate_senate": [],
                  "candidate_house_district": [], "attribution_reports": [report]}
        dirs = build_site.collect_cycle_page_dirs(bundle)
        self.assertIn("attribution/S6MI00418--nvidia/", dirs)
        with patch.object(build_site, "CYCLE_PAGE_DIRS", {2026: dirs, 2024: {"", "attribution/"}, 2022: {""}}):
            self.assertEqual(build_site.resolve_cycle_target_rel_dir(2026, "attribution/S6MI00418--nvidia/"),
                             "attribution/S6MI00418--nvidia/")
            self.assertEqual(build_site.resolve_cycle_target_rel_dir(2024, "attribution/S6MI00418--nvidia/"), "attribution/")
            self.assertEqual(build_site.resolve_cycle_target_rel_dir(2022, "attribution/S6MI00418--nvidia/"), "")

    def test_optional_reports_load_by_safe_filename_and_reject_wrong_cycle(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertEqual(attribution.load_reports(root, 2026), [])
            (root / "attribution").mkdir()
            path = root / "attribution" / "S6MI00418--nvidia.json"
            report = report_fixture()
            report.pop("_slug")
            path.write_text(json.dumps(report), encoding="utf-8")
            self.assertEqual(attribution.load_reports(root, 2026)[0]["_slug"], "S6MI00418--nvidia")
            with self.assertRaisesRegex(ValueError, "schema or cycle"):
                attribution.load_reports(root, 2024)


if __name__ == "__main__":
    unittest.main()
