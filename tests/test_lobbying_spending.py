import csv
from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from frontend.lobbying_spending import change, page, summarize
from pipeline.lda.build_spending import build_spending, company_quarters, quarter_complete, tag_reports
from scripts.validate_site import validate_lobbying_spending


def report(uid, client, registrant, quarter, income=None, expenses=None, posted="2025-07-25T10:00:00-04:00"):
    return {"filing_uuid": uid, "client_name": client, "registrant_name": registrant, "quarter": quarter,
            "income": income, "expenses": expenses, "filing_type": "Q1", "dt_posted": posted,
            "filing_url": f"https://lda.gov/filings/public/filing/{uid}/print/"}


REPORTS = pd.DataFrame([
    # OpenAI 2025 Q1: in-house 100 covers the 30 + 20 its firms reported.
    report("a", "OPENAI OPCO, LLC", "OPENAI OPCO, LLC", "2025 Q1", expenses=100.0),
    report("b", "OPENAI OPCO, LLC", "FIRM ONE", "2025 Q1", income=30.0),
    report("c", "Open  AI", "FIRM TWO", "2025 Q1", income=20.0),
    # OpenAI 2025 Q2: firms reported more than the company did.
    report("d", "OPENAI OPCO, LLC", "OPENAI OPCO, LLC", "2025 Q2", expenses=40.0),
    report("e", "OPENAI OPCO, LLC", "FIRM ONE", "2025 Q2", income=70.0),
    # Google 2025 Q1: two in-house filers (parent and subsidiary) add up.
    report("f", "GOOGLE CLIENT SERVICES LLC", "GOOGLE CLIENT SERVICES LLC", "2025 Q1", expenses=500.0),
    report("g", "WAYMO LLC", "WAYMO LLC", "2025 Q1", expenses=60.0),
    # Not a tracked name; a report with no amount; a quarter that is not complete yet.
    report("h", "ACME STEEL", "FIRM ONE", "2025 Q1", income=999.0),
    report("i", "WAYMO LLC", "FIRM TWO", "2025 Q2"),
    report("j", "OPENAI OPCO, LLC", "OPENAI OPCO, LLC", "2025 Q3", expenses=5.0, posted="2025-09-18T10:00:00-04:00"),
])
COMPANY_MAP = {"OPENAI OPCO, LLC": "openai", "OPEN AI": "openai",
               "GOOGLE CLIENT SERVICES LLC": "google", "WAYMO LLC": "google"}


class SpendingRuleTests(unittest.TestCase):
    def setUp(self):
        self.table = company_quarters(tag_reports(REPORTS, COMPANY_MAP)).set_index(["company_id", "year", "q"])

    def test_in_house_total_is_not_added_to_outside_firm_income(self):
        row = self.table.loc[("openai", 2025, 1)]
        self.assertEqual((row.in_house_expenses, row.outside_firm_income, row.total, row.basis),
                         (100.0, 50.0, 100.0, "in_house"))
        self.assertEqual((row.in_house_reports, row.outside_firm_reports), (1, 2))

    def test_larger_outside_firm_sum_is_used(self):
        row = self.table.loc[("openai", 2025, 2)]
        self.assertEqual((row.total, row.basis), (70.0, "outside_firm"))

    def test_separate_in_house_filers_add_up_and_blank_amounts_count_as_zero(self):
        self.assertEqual(self.table.loc[("google", 2025, 1)].total, 560.0)
        blank = self.table.loc[("google", 2025, 2)]
        self.assertEqual((blank.total, blank.basis, blank.outside_firm_reports), (0.0, "none_reported", 1))

    def test_untracked_names_are_left_out(self):
        self.assertEqual(set(self.table.index.get_level_values("company_id")), {"openai", "google"})

    def test_quarter_is_complete_only_after_its_filing_deadline(self):
        self.assertFalse(quarter_complete(2026, 2, date(2026, 7, 19)))
        self.assertTrue(quarter_complete(2026, 2, date(2026, 7, 20)))
        self.assertTrue(quarter_complete(2025, 4, date(2026, 1, 20)))
        self.assertFalse(quarter_complete(2026, 3, date(2026, 9, 18)))


class SpendingExportAndPageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.output = Path(cls.directory.name)
        cls.metadata = build_spending(cls.output, reports=REPORTS, company_map=COMPANY_MAP)
        cls.data = json.loads((cls.output / "spending.json").read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_export_marks_incomplete_quarter_and_lists_names(self):
        self.assertEqual([(q["id"], q["complete"]) for q in self.data["quarters"]],
                         [("2025 Q1", True), ("2025 Q2", True), ("2025 Q3", False)])
        openai = next(c for c in self.data["companies"] if c["id"] == "openai")
        self.assertEqual(openai["name"], "OpenAI")
        self.assertEqual(openai["client_names"], ["OPENAI OPCO, LLC", "Open  AI"])
        self.assertEqual([c["total"] for c in openai["quarters"]], [100.0, 70.0, 5.0])
        self.assertEqual(self.metadata["report_count"], 9)

    def test_csv_and_evidence_reconcile_with_the_json(self):
        with (self.output / "spending.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(sum(float(r["total"]) for r in rows), 100 + 70 + 5 + 560 + 0)
        with (self.output / "spending_reports.csv").open(newline="", encoding="utf-8") as handle:
            evidence = list(csv.DictReader(handle))
        self.assertEqual(len(evidence), 9)
        self.assertTrue(all(r["filing_url"].startswith("https://lda.gov/") for r in evidence))
        errors = []
        site = self.output / "site/lobbying/spending/data"
        site.mkdir(parents=True)
        for name in ("spending.json", "spending.csv", "spending_reports.csv"):
            (site / name).write_bytes((self.output / name).read_bytes())
        summary = validate_lobbying_spending(self.output / "site", errors)
        self.assertEqual(errors, [])
        self.assertEqual(summary["companies"], 2)

    def test_validator_catches_a_changed_total(self):
        site = self.output / "tampered/lobbying/spending/data"
        site.mkdir(parents=True)
        for name in ("spending.json", "spending_reports.csv"):
            (site / name).write_bytes((self.output / name).read_bytes())
        text = (self.output / "spending.csv").read_text(encoding="utf-8").replace("560.0,in_house", "561.0,in_house")
        (site / "spending.csv").write_text(text, encoding="utf-8")
        errors = []
        validate_lobbying_spending(self.output / "tampered", errors)
        self.assertTrue(errors)

    def test_page_uses_complete_quarters_only(self):
        summary = summarize(self.data)
        self.assertEqual(summary["year_labels"], {2025: "2025 Q1–Q2"})
        self.assertEqual(summary["all_companies"], {0: 660.0, 1: 70.0})
        html = page(self.data, [2026, 2024], explorer_available=False)
        self.assertIn("Latest complete quarter: 2025 Q2", html)
        self.assertIn("Not shown because reports were not yet due: 2025 Q3", html)
        self.assertIn("<td>OpenAI</td><td>AI</td>", html)
        self.assertIn('"sectors":[{"id":"ai","label":"AI"},{"id":"tech_giant","label":"Large tech companies"}]', html)
        self.assertIn('href="data/spending_reports.csv"', html)
        self.assertNotIn("AI lobbying explorer", html)
        self.assertIn("AI lobbying explorer", page(self.data, [2026], explorer_available=True))

    def test_unpublished_explorer_is_kept_out_of_the_site(self):
        from unittest.mock import patch
        from frontend import lobbying, lobbying_spending
        site = self.output / "switch"
        stale = site / "lobbying/data"
        stale.mkdir(parents=True)
        (stale / "explorer-data.js").write_text("stale", encoding="utf-8")
        (site / "lobbying/index.html").write_text("old explorer page", encoding="utf-8")
        (self.output / "explorer.json").write_text("{}", encoding="utf-8")
        with patch.object(lobbying, "EXPORT", self.output), patch.object(lobbying_spending, "EXPORT", self.output):
            self.assertFalse(lobbying.explorer_published())
            self.assertEqual(lobbying.lobbying_landing(), "lobbying/spending/")
            built = lobbying.build_lobbying_pages(site, [2026])
            with patch.object(lobbying, "PUBLISH_AI_EXPLORER", True):
                self.assertTrue(lobbying.explorer_published())
        self.assertEqual(built, {"explorer": False, "spending": True})
        self.assertFalse(stale.exists())
        self.assertIn('url=spending/', (site / "lobbying/index.html").read_text(encoding="utf-8"))
        html = (site / "lobbying/spending/index.html").read_text(encoding="utf-8")
        self.assertNotIn("AI lobbying explorer", html)
        self.assertNotIn('href="../"', html)

    def test_change_needs_both_quarters(self):
        self.assertEqual(change(150.0, 100.0), "+50%")
        self.assertEqual(change(50.0, 100.0), "-50%")
        self.assertEqual(change(50.0, None), "—")
        self.assertEqual(change(0.0, 100.0), "—")


if __name__ == "__main__":
    unittest.main()
