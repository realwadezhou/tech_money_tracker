import unittest

import pandas as pd

from pipeline.common.paths import company_curated_path
from pipeline.tagging import lda_clients
from pipeline.tagging.registry import company_labels, load_companies
from tools.lobbying_search.app import SearchIndex


class CompanyRegistryTests(unittest.TestCase):
    def test_every_fec_company_is_in_the_shared_list(self):
        curated = pd.read_csv(company_curated_path(), dtype="string", na_filter=False)
        used = set(curated.canonical_name[curated.include == "TRUE"])
        self.assertEqual(used - set(load_companies().canonical_name), set())

    def test_reviewed_lobbying_names_are_valid(self):
        # load_curated rejects duplicate names, bad include values, and unknown companies.
        curated = lda_clients.load_curated()
        self.assertGreater(len(curated), 0)
        mapping = lda_clients.client_to_company()
        self.assertEqual(mapping["OPEN AI"], "openai")
        self.assertNotIn("NATIONAL PHILANTHROPIC TRUST", mapping)
        self.assertEqual(company_labels()["x_twitter_spacex"], "X / Twitter / SpaceX")

    def test_candidates_use_broad_searches_and_ignore_case_and_spacing(self):
        reports = pd.DataFrame([
            ("OpenAI OpCo, LLC", "OPENAI OPCO, LLC", "2025 Q1", None, 100.0),
            ("OPENAI  OPCO, LLC", "BIG FIRM", "2025 Q2", 40.0, None),
            ("TIKTOK INC.", "BIG FIRM", "2025 Q2", 10.0, None),
            ("ACME STEEL", "BIG FIRM", "2025 Q2", 5.0, None),
        ], columns=["client_name", "registrant_name", "quarter", "income", "expenses"])
        candidates = lda_clients.build_candidates(reports)
        self.assertEqual(candidates.client_name.tolist(), ["OpenAI OpCo, LLC", "TIKTOK INC."])
        first = candidates.iloc[0]
        self.assertEqual((first.n_reports, first.n_registrants, bool(first.self_filed)), (2, 2, True))
        self.assertEqual((first.income_usd, first.expenses_usd), (40.0, 100.0))
        # The "tiktok" search label belongs to the bytedance company.
        self.assertEqual(candidates.matched_searches.tolist(), ["openai", "bytedance"])

    def test_search_tool_filters_by_tracked_company(self):
        reports = pd.DataFrame([
            ("r1", 20251, "OPEN AI", "OPEN AI", "FIRM A", 10.0, None),
            ("r2", 20251, "OPENAI OPCO, LLC", "OPENAI OPCO, LLC", "OPENAI OPCO, LLC", None, 50.0),
            ("r3", 20251, "NATIONAL PHILANTHROPIC TRUST", "NATIONAL PHILANTHROPIC TRUST", "FIRM B", 5.0, None),
        ], columns=["filing_uuid", "quarter", "client_name", "client_key", "registrant_name", "income", "expenses"])
        reports["filing_type"] = "Q1"
        reports["filing_url"] = ""
        reports["dt_posted"] = "2025-04-20T10:00:00-04:00"
        activities = pd.DataFrame([
            ("r1:1", "r1", "SCI", "Science", "AI policy"),
            ("r2:1", "r2", "SCI", "Science", "AI policy"),
            ("r3:1", "r3", "TAX", "Taxes", "AI policy and charitable giving"),
        ], columns=["activity_id", "filing_uuid", "general_issue_code", "general_issue_code_display", "description"])
        index = SearchIndex(reports, activities,
                            company_map={"OPEN AI": "openai", "OPENAI OPCO, LLC": "openai"},
                            labels={"openai": "OpenAI"})
        self.assertEqual(index.trend(["ai"], "word", company="openai")["series"][0]["entries"], [2])
        self.assertEqual(index.companies(), [{"id": "openai", "name": "OpenAI", "names": 2, "reports": 2}])
        names = index.clients("", company="openai")["names"]
        self.assertEqual({n["client_name"] for n in names}, {"OPEN AI", "OPENAI OPCO, LLC"})
        self.assertEqual({n["company"] for n in names}, {"OpenAI"})


if __name__ == "__main__":
    unittest.main()
