import unittest

import numpy as np
import pandas as pd

from tools.lobbying_search.app import SearchIndex, entry_row


def index() -> SearchIndex:
    reports = pd.DataFrame([
        # filing_uuid, quarter, client, registrant, income, expenses
        ("r1", 20251, "META PLATFORMS, INC.", "META PLATFORMS, INC.", np.nan, 100.0),
        ("r2", 20251, "ACME CORP", "BIG FIRM LLC", 50.0, np.nan),
        ("r3", 20252, "ACME CORP", "BIG FIRM LLC", 60.0, np.nan),
        ("r4", 20252, "Acme  Corp", "OTHER FIRM", 10.0, np.nan),
    ], columns=["filing_uuid", "quarter", "client_name", "registrant_name", "income", "expenses"])
    reports["client_key"] = reports.client_name.str.upper().str.split().str.join(" ")
    reports["filing_type"] = "Q1"
    reports["filing_url"] = "https://lda.gov/x"
    reports["dt_posted"] = "2025-07-21T10:00:00-04:00"
    activities = pd.DataFrame([
        ("r1:1", "r1", "SCI", "Science", "Artificial intelligence policy and AI safety."),
        ("r2:1", "r2", "TRD", "Trade", "Tariffs; supply chain."),
        ("r2:2", "r2", "TAX", "Taxes", "He said the AI tax credit matters."),
        ("r3:1", "r3", "SCI", "Science", "AI chips and data centers."),
        ("r4:1", "r4", "SCI", "Science", "Data center permitting."),
    ], columns=["activity_id", "filing_uuid", "general_issue_code", "general_issue_code_display", "description"])
    return SearchIndex(reports, activities)


class LobbyingSearchTests(unittest.TestCase):
    def test_whole_word_does_not_match_inside_words(self):
        ix = index()
        # "said" and "chain" contain "ai" but must not count in whole-word mode.
        self.assertEqual(ix.term_mask("ai", "word").sum(), 3)
        self.assertEqual(ix.term_mask("ai", "substring").sum(), 4)

    def test_trend_counts_entries_and_distinct_normalized_clients(self):
        ix = index()
        result = ix.trend(["data center"], "word")
        self.assertEqual([q["id"] for q in result["quarters"]], [20251, 20252])
        # Two entries in Q2 ("data centers" plural counts) from "ACME CORP" and
        # "Acme  Corp", which normalize to one client.
        self.assertEqual(result["series"][0]["entries"], [0, 2])
        self.assertEqual(result["series"][0]["clients"], [0, 1])
        self.assertEqual(result["totals"]["clients"], [2, 1])

    def test_client_filter_limits_trend(self):
        result = index().trend(["ai"], "word", client="meta")
        self.assertEqual(result["series"][0]["entries"], [1, 0])
        self.assertEqual(result["totals"]["entries"], [1, 0])

    def test_matching_entries_highlights_original_text(self):
        ix = index()
        result = ix.matching_entries("artificial intelligence", "word")
        row = result["entries"][0]
        start, end = row["match"]
        self.assertEqual(row["snippet"][start:end], "Artificial intelligence")

    def test_clients_keeps_spellings_separate_and_sums_amounts(self):
        result = index().clients("acme")
        names = {n["client_name"]: n for n in result["names"]}
        self.assertEqual(set(names), {"ACME CORP", "Acme  Corp"})
        self.assertEqual(names["ACME CORP"]["income"], [50.0, 60.0])
        self.assertEqual(names["ACME CORP"]["expenses"], [None, None])
        self.assertTrue(index().clients("meta")["names"][0]["self_filed"])

    def test_invalid_regex_is_reported(self):
        with self.assertRaises(Exception):
            index().term_mask("(", "regex")

    def test_entry_row_handles_missing_match(self):
        ix = index()
        row = next(ix.entries.itertuples())
        result = entry_row(row, SearchIndex.pattern("zzz", "word"))
        self.assertEqual(result["match"], [0, 0])


if __name__ == "__main__":
    unittest.main()
