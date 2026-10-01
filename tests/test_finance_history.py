import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from pipeline.build_summaries import _candidate_receipt_links
from pipeline.fec.committee_history import load_converted_campaigns, parse_public_conversion


def conversion_page(cycle=2024, out_of_range=False, notice=True):
    context = json.dumps({"cycle": cycle, "cycleOutOfRange": out_of_range, "name": "NEW PAC"})
    text = f"<script>window.context = {context};</script>"
    if notice:
        text += '<p>Financial data for this committee contains funds raised and spent under the former name <strong>OLD CAMPAIGN &amp; TEAM</strong> which, before it was converted, was a principal campaign committee. Before its conversion this committee was authorized by candidate <strong><a href="/data/candidate/P80001571">TRUMP, DONALD J.</a></strong>.</p>'
    return text


class PublicConversionTests(unittest.TestCase):
    def test_reads_official_candidate_id_and_former_name_for_rendered_cycle(self):
        result = parse_public_conversion(conversion_page(), 2024)
        self.assertEqual(result["cand_id"], "P80001571")
        self.assertEqual(result["former_committee_name"], "OLD CAMPAIGN & TEAM")
        self.assertEqual(result["former_candidate_election_year"], 2024)

    def test_out_of_range_page_cannot_restore_a_past_campaign_in_a_new_cycle(self):
        self.assertIsNone(parse_public_conversion(conversion_page(2024, True), 2026))
        self.assertIsNone(parse_public_conversion(conversion_page(2024, True), 2024))
        self.assertIsNone(parse_public_conversion(conversion_page(2024, False), 2026))

    def test_ordinary_committee_page_is_not_a_conversion(self):
        self.assertIsNone(parse_public_conversion(conversion_page(notice=False), 2024))

    def test_unrecognized_page_does_not_silently_erase_conversion_cache(self):
        with self.assertRaises(ValueError):
            parse_public_conversion("<html>Service unavailable</html>", 2024)

    def test_wrong_year_supplement_record_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "conversions.json"
            path.write_text(json.dumps({"cycle": 2026, "committees": [
                {"cmte_id": "C00828541", "cand_id": "P80001571", "converted_to_pac": True, "former_candidate_election_year": 2024},
                {"cmte_id": "C00111111", "cand_id": "H6CA01111", "converted_to_pac": False, "former_candidate_election_year": 2026},
            ]}), encoding="utf-8")
            with patch("pipeline.fec.committee_history.converted_campaigns_path", return_value=path):
                self.assertTrue(load_converted_campaigns(2026).empty)

    def test_conflicting_former_candidates_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "conversions.json"
            path.write_text(json.dumps({"cycle": 2024, "committees": [
                {"cmte_id": "C00828541", "cand_id": candidate, "converted_to_pac": True, "former_candidate_election_year": 2024}
                for candidate in ["P80001571", "P00009423"]
            ]}), encoding="utf-8")
            with patch("pipeline.fec.committee_history.converted_campaigns_path", return_value=path):
                with self.assertRaises(ValueError):
                    load_converted_campaigns(2024)


class HistoricalAttributionTests(unittest.TestCase):
    def setUp(self):
        self.candidates = pd.DataFrame({"cand_id": ["candidate", "other"]})
        self.linkage = pd.DataFrame([
            {"cand_id": "candidate", "cmte_id": "converted", "cmte_tp": "N", "cmte_dsgn": "D"},
            {"cand_id": "candidate", "cmte_id": "shared", "cmte_tp": "P", "cmte_dsgn": "P"},
            {"cand_id": "other", "cmte_id": "shared", "cmte_tp": "P", "cmte_dsgn": "P"},
            {"cand_id": "candidate", "cmte_id": "leadership", "cmte_tp": "N", "cmte_dsgn": "D"},
        ])
        self.committees = pd.DataFrame([
            {"cand_id": "candidate", "cmte_id": "converted", "cmte_tp": "N", "cmte_dsgn": "D"},
            {"cand_id": "other", "cmte_id": "shared", "cmte_tp": "P", "cmte_dsgn": "P"},
            {"cand_id": "candidate", "cmte_id": "leadership", "cmte_tp": "N", "cmte_dsgn": "D"},
        ])

    def test_confirmed_conversion_restores_receipts_and_preserves_shared_owner(self):
        conversions = pd.DataFrame({"cmte_id": ["converted"], "cand_id": ["candidate"]})
        result = _candidate_receipt_links(self.linkage, self.candidates, self.committees, conversions)
        self.assertEqual(set(map(tuple, result.to_numpy())), {("candidate", "converted"), ("other", "shared")})

    def test_conversion_cannot_invent_an_unlinked_former_candidate(self):
        conversions = pd.DataFrame({"cmte_id": ["converted"], "cand_id": ["other"]})
        result = _candidate_receipt_links(self.linkage, self.candidates, self.committees, conversions)
        self.assertEqual(set(map(tuple, result.to_numpy())), {("other", "shared")})


if __name__ == "__main__":
    unittest.main()
