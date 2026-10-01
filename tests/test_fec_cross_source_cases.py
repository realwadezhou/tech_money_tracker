"""Frozen original-filing counterexamples from the September methodology review.

These are narrow component or uncertainty assertions, not whole-campaign totals.
The source hashes and transaction IDs permit an independent source recheck.
"""
from decimal import Decimal
import json
from pathlib import Path
import unittest

from pipeline.fec.attribution import attribute_campaign
from pipeline.fec.campaign_reports import classify_records


FIXTURE = Path(__file__).parent / "fixtures/fec_attribution/rounds_cross_source_cases.json"


class OriginalRoundsCases(unittest.TestCase):
    def setUp(self):
        self.case = json.loads(FIXTURE.read_text())

    def test_jfc_allocations_count_donor_shares_without_adding_transfers(self):
        reviews = self.case["reviews"]
        ids = {r["record_id"] for r in reviews}
        records = [r for r in self.case["records"] if r["record_id"] in ids]
        aliases = {"ANDURIL INDUSTRIES": "anduril", "PALANTIR TECHNOLOGIES": "palantir"}
        for company in ("anduril", "palantir"):
            result = attribute_campaign(classify_records(records, reviews), aliases, target_company=company)
            with self.subTest(company=company):
                self.assertEqual(result.components["jfc_allocations"], Decimal("7000.00"))
                self.assertEqual(result.attributed_total, Decimal("7000.00"))
                self.assertEqual(result.components["direct_receipts"], 0)
                self.assertIsNone(result.net_total)

    def test_unexplained_negative_google_receipts_do_not_disappear_into_a_certified_total(self):
        records = [r for r in self.case["records"] if Decimal(r["amount"]) < 0]
        self.assertEqual(sum(Decimal(r["amount"]) for r in records), Decimal("-7000.00"))
        result = attribute_campaign(classify_records(records, []), {"GOOGLE LLC": "google"},
                                    target_company="google")
        self.assertIsNone(result.attributed_total)
        self.assertTrue(any(i.blocks_attributed_total for i in result.issues))

    def test_actual_filing_cents_survive_selection_without_bulk_amount_substitution(self):
        ids = {"1997182:AEB28BCE315604F2EB3D", "1997202:A1D4519538AAE47979E8"}
        records = [r for r in self.case["records"] if r["record_id"] in ids]
        for company, employer, expected in [("coinbase", "COINBASE", "143.56"),
                                            ("meta", "META PLATFORMS INC", "2082.03")]:
            result = attribute_campaign(classify_records(records, []), {employer: company},
                                        target_company=company)
            with self.subTest(company=company):
                self.assertEqual(result.components["direct_receipts"], Decimal(expected))
                self.assertEqual(result.attributed_total, Decimal(expected))


if __name__ == "__main__":
    unittest.main()
