import unittest
from unittest.mock import patch

import pandas as pd

from pipeline.build_frontend_exports import (
    build_company_partisan,
    build_company_payloads,
    build_weekly_by_company,
    build_weekly_by_recipient_bucket,
    build_weekly_by_recipient_party,
    build_weekly_totals,
    parse_dates,
    recipient_bucket,
)


def contributions(rows):
    defaults = {
        "transaction_dt": "01012026",
        "transaction_amt": 100,
        "net_amt": 100,
        "name": "DONOR",
        "cmte_id": "C1",
        "cmte_nm": "COMMITTEE",
        "cmte_tp": "H",
        "cmte_party_simple": "D",
        "tech_canonical_name": "Example",
        "is_tech_employer": True,
        "employer": "EXAMPLE",
        "state": "CA",
        "cycle": 2026,
    }
    return parse_dates(pd.DataFrame([defaults | row for row in rows]))


def company_payload(tech):
    companies = tech.groupby("tech_canonical_name")["net_amt"].sum().reset_index()
    companies = companies.rename(columns={"net_amt": "net_total"})
    partisan = pd.DataFrame({"tech_canonical_name": ["Example"]})
    committees = pd.DataFrame({
        "cmte_id": ["C1", "C2", "C3"],
        "party_dr": ["D", "D", "R"],
        "classification_source": ["party_field"] * 3,
    })
    _, payloads = build_company_payloads(tech, companies, partisan, committees)
    return payloads[0][1]


class WeeklyExportTests(unittest.TestCase):
    def test_undated_receipts_remain_in_totals_but_not_weekly_series(self):
        tech = contributions([
            {},
            {"transaction_amt": 25, "net_amt": -25},
            {"transaction_dt": "02302026", "transaction_amt": 80, "net_amt": 80},
            {"transaction_dt": "", "transaction_amt": 10, "net_amt": -10},
        ])
        self.assertEqual(len(tech), 4)
        self.assertEqual(tech["transaction_date"].isna().sum(), 2)
        self.assertEqual(tech["net_amt"].sum(), 145)
        for build in (
            build_weekly_totals,
            build_weekly_by_company,
            build_weekly_by_recipient_bucket,
            build_weekly_by_recipient_party,
        ):
            with self.subTest(export=build.__name__):
                weekly = build(tech, 2026)
                self.assertEqual(weekly["net_total"].sum(), 75)
                self.assertEqual(weekly["n_contributions"].sum(), 2)
                self.assertEqual(weekly["week_end"].tolist(), ["2026-01-04"])
                if "gross_positive" in weekly:
                    self.assertEqual(weekly["gross_positive"].sum(), 100)

    def test_refunds_do_not_inflate_gross_receipts(self):
        tech = contributions([
            {},
            {"transaction_amt": 25, "net_amt": -25},
            {"transaction_amt": -5, "net_amt": 5},
        ])
        weekly = build_weekly_totals(tech, 2026).iloc[0]
        self.assertEqual(weekly["net_total"], 80)
        self.assertEqual(weekly["gross_positive"], 105)
        donor = company_payload(tech)["top_donors"][0]
        self.assertEqual(donor["net_total"], 80)
        self.assertEqual(donor["gross_positive"], 105)


class CompanyPayloadTests(unittest.TestCase):
    def test_undated_only_company_has_totals_and_empty_chart(self):
        payload = company_payload(contributions([{"transaction_dt": None}]))
        self.assertEqual(payload["summary"]["net_total"], 100)
        self.assertEqual(payload["weekly_series"], [])
        self.assertEqual(payload["undated_tech_contribution_count"], 1)
        self.assertEqual(payload["undated_tech_net_total"], 100)
        self.assertEqual(payload["top_donors"][0]["net_total"], 100)

    def test_missing_committee_metadata_does_not_hide_receipts(self):
        payload = company_payload(contributions([
            {"cmte_id": "UNKNOWN", "cmte_nm": None, "cmte_tp": None},
        ]))
        self.assertEqual(len(payload["top_committees"]), 1)
        committee = payload["top_committees"][0]
        self.assertEqual(committee["cmte_id"], "UNKNOWN")
        self.assertEqual(committee["net_total"], 100)
        self.assertEqual(committee["recipient_bucket"], "other")
        self.assertEqual(committee["party_dr"], "Unknown")
        self.assertEqual(payload["top_donors"][0]["top_committee_amt"], 100)

    def test_top_committee_distinguishes_committees_with_identical_names(self):
        payload = company_payload(contributions([
            {"cmte_id": "C1", "net_amt": 60},
            {"cmte_id": "C2", "net_amt": 60},
            {"cmte_id": "C3", "cmte_nm": "DIFFERENT COMMITTEE", "net_amt": 100},
        ]))
        donor = payload["top_donors"][0]
        self.assertEqual(donor["top_committee_id"], "C3")
        self.assertEqual(donor["top_committee_amt"], 100)
        self.assertEqual(donor["top_committee"], "DIFFERENT COMMITTEE")


class PartisanExportTests(unittest.TestCase):
    def test_negative_company_party_subtotals_do_not_create_invalid_percentages(self):
        donor_class = pd.DataFrame({
            "name": ["DEM", "REP"],
            "donor_party": ["D", "R"],
            "pct_d": [1.0, 0.0],
            "classified_total": [100, 100],
        })
        for dem, rep, expected in [(-100, 200, None), (-100, -200, None), (0, 0, None), (80, 20, 80)]:
            with self.subTest(dem=dem, rep=rep):
                tech = contributions([
                    {"name": "DEM", "net_amt": dem},
                    {"name": "REP", "net_amt": rep},
                ])
                with patch("pipeline.build_frontend_exports.classify_donors", return_value=donor_class):
                    result = build_company_partisan(tech, pd.DataFrame()).iloc[0]
                self.assertEqual(result["donor_amt_total"], dem + rep)
                if expected is None:
                    self.assertTrue(pd.isna(result["pct_dem_by_donor"]))
                else:
                    self.assertEqual(result["pct_dem_by_donor"], expected)

    def test_hybrid_pac_and_outside_communicator_buckets(self):
        for code in ("V", "W", "E", "I", "O", "U"):
            with self.subTest(code=code):
                self.assertEqual(recipient_bucket(code), "outside_spending")
        for code in ("N", "Q"):
            self.assertEqual(recipient_bucket(code), "pac")
        self.assertEqual(recipient_bucket("C"), "communication_cost")


if __name__ == "__main__":
    unittest.main()
