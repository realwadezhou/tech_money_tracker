"""Small in-memory regressions for dollar attribution and partisan evidence."""

import unittest
from unittest.mock import patch

import pandas as pd

from pipeline.classify_partisan import (
    CCL_COLS,
    CN_COLS,
    build_behavioral_committee_classification,
    classify_committees_from_party_field,
    classify_donors,
    load_candidate_party_table,
)
from pipeline.build_summaries import (
    _assign_party_label,
    _candidate_receipt_links,
    _meaningful_share,
    build_candidate_house_district_summary,
    build_candidate_race_summary,
    build_candidate_senate_summary,
    build_candidate_state_summary,
    build_committee_tech_receipts,
    build_tech_company_summary,
    build_tech_donor_summary,
)
from pipeline.fec.load import CM_COLS, ITCONT_COLS, ITPAS2_COLS, _filter_donor_contributions, tag_tech_donors
from pipeline.build_frontend_exports import records, tech_dominated_mask


def table(columns, rows):
    return pd.DataFrame([{**dict.fromkeys(columns, ""), **row} for row in rows], columns=columns)


class PartisanEvidenceTests(unittest.TestCase):
    def test_only_candidate_and_party_committees_use_filed_party(self):
        committees = table(CM_COLS, [
            {"cmte_id": "campaign", "cmte_tp": "H", "cmte_pty_affiliation": "DEM"},
            {"cmte_id": "party", "cmte_tp": "Y", "cmte_pty_affiliation": "REP"},
            {"cmte_id": "pac", "cmte_tp": "Q", "cmte_pty_affiliation": "REP"},
        ])
        result = classify_committees_from_party_field(committees).set_index("cmte_id")
        self.assertEqual(result.loc["campaign", "party_dr"], "D")
        self.assertEqual(result.loc["party", "party_dr"], "R")
        self.assertTrue(pd.isna(result.loc["pac", "party_dr"]))
        self.assertEqual(result.loc["pac", "classification_source"], "")

    def test_candidate_party_includes_other_election_years_in_cycle_master(self):
        candidates = table(CN_COLS, [
            {"cand_id": "senator", "cand_election_yr": "2028", "cand_pty_affiliation": "DEM"},
            {"cand_id": "house", "cand_election_yr": "2026", "cand_pty_affiliation": "REP"},
        ])
        with patch("pipeline.classify_partisan.load_candidate_master_table", return_value=candidates):
            parties = load_candidate_party_table(pd.DataFrame(), 2026).set_index("cand_id")
        self.assertEqual(parties.loc["senator", "party_dr"], "D")

    def test_ie_memos_do_not_change_lean_and_opposition_reverses_effect(self):
        transactions = table(ITCONT_COLS, [
            {"cmte_id": "pac", "transaction_tp": "24E", "other_id": "dem", "transaction_amt": "100"},
            {"cmte_id": "pac", "transaction_tp": "24E", "other_id": "rep", "transaction_amt": "1000", "memo_cd": "X"},
            {"cmte_id": "pac", "transaction_tp": "24A", "other_id": "rep", "transaction_amt": "50"},
            {"cmte_id": "correction", "transaction_tp": "24E", "other_id": "rep", "transaction_amt": "-50"},
        ]).astype("string")
        with (
            patch("pipeline.classify_partisan.pd.read_csv", return_value=transactions),
            patch("pipeline.classify_partisan.load_candidate_linkage_table", return_value=table(CCL_COLS, [])),
            patch("pipeline.classify_partisan._load_itpas2", return_value=table(ITPAS2_COLS, [])),
        ):
            result = build_behavioral_committee_classification(2026, {"dem": "D", "rep": "R"}).set_index("cmte_id")
        self.assertEqual(result.loc["pac", "evidence_total"], 150)
        self.assertEqual(result.loc["pac", "behavioral_party"], "D")
        self.assertEqual(result.loc["correction", "behavioral_party"], "Unknown")
        self.assertTrue(pd.isna(result.loc["correction", "evidence_pct_dem"]))

    def test_refund_dominated_donor_has_no_directional_percentage(self):
        tagged = pd.DataFrame([
            {"name": "refund", "cmte_id": "d", "net_amt": -100},
            {"name": "mixed_sign", "cmte_id": "d", "net_amt": -100},
            {"name": "mixed_sign", "cmte_id": "r", "net_amt": 200},
            {"name": "balanced", "cmte_id": "d", "net_amt": 50},
            {"name": "balanced", "cmte_id": "r", "net_amt": 50},
        ])
        committees = pd.DataFrame({"cmte_id": ["d", "r"], "party_dr": ["D", "R"]})
        result = classify_donors(tagged, committees).set_index("name")
        for name in ["refund", "mixed_sign"]:
            self.assertEqual(result.loc[name, "donor_party"], "Unknown")
            self.assertTrue(pd.isna(result.loc[name, "pct_d"]))
        self.assertEqual(result.loc["mixed_sign", "D"], -100)
        self.assertEqual(result.loc["balanced", "donor_party"], "Mixed")

    def test_refund_dominated_regions_have_no_party_label(self):
        result = _assign_party_label(pd.DataFrame({"d": [-100, -100, 70], "r": [0, 200, 30]}), "d", "r")
        self.assertEqual(result.party_label.tolist(), ["Unknown", "Unknown", "D"])
        self.assertTrue(result.loc[:1, "pct_dem"].isna().all())


class ReceiptSummaryTests(unittest.TestCase):
    def setUp(self):
        self.committees = table(CM_COLS, [
            {"cmte_id": "main", "cmte_nm": "Main", "cmte_tp": "H", "cmte_dsgn": "P", "cand_id": "candidate"},
            {"cmte_id": "authorized", "cmte_nm": "Authorized", "cmte_tp": "H", "cmte_dsgn": "A", "cand_id": "candidate"},
            {"cmte_id": "joint", "cmte_nm": "Joint", "cmte_tp": "H", "cmte_dsgn": "J", "cand_id": "candidate"},
            {"cmte_id": "leadership", "cmte_nm": "Leadership", "cmte_tp": "Q", "cmte_dsgn": "D", "cand_id": "candidate"},
        ])
        self.candidates = table(CN_COLS, [
            {"cand_id": "candidate", "cand_name": "Candidate", "cand_election_yr": "2026", "cand_office": "H", "cand_office_st": "CA", "cand_office_district": "01", "cand_pty_affiliation": "DEM"},
            {"cand_id": "other", "cand_name": "Other", "cand_election_yr": "2026", "cand_office": "H", "cand_office_st": "CA", "cand_office_district": "02", "cand_pty_affiliation": "REP"},
        ])
        self.linkage = table(CCL_COLS, [
            {"cand_id": cand, "cand_election_yr": "2026", "fec_election_yr": "2026", "cmte_id": committee, "cmte_tp": "H", "cmte_dsgn": designation}
            for cand, committee, designation in [
                ("candidate", "main", "P"), ("other", "main", "P"),
                ("candidate", "authorized", "A"), ("candidate", "joint", "J"),
                ("candidate", "leadership", "D"),
            ]
        ])

    def test_campaign_receipts_exclude_joint_fundraisers_and_resolve_owner(self):
        result = _candidate_receipt_links(self.linkage, self.candidates, self.committees)
        self.assertEqual(set(map(tuple, result.to_numpy())), {("candidate", "main"), ("candidate", "authorized")})

    def test_ambiguous_committee_without_master_owner_is_not_duplicated(self):
        self.committees.loc[self.committees.cmte_id == "main", "cand_id"] = ""
        result = _candidate_receipt_links(self.linkage, self.candidates, self.committees)
        self.assertEqual(result.cmte_id.tolist(), ["authorized"])

    def test_candidate_totals_count_each_donor_once_across_campaign_committees(self):
        tagged = pd.DataFrame([
            {"cmte_id": "main", "name": "Same donor", "net_amt": 100, "is_tech_employer": True, "tech_canonical_name": "Tech"},
            {"cmte_id": "authorized", "name": "Same donor", "net_amt": 50, "is_tech_employer": True, "tech_canonical_name": "Tech"},
            {"cmte_id": "main", "name": "Other donor", "net_amt": 25, "is_tech_employer": False, "tech_canonical_name": ""},
            {"cmte_id": "joint", "name": "Same donor", "net_amt": 1000, "is_tech_employer": True, "tech_canonical_name": "Tech"},
        ])
        spending = pd.DataFrame(columns=["cmte_id", "transaction_tp", "other_id", "net_amt"])
        receipts = pd.DataFrame(columns=["cmte_id", "tech_receipts"])
        with (
            patch("pipeline.build_summaries.load_candidate_master_table", return_value=self.candidates),
            patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=self.linkage),
        ):
            result = build_candidate_race_summary(2026, tagged, spending, receipts, self.committees).set_index("cand_id")
        self.assertEqual(result.loc["candidate", "total_itemized_receipts"], 175)
        self.assertEqual(result.loc["candidate", "tech_itemized_receipts"], 150)
        self.assertEqual(result.loc["candidate", "total_itemized_donors"], 2)
        self.assertEqual(result.loc["candidate", "tech_itemized_donors"], 1)
        self.assertEqual(result.loc["other", "total_itemized_receipts"], 0)

    def test_converted_account_is_reported_separately_without_inventing_conversion_date(self):
        former = table(CM_COLS, [{"cmte_id": "former", "cmte_nm": "Former campaign PAC",
                                 "cmte_tp": "N", "cmte_dsgn": "D", "cand_id": ""}])
        committees = pd.concat([self.committees, former], ignore_index=True)
        linkage = pd.concat([self.linkage, table(CCL_COLS, [{"cand_id": "candidate",
            "cand_election_yr": "2026", "fec_election_yr": "2026", "cmte_id": "former",
            "cmte_tp": "N", "cmte_dsgn": "D"}])], ignore_index=True)
        tagged = pd.DataFrame([
            {"cmte_id": "main", "name": "Donor", "net_amt": 100, "is_tech_employer": True, "tech_canonical_name": "Tech"},
            {"cmte_id": "former", "name": "Donor", "net_amt": 1000, "is_tech_employer": True, "tech_canonical_name": "Tech"},
        ])
        with (
            patch("pipeline.build_summaries.load_candidate_master_table", return_value=self.candidates),
            patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=linkage),
            patch("pipeline.build_summaries.load_converted_campaigns", return_value=pd.DataFrame({"cmte_id": ["former"], "cand_id": ["candidate"]})),
        ):
            result = build_candidate_race_summary(2026, tagged,
                pd.DataFrame([
                    {"cmte_id": "spender", "transaction_tp": "24E", "other_id": "main", "net_amt": 20},
                    {"cmte_id": "spender", "transaction_tp": "24E", "other_id": "candidate", "net_amt": 50},
                    {"cmte_id": "spender", "transaction_tp": "24E", "other_id": "former", "net_amt": 999},
                ]),
                pd.DataFrame(columns=["cmte_id", "tech_receipts"]), committees).set_index("cand_id")
        row = result.loc["candidate"]
        self.assertEqual(row.tech_itemized_receipts, 1100)
        self.assertEqual(row.current_campaign_account_tech_itemized_receipts, 100)
        self.assertEqual(row.former_campaign_account_tech_itemized_receipts, 1000)
        self.assertEqual(row.former_campaign_account_committee_ids, "former")
        self.assertEqual(row.former_campaign_account_committee_count, 1)
        self.assertEqual(row.tech_itemized_donors, 1)
        self.assertEqual(row.ie_support_total, 70)
        for column in ["total_itemized_receipts", "tech_itemized_receipts", "tech_itemized_contributions"]:
            self.assertEqual(row[column], row[f"current_campaign_account_{column}"] + row[f"former_campaign_account_{column}"])

    def test_refund_only_and_net_zero_former_accounts_remain_visible(self):
        committees = self.committees.copy()
        committees.loc[committees.cmte_id.eq("main"), ["cmte_tp", "cmte_dsgn", "cand_id"]] = ["N", "D", ""]
        linkage = self.linkage[self.linkage.cmte_id.eq("main")].copy()
        linkage["cmte_dsgn"] = "D"
        for amounts in [[-100], [100, -100]]:
            tagged = pd.DataFrame([{"cmte_id": "main", "name": "Donor", "net_amt": amount,
                "is_tech_employer": True, "tech_canonical_name": "Tech"} for amount in amounts])
            with (
                patch("pipeline.build_summaries.load_candidate_master_table", return_value=self.candidates),
                patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=linkage),
                patch("pipeline.build_summaries.load_converted_campaigns", return_value=pd.DataFrame({"cmte_id": ["main"], "cand_id": ["candidate"]})),
            ):
                result = build_candidate_race_summary(2026, tagged,
                    pd.DataFrame(columns=["cmte_id", "transaction_tp", "other_id", "net_amt"]),
                    pd.DataFrame(columns=["cmte_id", "tech_receipts"]), committees).set_index("cand_id")
            self.assertTrue(result.loc["candidate", "is_display_candidate"])
            self.assertEqual(result.loc["candidate", "tech_itemized_receipts"], sum(amounts))

    def test_regional_donor_counts_are_explicit_candidate_pairs(self):
        # The same name gives to two candidates in one district and must not
        # be described as two distinct people in the regional export.
        candidates = self.candidates.copy()
        candidates["cand_office_district"] = "01"
        committees = pd.concat([self.committees, table(CM_COLS, [{"cmte_id": "second",
            "cmte_nm": "Other campaign", "cmte_tp": "H", "cmte_dsgn": "P", "cand_id": "other"}])])
        linkage = pd.concat([self.linkage, table(CCL_COLS, [{"cand_id": "other",
            "cand_election_yr": "2026", "fec_election_yr": "2026", "cmte_id": "second",
            "cmte_tp": "H", "cmte_dsgn": "P"}])])
        tagged = pd.DataFrame([{"cmte_id": committee, "name": "Same donor", "net_amt": 100,
                               "is_tech_employer": True, "tech_canonical_name": "Tech"}
                              for committee in ["main", "second"]])
        with (
            patch("pipeline.build_summaries.load_candidate_master_table", return_value=candidates),
            patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=linkage),
        ):
            result = build_candidate_race_summary(2026, tagged,
                pd.DataFrame(columns=["cmte_id", "transaction_tp", "other_id", "net_amt"]),
                pd.DataFrame(columns=["cmte_id", "tech_receipts"]), committees)
        for builder, office in [(build_candidate_house_district_summary, "H"), (build_candidate_senate_summary, "S")]:
            result["cand_office"] = office
            row = builder(result).iloc[0]
            self.assertEqual(row.tech_donor_candidate_pairs, 2)
            self.assertEqual(row.tech_itemized_donors, row.tech_donor_candidate_pairs)

    def test_gross_positive_uses_signed_refunds_and_keeps_unknown_committee(self):
        raw = table(ITCONT_COLS, [
            {"cmte_id": "unknown", "name": "Donor", "transaction_tp": "15", "transaction_amt": 100},
            {"cmte_id": "unknown", "name": "Donor", "transaction_tp": "22Y", "transaction_amt": 30},
            {"cmte_id": "unknown", "name": "Donor", "transaction_tp": "22Y", "transaction_amt": -10},
        ])
        tagged = _filter_donor_contributions(raw, self.committees)
        tagged["is_tech_employer"] = True
        tagged["tech_canonical_name"] = "Tech"
        tagged["tech_sector"] = "Software"
        result = build_tech_donor_summary(tagged).iloc[0]
        self.assertEqual(result.net_total, 80)
        self.assertEqual(result.gross_positive, 110)
        self.assertEqual(result.top_committee_amt, 80)

    def test_special_account_refunds_reduce_receipts_and_reversals_restore_them(self):
        # FEC codebook: 40/41/42 = convention/headquarters/recount refunds;
        # Y = individuals, T = tribes. Test each code independently so a
        # positive refund cannot silently become a contribution or disappear.
        for receipt_type, refund_type in [
            ("30", "40Y"), ("30T", "40T"),
            ("31", "41Y"), ("31T", "41T"),
            ("32", "42Y"), ("32T", "42T"),
        ]:
            with self.subTest(refund_type=refund_type):
                raw = table(ITCONT_COLS, [
                    {"cmte_id": "main", "transaction_tp": receipt_type, "transaction_amt": 100},
                    {"cmte_id": "main", "transaction_tp": refund_type, "transaction_amt": 30},
                    {"cmte_id": "main", "transaction_tp": refund_type, "transaction_amt": -10},
                ])
                result = _filter_donor_contributions(raw, self.committees)
                self.assertEqual(result.net_amt.tolist(), [100, -30, 10])
                self.assertEqual(result.is_refund.tolist(), [False, True, True])
                self.assertEqual(result.net_amt.sum(), 80)

    def test_company_negative_party_net_keeps_dollars_and_omits_percentage(self):
        tagged = pd.DataFrame([
            {"cmte_id": "dem", "name": "Donor", "net_amt": -100, "is_tech_employer": True, "tech_canonical_name": "Tech", "tech_sector": "Software"},
            {"cmte_id": "rep", "name": "Donor", "net_amt": 200, "is_tech_employer": True, "tech_canonical_name": "Tech", "tech_sector": "Software"},
        ])
        result = build_tech_company_summary(tagged, pd.DataFrame({"cmte_id": ["dem", "rep"], "party_dr": ["D", "R"]})).iloc[0]
        self.assertEqual(result.net_total, 100)
        self.assertEqual(result.amt_dem, -100)
        self.assertTrue(pd.isna(result.pct_dem))


class TechShareTests(unittest.TestCase):
    def setUp(self):
        ReceiptSummaryTests.setUp(self)

    def test_nullable_raw_receipts_produce_boolean_risk_flags_for_empty_candidates(self):
        # Match production's StringDtype source fields and nullable numeric
        # amounts. Object/int64 fixtures cannot reproduce the BooleanDtype
        # error when a candidate without receipts is introduced by a left join.
        raw = table(ITCONT_COLS, [
            {"cmte_id": "main", "name": "Partnership", "transaction_tp": "10", "transaction_amt": "100"},
            {"cmte_id": "main", "name": "Partner", "transaction_tp": "10", "transaction_amt": "100", "memo_cd": "X", "employer": "TECH"},
        ]).astype("string")
        raw["transaction_amt"] = pd.to_numeric(raw.transaction_amt).astype("Int64")
        committees = self.committees.astype("string")
        tagged = tag_tech_donors(
            _filter_donor_contributions(raw, committees, cycle=2026),
            pd.DataFrame({"employer_upper": ["TECH"], "canonical_name": ["Tech"], "sector": ["Software"]}).astype("string"),
        )
        self.assertEqual(str(tagged.net_amt.dtype), "Int64")
        self.assertEqual(str(tagged.memo_cd.dtype), "string")
        spending = pd.DataFrame({
            "cmte_id": pd.Series(dtype="string"), "transaction_tp": pd.Series(dtype="string"),
            "other_id": pd.Series(dtype="string"), "net_amt": pd.Series(dtype="Int64"),
        })
        receipts = build_committee_tech_receipts(tagged, committees)
        with (
            patch("pipeline.build_summaries.load_candidate_master_table", return_value=self.candidates.astype("string")),
            patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=self.linkage.astype("string")),
            patch("pipeline.build_summaries.load_converted_campaigns", return_value=pd.DataFrame({"cand_id": pd.Series(dtype="string"), "cmte_id": pd.Series(dtype="string")})),
        ):
            candidates = build_candidate_race_summary(2026, tagged, spending, receipts, committees)

        def assert_boolean_flags(frame):
            columns = [column for column in frame if column.endswith("has_unreconciled_memo_attributions")]
            self.assertTrue(columns)
            for column in columns:
                self.assertEqual(frame[column].dtype, bool, column)
                self.assertFalse(frame[column].isna().any(), column)
            for row in records(frame):
                for column in columns:
                    self.assertIs(type(row[column]), bool, column)

        assert_boolean_flags(receipts)
        assert_boolean_flags(candidates)
        by_candidate = candidates.set_index("cand_id")
        self.assertTrue(by_candidate.loc["candidate", "has_unreconciled_memo_attributions"])
        self.assertFalse(by_candidate.loc["other", "has_unreconciled_memo_attributions"])
        self.assertEqual(by_candidate.loc["other", "selected_record_net_total"], 0)
        self.assertTrue(pd.isna(by_candidate.loc["other", "tech_pct_itemized_receipts"]))
        for column in ["current_campaign_account_has_unreconciled_memo_attributions", "former_campaign_account_has_unreconciled_memo_attributions"]:
            self.assertFalse(by_candidate.loc["other", column])
        for builder in [build_candidate_state_summary, build_candidate_house_district_summary, build_candidate_senate_summary]:
            with self.subTest(builder=builder.__name__):
                regional_input = candidates.copy()
                # A separate state for the empty candidate exercises an
                # all-false regional group as well as the affected group.
                regional_input.loc[regional_input.cand_id.eq("other"), "cand_office_st"] = "NY"
                if builder == build_candidate_senate_summary:
                    regional_input["cand_office"] = "S"
                region = builder(regional_input)
                assert_boolean_flags(region)
                self.assertEqual(region.set_index("state_code")["has_unreconciled_memo_attributions"].to_dict(), {"CA": True, "NY": False})

    def test_partnership_attributions_preserve_tech_dollars_without_false_share(self):
        tagged = pd.DataFrame([
            {"cmte_id": "main", "name": "Partnership", "net_amt": 25_000_000, "is_tech_employer": False, "tech_canonical_name": "", "memo_cd": ""},
            {"cmte_id": "main", "name": "Partner A", "net_amt": 12_500_000, "is_tech_employer": True, "tech_canonical_name": "Tech", "memo_cd": "X"},
            {"cmte_id": "main", "name": "Partner B", "net_amt": 12_500_000, "is_tech_employer": True, "tech_canonical_name": "Tech", "memo_cd": "X"},
            {"cmte_id": "authorized", "name": "Tech donor", "net_amt": 50, "is_tech_employer": True, "tech_canonical_name": "Tech", "memo_cd": ""},
            {"cmte_id": "authorized", "name": "Other donor", "net_amt": 50, "is_tech_employer": False, "tech_canonical_name": "", "memo_cd": ""},
        ])
        result = build_committee_tech_receipts(tagged, self.committees).set_index("cmte_id")
        row = result.loc["main"]
        self.assertEqual(row.tech_receipts, 25_000_000)
        self.assertEqual(row.total_receipts, 50_000_000)
        self.assertEqual(row.selected_record_net_total, 50_000_000)
        self.assertEqual(row.nonmemo_receipt_net_total, 25_000_000)
        self.assertEqual(row.memo_receipt_record_count, 2)
        self.assertTrue(row.has_unreconciled_memo_attributions)
        self.assertTrue(pd.isna(row.tech_pct))
        self.assertEqual(records(result.reset_index())[0]["tech_pct"], None)
        self.assertEqual(result.loc["authorized", "tech_pct"], 50)
        self.assertEqual(tech_dominated_mask(pd.Series([float("nan"), 50, 50.01])).tolist(), [False, False, True])

    def test_offsetting_memos_still_require_reconciliation(self):
        tagged = pd.DataFrame([
            {"cmte_id": "main", "name": "Donor", "net_amt": amount, "is_tech_employer": True, "tech_canonical_name": "Tech", "memo_cd": memo}
            for amount, memo in [(100, ""), (-50, "X"), (50, "X")]
        ])
        row = build_committee_tech_receipts(tagged, self.committees).iloc[0]
        self.assertEqual(row.memo_receipt_net_total, 0)
        self.assertEqual(row.tech_receipts, 100)
        self.assertTrue(row.has_unreconciled_memo_attributions)
        self.assertTrue(pd.isna(row.tech_pct))

    def test_candidate_and_regional_memo_risk_carries_current_and_former_scope(self):
        spending = pd.DataFrame(columns=["cmte_id", "transaction_tp", "other_id", "net_amt"])
        receipts = pd.DataFrame(columns=["cmte_id", "tech_receipts"])
        converted = pd.DataFrame({"cand_id": ["candidate"], "cmte_id": ["leadership"]})
        for memo_committee, scope in [("main", "current_campaign_account"), ("leadership", "former_campaign_account")]:
            with self.subTest(scope=scope):
                tagged = pd.DataFrame([
                    {"cmte_id": memo_committee, "name": "Parent", "net_amt": 100, "is_tech_employer": False, "tech_canonical_name": "", "memo_cd": ""},
                    {"cmte_id": memo_committee, "name": "Partner", "net_amt": 100, "is_tech_employer": True, "tech_canonical_name": "Tech", "memo_cd": "X"},
                ])
                with (
                    patch("pipeline.build_summaries.load_candidate_master_table", return_value=self.candidates),
                    patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=self.linkage),
                    patch("pipeline.build_summaries.load_converted_campaigns", return_value=converted),
                ):
                    candidates = build_candidate_race_summary(2026, tagged, spending, receipts, self.committees)
                row = candidates.set_index("cand_id").loc["candidate"]
                self.assertEqual(row.selected_record_net_total, 200)
                self.assertEqual(row.nonmemo_receipt_net_total, 100)
                self.assertEqual(row.tech_itemized_receipts, 100)
                self.assertTrue(row.has_unreconciled_memo_attributions)
                self.assertTrue(row[f"{scope}_has_unreconciled_memo_attributions"])
                self.assertTrue(pd.isna(row.tech_pct_itemized_receipts))
                for builder in [build_candidate_state_summary, build_candidate_house_district_summary, build_candidate_senate_summary]:
                    regional_input = candidates.copy()
                    if builder == build_candidate_senate_summary:
                        regional_input["cand_office"] = "S"
                    region = builder(regional_input).iloc[0]
                    self.assertTrue(region.has_unreconciled_memo_attributions)
                    self.assertEqual(region.selected_record_net_total, 200)
                    self.assertEqual(region.nonmemo_receipt_net_total, 100)

    def test_committee_shares_omit_refund_distortions_and_preserve_dollars(self):
        cases = [
            ("above_total", 100, 10, None),
            ("negative_tech", -10, 10, None),
            ("negative_total", -10, -20, None),
            ("zero_total", 0, 0, None),
            ("no_tech", 0, 50, 0),
            ("all_tech", 50, 50, 100),
            ("valid", 25, 100, 25),
        ]
        tagged = pd.DataFrame([
            {"cmte_id": name, "name": f"Donor {is_tech}", "net_amt": amount,
             "is_tech_employer": is_tech, "tech_canonical_name": "Tech" if is_tech else ""}
            for name, tech, total, _ in cases
            for is_tech, amount in [(True, tech), (False, total - tech)]
        ])
        committees = table(CM_COLS, [{"cmte_id": name} for name, _, _, _ in cases])
        result = build_committee_tech_receipts(tagged, committees).set_index("cmte_id")
        for name, tech, total, expected in cases:
            with self.subTest(committee=name):
                self.assertEqual(result.loc[name, "tech_receipts"], tech)
                self.assertEqual(result.loc[name, "total_receipts"], total)
                share = result.loc[name, "tech_pct"]
                if expected is None:
                    self.assertTrue(pd.isna(share))
                else:
                    self.assertEqual(share, expected)

    def test_candidate_shares_omit_invalid_nets_including_zero_receipts(self):
        for tech, total, expected in [(100, 10, None), (-10, 10, None), (-10, -20, None), (0, 0, None), (0, 50, 0), (25, 100, 25)]:
            with self.subTest(tech=tech, total=total):
                tagged = pd.DataFrame([
                    {"cmte_id": "main", "name": "Tech donor", "net_amt": tech, "is_tech_employer": True, "tech_canonical_name": "Tech"},
                    {"cmte_id": "main", "name": "Other donor", "net_amt": total - tech, "is_tech_employer": False, "tech_canonical_name": ""},
                ])
                spending = pd.DataFrame({"cmte_id": pd.Series(dtype=str), "transaction_tp": pd.Series(dtype=str), "other_id": pd.Series(dtype=str), "net_amt": pd.Series(dtype=float)})
                receipts = pd.DataFrame(columns=["cmte_id", "tech_receipts"])
                with (
                    patch("pipeline.build_summaries.load_candidate_master_table", return_value=self.candidates),
                    patch("pipeline.build_summaries.load_candidate_linkage_table", return_value=self.linkage),
                ):
                    result = build_candidate_race_summary(2026, tagged, spending, receipts, self.committees).set_index("cand_id")
                self.assertEqual(result.loc["candidate", "tech_itemized_receipts"], tech)
                self.assertEqual(result.loc["candidate", "total_itemized_receipts"], total)
                share = result.loc["candidate", "tech_pct_itemized_receipts"]
                if expected is None:
                    self.assertTrue(pd.isna(share))
                else:
                    self.assertEqual(share, expected)
                self.assertTrue(pd.isna(result.loc["other", "tech_pct_itemized_receipts"]))

    def test_regional_party_shares_preserve_refunds_without_invalid_percentages(self):
        candidates = pd.DataFrame([
            {"cand_id": f"{office}{party}", "cand_name": f"{office} {party}",
             "cand_office": office, "cand_office_st": "CA", "cand_office_district": "01",
             "party_dr": party, "is_major_party": True, "is_display_candidate": True,
             "linked_committee_count": 1, "total_itemized_receipts": 100,
             "tech_itemized_receipts": amount, "tech_itemized_donors": 1,
             "ie_support_total": 0, "ie_oppose_total": 0,
             "tech_funded_ie_support_total": 0, "tech_funded_ie_oppose_total": 0}
            for office in ["H", "S"] for party, amount in [("D", -100), ("R", 200)]
        ])
        for builder, multiplier in [(build_candidate_state_summary, 2), (build_candidate_house_district_summary, 1), (build_candidate_senate_summary, 1)]:
            with self.subTest(builder=builder.__name__):
                result = builder(candidates).iloc[0]
                self.assertEqual(result.dem_tech_itemized_receipts, -100 * multiplier)
                self.assertEqual(result.rep_tech_itemized_receipts, 200 * multiplier)
                self.assertEqual(result.party_label, "Unknown")
                self.assertTrue(pd.isna(result.pct_dem))
                self.assertTrue(pd.isna(result.pct_rep))

    def test_missing_or_empty_amounts_do_not_create_zero_percent(self):
        result = _meaningful_share(pd.Series([float("nan"), 0.0]), pd.Series([100.0, float("nan")]))
        self.assertTrue(result.isna().all())
        self.assertTrue(_meaningful_share(pd.Series(dtype=float), pd.Series(dtype=float)).empty)


if __name__ == "__main__":
    unittest.main()
