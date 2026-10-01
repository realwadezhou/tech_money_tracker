import tempfile
import unittest
from pathlib import Path

import pandas as pd

from pipeline.fec.load import (
    CM_COLS,
    INCLUDE_TYPES_ITCONT,
    ITCONT_COLS,
    REFUND_TYPES_ITCONT,
    _filter_committee_spending,
    _filter_donor_contributions,
    tag_tech_donors,
)
from pipeline.rebuild_candidate_exports import (
    CONTRIBUTION_COLUMNS,
    IE_COLUMNS,
    load_candidate_contributions,
    load_candidate_spending,
)


def raw_rows(rows):
    defaults = {column: "" for column in ITCONT_COLS}
    defaults.update({
        "cmte_id": "C1", "name": "SAME DONOR", "employer": " Example ",
        "transaction_tp": "15", "transaction_amt": "100", "other_id": "H1",
        # Missing or invalid dates must not affect receipt inclusion.
        "transaction_dt": "INVALID",
    })
    return pd.DataFrame([defaults | row for row in rows], columns=ITCONT_COLS).astype("string")


class CandidateInputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.committees = pd.DataFrame([
            {column: (committee if column == "cmte_id" else "") for column in CM_COLS}
            for committee in ["C1", "C2", "OTHER"]
        ]).astype("string")
        self.tech = pd.DataFrame({
            "employer_upper": ["EXAMPLE", "EXAMPLE", "SECOND", "EMPTY"],
            "canonical_name": ["Example", "Example", "Second", ""],
            "sector": ["Software", "Software", "Hardware", ""],
        }).astype("string")

    def write_raw(self, frame, name):
        path = self.base / name
        frame.to_csv(path, sep="|", header=False, index=False)
        return path

    def test_chunked_receipts_match_core_filters_and_tags(self):
        rows = []
        for transaction_type in sorted(INCLUDE_TYPES_ITCONT | REFUND_TYPES_ITCONT | {"24T", "99"}):
            for memo in ["", "X"]:
                rows.append({"transaction_tp": transaction_type, "memo_cd": memo})
        rows.extend([
            {"cmte_id": "OTHER"},
            {"cmte_id": "C2", "employer": "second", "transaction_amt": "-25"},
            {"transaction_tp": "22Y", "transaction_amt": "-10"},
            {"transaction_tp": "21Y", "transaction_amt": "30"},
            {"employer": "UNMATCHED", "transaction_amt": "75"},
            {"employer": "EMPTY", "transaction_amt": "40"},
            {"employer": "", "transaction_amt": "not numeric"},
            {"transaction_amt": ""},
        ])
        raw = raw_rows(rows)
        path = self.write_raw(raw, "itcont.txt")
        core_raw = raw.copy()
        core_raw["transaction_amt"] = pd.to_numeric(
            core_raw["transaction_amt"], errors="coerce"
        ).fillna(0.0)
        core = tag_tech_donors(_filter_donor_contributions(core_raw, self.committees), self.tech)
        expected = core.loc[core["cmte_id"].isin({"C1", "C2"}), CONTRIBUTION_COLUMNS].reset_index(drop=True)
        actual = load_candidate_contributions(path, {"C1", "C2"}, self.tech, chunksize=3)
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
        # Identical donor rows across chunks stay distinct for contribution counts.
        self.assertGreater(len(actual), actual["name"].nunique())

    def test_conflicting_aliases_fail_in_both_loading_paths(self):
        self.tech.loc[1, "canonical_name"] = "Different company"
        path = self.write_raw(raw_rows([{}]), "itcont.txt")
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            load_candidate_contributions(path, {"C1"}, self.tech, chunksize=1)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            tag_tech_donors(pd.DataFrame({"employer": ["EXAMPLE"], "net_amt": [100]}), self.tech)

    def test_chunked_ie_matches_core_spending_filters(self):
        raw = raw_rows([
            {"transaction_tp": "24E"},
            {"transaction_tp": "24A", "transaction_amt": "-30"},
            {"transaction_tp": "24E", "memo_cd": "X"},
            {"transaction_tp": "24A", "memo_cd": "X"},
            {"transaction_tp": "24K"},
            {"transaction_tp": "24C"},
            {"transaction_tp": "24Z"},
            {"transaction_tp": "15"},
            {"transaction_tp": "24A", "other_id": "C2", "transaction_amt": "invalid"},
        ])
        path = self.write_raw(raw, "itoth.txt")
        core_raw = raw.copy()
        core_raw["transaction_amt"] = pd.to_numeric(
            core_raw["transaction_amt"], errors="coerce"
        ).fillna(0.0)
        core = _filter_committee_spending(core_raw, self.committees)
        expected = core.loc[core["transaction_tp"].isin(["24A", "24E"]), IE_COLUMNS].reset_index(drop=True)
        actual = load_candidate_spending(path, chunksize=2)
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)

    def test_no_matching_rows_keeps_required_schema(self):
        raw = raw_rows([{"cmte_id": "OTHER"}])
        path = self.write_raw(raw, "empty_matches.txt")
        receipts = load_candidate_contributions(path, {"C1"}, self.tech, chunksize=1)
        spending = load_candidate_spending(path, chunksize=1)
        self.assertTrue(receipts.empty)
        self.assertEqual(receipts.columns.tolist(), CONTRIBUTION_COLUMNS)
        self.assertEqual(receipts["is_tech_employer"].dtype, bool)
        self.assertTrue(spending.empty)
        self.assertEqual(spending.columns.tolist(), IE_COLUMNS)


if __name__ == "__main__":
    unittest.main()
