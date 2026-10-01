"""Reject joins that would silently multiply dollars or choose an arbitrary alias."""
import unittest

import pandas as pd

from pipeline.fec.load import (
    CM_COLS, ITCONT_COLS, _filter_donor_contributions,
    _filter_committee_spending, tag_tech_donors,
)


def rows(columns, values):
    return pd.DataFrame([{**dict.fromkeys(columns, ""), **row} for row in values])


class JoinIntegrityTests(unittest.TestCase):
    def test_duplicate_committee_directory_cannot_multiply_receipts_or_spending(self):
        committees = rows(CM_COLS, [{"cmte_id": "C1"}, {"cmte_id": "C1"}])
        for loader, transaction_type in [
            (_filter_donor_contributions, "15"),
            (_filter_committee_spending, "24E"),
        ]:
            raw = rows(ITCONT_COLS, [{
                "cmte_id": "C1", "transaction_tp": transaction_type,
                "transaction_amt": 100,
            }])
            with self.subTest(loader=loader.__name__), self.assertRaises(pd.errors.MergeError):
                loader(raw, committees)

    def test_unknown_committee_does_not_erase_a_receipt(self):
        raw = rows(ITCONT_COLS, [{
            "cmte_id": "UNKNOWN", "transaction_tp": "15", "transaction_amt": 100,
        }])
        result = _filter_donor_contributions(raw, rows(CM_COLS, [{"cmte_id": "C1"}]))
        self.assertEqual(result["net_amt"].sum(), 100)
        self.assertEqual(len(result), 1)
        self.assertTrue(result["cmte_nm"].isna().all())

    def test_conflicting_alias_and_blank_alias_fail_before_arbitrary_attribution(self):
        contributions = pd.DataFrame({"employer": ["Example"], "net_amt": [100]})
        for aliases in [
            [("EXAMPLE", "a", "tech"), ("EXAMPLE", "b", "tech")],
            [("EXAMPLE", "a", "tech"), ("EXAMPLE", "a", "finance")],
            [("", "a", "tech")],
        ]:
            with self.subTest(aliases=aliases), self.assertRaises(ValueError):
                tag_tech_donors(contributions, pd.DataFrame(
                    aliases, columns=["employer_upper", "canonical_name", "sector"]
                ))

    def test_identical_alias_repetitions_do_not_multiply_real_repeat_gifts(self):
        contributions = pd.DataFrame({"employer": [" example ", "EXAMPLE"], "net_amt": [100, 100]})
        aliases = pd.DataFrame({
            "employer_upper": ["EXAMPLE", "EXAMPLE"],
            "canonical_name": ["a", "a"], "sector": ["tech", "tech"],
        })
        result = tag_tech_donors(contributions, aliases)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.loc[result.is_tech_employer, "net_amt"].sum(), 200)


if __name__ == "__main__":
    unittest.main()
