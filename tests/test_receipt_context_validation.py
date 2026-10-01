import unittest

from scripts.validate_site import validate_receipt_context


class ReceiptContextValidationTests(unittest.TestCase):
    def check(self, **changes):
        row = dict(selected_record_net_total=200, nonmemo_receipt_net_total=100,
                   memo_receipt_net_total=100, memo_receipt_record_count=2,
                   total_receipts=200, has_unreconciled_memo_attributions=True,
                   tech_pct=None, is_tech_dominated=False)
        row.update(changes)
        errors = []
        validate_receipt_context("2026 C00000001", row, errors,
                                 total_key="total_receipts", share_key="tech_pct")
        return errors

    def test_unavailable_share_with_consistent_diagnostics_passes(self):
        self.assertEqual(self.check(), [])
        self.assertEqual(self.check(memo_receipt_net_total=0,
                                   nonmemo_receipt_net_total=200), [])

    def test_reintroduced_percentage_or_dominance_fails(self):
        self.assertIn("displayed Tech Share", self.check(tech_pct=50)[0])
        self.assertIn("tech dominated", self.check(is_tech_dominated=True)[0])

    def test_lost_flag_or_corrupted_diagnostic_fails(self):
        for change in (dict(has_unreconciled_memo_attributions=False),
                       dict(nonmemo_receipt_net_total=99), dict(total_receipts=100),
                       dict(memo_receipt_record_count=0), dict(selected_record_net_total=float("nan"))):
            with self.subTest(change=change):
                self.assertTrue(self.check(**change))
