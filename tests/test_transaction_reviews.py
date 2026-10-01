"""Source-reviewed repeat removal must never become approximate gift deduping."""
import unittest
from unittest.mock import patch
import tempfile
from pathlib import Path

import pandas as pd

from pipeline.fec.load import CM_COLS, ITCONT_COLS, _filter_donor_contributions, load_cycle
from pipeline.fec.transaction_reviews import (
    ReviewedExclusionDependencies, load_reviews, reviewed_exclusion_mask, source_row_sha256,
)
from pipeline.rebuild_candidate_exports import filter_candidate_contribution_chunk, load_candidate_contributions
from scripts.audit_candidate_receipts import scan, select_evidence


class ReviewedTransactionTests(unittest.TestCase):
    def setUp(self):
        self.review = next(r for r in load_reviews()
                           if r["source_signature"]["sub_id"] == "4070920241972634559")
        # Test source locations are invented; references store only their hash.
        self.memo = {**self.review["source_signature"], "city": "EXAMPLE CITY", "state": "XX", "zip_code": "00000"}
        self.original = {**self.review["retained_original_source_signature"], "city": "EXAMPLE CITY", "state": "XX", "zip_code": "00000"}
        self.review = {
            **self.review, "source_row_sha256": source_row_sha256(self.memo),
            "retained_original_source_row_sha256": source_row_sha256(self.original),
        }
        review_patch = patch("pipeline.fec.transaction_reviews.load_reviews", return_value=(self.review,))
        review_patch.start()
        self.addCleanup(review_patch.stop)
        self.cmte = self.memo["cmte_id"]
        self.committees = pd.DataFrame([{key: self.cmte if key == "cmte_id" else "" for key in CM_COLS}])

    def records(self):
        original = self.original.copy()
        # Same name/date/value can also be a separate legitimate gift.
        other_gift = {**original, "sub_id": "separate-gift", "tran_id": "separate-transaction"}
        negative = {**self.memo, "sub_id": "negative-adjustment", "tran_id": "SA11A.84136",
                    "transaction_amt": "-3300", "memo_text": "REDESIGNATION TO GENERAL"}
        positive = {**self.memo, "sub_id": "positive-adjustment", "tran_id": "SA11A.84137",
                    "transaction_amt": "3300", "memo_text": "REDESIGNATION FROM PRIMARY"}
        return pd.DataFrame([original, self.memo, negative, positive, other_gift]).astype("string")

    def test_only_reviewed_repeat_removed_and_equal_gift_and_adjustments_retained(self):
        rows = self.records()
        mask = reviewed_exclusion_mask(rows, 2024)
        self.assertEqual(mask.tolist(), [False, True, False, False, False])
        numeric = rows.copy()
        numeric["transaction_amt"] = pd.to_numeric(numeric.transaction_amt).astype(float)
        core = _filter_donor_contributions(numeric, self.committees, cycle=2024)
        self.assertEqual(core.net_amt.sum(), 13200)
        self.assertEqual(len(core), 4)
        self.assertEqual(set(core.memo_cd), {"", "X"})
        narrow = filter_candidate_contribution_chunk(rows, {self.cmte},
            pd.Series({"PALANTIR": "palantir"}), cycle=2024)
        self.assertEqual(narrow.net_amt.sum(), 13200)
        self.assertEqual(len(narrow), len(core))
        audit = select_evidence(rows, {"PALANTIR"}, {self.cmte}, set(), "candidate", "palantir", cycle=2024)
        self.assertEqual(audit.loc[audit.core_included, "signed_amount"].sum(), 13200)
        self.assertEqual(audit.loc[audit.sub_id.eq(self.memo["sub_id"]), "exclusion_reason"].iloc[0],
                         "source_reviewed_repeated_original")
        self.assertEqual(len(audit), 5)  # The audit evidence still exposes the excluded row.

    def test_stale_signature_stops_instead_of_silently_excluding_new_contents(self):
        for key, changed in (("transaction_amt", "6601"), ("employer", "OTHER"),
                             ("file_num", "new-filing"), ("memo_text", "changed passage")):
            rows = pd.DataFrame([{**self.memo, key: changed}]).astype("string")
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "Stale reviewed"):
                reviewed_exclusion_mask(rows, 2024)
        with self.assertRaisesRegex(ValueError, "Stale reviewed"):
            select_evidence(pd.DataFrame([{**self.memo, "transaction_amt": "6601"}]).astype("string"),
                {"PALANTIR"}, {self.cmte}, set(), "candidate", "palantir", cycle=2024)

    def test_new_id_wrong_cycle_or_different_source_is_never_automatically_excluded(self):
        row = pd.DataFrame([self.memo]).astype("string")
        self.assertFalse(reviewed_exclusion_mask(row, 2026).any())
        self.assertFalse(reviewed_exclusion_mask(row, 2024, source_file="itoth.txt").any())
        row["sub_id"] = "new-source-id"
        self.assertFalse(reviewed_exclusion_mask(row, 2024).any())

    def test_known_ids_require_full_signature_and_explicit_cycle(self):
        row = pd.DataFrame([self.memo]).astype("string")
        with self.assertRaisesRegex(ValueError, "explicit source cycle"):
            reviewed_exclusion_mask(row)
        with self.assertRaisesRegex(ValueError, "source fields missing"):
            reviewed_exclusion_mask(row.drop(columns=["image_num"]), 2024)

    def test_duplicate_dataframe_index_cannot_remove_an_unreviewed_row(self):
        rows = self.records()
        rows.index = [0] * len(rows)
        self.assertEqual(reviewed_exclusion_mask(rows, 2024).tolist(), [False, True, False, False, False])

    def test_dependencies_accept_original_in_either_chunk_order(self):
        for rows in [(self.memo, self.original), (self.original, self.memo)]:
            with self.subTest(original_first=rows[0] is self.original):
                tracker = ReviewedExclusionDependencies(2024)
                for row in rows:
                    tracker.observe(pd.DataFrame([row]).astype("string"))
                result = tracker.finalize()
                self.assertEqual(result["reviewed_exclusions_observed"], 1)
                self.assertEqual(result["retained_originals_verified"], 1)

    def test_dependencies_require_one_exact_original_only_when_review_seen(self):
        tracker = ReviewedExclusionDependencies(2024)
        self.assertEqual(tracker.finalize()["reviewed_exclusions_observed"], 0)
        tracker.observe(pd.DataFrame([self.memo]).astype("string"))
        with self.assertRaisesRegex(ValueError, "found 0"):
            tracker.finalize()
        tracker.observe(pd.DataFrame([self.original, self.original]).astype("string"))
        with self.assertRaisesRegex(ValueError, "found 2"):
            tracker.finalize()

    def test_changed_original_cannot_satisfy_dependency(self):
        for key, value in [("cmte_id", "different"), ("transaction_amt", "6601"),
                           ("name", "different"), ("city", "changed location")]:
            with self.subTest(changed_field=key):
                tracker = ReviewedExclusionDependencies(2024)
                tracker.observe(pd.DataFrame([self.memo, {**self.original, key: value}]).astype("string"))
                with self.assertRaisesRegex(ValueError, "Stale retained original"):
                    tracker.finalize()

    def test_other_cycle_or_source_cannot_supply_original(self):
        main = ReviewedExclusionDependencies(2024)
        main.observe(pd.DataFrame([self.memo]).astype("string"))
        for tracker in [ReviewedExclusionDependencies(2026),
                        ReviewedExclusionDependencies(2024, source_file="itoth.txt")]:
            tracker.observe(pd.DataFrame([self.original]).astype("string"))
            self.assertEqual(tracker.finalize()["retained_originals_verified"], 0)
        with self.assertRaisesRegex(ValueError, "found 0"):
            main.finalize()

    def test_full_loader_stops_before_returning_a_missing_original(self):
        raw = pd.DataFrame([self.memo], columns=ITCONT_COLS).astype("string")
        raw["transaction_amt"] = pd.to_numeric(raw.transaction_amt)
        with (
            patch("pipeline.fec.load._load_raw", return_value=(raw, raw.iloc[:0], self.committees)),
            self.assertRaisesRegex(ValueError, "found 0"),
        ):
            load_cycle(2024)

    def test_chunked_loaders_check_full_source_before_return(self):
        aliases = pd.DataFrame({"employer_upper": ["PALANTIR"], "canonical_name": ["palantir"], "sector": ["Software"]})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "itcont.txt"
            rows = pd.DataFrame([self.memo], columns=ITCONT_COLS).astype("string")
            rows.to_csv(path, sep="|", index=False, header=False)
            with self.assertRaisesRegex(ValueError, "found 0"):
                load_candidate_contributions(path, {self.cmte}, aliases, chunksize=1, cycle=2024)
            with self.assertRaisesRegex(ValueError, "found 0"):
                scan(path, {"PALANTIR"}, {self.cmte}, set(), "candidate", "palantir", 1, cycle=2024)
            rows = pd.DataFrame([self.memo, self.original], columns=ITCONT_COLS).astype("string")
            rows.to_csv(path, sep="|", index=False, header=False)
            narrow = load_candidate_contributions(path, {self.cmte}, aliases, chunksize=1, cycle=2024)
            self.assertEqual(narrow.net_amt.sum(), 6600)
            evidence, count, source = scan(path, {"PALANTIR"}, {self.cmte}, set(), "candidate", "palantir", 1, cycle=2024)
            self.assertEqual(count, 2)
            self.assertEqual(len(evidence), 2)
            self.assertEqual(source["review_dependencies"]["retained_originals_verified"], 1)


if __name__ == "__main__":
    unittest.main()
