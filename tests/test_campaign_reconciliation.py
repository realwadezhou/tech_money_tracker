from decimal import Decimal
import csv
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from pipeline.fec.load import ITCONT_COLS, CM_COLS, _filter_donor_contributions
from pipeline.fec.filings import Filing, latest_filings, parse_filing
from pipeline.fec.campaign_reports import classify_records
from pipeline.fec.attribution import attribute_campaign
from scripts.reconcile_campaign_sources import (
    PUBLIC_COLUMNS, legacy_selection, public_bulk_row, exact_source_differences, compare_sources,
)
from tests.test_fec_filings import report_rows, transaction
from tests.test_campaign_reports import manifest_fixture


def bulk_row(**changes):
    row = dict.fromkeys(ITCONT_COLS, "")
    row.update(cmte_id="C00123456", transaction_tp="15", entity_tp="IND", name="EXAMPLE, ALEX",
               employer="NVIDIA", transaction_dt="03012026", transaction_pgi="P2026",
               transaction_amt="100.00", file_num="100", tran_id="A1", sub_id="9000000000000000001",
               city="PRIVATE CITY", state="VA", zip_code="00000")
    row.update(changes)
    return row


def original_row(**changes):
    return {"employer": "NVIDIA", "donor_name": "EXAMPLE, ALEX", "entity_type": "IND",
            "date": "2026-03-01", "election": "P2026", "memo": False, "amount": "100.00", **changes}


class BulkSelectionParityTests(unittest.TestCase):
    def test_scan_selection_and_signs_match_production_filter_including_memos_and_adjustments(self):
        rows = [bulk_row(transaction_tp=code, memo_cd=memo, transaction_amt=amount,
                         sub_id=f"synthetic-{index}-{memo}-{amount}")
                for index, code in enumerate(["10", "11", "15", "15E", "15J", "24T", "22Y", "40T", "41Y", "42T"])
                for memo in ("", "X") for amount in ("100.25", "-10.25", "bad-value")]
        frame = pd.DataFrame(rows)
        frame["transaction_amt"] = pd.to_numeric(frame["transaction_amt"], errors="coerce").fillna(0.0)
        cm = pd.DataFrame([{**dict.fromkeys(CM_COLS, ""), "cmte_id": "C00123456"}])
        selected = _filter_donor_contributions(frame, cm, cycle=2026).set_index("sub_id")
        for row in rows:
            included, reason, amount = legacy_selection(row, set())
            with self.subTest(code=row["transaction_tp"], memo=row["memo_cd"], amount=row["transaction_amt"]):
                self.assertEqual(included, row["sub_id"] in selected.index)
                if included:
                    self.assertEqual(amount, Decimal(str(selected.loc[row["sub_id"], "net_amt"])))
                    self.assertEqual(reason, "")

    def test_exact_review_id_exclusion_does_not_exclude_a_same_value_gift(self):
        excluded = bulk_row(sub_id="reviewed")
        retained = bulk_row(sub_id="different")
        self.assertEqual(legacy_selection(excluded, {"reviewed"})[:2], (False, "reviewed_exact_source_exclusion"))
        self.assertTrue(legacy_selection(retained, {"reviewed"})[0])

    def test_public_rows_keep_raw_cents_and_identity_but_omit_location_fields(self):
        row = bulk_row(employer="  Nvidia  ", transaction_amt="100.25")
        result = public_bulk_row(row, 12, {"NVIDIA": "nvidia"}, {}, set())
        self.assertEqual(result["canonical_company"], "nvidia")
        self.assertEqual(result["transaction_amt"], "100.25")
        self.assertEqual(result["legacy_net_amount"], "100.25")
        self.assertEqual(result["source_row_number"], 12)
        self.assertNotIn("PRIVATE CITY", str(result))
        self.assertTrue(all(field not in result for field in ("city", "state", "zip_code")))


class ExactSourceComparisonTests(unittest.TestCase):
    def test_precision_bucket_requires_actual_raw_difference_not_our_numeric_conversion(self):
        row = {**bulk_row(transaction_amt="100"), "legacy_net_amount": "100.00"}
        self.assertEqual(exact_source_differences(row, original_row(amount="100.25")), ["bulk_precision_difference"])
        exact = {**bulk_row(transaction_amt="100.25"), "legacy_net_amount": "100.00"}
        self.assertEqual(exact_source_differences(exact, original_row(amount="100.25")), ["legacy_numeric_conversion_difference"])
        self.assertEqual(exact_source_differences(row, original_row(amount="150.25")), ["amount_difference"])

    def test_reported_identity_changes_are_not_silently_matched_by_name_or_amount(self):
        row = {**bulk_row(), "legacy_net_amount": "100.00"}
        changes = exact_source_differences(row, original_row(employer="GOOGLE", donor_name="OTHER, ALEX", entity_type="ORG"))
        self.assertEqual(changes, ["employer_difference", "reported_name_difference", "entity_type_difference"])
        self.assertEqual(exact_source_differences(row, original_row(donor_name="Example, Alex")), [])

    def test_exact_keys_preserve_two_equal_gifts_and_do_not_guess_missing_originals(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            filing_path = root / "100.fec"
            with filing_path.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerows(report_rows(individual="200.00", records=[transaction("A1", "100.00"), transaction("A2", "100.00")]))
            filing = parse_filing(filing_path, 100)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest_fixture([filing])), encoding="utf-8")
            index = root / "bulk.csv.gz"
            # Equal name/amount but different transaction key must stay bulk-only.
            with gzip.open(index, "wt", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=PUBLIC_COLUMNS)
                writer.writeheader()
                for number, row in enumerate([bulk_row(), bulk_row(tran_id="DIFFERENT", sub_id="9000000000000000002")], 1):
                    writer.writerow(public_bulk_row(row, number, {"NVIDIA": "nvidia"}, {}, set()))
            with patch("scripts.reconcile_campaign_sources.aliases_from_curated", return_value={"NVIDIA": "nvidia"}):
                result = compare_sources(manifest_path, index, [root], root / "comparison")
            self.assertEqual(result["category_counts"]["original_only"], 1)
            self.assertEqual(result["category_counts"]["bulk_only"], 1)
            self.assertEqual(result["company_results"][0]["legacy_selected_record_sum"], "200.00")
            self.assertEqual(result["company_results"][0]["combined_amount"], "200.00")
            self.assertEqual(result["comparison_row_count"], 3)
            with (root / "comparison/record_comparison.csv").open(encoding="utf-8", newline="") as stream:
                comparisons = {r["transaction_id"]: r for r in csv.DictReader(stream)}
            self.assertIn("original_only", comparisons["A2"]["categories"])
            self.assertIn("bulk_only", comparisons["DIFFERENT"]["categories"])
            self.assertEqual(comparisons["A1"]["categories"], "same_source_facts")


class LargeMemoSourceEvidenceTests(unittest.TestCase):
    def fixture(self):
        return json.loads((Path(__file__).parent / "fixtures/fec_attribution/musk_partnership_in_kind.json").read_text(encoding="utf-8"))

    def test_fourteen_in_kind_families_conserve_each_parent_and_are_counted_once(self):
        case = self.fixture()
        rows = classify_records(case["records"], case["reviewed_context"])
        result = attribute_campaign(rows, case["employer_aliases"], target_company="x_twitter_spacex")
        by_id = {row.record_id: row for row in rows}
        parents = [r for r in rows if not r.memo]
        children = [r for r in rows if r.memo]
        self.assertEqual(len(parents), 14)
        self.assertEqual(len(children), 14)
        self.assertTrue(all(not r.employer for r in parents))
        for child in children:
            parent = by_id[child.related_record_id]
            self.assertEqual(child.amount, parent.amount)
            self.assertEqual(child.date, parent.date)
            self.assertIn("INKIND", child.memo_text)
            self.assertEqual(sum(r.related_record_id == parent.record_id for r in children), 1)
            self.assertIn(int(child.file_number), case["api_latest_file_numbers"])
        self.assertEqual(result.components["partnership_attributions"], Decimal("30209514.37"))
        self.assertEqual(result.attributed_total, Decimal("30209514.37"))
        self.assertEqual(sum(r.amount for r in rows), Decimal("60419028.74"))
        self.assertIsNone(result.net_total)
        self.assertTrue(all(d.status == "context" for d in result.decisions if d.record_id in {p.record_id for p in parents}))

    def test_eight_bulk_precision_differences_are_pinned_to_exact_original_rows(self):
        case = self.fixture()
        source = {r["record_id"]: r for r in case["records"]}
        differences = []
        for pair in case["bulk_comparison"]:
            self.assertEqual(pair["original_source_row_sha256"], source[pair["record_id"]]["source_row_sha256"])
            self.assertEqual(pair["original_amount"], source[pair["record_id"]]["amount"])
            self.assertEqual(len(pair["bulk_source_row_sha256"]), 64)
            delta = Decimal(pair["original_amount"]) - Decimal(pair["bulk_raw_amount"])
            if delta:
                self.assertGreater(delta, 0)
                self.assertLess(delta, 1)
                differences.append(delta)
        self.assertEqual(len(differences), 8)
        self.assertEqual(sum(differences), Decimal("5.37"))
        self.assertEqual(sum(Decimal(p["bulk_raw_amount"]) for p in case["bulk_comparison"]), Decimal("30209509"))
        self.assertTrue(all(not {"city", "state", "zip_code", "address"} & set(r) for r in case["records"]))

    def test_reviewed_changed_coverage_case_does_not_relax_generic_amendment_selection(self):
        case = self.fixture()
        chain = [Filing(source["metadata"], []) for source in case["sources"]
                 if source["file_number"] in {1941882, 1947425}]
        self.assertEqual(case["amendment_coverage_review"]["changed_schedule_row_hash_ids"], [])
        with self.assertRaisesRegex(ValueError, "Changed report period"):
            latest_filings(chain)


if __name__ == "__main__":
    unittest.main()
