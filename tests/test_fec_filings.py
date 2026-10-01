"""Synthetic original-file tests for report replacement and financial provenance.

Cover rows populate the documented positions consumed by the parser. Their padded
length is not a claim about the complete field count of every EFO cover layout.
"""

import csv
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from pipeline.fec.filings import decimal_amount, iso_date, latest_filings, parse_filing


# Independent fixtures: report code, start/end dates, individual/refund totals,
# and corresponding itemized Schedule A/B lines (zero-based EFO positions).
FORM_FIELDS = {
    "F3": (11, 15, 16, 32, 51, "SA11AI", "SB20A"),
    "F3P": (11, 15, 16, 34, 58, "SA17AI", "SB28A"),
    "F3X": (9, 13, 14, 29, 56, "SA11AI", "SB28A"),
}
COMMITTEE = "C00123456"


def transaction(tx_id, amount, *, schedule="SA11AI", committee=COMMITTEE,
                memo=False, date="20260301", name="EXAMPLE", first="ALEX",
                employer="NVIDIA", back_reference="", memo_text=""):
    is_a = schedule.startswith("SA")
    row = [""] * (45 if is_a else 44)
    row[0:6] = [schedule, committee, tx_id, back_reference, "SA11AI" if back_reference else "", "IND"]
    row[7], row[8] = name, first
    row[12], row[14], row[15], row[16] = "PRIVATE STREET", "PRIVATE CITY", "VA", "00000"
    row[17], row[19], row[20] = "P2026", date, amount
    if is_a:
        row[23] = employer
        row[25] = "C00999999"
    else:
        row[24] = "C00999999"
    row[42 if is_a else 41] = "X" if memo else ""
    row[43 if is_a else 42] = memo_text
    return row


def report_rows(*, form_type="F3", file_number=100, version="8.4", sequence=0,
                original=None, committee=COMMITTEE, start="20260101", end="20260331",
                report_type="Q1", individual="100.00", refunds="0.00", records=None):
    fields = FORM_FIELDS[form_type]
    header = ["HDR", "FEC", version, "TEST SOFTWARE", "1", "", str(sequence)]
    if original is not None:
        header[5] = f"FEC-{original}"
    cover = [""] * 70
    cover[0], cover[1] = form_type + ("A" if sequence else "N"), committee
    cover[fields[0]], cover[fields[1]], cover[fields[2]] = report_type, start, end
    cover[fields[3]], cover[fields[4]] = individual, refunds
    if records is None:
        records = [transaction("A1", "100.00", schedule=fields[5], committee=committee)]
    return [header, cover, *records]


class FilingParserTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write_report(self, rows, file_number=100, *, delimiter=",", encoding="utf-8"):
        text = io.StringIO(newline="")
        writer = csv.writer(text, delimiter=delimiter, lineterminator="\n")
        writer.writerows(rows)
        path = self.root / f"{file_number}.fec"
        path.write_bytes(text.getvalue().encode(encoding))
        return path

    def parse(self, rows=None, file_number=100, **write_options):
        return parse_filing(self.write_report(rows or report_rows(), file_number, **write_options), file_number)

    def test_vendor_trailing_empty_columns_preserve_original_provenance(self):
        # Campaign Manager 360 writes 46 fields for the 45-field Schedule A.
        # All 4,860 observed Rounds Schedule A rows have an empty final field.
        row = transaction("GIFT", "100.00") + [""]
        parsed = self.parse(report_rows(records=[row])).records[0]
        self.assertEqual(parsed["amount"], "100.00")
        self.assertEqual(parsed["source_row_sha256"], hashlib.sha256(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest())
        with self.assertRaises(ValueError):
            self.parse(report_rows(records=[transaction("GIFT", "100.00") + ["UNSUPPORTED DATA"]]))

    def test_all_supported_forms_reconcile_itemized_receipts_without_counting_memos_twice(self):
        for form_type, fields in FORM_FIELDS.items():
            for version in ("8.4", "8.5"):
                with self.subTest(form=form_type, version=version):
                    records = [
                        transaction("PARENT", "125.50", schedule=fields[5]),
                        transaction("ADJUST", "-25.50", schedule=fields[5]),
                        transaction("ATTRIBUTION", "125.50", schedule=fields[5], memo=True,
                                    back_reference="PARENT", memo_text="Partner attribution"),
                        transaction("OTHER", "700.00", schedule="SA11B"),
                        transaction("REFUND", "20.00", schedule=fields[6]),
                        transaction("REFMEMO", "20.00", schedule=fields[6], memo=True),
                        transaction("EXPENSE", "999.00", schedule="SB21B"),
                    ]
                    filing = self.parse(report_rows(form_type=form_type, version=version,
                                                    refunds="25.00", records=records))
                    meta = filing.metadata
                    self.assertEqual(meta["form_type"], form_type)
                    self.assertEqual(meta["format_version"], version)
                    self.assertEqual(meta["individual_itemized_calculated"], "100.00")
                    self.assertTrue(meta["individual_itemized_reconciles"])
                    self.assertEqual(meta["refunds_itemized"], "20.00")
                    self.assertEqual(meta["refunds_unitemized_or_unreconciled"], "5.00")
                    self.assertEqual(len(filing.records), 7)
                    memo = filing.records[2]
                    self.assertTrue(memo["memo"])
                    self.assertEqual(memo["back_reference_transaction_id"], "PARENT")
                    self.assertEqual(memo["memo_text"], "Partner attribution")

    def test_summary_mismatch_is_explicit_and_never_replaces_itemized_amounts(self):
        filing = self.parse(report_rows(individual="999.00", refunds="10.00", records=[
            transaction("A1", "100.00"), transaction("R1", "20.00", schedule="SB20A")]))
        self.assertFalse(filing.metadata["individual_itemized_reconciles"])
        self.assertEqual(filing.metadata["individual_itemized_reported"], "999.00")
        self.assertEqual(filing.metadata["individual_itemized_calculated"], "100.00")
        self.assertEqual(filing.metadata["refunds_unitemized_or_unreconciled"], "-10.00")
        self.assertEqual(filing.records[0]["amount"], "100.00")

    def test_source_hash_and_row_provenance_are_preserved_but_addresses_are_not_exported(self):
        path = self.write_report(report_rows())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        filing = parse_filing(path, 100, expected_sha256=digest)
        self.assertEqual(filing.metadata["sha256"], digest)
        row = filing.records[0]
        self.assertEqual(row["record_id"], "100:A1")
        self.assertEqual(row["source_record_number"], 3)
        self.assertEqual(row["donor_name"], "EXAMPLE, ALEX")
        self.assertEqual(row["date"], "2026-03-01")
        self.assertEqual(row["source_url"], "https://docquery.fec.gov/dcdev/posted/100.fec")
        self.assertEqual(len(row["source_row_sha256"]), 64)
        self.assertNotIn("PRIVATE STREET", str(row))
        self.assertNotIn("PRIVATE CITY", str(row))
        with self.assertRaisesRegex(ValueError, "Source hash changed"):
            parse_filing(path, 100, expected_sha256="0" * 64)

    def test_field_separator_and_legacy_encoding_preserve_reported_names(self):
        rows = report_rows(records=[transaction("A1", "100.00", name="GARCÍA")])
        for delimiter, encoding in [("\x1c", "cp1252"), (",", "utf-8-sig")]:
            with self.subTest(delimiter=delimiter, encoding=encoding):
                filing = self.parse(rows, delimiter=delimiter, encoding=encoding)
                self.assertEqual(filing.records[0]["donor_name"], "GARCÍA, ALEX")
                self.assertEqual(filing.metadata["encoding"], encoding)

    def test_missing_date_is_explicit_but_malformed_dates_are_rejected(self):
        filing = self.parse(report_rows(records=[transaction("A1", "100.00", date="")]))
        self.assertEqual(filing.records[0]["date"], "")
        for value in ["202611", "20261301", "20260230", "2026-03-01", " 20260301", "20260301 "]:
            with self.subTest(date=value), self.assertRaises(ValueError):
                iso_date(value)
        with self.assertRaisesRegex(ValueError, "Reversed reporting period"):
            self.parse(report_rows(start="20260401", end="20260331"))

    def test_invalid_layouts_identities_memo_flags_and_duplicate_transactions_fail(self):
        mutations = [
            lambda rows: rows[2].pop(),
            lambda rows: rows[2].append("unexpected"),
            lambda rows: rows[2].__setitem__(1, "C00000001"),
            lambda rows: rows[2].__setitem__(2, ""),
            lambda rows: rows[2].__setitem__(42, "Y"),
            lambda rows: rows.append(list(rows[2])),
            lambda rows: rows[1].__setitem__(1, "not-a-committee"),
            lambda rows: rows[1].__setitem__(0, "F9N"),
            lambda rows: rows[0].__setitem__(2, "8.3"),
            lambda rows: rows.__setitem__(1, rows[1][:30]),
        ]
        for index, mutate in enumerate(mutations):
            rows = report_rows()
            mutate(rows)
            with self.subTest(mutation=index), self.assertRaises(ValueError):
                self.parse(rows)

    def test_truncated_or_conflicting_amendment_headers_fail_clearly(self):
        for length in range(2, 7):
            rows = report_rows()
            rows[0] = rows[0][:length]
            with self.subTest(header_fields=length), self.assertRaises(ValueError):
                self.parse(rows)
        mutations = [
            lambda rows: rows.__setitem__(1, []),
            lambda rows: rows[0].__setitem__(5, "FEC-not-a-number"),
            lambda rows: rows[0].__setitem__(6, "not-a-number"),
            lambda rows: rows[0].__setitem__(6, "-1"),
            lambda rows: rows[0].__setitem__(6, "1"),  # Original form with amendment sequence.
            lambda rows: rows[0].__setitem__(5, "FEC-99"),  # Original points elsewhere.
            lambda rows: rows[1].__setitem__(0, "F3A"),  # Amendment with sequence zero.
        ]
        for index, mutate in enumerate(mutations):
            rows = report_rows()
            mutate(rows)
            with self.subTest(mutation=index), self.assertRaises(ValueError):
                self.parse(rows)

    def test_invalid_or_excess_precision_amounts_fail_instead_of_rounding(self):
        self.assertEqual(decimal_amount("-12.34"), Decimal("-12.34"))
        self.assertEqual(decimal_amount("0"), Decimal("0"))
        for value in ["", "NaN", "Infinity", "1.001", "-0.001", "12,345.67", "9" * 100]:
            with self.subTest(amount=value), self.assertRaises(ValueError):
                decimal_amount(value)
        with self.assertRaises(ValueError):
            self.parse(report_rows(records=[transaction("A1", "1.001")]))

    def test_amendment_replaces_whole_report_including_deleted_rows(self):
        original = self.parse(report_rows(individual="150", records=[
            transaction("KEEP", "100"), transaction("DELETE", "50")]), 100)
        amendment = self.parse(report_rows(file_number=101, original=100, sequence=1,
                                           individual="75", records=[transaction("KEEP", "75")]), 101)
        selected = latest_filings([amendment, original])
        self.assertEqual([f.metadata["file_number"] for f in selected], [101])
        self.assertEqual([(r["transaction_id"], r["amount"]) for f in selected for r in f.records], [("KEEP", "75.00")])
        self.assertEqual(len(original.records), 2)

    def test_missing_duplicate_or_changed_amendment_chains_fail(self):
        original = self.parse(file_number=100)
        a1 = self.parse(report_rows(original=100, sequence=1), 101)
        a2 = self.parse(report_rows(original=100, sequence=2), 102)
        duplicate_sequence = self.parse(report_rows(original=100, sequence=1), 103)
        changed_period = self.parse(report_rows(original=100, sequence=1, end="20260401"), 104)
        changed_type = self.parse(report_rows(original=100, sequence=1, report_type="M3"), 105)
        for label, filings in [("missing original", [a1]), ("missing middle", [original, a2]),
                               ("duplicate file", [original, original]),
                               ("duplicate sequence", [original, a1, duplicate_sequence]),
                               ("changed period", [original, changed_period]),
                               ("changed report type", [original, changed_type])]:
            with self.subTest(case=label), self.assertRaises(ValueError):
                latest_filings(filings)
        self.assertEqual([f.metadata["file_number"] for f in latest_filings([a2, original, a1])], [102])

    def test_overlapping_periods_fail_but_separate_periods_and_committees_preserve_identical_gifts(self):
        q1 = self.parse(file_number=100)
        boundary_overlap = self.parse(report_rows(start="20260331", end="20260630", report_type="Q2"), 200)
        nested = self.parse(report_rows(start="20260201", end="20260228", report_type="M2"), 201)
        for other in [boundary_overlap, nested]:
            with self.assertRaisesRegex(ValueError, "Overlapping report periods"):
                latest_filings([q1, other])
        q2 = self.parse(report_rows(start="20260401", end="20260630", report_type="Q2"), 202)
        other_committee = self.parse(report_rows(committee="C00654321"), 203)
        selected = latest_filings([q2, other_committee, q1])
        self.assertEqual(len(selected), 3)
        self.assertEqual(sum(Decimal(r["amount"]) for f in selected for r in f.records), Decimal("300.00"))
        self.assertEqual(len({r["record_id"] for f in selected for r in f.records}), 3)


if __name__ == "__main__":
    unittest.main()
