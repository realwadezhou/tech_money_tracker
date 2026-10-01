"""Manifest, review-dependency, and public-output tests for bounded FEC reports."""

from copy import deepcopy
from decimal import Decimal
import hashlib
import unittest

from pipeline.fec.campaign_reports import build_report, classify_records
from pipeline.fec.filings import Filing


def source_record(record_id="100:A1", amount="100.00", **changes):
    file_number, transaction_id = record_id.split(":")
    return {
        "record_id": record_id, "committee_id": "C00123456", "file_number": file_number,
        "transaction_id": transaction_id, "schedule": "SA11AI", "entity_type": "IND",
        "amount": amount, "employer": "NVIDIA", "memo": False,
        "back_reference_transaction_id": "", "date": "2026-03-01", "election": "P2026",
        "donor_name": "EXAMPLE, ALEX", "organization": "", "back_reference_schedule": "",
        "memo_text": "", "other_committee_id": "",
        "source_row_sha256": hashlib.sha256(record_id.encode()).hexdigest(),
        "source_record_number": 3,
        "source_url": f"https://docquery.fec.gov/dcdev/posted/{file_number}.fec",
        **changes,
    }


def filing_fixture(records=None, *, file_number=100, sequence=0, original=100, **metadata):
    return Filing({
        "file_number": file_number, "committee_id": "C00123456", "form_type": "F3",
        "format_version": "8.4", "encoding": "utf-8-sig", "original_file_number": original,
        "amendment_sequence": sequence, "coverage_start": "2026-01-01", "coverage_end": "2026-03-31",
        "report_type": "Q1", "sha256": hashlib.sha256(str(file_number).encode()).hexdigest(),
        "url": f"https://docquery.fec.gov/dcdev/posted/{file_number}.fec",
        "individual_itemized_reported": "100.00", "individual_itemized_calculated": "100.00",
        "individual_itemized_reconciles": True, "refunds_reported": "0.00", "refunds_itemized": "0.00",
        "refunds_unitemized_or_unreconciled": "0.00", **metadata,
    }, [source_record()] if records is None else records)


def manifest_fixture(filings=None):
    filings = [filing_fixture()] if filings is None else filings
    return {
        "schema_version": 1, "report_id": "example--nvidia", "cycle": 2026,
        "candidate": {"id": "H0VA00001", "name": "Example Candidate"},
        "company": {"key": "nvidia", "label": "NVIDIA"},
        "scope": {"kind": "campaign_reports", "description": "Reviewed first-quarter report.",
                  "coverage_start": "2026-01-01", "coverage_end": "2026-03-31",
                  "committee_ids": ["C00123456"], "account_ownership": "Reviewed for this period.",
                  "source_completeness": "The first-quarter report only; other periods are not covered."},
        "sources": [{"file_number": f.metadata["file_number"], "sha256": f.metadata["sha256"]} for f in filings],
        "latest_file_numbers": [filings[-1].metadata["file_number"]],
        "committee_scope_review": {"status": "resolved_for_scope", "evidence": ["Official committee registration"]},
        "employer_aliases": {"NVIDIA": "nvidia"}, "reviews": [],
        "source_catalog_as_of": "2026-09-19", "limitations": ["Not a complete-cycle estimate."],
    }


def annotation(record, **changes):
    return {"record_id": record["record_id"], "source_row_sha256": record["source_row_sha256"],
            "role": "direct_receipt", "evidence": [record["source_url"]],
            "rationale": "Reviewed original contribution line.", **changes}


class CampaignReportManifestTests(unittest.TestCase):
    def test_cycle_is_safe_and_matches_report_headers_without_filtering_old_correction_dates(self):
        filing = filing_fixture([source_record(date="2024-12-15", election="G2024")])
        # Report cycle, receipt date and election designation are distinct.
        report = build_report(manifest_fixture([filing]), [filing])
        self.assertEqual(report["combined_amount"], "100.00")
        for cycle in [2024, 2025, "2026", "../../other", True, None, 1978, 10000]:
            manifest = manifest_fixture([filing])
            manifest["cycle"] = cycle
            with self.subTest(cycle=cycle), self.assertRaisesRegex(ValueError, "Cycle|cycle"):
                build_report(manifest, [filing])

    def test_valid_scope_outputs_supported_amounts_counts_and_only_relevant_evidence(self):
        target = source_record()
        unrelated = source_record("100:A2", "50.00", employer="UNTRACKED")
        filing = filing_fixture([target, unrelated], individual_itemized_reported="150.00",
                                individual_itemized_calculated="150.00")
        report = build_report(manifest_fixture([filing]), [filing])
        self.assertEqual(report["combined_amount"], "100.00")
        self.assertEqual(report["net_amount"], "100.00")
        self.assertEqual(report["combined_status"], "resolved_for_scope")
        self.assertEqual(report["components"]["direct_receipts"]["count"], 1)
        self.assertEqual(report["components"]["refunds"]["amount"], "0.00")
        self.assertEqual(report["diagnostics"]["source_records"], 2)
        self.assertEqual(report["diagnostics"]["campaign_nonmemo_receipts"], "150.00")
        self.assertEqual([r["record_id"] for r in report["evidence"]], ["100:A1"])
        self.assertEqual(report["evidence"][0]["counted_amount"], "100.00")
        self.assertEqual(report["source_inventory"][0]["sha256"], filing.metadata["sha256"])
        self.assertIn("not a complete employee-giving estimate", report["combined_reason"])
        self.assertIn("first-quarter report only", report["scope"]["source_completeness"])

    def test_bad_schema_unsafe_id_and_source_inventory_fail_closed(self):
        filing = filing_fixture()
        for label, modify in [
            ("schema", lambda m: m.update(schema_version=2)),
            ("unsafe id", lambda m: m.update(report_id="../escape")),
            ("empty inventory", lambda m: m.update(sources=[])),
            ("duplicate inventory", lambda m: m["sources"].append(dict(m["sources"][0]))),
            ("missing source", lambda m: m["sources"][0].update(file_number=101)),
            ("hash changed", lambda m: m["sources"][0].update(sha256="0" * 64)),
            ("latest mismatch", lambda m: m.update(latest_file_numbers=[101])),
            ("committee mismatch", lambda m: m["scope"].update(committee_ids=["C00000001"])),
            ("start mismatch", lambda m: m["scope"].update(coverage_start="2025-01-01")),
            ("end mismatch", lambda m: m["scope"].update(coverage_end="2026-12-31")),
        ]:
            manifest = manifest_fixture([filing])
            modify(manifest)
            with self.subTest(case=label), self.assertRaises(ValueError):
                build_report(manifest, [filing])

    def test_latest_amendment_inventory_does_not_reintroduce_deleted_original_record(self):
        original = filing_fixture([source_record(), source_record("100:DELETE", "50.00")])
        amendment = filing_fixture([source_record("101:A1", "75.00")], file_number=101, sequence=1,
                                   individual_itemized_reported="75.00", individual_itemized_calculated="75.00")
        manifest = manifest_fixture([original, amendment])
        report = build_report(manifest, [original, amendment])
        self.assertEqual(report["combined_amount"], "75.00")
        self.assertEqual([f["file_number"] for f in report["source_inventory"]], [100, 101])
        self.assertEqual([f["file_number"] for f in report["sources"]], [101])
        self.assertEqual([r["record_id"] for r in report["evidence"]], ["101:A1"])
        manifest["latest_file_numbers"] = [100]
        with self.assertRaisesRegex(ValueError, "amendment selection"):
            build_report(manifest, [original, amendment])

    def test_unreviewed_ownership_or_unreconciled_cover_blocks_combined_without_hiding_components(self):
        for kind in ("ownership", "missing ownership evidence", "summary mismatch"):
            filing = filing_fixture()
            manifest = manifest_fixture([filing])
            if kind == "ownership":
                manifest.pop("committee_scope_review")
            elif kind == "missing ownership evidence":
                manifest["committee_scope_review"]["evidence"] = []
            else:
                filing.metadata["individual_itemized_reconciles"] = False
            report = build_report(manifest, [filing])
            with self.subTest(case=kind):
                self.assertIsNone(report["combined_amount"])
                self.assertIsNone(report["net_amount"])
                self.assertEqual(report["components"]["direct_receipts"]["amount"], "100.00")
                self.assertTrue(any(r["scope"] == "receipts" for r in report["unresolved"]))

    def test_gaps_or_a_partially_covered_second_account_block_combined_amount(self):
        q1 = filing_fixture()
        q2 = filing_fixture([source_record("200:A1")], file_number=200, original=200,
                            coverage_start="2026-04-01", coverage_end="2026-06-30", report_type="Q2")
        manifest = manifest_fixture([q1, q2])
        manifest["scope"]["coverage_end"] = "2026-06-30"
        manifest["latest_file_numbers"] = [100, 200]
        self.assertEqual(build_report(manifest, [q1, q2])["combined_amount"], "200.00")
        for kind in ("period gap", "second account starts late"):
            later, changed = deepcopy(q2), deepcopy(manifest)
            if kind == "period gap":
                later.metadata.update(coverage_start="2026-07-01", coverage_end="2026-09-30", report_type="Q3")
                changed["scope"]["coverage_end"] = "2026-09-30"
            else:
                later.metadata["committee_id"] = "C00999999"
                later.records[0]["committee_id"] = "C00999999"
                changed["scope"]["committee_ids"].append("C00999999")
            report = build_report(changed, [q1, later])
            with self.subTest(case=kind):
                self.assertIsNone(report["combined_amount"])
                self.assertIsNone(report["net_amount"])
                self.assertTrue(any("continuously cover" in row["reason"] for row in report["unresolved"]))

    def test_unknown_refund_preserves_receipts_but_never_implies_zero_or_known_net(self):
        refund = source_record("100:R1", "25.00", schedule="SB20A", employer="")
        filing = filing_fixture([source_record(), refund], refunds_reported="25.00", refunds_itemized="25.00")
        report = build_report(manifest_fixture([filing]), [filing])
        self.assertEqual(report["combined_amount"], "100.00")
        self.assertIsNone(report["net_amount"])
        self.assertEqual(report["net_status"], "unresolved")
        self.assertIsNone(report["components"]["refunds"]["amount"])
        self.assertEqual(report["components"]["refunds"]["count"], 0)
        self.assertTrue(any(r["record_id"] == "100:R1" and r["scope"] == "refunds" for r in report["unresolved"]))
        self.assertEqual({r["record_id"] for r in report["evidence"]}, {"100:A1", "100:R1"})

    def test_unitemized_or_unreconciled_refunds_block_net_even_without_itemized_refund_rows(self):
        filing = filing_fixture(refunds_reported="5.00", refunds_unitemized_or_unreconciled="5.00")
        report = build_report(manifest_fixture([filing]), [filing])
        self.assertEqual(report["combined_amount"], "100.00")
        self.assertIsNone(report["net_amount"])
        self.assertTrue(any(r["record_id"] == "refund_coverage" for r in report["unresolved"]))

    def test_subset_requires_explicit_existing_unique_scope_and_cannot_establish_refund_completeness(self):
        filing = filing_fixture([source_record(), source_record("100:A2", "50.00", employer="UNTRACKED")])
        valid = manifest_fixture([filing])
        valid["scope"]["kind"] = "reviewed_record_group"
        valid["selected_record_ids"] = ["100:A1"]
        report = build_report(valid, [filing])
        self.assertEqual(report["combined_amount"], "100.00")
        self.assertIsNone(report["net_amount"])
        self.assertEqual(report["diagnostics"]["source_records"], 1)
        for label, modify in [
            ("undeclared subset", lambda m: m["scope"].update(kind="campaign_reports")),
            ("empty subset", lambda m: m.update(selected_record_ids=[])),
            ("duplicate subset", lambda m: m.update(selected_record_ids=["100:A1", "100:A1"])),
            ("missing record", lambda m: m.update(selected_record_ids=["100:NO-SUCH-RECORD"])),
            ("subset label without subset", lambda m: m.pop("selected_record_ids")),
        ]:
            manifest = deepcopy(valid)
            modify(manifest)
            with self.subTest(case=label), self.assertRaises(ValueError):
                build_report(manifest, [filing])


class CampaignRecordClassificationTests(unittest.TestCase):
    def test_transfer_structure_alone_does_not_establish_joint_fundraising_but_explicit_review_can(self):
        transfer = source_record("100:T", "500.00", schedule="SA12", entity_type="PAC", employer="")
        allocation = source_record("100:J", "100.00", schedule="SA12", memo=True,
                                   back_reference_transaction_id="T")
        parent = source_record("100:P", "200.00", entity_type="ORG", employer="")
        partner = source_record("100:PART", "100.00", memo=True, back_reference_transaction_id="P",
                                memo_text="Partnership attribution")
        unknown = source_record("100:UNKNOWN", "100.00", memo=True, memo_text="Partnership attribution")
        rows = classify_records([transfer, allocation, parent, partner, unknown], [])
        by_id = {r.record_id: r for r in rows}
        self.assertIn(by_id["100:J"].role, {"auto", "unresolved"})
        reviewed = classify_records([transfer, allocation], [annotation(
            allocation, role="jfc_allocation", related_record_id="100:T",
            rationale="Reviewed campaign Schedule A allocation and original JFC transfer.")])
        self.assertEqual(reviewed[1].role, "jfc_allocation")
        self.assertEqual(reviewed[1].related_record_id, "100:T")
        self.assertEqual(by_id["100:PART"].role, "partnership_attribution")
        self.assertEqual(by_id["100:PART"].related_record_id, "100:P")
        self.assertEqual(by_id["100:UNKNOWN"].role, "auto")

    def test_unreviewed_positive_adjustment_does_not_become_a_second_gift(self):
        for description in ["Election redesignation", "Re-attribution", "Adjustment", "Original contribution"]:
            row = source_record(memo_text=description)
            with self.subTest(description=description):
                self.assertEqual(classify_records([row], [])[0].role, "unresolved")

    def test_complete_review_dependency_set_is_required_with_unchanged_hashes(self):
        one, sibling = source_record(), source_record("100:A2", "-100.00")
        reviews = [annotation(one), annotation(sibling, role="adjustment", related_record_id=one["record_id"])]
        self.assertEqual(len(classify_records([one, sibling], reviews)), 2)
        with self.assertRaisesRegex(ValueError, "missing or changed"):
            classify_records([one], reviews)
        changed = {**sibling, "source_row_sha256": "0" * 64}
        with self.assertRaisesRegex(ValueError, "missing or changed"):
            classify_records([one, changed], reviews)
        with self.assertRaisesRegex(ValueError, "reviewed parent missing"):
            classify_records([sibling], [reviews[1]])

    def test_reviews_require_unique_rows_and_source_evidence_with_rationale(self):
        row = source_record()
        with self.assertRaisesRegex(ValueError, "Repeated source"):
            classify_records([row, row], [])
        review = annotation(row)
        with self.assertRaisesRegex(ValueError, "Repeated review"):
            classify_records([row], [review, review])
        for field in ("evidence", "rationale"):
            invalid = {**review, field: [] if field == "evidence" else ""}
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "evidence and rationale"):
                classify_records([row], [invalid])

    def test_review_annotations_cannot_overwrite_hash_pinned_filed_values(self):
        row = source_record()
        for field, value in [("amount", "999.00"), ("employer", "OTHER COMPANY"), ("memo", True),
                             ("committee_id", "C00000001"), ("date", "2020-01-01")]:
            review = annotation(row, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                classify_records([row], [review])
        normalized = classify_records([row], [annotation(row, identity_key="reviewed-person-1")])[0]
        self.assertEqual(normalized.amount, Decimal("100.00"))
        self.assertEqual(normalized.identity_key, "reviewed-person-1")


if __name__ == "__main__":
    unittest.main()
