from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from scripts.validate_site import validate_attribution, validate_attribution_report


def fixture():
    source = {
        "file_number": 123, "committee_id": "C00000001", "sha256": "a" * 64,
        "url": "https://docquery.fec.gov/dcdev/posted/123.fec",
        "original_file_number": 123, "amendment_sequence": 0,
        "coverage_start": "2026-01-01", "coverage_end": "2026-03-31",
        "form_type": "F3", "report_type": "Q1",
    }
    values = {"direct_receipts": "100.10", "jfc_allocations": "25.20",
              "partnership_attributions": "0.00", "signed_adjustments": "-0.30", "refunds": "-20.00"}
    evidence = []
    for position, (component, value) in enumerate(values.items(), start=3):
        if component == "partnership_attributions":
            continue
        transaction = f"R{position}"
        evidence.append({
            "record_id": f"123:{transaction}", "file_number": 123, "transaction_id": transaction,
            "committee_id": "C00000001", "source_url": source["url"],
            "source_row_sha256": "b" * 64, "source_record_number": position,
            "amount": "20.00" if component == "refunds" else value,
            "counted_amount": value, "component": component, "disposition": "included",
        })
    return {
        "schema_version": 1, "report_id": "example--nvidia", "cycle": 2026,
        "sources": [source], "source_inventory": [deepcopy(source)], "manifest_sha256": "c" * 64,
        "components": {key: {"amount": value, "count": 0 if key == "partnership_attributions" else 1,
                             "status": "supported_component"} for key, value in values.items()},
        "combined_amount": "125.00", "combined_status": "resolved_for_scope",
        "net_amount": "105.00", "net_status": "resolved_for_scope", "unresolved": [],
        "evidence": evidence,
    }


class AttributionValidationTests(unittest.TestCase):
    def errors(self, report):
        errors = []
        validate_attribution_report("test", report, errors)
        return errors

    def test_decimal_components_and_evidence_reconcile_exactly(self):
        self.assertEqual(self.errors(fixture()), [])

    def test_changed_combined_and_net_amounts_are_detected(self):
        for key in ("combined_amount", "net_amount"):
            with self.subTest(key=key):
                report = fixture()
                report[key] = "999.00"
                self.assertTrue(self.errors(report))

    def test_component_amount_count_and_filed_sign_tampering_are_detected(self):
        changes = [
            lambda r: r["components"]["direct_receipts"].update(amount="100.11"),
            lambda r: r["components"]["direct_receipts"].update(count=2),
            lambda r: r["evidence"][0].update(counted_amount="100.11"),
            lambda r: r["evidence"][-1].update(amount="-20.00"),
        ]
        for change in changes:
            report = fixture()
            change(report)
            self.assertTrue(self.errors(report))

    def test_unknown_refunds_preserve_receipts_but_cannot_be_zero_net(self):
        report = fixture()
        report["components"]["refunds"].update(amount=None, status="unresolved")
        report.update(net_amount=None, net_status="unresolved")
        report["unresolved"] = [{"record_id": "refund_scope", "scope": "refunds"}]
        self.assertEqual(self.errors(report), [])
        report["net_amount"] = "0.00"
        self.assertTrue(any("must be null" in x for x in self.errors(report)))

    def test_unknown_combined_or_component_cannot_be_filled_with_a_subtotal(self):
        report = fixture()
        report.update(combined_status="unresolved", combined_amount=None, net_status="unresolved", net_amount=None)
        report["unresolved"] = [{"record_id": "memo", "scope": "receipts"}]
        self.assertEqual(self.errors(report), [])
        report["combined_amount"] = "125.00"
        self.assertTrue(self.errors(report))
        report = fixture()
        report["components"]["refunds"].update(status="unresolved", amount="0.00")
        self.assertTrue(self.errors(report))

    def test_resolved_status_cannot_ignore_unresolved_scope(self):
        for scope in ("receipts", "refunds"):
            with self.subTest(scope=scope):
                report = fixture()
                report["unresolved"] = [{"record_id": "unknown", "scope": scope}]
                self.assertTrue(self.errors(report))

    def test_duplicate_evidence_source_and_wrong_committee_are_detected(self):
        report = fixture()
        report["evidence"].append(deepcopy(report["evidence"][0]))
        self.assertTrue(any("Duplicate" in x for x in self.errors(report)))
        report = fixture()
        report["sources"].append(deepcopy(report["sources"][0]))
        self.assertTrue(self.errors(report))
        report = fixture()
        report["evidence"][0]["committee_id"] = "C99999999"
        self.assertTrue(self.errors(report))

    def test_hash_source_link_record_position_and_parent_are_validated(self):
        for key, value in [("source_row_sha256", "missing"), ("source_url", "https://example.org/123.fec"),
                           ("source_record_number", 0), ("related_record_id", "123:missing")]:
            with self.subTest(key=key):
                report = fixture()
                report["evidence"][0][key] = value
                self.assertTrue(self.errors(report))
        report = fixture()
        report["sources"][0]["sha256"] = "changed"
        self.assertTrue(self.errors(report))

    def test_stale_latest_source_and_missing_amendment_are_detected(self):
        report = fixture()
        amendment = deepcopy(report["sources"][0])
        amendment.update(file_number=124, amendment_sequence=1,
                         url="https://docquery.fec.gov/dcdev/posted/124.fec", sha256="d" * 64)
        report["source_inventory"].append(amendment)
        self.assertTrue(any("latest source" in x for x in self.errors(report)))
        report["source_inventory"][-1]["amendment_sequence"] = 2
        self.assertTrue(any("amendment inventory" in x for x in self.errors(report)))

    def test_context_records_cannot_leak_into_counted_totals(self):
        report = fixture()
        report["evidence"][0]["disposition"] = "context"
        self.assertTrue(self.errors(report))

    def test_nonfinite_float_subcent_and_boolean_values_are_rejected(self):
        for value in ("NaN", "Infinity", "0.001", 100.1, True):
            with self.subTest(value=value):
                report = fixture()
                report["components"]["direct_receipts"]["amount"] = value
                self.assertTrue(self.errors(report))
        report = fixture()
        report["components"]["direct_receipts"]["count"] = True
        self.assertTrue(self.errors(report))

    def test_optional_directory_integrates_without_requiring_old_exports_to_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            errors = []
            self.assertIsNone(validate_attribution(root, errors))
            self.assertEqual(errors, [])
            output = root / "2026/data/attribution/example--nvidia.json"
            output.parent.mkdir(parents=True)
            output.write_text(json.dumps(fixture()), encoding="utf-8")
            self.assertEqual(validate_attribution(root, errors)["reports"], 1)
            self.assertEqual(errors, [])
            report = fixture()
            report["cycle"] = 2024
            output.write_text(json.dumps(report), encoding="utf-8")
            validate_attribution(root, errors)
            self.assertTrue(any("cycle differs" in x for x in errors))


if __name__ == "__main__":
    unittest.main()
