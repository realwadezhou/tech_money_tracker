import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline.lda import ingest, reconcile


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class LDARefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.year_dir = Path(self.temp.name) / "2026"
        self.endpoint_dir = self.year_dir / "filings"
        self.spec = ingest.YEAR_ENDPOINTS[0]
        self.path_patch = patch.object(ingest, "lda_year_raw_dir", return_value=self.year_dir)
        self.path_patch.start()
        self.addCleanup(self.path_patch.stop)

    def old_snapshot(self):
        write_json(self.endpoint_dir / "manifest.json", {"complete": True, "row_count": 2})
        write_json(self.endpoint_dir / "page_00001.json", {"results": [{"filing_uuid": "old"}]})
        write_json(self.endpoint_dir / "page_00002.json", {"results": [{"filing_uuid": "stale"}]})
        (self.endpoint_dir / "snapshot.jsonl").write_text('{"filing_uuid":"old"}\n')
        (self.endpoint_dir / "supplemental.jsonl").write_text('{"filing_uuid":"old-repair"}\n')
        return {p.name: p.read_bytes() for p in self.endpoint_dir.iterdir()}

    def test_explicit_refresh_replaces_complete_snapshot_and_removes_stale_artifacts(self):
        self.old_snapshot()
        client = Mock()
        client.get.return_value = {"count": 1, "next": None, "results": [{"filing_uuid": "new"}]}
        cached = ingest.ingest_year_endpoint(2026, self.spec, client)
        self.assertEqual(cached["row_count"], 2)
        client.get.assert_not_called()
        fresh = ingest.ingest_year_endpoint(2026, self.spec, client, refresh=True)
        self.assertTrue(fresh["complete"])
        self.assertEqual(fresh["unique_id_count"], 1)
        self.assertEqual(fresh["effective_page_size"], 1)
        self.assertEqual({p.name for p in self.endpoint_dir.iterdir()}, {"page_00001.json", "manifest.json"})
        self.assertIn('"new"', (self.endpoint_dir / "page_00001.json").read_text())

    def test_empty_endpoint_is_a_complete_snapshot(self):
        client = Mock()
        client.get.return_value = {"count": 0, "next": None, "results": []}
        result = ingest.ingest_year_endpoint(2026, self.spec, client, refresh=True)
        self.assertTrue(result["complete"])
        self.assertEqual(result["row_count"], 0)

    def test_network_failure_preserves_every_existing_file(self):
        original = self.old_snapshot()
        client = Mock()
        client.get.side_effect = [
            {"count": 2, "next": "next-page", "results": [{"filing_uuid": "new"}]},
            RuntimeError("network failure"),
        ]
        with self.assertRaisesRegex(RuntimeError, "network failure"):
            ingest.ingest_year_endpoint(2026, self.spec, client, refresh=True)
        self.assertEqual(original, {p.name: p.read_bytes() for p in self.endpoint_dir.iterdir()})
        self.assertEqual(list(self.year_dir.iterdir()), [self.endpoint_dir])

    def test_duplicate_missing_and_incomplete_ids_cannot_replace_complete_snapshot(self):
        original = self.old_snapshot()
        cases = [
            {"count": 2, "next": None, "results": [{"filing_uuid": "same"}, {"filing_uuid": "same"}]},
            {"count": 2, "next": None, "results": [{"filing_uuid": "one"}, {}]},
            {"count": 2, "next": None, "results": [{"filing_uuid": "one"}]},
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                client = Mock()
                client.get.return_value = payload
                with self.assertRaisesRegex(RuntimeError, "existing data were preserved"):
                    ingest.ingest_year_endpoint(2026, self.spec, client, refresh=True)
                self.assertEqual(original, {p.name: p.read_bytes() for p in self.endpoint_dir.iterdir()})

    def test_partial_refresh_preserves_existing_snapshot(self):
        original = self.old_snapshot()
        client = Mock()
        client.get.return_value = {"count": 2, "next": "next", "results": [{"filing_uuid": "one"}]}
        with self.assertRaises(RuntimeError):
            ingest.ingest_year_endpoint(2026, self.spec, client, refresh=True, max_pages=1)
        self.assertEqual(original, {p.name: p.read_bytes() for p in self.endpoint_dir.iterdir()})


class LDAReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.year_dir = Path(self.temp.name) / "2026"
        self.endpoint_dir = self.year_dir / "filings"
        write_json(self.endpoint_dir / "manifest.json", {"api_reported_count": 2})
        write_json(self.endpoint_dir / "page_00001.json", {"results": [{"filing_uuid": "base"}]})
        (self.endpoint_dir / "supplemental.jsonl").write_text('{"filing_uuid":"previous-recovery"}\n')
        self.path_patch = patch.object(reconcile, "lda_year_raw_dir", return_value=self.year_dir)
        self.path_patch.start()
        self.addCleanup(self.path_patch.stop)

    def test_repair_preserves_previously_recovered_records(self):
        client = Mock()
        client.get.return_value = {"count": 3, "results": [{"filing_uuid": "new-recovery"}]}
        with patch.object(reconcile, "LDAClient", return_value=client), patch.object(
            reconcile, "_tied_timestamp_boundaries", return_value=[{"page_left": 1, "page_right": 2, "same_uuid": True}],
        ):
            result = reconcile.repair_filings(2026, max_attempts=1)
        recovered = reconcile._load_supplemental_rows(self.endpoint_dir, "filings")
        self.assertEqual(set(recovered), {"previous-recovery", "new-recovery"})
        self.assertEqual(result["snapshot_unique_id_count"], 3)
        self.assertTrue(result["complete_as_of_repair"])

    def test_snapshot_api_failure_preserves_old_snapshot(self):
        snapshot = self.endpoint_dir / "snapshot.jsonl"
        snapshot.write_text("old snapshot\n")
        client = Mock()
        client.get.side_effect = RuntimeError("API unavailable")
        with patch.object(reconcile, "LDAClient", return_value=client):
            with self.assertRaisesRegex(RuntimeError, "API unavailable"):
                reconcile.build_snapshot(2026, "filings")
        self.assertEqual(snapshot.read_text(), "old snapshot\n")

    def test_reconciliation_preserves_source_collection_date(self):
        collected = "2026-04-09T00:00:00+00:00"
        write_json(self.endpoint_dir / "manifest.json", {
            "api_reported_count": 2, "fetched_at_utc": collected})
        client = Mock()
        client.get.return_value = {"count": 2}
        with patch.object(reconcile, "LDAClient", return_value=client):
            result = reconcile.build_snapshot(2026, "filings")
        self.assertEqual(result["snapshot_cutoff_utc"], collected)
        self.assertNotEqual(result["snapshot_cutoff_utc"], result["snapshot_built_at_utc"])

    def test_missing_uuid_prevents_completeness_claim_even_when_count_matches(self):
        write_json(self.endpoint_dir / "page_00001.json", {"results": [{"filing_uuid": "base"}, {}]})
        client = Mock()
        client.get.return_value = {"count": 2}
        with patch.object(reconcile, "LDAClient", return_value=client):
            result = reconcile.build_snapshot(2026, "filings")
        self.assertEqual(result["snapshot_unique_id_count"], 2)
        self.assertEqual(result["missing_id_count"], 1)
        self.assertFalse(result["complete_as_of_snapshot"])
        self.assertIn("count only", result["verification_scope"])

    def test_tail_top_up_preserves_existing_payload_corrections(self):
        (self.endpoint_dir / "supplemental.jsonl").write_text(
            '{"filing_uuid":"base","correction":true}\n{"filing_uuid":"previous-recovery"}\n'
        )
        client = Mock()
        client.get.return_value = {"count": 3, "results": [{"filing_uuid": "new-recovery"}]}
        with patch.object(reconcile, "LDAClient", return_value=client):
            result = reconcile.top_up_tail_pages(2026, "filings", safety_pages=1)
        recovered = reconcile._load_supplemental_rows(self.endpoint_dir, "filings")
        self.assertEqual(set(recovered), {"base", "previous-recovery", "new-recovery"})
        self.assertTrue(recovered["base"]["correction"])
        self.assertTrue(result["complete_as_of_top_up"])


if __name__ == "__main__":
    unittest.main()
