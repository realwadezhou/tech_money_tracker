import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline.lda.refresh import download_endpoint, install_endpoint, read, save, instant


def record(uid, second):
    return {"filing_uuid": uid, "filing_year": 2026,
            "dt_posted": f"2026-01-01T00:00:{second:02d}+00:00"}


class FakeAPI:
    def __init__(self, records, *, page_size=3, fail_at=None):
        self.records = records
        self.page_size = page_size
        self.calls = 0
        self.fail_at = fail_at

    def get(self, endpoint, **params):
        self.calls += 1
        if self.calls == self.fail_at:
            raise RuntimeError("interrupted network")
        rows = self.records
        for key, condition in (("filing_dt_posted_after", lambda a, b: a >= b),
                               ("filing_dt_posted_before", lambda a, b: a <= b)):
            if params.get(key):
                rows = [r for r in rows if condition(instant(r["dt_posted"]), instant(params[key]))]
        rows = sorted(rows, key=lambda r: (instant(r["dt_posted"]), r["filing_uuid"]))
        size = min(self.page_size, params.get("page_size", 25))
        start = (params.get("page", 1) - 1) * size
        return {"count": len(rows), "results": rows[start:start + size],
                "next": "next" if start + size < len(rows) else None}


class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.stage = self.root / "stage"
        self.cutoff = "2026-09-08T00:00:00+00:00"

    def download(self, api):
        return download_endpoint(2026, "filings", self.stage, api, cutoff=self.cutoff)

    def test_overlapping_timestamp_pages_preserve_every_id_once(self):
        rs = [record(str(i), second) for i, second in enumerate((1, 2, 2, 2, 3, 4, 5))]
        manifest = self.download(FakeAPI(rs))
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["unique_id_count"], 7)
        self.assertGreater(manifest["duplicate_row_count"], 0)
        lines = [json.loads(line) for line in (self.stage / "snapshot.jsonl").read_text().splitlines()]
        self.assertEqual({r["filing_uuid"] for r in lines}, {r["filing_uuid"] for r in rs})
        self.assertEqual(len(lines), len(rs))

    def test_large_tied_timestamp_bucket_is_paginated_and_verified(self):
        rs = [record(str(i), 1) for i in range(9)] + [record("later", 2)]
        manifest = self.download(FakeAPI(rs))
        self.assertEqual(manifest["unique_id_count"], 10)

    def test_interruption_resumes_without_losing_completed_pages(self):
        rs = [record(str(i), i) for i in range(8)]
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.download(FakeAPI(rs, fail_at=3))
        first = (self.stage / "page_00001.json").read_bytes()
        manifest = self.download(FakeAPI(rs))
        self.assertEqual(manifest["unique_id_count"], 8)
        self.assertEqual((self.stage / "page_00001.json").read_bytes(), first)

    def test_source_count_change_fails_before_snapshot_install(self):
        api = FakeAPI([record("one", 1), record("two", 2)])
        get = api.get
        def changed(endpoint, **params):
            value = get(endpoint, **params)
            if api.calls == 1:
                value["count"] = 3
            return value
        api.get = changed
        with self.assertRaisesRegex(RuntimeError, "expected 3 IDs"):
            self.download(api)
        self.assertFalse((self.stage / "snapshot.jsonl").exists())

    def test_empty_endpoint_and_post_cutoff_records_are_distinguished(self):
        manifest = self.download(FakeAPI([]))
        self.assertEqual(manifest["unique_id_count"], 0)
        self.assertTrue(read(self.stage / "verification.json")["complete_as_of_verification"])

    def test_newer_filings_do_not_turn_a_bounded_snapshot_into_current_coverage(self):
        rs = [record("old", 1), {**record("new", 2), "dt_posted": "2026-09-09T00:00:00+00:00"}]
        self.download(FakeAPI(rs))
        verification = read(self.stage / "verification.json")
        self.assertTrue(verification["complete_before_cutoff_by_count"])
        self.assertFalse(verification["complete_as_of_verification"])
        self.assertEqual(verification["live_api_count"], 2)

    def test_missing_id_and_wrong_year_are_rejected(self):
        for row in ({**record("wrong", 1), "filing_year": 2025}, record("", 1)):
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.download(FakeAPI([row]))

    def test_verified_install_archives_old_files_and_is_recoverable(self):
        self.stage = self.root / "data/lda/refresh/run/2026/filings"
        target = self.root / "data/lda/raw/2026/filings"
        save(target / "manifest.json", {"old": True})
        self.download(FakeAPI([record("new", 1)]))
        install_endpoint(self.root, "run", 2026, "filings")
        self.assertEqual(read(self.root / "data/lda/archive/run/2026/filings/manifest.json"), {"old": True})
        self.assertEqual(read(target / "manifest.json")["unique_id_count"], 1)
        install_endpoint(self.root, "run", 2026, "filings")

    def test_download_to_normalized_tables_to_validated_explorer(self):
        from pipeline.lda import normalize
        from pipeline.lda.build_explorer import build_explorer
        from frontend import lobbying
        from scripts.validate_site import validate_lobbying
        description = "Artificial intelligence\u2028and copyright\u2029and standards\u0085research"
        filing = {**record("example", 1), "filing_type": "Q1", "filing_period": "first_quarter",
            "filing_document_url": "https://lda.gov/filings/public/filing/example/print/",
            "client": {"id": 1, "name": "Example Research Organization"},
            "registrant": {"id": 2, "name": "Example Lobbying Firm"},
            "lobbying_activities": [{"general_issue_code": "SCI", "general_issue_code_display": "Science",
                                     "description": description}]}
        for ep, values in (("filings", [filing]), ("contributions", [])):
            stage = self.root / "data/lda/refresh/run/2026" / ep
            download_endpoint(2026, ep, stage, FakeAPI(values), cutoff=self.cutoff)
            install_endpoint(self.root, "run", 2026, ep)
        raw = self.root / "data/lda/raw/2026"
        interim = self.root / "data/lda/interim/2026"
        with patch.object(normalize, "lda_year_raw_dir", return_value=raw), patch.object(
                normalize, "lda_year_interim_dir", return_value=interim):
            normalize.normalize_year(2026)
        output = self.root / "exports/lobbying"
        metadata = build_explorer([2026], root=self.root, output=output)
        self.assertEqual(metadata["activity_count"], 1)
        self.assertEqual(read(output / "explorer.json")["activities"][0]["description"], description)
        self.assertTrue(metadata["sources"][0]["complete_before_cutoff_by_count"])
        self.assertEqual(metadata["sources"][0]["snapshot_at"], self.cutoff)
        site = self.root / "docs"
        with patch.object(lobbying, "EXPORT", output):
            lobbying.build_lobbying(site, [])
        errors = []
        self.assertEqual(validate_lobbying(site, errors)["topic_matches"], 2)
        self.assertEqual(errors, [])
        html = (site / "lobbying/index.html").read_text(encoding="utf-8")
        self.assertIn("Full download counts checked", html)
        self.assertNotIn("particularly incomplete", html)


if __name__ == "__main__":
    unittest.main()
