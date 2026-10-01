import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from pipeline.fec import sources, update_bulk


class Response(io.BytesIO):
    def __init__(self, body=b"", headers=None):
        super().__init__(body)
        self.headers = headers or {}


class BulkRefreshTests(unittest.TestCase):
    def test_freshness_distinguishes_installed_release_from_newer_remote_release(self):
        row = {
            "key": "individuals", "local_exists": True,
            "local_last_modified_utc": "2026-09-07T05:00:00+00:00",
            "remote_last_modified_utc": "2026-09-18T05:00:00+00:00",
            "remote_is_newer": True, "remote_error": "",
        }
        with patch.object(sources, "get_cycle_source_status", return_value=[row]):
            result = sources.build_source_manifest(2026)
        self.assertEqual(result["source_check_status"], "stale")
        self.assertEqual(result["latest_local_bulk_release_utc"], row["local_last_modified_utc"])
        self.assertEqual(result["latest_bulk_release_utc"], row["remote_last_modified_utc"])

    def test_unavailable_release_check_never_means_current(self):
        for remote_error in ("Network unavailable", ""):
            row = {
                "key": "individuals", "local_exists": True,
                "local_last_modified_utc": "2026-09-07T05:00:00+00:00",
                "remote_last_modified_utc": None,
                "remote_is_newer": False, "remote_error": remote_error,
            }
            with self.subTest(error=remote_error), patch.object(
                sources, "get_cycle_source_status", return_value=[row]
            ):
                result = sources.build_source_manifest(2026)
            self.assertEqual(result["source_check_status"], "unknown")
            self.assertEqual(result["sources_with_unverified_freshness"], ["individuals"])

    def test_short_download_keeps_previous_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "cm26.zip"
            archive.write_bytes(b"previous archive")
            response = Response(b"truncated", {"Content-Length": "100"})
            with patch.object(update_bulk, "urlopen", return_value=response):
                with self.assertRaisesRegex(OSError, "Incomplete download"):
                    update_bulk._download("https://example.test/cm26.zip", archive)
            self.assertEqual(archive.read_bytes(), b"previous archive")
            self.assertEqual(list(Path(directory).glob("*.download")), [])

    def test_non_zip_download_keeps_previous_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "cm26.zip"
            archive.write_bytes(b"previous archive")
            with patch.object(update_bulk, "urlopen", return_value=Response(b"error page")):
                with self.assertRaises(zipfile.BadZipFile):
                    update_bulk._download("https://example.test/cm26.zip", archive)
            self.assertEqual(archive.read_bytes(), b"previous archive")

    def test_missing_expected_member_keeps_previous_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "cm26"
            target.mkdir()
            (target / "cm.txt").write_text("previous data")
            archive = root / "cm26.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr("unexpected.txt", "new data")
            with self.assertRaises(FileNotFoundError):
                update_bulk._refresh_extract_dir(
                    archive, target, None, expected_file="cm.txt"
                )
            self.assertEqual((target / "cm.txt").read_text(), "previous data")
            self.assertEqual(list(root.glob("*.tmp")), [])

    def test_valid_refresh_replaces_data_and_retains_release_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "cm26"
            target.mkdir()
            (target / "cm.txt").write_text("previous data")
            archive = root / "cm26.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr("cm.txt", "new data")
            update_bulk._refresh_extract_dir(
                archive, target, 1_700_000_000, expected_file="cm.txt"
            )
            self.assertEqual((target / "cm.txt").read_text(), "new data")
            self.assertEqual((target / "cm.txt").stat().st_mtime, 1_700_000_000)


class SourceStatusTests(unittest.TestCase):
    def test_http_headers_are_case_insensitive(self):
        for headers in [
            {"Last-Modified": "Mon, 07 Sep 2026 05:51:20 GMT", "ETag": '"abc"', "Content-Length": "123"},
            {"last-modified": "Mon, 07 Sep 2026 05:51:20 GMT", "etag": '"abc"', "content-length": "123"},
        ]:
            with self.subTest(headers=headers), tempfile.TemporaryDirectory() as directory:
                local = Path(directory) / "cm.txt"
                local.write_text("data")
                os.utime(local, (1_700_000_000, 1_700_000_000))
                with (
                    patch.object(sources, "BULK_FILE_SPECS", [sources.BULK_FILE_SPECS[0]]),
                    patch.object(sources, "local_bulk_path", return_value=local),
                    patch.object(sources, "urlopen", return_value=Response(headers=headers)),
                ):
                    status = sources.get_cycle_source_status(2026)[0]
                self.assertTrue(status["remote_is_newer"])
                self.assertEqual(status["remote_content_length"], 123)
                self.assertEqual(status["remote_etag"], '"abc"')
                self.assertEqual(status["remote_last_modified_utc"], "2026-09-07T05:51:20+00:00")


if __name__ == "__main__":
    unittest.main()
