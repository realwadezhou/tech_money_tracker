from __future__ import annotations

import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.request import Request, urlopen

from pipeline.fec_sources import (
    BULK_FILE_SPECS,
    bulk_download_url,
    local_bulk_path,
    local_zip_path,
)


CHUNK_SIZE = 1024 * 1024 * 8


def _remote_timestamp(response) -> float | None:
    last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        return None
    parsed = parsedate_to_datetime(last_modified)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _download(url: str, destination: Path) -> float | None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        delete=False,
        dir=destination.parent,
        prefix=destination.name + ".",
        suffix=".download",
    ) as tmp_file:
        temp_path = Path(tmp_file.name)

    try:
        request = Request(url, headers={"User-Agent": "tech-money/1.0"})
        with urlopen(request, timeout=120) as response:
            remote_ts = _remote_timestamp(response)
            total = int(response.headers.get("Content-Length") or 0)
            downloaded = 0
            with temp_path.open("wb") as out_file:
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded / total * 100
                        print(
                            f"    Downloaded {downloaded:,} / {total:,} bytes "
                            f"({pct:.1f}%)"
                        )
                    else:
                        print(f"    Downloaded {downloaded:,} bytes")
        temp_path.replace(destination)
        if remote_ts is not None:
            os.utime(destination, (remote_ts, remote_ts))
        return remote_ts
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _apply_timestamp(path: Path, timestamp: float | None) -> None:
    if timestamp is None:
        return
    for item in sorted(path.rglob("*"), reverse=True):
        os.utime(item, (timestamp, timestamp))
    os.utime(path, (timestamp, timestamp))


def _refresh_extract_dir(zip_path: Path, extract_dir: Path, timestamp: float | None) -> None:
    extract_parent = extract_dir.parent
    extract_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(
        tempfile.mkdtemp(
            dir=extract_parent,
            prefix=extract_dir.name + ".",
            suffix=".tmp",
        )
    )
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(temp_dir)

        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        temp_dir.replace(extract_dir)
        _apply_timestamp(extract_dir, timestamp)
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


def update_cycle(cycle: int, *, force: bool = False) -> None:
    print(f"Updating FEC bulk files for {cycle}...")
    for spec in BULK_FILE_SPECS:
        extract_path = local_bulk_path(cycle, spec)
        zip_path = local_zip_path(cycle, spec)
        url = bulk_download_url(cycle, spec)

        local_mtime = extract_path.stat().st_mtime if extract_path.exists() else 0.0
        remote_mtime = None
        should_download = force or not extract_path.exists()

        if not should_download:
            request = Request(url, method="HEAD", headers={"User-Agent": "tech-money/1.0"})
            with urlopen(request, timeout=60) as response:
                remote_mtime = _remote_timestamp(response)
            should_download = remote_mtime is None or remote_mtime > local_mtime

        if not should_download:
            print(f"  {spec.label}: already current")
            continue

        print(f"  {spec.label}: downloading {url}")
        if remote_mtime is None:
            remote_mtime = _download(url, zip_path)
        else:
            remote_mtime = _download(url, zip_path)
        print(f"  {spec.label}: extracting to {extract_path.parent}")
        _refresh_extract_dir(zip_path, extract_path.parent, remote_mtime)

        expected_path = extract_path.parent / spec.local_file
        if not expected_path.exists():
            raise FileNotFoundError(
                f"Expected extracted file missing: {expected_path}"
            )
        print(
            "  "
            f"{spec.label}: updated "
            f"(zip={zip_path.name}, extracted={expected_path.name})"
        )


def main(args: list[str] | None = None) -> None:
    argv = args if args is not None else sys.argv[1:]
    force = False
    cycles: list[int] = []
    for arg in argv:
        if arg == "--force":
            force = True
            continue
        cycles.append(int(arg))
    if not cycles:
        cycles = [2026]
    for cycle in cycles:
        update_cycle(cycle, force=force)


if __name__ == "__main__":
    main()
