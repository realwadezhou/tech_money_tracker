"""Resumable, rate-limited full LDA snapshots and downstream rebuilds.

Use one process: python -m pipeline.lda.refresh 2026 2025 2024 2023 2022 2021 2020
Re-running resumes the saved job. --new-run explicitly begins another refresh.
API pages deliberately overlap at timestamp boundaries to avoid losing records
with tied posting times. The reconciled snapshot contains each UUID once.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import threading
import time

from pipeline.common.paths import PROJECT_ROOT
from pipeline.lda.client import LDAClient


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def instant(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


class RequestPacer:
    def __init__(self, requests_per_minute=110):
        self.interval = 60 / requests_per_minute
        self.lock = threading.Lock()
        self.previous = 0.0

    def __call__(self):
        with self.lock:
            delay = self.interval - (time.monotonic() - self.previous)
            if delay > 0:
                time.sleep(delay)
            self.previous = time.monotonic()


def load_pages(directory):
    records, raw_count = {}, 0
    paths = sorted(directory.glob("page_*.json"))
    for path in paths:
        for row in read(path)["results"]:
            uid = row.get("filing_uuid")
            if not uid:
                raise ValueError(f"Missing filing UUID in {path}")
            records[uid] = row
            raw_count += 1
    return records, raw_count, len(paths)


def download_endpoint(year, endpoint, directory, client, *, cutoff=None):
    """Stage a full year, resuming interrupted pages without touching installed data."""
    checkpoint = directory / "download_state.json"
    if checkpoint.exists():
        state = read(checkpoint)
        if state["year"] != year or state["endpoint"] != endpoint:
            raise ValueError("Download checkpoint does not match requested endpoint")
    else:
        state = {"year": year, "endpoint": endpoint, "started_at": now(),
                 "cutoff": cutoff or now(), "cursor": None, "exhausted": False}
        save(checkpoint, state)
    base = {"filing_year": year, "filing_dt_posted_before": state["cutoff"]}
    if "expected_count" not in state:
        state["expected_count"] = client.get(endpoint + "/", page_size=1, **base)["count"]
        save(checkpoint, state)
    records, raw_count, page_number = load_pages(directory)
    last_log = 0.0

    def store_page(payload):
        nonlocal raw_count, page_number
        for row in payload["results"]:
            if not row.get("filing_uuid") or row.get("filing_year") != year:
                raise ValueError("API returned a missing ID or an unexpected filing year")
            if instant(row["dt_posted"]) > instant(state["cutoff"]):
                raise ValueError("API returned a record beyond the snapshot cutoff")
        page_number += 1
        save(directory / f"page_{page_number:05d}.json", payload)
        for row in payload["results"]:
            records[row["filing_uuid"]] = row
            raw_count += 1

    while not state["exhausted"]:
        query = {**base, "ordering": "dt_posted", "page_size": 25}
        if state["cursor"]:
            query["filing_dt_posted_after"] = state["cursor"]
        payload = client.get(endpoint + "/", **query)
        batch = payload["results"]
        store_page(payload)
        if not batch or payload.get("next") is None:
            state["exhausted"] = True
        else:
            newest = max((row["dt_posted"] for row in batch), key=instant)
            if state["cursor"] and instant(newest) <= instant(state["cursor"]):
                # A timestamp with >=25 records cannot advance through page 1.
                # Fetch that exact bucket, then verify its unique IDs before moving.
                tied, tie_page, tie_count = {}, 1, None
                while True:
                    bucket = client.get(endpoint + "/", filing_year=year,
                        filing_dt_posted_after=newest, filing_dt_posted_before=newest,
                        ordering="dt_posted,registrant__name,client__name", page_size=25, page=tie_page)
                    if tie_count is None:
                        tie_count = bucket["count"]
                    if bucket["count"] != tie_count:
                        raise RuntimeError("Timestamp bucket changed during download; resume to retry")
                    for row in bucket["results"]:
                        if instant(row["dt_posted"]) != instant(newest):
                            raise ValueError("API ignored the exact timestamp filter")
                        tied[row["filing_uuid"]] = row
                    store_page(bucket)
                    if bucket.get("next") is None:
                        break
                    tie_page += 1
                if len(tied) != tie_count:
                    raise RuntimeError("Timestamp bucket has missing IDs; saved data were not replaced")
                state["cursor"] = (instant(newest) + timedelta(microseconds=1)).isoformat()
            else:
                state["cursor"] = newest
        state.update(unique_id_count=len(records), raw_row_count=raw_count,
                     page_count=page_number, updated_at=now(), status="downloading")
        save(checkpoint, state)
        if time.monotonic() - last_log >= 30 or state["exhausted"]:
            print(f"{now()} {year}/{endpoint}: {len(records):,}/{state['expected_count']:,} records", flush=True)
            last_log = time.monotonic()

    final_count = client.get(endpoint + "/", page_size=1, **base)["count"]
    live_count = client.get(endpoint + "/", filing_year=year, page_size=1)["count"]
    if len(records) != state["expected_count"] or len(records) != final_count:
        raise RuntimeError(f"{year}/{endpoint}: expected {state['expected_count']} IDs, got {len(records)}, "
                           f"source now has {final_count} before cutoff. Staging retained for inspection.")
    snapshot = directory / "snapshot.jsonl"
    temporary = snapshot.with_suffix(".jsonl.tmp")
    checksum = sha256()
    with temporary.open("wb") as handle:
        for uid in sorted(records):
            line = (json.dumps(records[uid], ensure_ascii=False) + "\n").encode("utf-8")
            handle.write(line)
            checksum.update(line)
    temporary.replace(snapshot)
    finished = now()
    posted = [row["dt_posted"] for row in records.values()]
    verification = {"verified_at_utc": finished, "cutoff": state["cutoff"],
        "initial_api_count": state["expected_count"], "final_api_count_before_cutoff": final_count,
        "live_api_count": live_count, "unique_id_count": len(records),
        "complete_before_cutoff_by_count": True, "complete_as_of_verification": len(records) == live_count,
        "snapshot_sha256": checksum.hexdigest(),
        "verification_scope": "Full download bounded by posting timestamp; unique ID counts match source counts before and after download. This is not a transactional database snapshot or an independent comparison of every live record."}
    manifest = {"year": year, "endpoint": endpoint, "path": endpoint + "/", "page_size": 25,
        "effective_page_size": 25, "page_count": page_number, "row_count": raw_count,
        "unique_id_count": len(records), "missing_id_count": 0,
        "duplicate_row_count": raw_count - len(records), "api_reported_count": final_count,
        "complete": True, "stop_reason": "pagination_exhausted", "strategy": "timestamp_cursor_overlap",
        "first_dt_posted": min(posted, key=instant) if posted else None,
        "last_dt_posted": max(posted, key=instant) if posted else None,
        "fetch_started_at_utc": state["started_at"], "fetched_at_utc": finished,
        "snapshot_cutoff_utc": state["cutoff"], "params": base}
    save(directory / "manifest.json", manifest)
    save(directory / "verification.json", verification)
    save(directory / "snapshot_manifest.json", {"year": year, "endpoint": endpoint,
        "snapshot_built_at_utc": finished, "snapshot_cutoff_utc": state["cutoff"],
        "raw_row_count": raw_count, "raw_unique_id_count": len(records),
        "duplicate_ids": raw_count - len(records), "missing_id_count": 0,
        "snapshot_unique_id_count": len(records), "manifest_api_reported_count": final_count,
        "live_api_count": live_count, "complete_as_of_snapshot": len(records) == live_count,
        "complete_before_cutoff_by_count": True, "verification_scope": verification["verification_scope"],
        "snapshot_sha256": checksum.hexdigest(), "verified_at_utc": finished})
    state.update(status="verified", finished_at=finished, verification=verification)
    save(checkpoint, state)
    return manifest


def install_endpoint(root, run_id, year, endpoint):
    raw = (root / "data/lda/raw").resolve()
    target = raw / str(year) / endpoint
    stage = root / "data/lda/refresh" / run_id / str(year) / endpoint
    backup = root / "data/lda/archive" / run_id / str(year) / endpoint
    for path in (target, stage, backup):
        if not path.resolve().is_relative_to((root / "data/lda").resolve()):
            raise ValueError("Install path is outside the lobbying data directory")
    # Recover a crash after the staging rename but before job state was saved.
    if not stage.exists() and (target / "download_state.json").exists():
        if read(target / "download_state.json").get("run_id") == run_id:
            return
    if read(stage / "download_state.json")["status"] != "verified":
        raise ValueError("Refusing to install an unverified endpoint")
    marker = read(stage / "download_state.json")
    marker["run_id"] = run_id
    save(stage / "download_state.json", marker)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if backup.exists():
            raise RuntimeError("Previous snapshot backup already exists; inspect before replacing")
        backup.parent.mkdir(parents=True, exist_ok=True)
        target.rename(backup)
    try:
        stage.rename(target)
    except Exception:
        if backup.exists() and not target.exists():
            backup.rename(target)
        raise


def rebuild(root, years):
    import tempfile
    from pipeline.lda.build_explorer import build_explorer
    from pipeline.lda.build_spending import build_spending
    from frontend.lobbying import build_lobbying, build_lobbying_pages
    from scripts.validate_site import validate_lobbying, validate_lobbying_spending
    metadata = build_explorer(years)
    build_spending()
    # Validate the explorer in a scratch folder: it is checked on every refresh
    # even while it is not published (frontend.lobbying.PUBLISH_AI_EXPLORER).
    with tempfile.TemporaryDirectory() as scratch:
        errors = []
        build_lobbying(Path(scratch), [])
        validate_lobbying(Path(scratch), errors)
        if errors:
            raise RuntimeError("Lobbying site validation failed: " + "; ".join(errors[:10]))
    for site in (root / "docs", root / "frontend/site"):
        cycles = sorted(int(p.name) for p in site.iterdir() if p.is_dir() and p.name.isdigit())
        build_lobbying_pages(site, cycles)
        errors = []
        validate_lobbying(site, errors)
        validate_lobbying_spending(site, errors)
        if errors:
            raise RuntimeError("Lobbying site validation failed: " + "; ".join(errors[:10]))
    print(f"{now()} Explorer rebuilt: years {years}, {metadata['activity_count']:,} issue entries", flush=True)


def run(years, *, new_run=False, root=PROJECT_ROOT):
    from pipeline.lda.normalize import normalize_year
    from pipeline.lda.ingest import fetch_lookup_snapshots
    control = root / "data/lda/refresh/job.json"
    if control.exists() and not new_run:
        job = read(control)
        if job["years"] != years:
            raise ValueError("Existing job has different years. Resume its years or use --new-run.")
        if job["status"] == "complete":
            return job
    else:
        if control.exists() and read(control).get("status") == "running":
            raise ValueError("A job is marked running; inspect it before starting another")
        job = {"run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "years": years, "started_at": now(), "completed_endpoints": [], "completed_years": [],
            "status": "running"}
        save(control, job)
    client = LDAClient()
    client.before_request = RequestPacer(110 if client.api_key else 14)
    job.update(status="running", updated_at=now(), pid=os.getpid())
    job.pop("error", None)
    save(control, job)
    try:
        fetch_lookup_snapshots(client, force=True)
        if "planned_counts" not in job:
            job["planned_counts"] = {str(year): {ep: client.get(ep + "/", filing_year=year, page_size=1)["count"]
                for ep in ("filings", "contributions")} for year in years}
            save(control, job)
        for year in years:
            if year in job["completed_years"]:
                continue
            job.update(active_year=year, phase="downloading", updated_at=now())
            save(control, job)
            for ep in ("filings", "contributions"):
                installed = root / "data/lda/raw" / str(year) / ep / "download_state.json"
                if installed.exists() and read(installed).get("run_id") == job["run_id"]:
                    if f"{year}/{ep}" not in job["completed_endpoints"]:
                        job["completed_endpoints"].append(f"{year}/{ep}")
            save(control, job)
            pending = [ep for ep in ("filings", "contributions")
                       if f"{year}/{ep}" not in job["completed_endpoints"]]
            cutoff = now()
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {ep: pool.submit(download_endpoint, year, ep,
                    root / "data/lda/refresh" / job["run_id"] / str(year) / ep, client, cutoff=cutoff)
                    for ep in pending}
                for endpoint, future in futures.items():
                    future.result()
                    install_endpoint(root, job["run_id"], year, endpoint)
                    job["completed_endpoints"].append(f"{year}/{endpoint}")
                    save(control, job)
            job.update(phase="normalizing", updated_at=now())
            save(control, job)
            normalize_year(year)
            # Include existing years while the remaining historical years download.
            available = sorted(int(p.name) for p in (root / "data/lda/interim").iterdir()
                if p.is_dir() and p.name.isdigit() and (p / "normalization_manifest.json").exists())
            job.update(phase="rebuilding", updated_at=now())
            save(control, job)
            rebuild(root, available)
            job["completed_years"].append(year)
            job.update(updated_at=now())
            save(control, job)
        job.update(status="complete", phase="complete", finished_at=now())
        save(control, job)
    except BaseException as exc:
        job.update(status="failed", error=str(exc), updated_at=now())
        save(control, job)
        raise
    return job


def status(root=PROJECT_ROOT):
    job = read(root / "data/lda/refresh/job.json")
    endpoints = []
    for year in job["years"]:
        for ep in ("filings", "contributions"):
            path = root / "data/lda/refresh" / job["run_id"] / str(year) / ep / "download_state.json"
            if f"{year}/{ep}" in job["completed_endpoints"]:
                path = root / "data/lda/raw" / str(year) / ep / "download_state.json"
            if path.exists():
                saved = read(path)
                endpoints.append({k: saved.get(k) for k in ("year", "endpoint", "status", "unique_id_count",
                    "expected_count", "started_at", "updated_at", "cutoff")})
    return {**job, "endpoint_progress": endpoints}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("years", nargs="*", type=int)
    parser.add_argument("--new-run", action="store_true")
    parser.add_argument("--status", action="store_true", help="Print current job progress without downloading")
    args = parser.parse_args()
    if args.status:
        print(json.dumps(status(), indent=2))
        return
    if not args.years:
        parser.error("Specify filing years, or use --status")
    if len(set(args.years)) != len(args.years) or any(y < 2008 or y > datetime.now().year for y in args.years):
        parser.error("Use distinct filing years between 2008 and the current year")
    lock = PROJECT_ROOT / "data/lda/refresh/worker.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    # OS-held lock is released on crash. The file itself is deliberately retained.
    with lock.open("a+b") as handle:
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(json.dumps(run(args.years, new_run=args.new_run), indent=2), flush=True)


if __name__ == "__main__":
    main()
