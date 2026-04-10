from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.common.paths import lda_year_raw_dir
from pipeline.lda.client import LDAClient


ID_FIELD_BY_ENDPOINT = {
    "filings": "filing_uuid",
    "contributions": "filing_uuid",
}


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _snapshot_path(base_dir: Path) -> Path:
    return base_dir / "snapshot.jsonl"


def _snapshot_manifest_path(base_dir: Path) -> Path:
    return base_dir / "snapshot_manifest.json"


def _supplemental_path(base_dir: Path) -> Path:
    return base_dir / "supplemental.jsonl"


def _repair_manifest_path(base_dir: Path) -> Path:
    return base_dir / "repair_manifest.json"


def _iter_page_payloads(base_dir: Path):
    for page_path in sorted(base_dir.glob("page_*.json")):
        page_num = int(page_path.stem.split("_")[1])
        yield page_num, _read_json(page_path)


def _load_rows_by_id(
    base_dir: Path,
    endpoint: str,
    *,
    include_supplemental: bool = True,
) -> dict[str, dict[str, Any]]:
    id_field = ID_FIELD_BY_ENDPOINT[endpoint]
    rows_by_id: dict[str, dict[str, Any]] = {}

    for _, payload in _iter_page_payloads(base_dir):
        for row in payload.get("results", []):
            row_id = row.get(id_field)
            if row_id:
                rows_by_id.setdefault(row_id, row)

    supplemental_path = _supplemental_path(base_dir)
    if include_supplemental and supplemental_path.exists():
        for raw_line in supplemental_path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            row = json.loads(raw_line)
            row_id = row.get(id_field)
            if row_id:
                rows_by_id[row_id] = row

    return rows_by_id


def _write_supplemental(base_dir: Path, rows_by_id: dict[str, dict[str, Any]]) -> None:
    supplemental_path = _supplemental_path(base_dir)
    with supplemental_path.open("w", encoding="utf-8") as handle:
        for row_id in sorted(rows_by_id):
            handle.write(json.dumps(rows_by_id[row_id]) + "\n")


def build_snapshot(year: int, endpoint: str) -> dict[str, Any]:
    base_dir = lda_year_raw_dir(year) / endpoint
    manifest = _read_json(base_dir / "manifest.json")
    id_field = ID_FIELD_BY_ENDPOINT[endpoint]

    row_ids: list[str] = []
    for _, payload in _iter_page_payloads(base_dir):
        row_ids.extend(
            row.get(id_field)
            for row in payload.get("results", [])
            if row.get(id_field)
        )
    duplicate_ids = [row_id for row_id, count in Counter(row_ids).items() if count > 1]

    rows_by_id = _load_rows_by_id(base_dir, endpoint)
    snapshot_path = _snapshot_path(base_dir)
    with snapshot_path.open("w", encoding="utf-8") as handle:
        for row_id in sorted(rows_by_id):
            handle.write(json.dumps(rows_by_id[row_id]) + "\n")

    live_api_count = LDAClient().get(f"{endpoint}/", filing_year=year, page_size=1).get("count")
    summary = {
        "endpoint": endpoint,
        "year": year,
        "snapshot_built_at_utc": _iso_utc_now(),
        "raw_row_count": len(row_ids),
        "raw_unique_id_count": len(set(row_ids)),
        "duplicate_ids": len(duplicate_ids),
        "snapshot_unique_id_count": len(rows_by_id),
        "manifest_api_reported_count": manifest.get("api_reported_count"),
        "live_api_count": live_api_count,
        "complete_as_of_snapshot": len(rows_by_id) == live_api_count,
        "snapshot_path": str(snapshot_path),
    }
    _write_json(_snapshot_manifest_path(base_dir), summary)
    return summary


def top_up_tail_pages(
    year: int,
    endpoint: str,
    *,
    safety_pages: int = 12,
) -> dict[str, Any]:
    client = LDAClient()
    base_dir = lda_year_raw_dir(year) / endpoint
    manifest = _read_json(base_dir / "manifest.json")
    base_rows = _load_rows_by_id(base_dir, endpoint, include_supplemental=False)
    rows_by_id = _load_rows_by_id(base_dir, endpoint, include_supplemental=True)
    id_field = ID_FIELD_BY_ENDPOINT[endpoint]

    live_probe = client.get(f"{endpoint}/", filing_year=year, page_size=100, ordering="dt_posted")
    live_api_count = live_probe.get("count") or 0
    page_size_effective = max(1, len(live_probe.get("results", [])) or 25)
    live_page_count = (live_api_count + page_size_effective - 1) // page_size_effective
    start_page = max(1, live_page_count - safety_pages + 1)

    supplemental_rows = {
        row_id: row
        for row_id, row in rows_by_id.items()
        if row_id not in base_rows
    }
    initial_count = len(rows_by_id)

    for page in range(start_page, live_page_count + 1):
        payload = client.get(
            f"{endpoint}/",
            filing_year=year,
            ordering="dt_posted",
            page=page,
            page_size=100,
        )
        for row in payload.get("results", []):
            row_id = row.get(id_field)
            if row_id and row_id not in rows_by_id and row_id not in supplemental_rows:
                supplemental_rows[row_id] = row

    _write_supplemental(base_dir, supplemental_rows)
    snapshot_summary = build_snapshot(year, endpoint)
    top_up_summary = {
        "endpoint": endpoint,
        "year": year,
        "topped_up_at_utc": _iso_utc_now(),
        "live_api_count_before": live_api_count,
        "start_page": start_page,
        "end_page": live_page_count,
        "initial_snapshot_unique_id_count": initial_count,
        "supplemental_rows_after_top_up": len(supplemental_rows),
        "snapshot_unique_id_count": snapshot_summary["snapshot_unique_id_count"],
        "complete_as_of_top_up": snapshot_summary["complete_as_of_snapshot"],
    }
    return top_up_summary


def _tied_timestamp_boundaries(year: int, endpoint: str) -> list[dict[str, Any]]:
    base_dir = lda_year_raw_dir(year) / endpoint
    id_field = ID_FIELD_BY_ENDPOINT[endpoint]
    boundaries: list[dict[str, Any]] = []
    prev_tail: dict[str, Any] | None = None

    for page_num, payload in _iter_page_payloads(base_dir):
        rows = payload.get("results", [])
        if not rows:
            continue

        head = rows[0]
        tail = rows[-1]
        if prev_tail and prev_tail.get("dt_posted") == head.get("dt_posted"):
            boundaries.append(
                {
                    "page_left": page_num - 1,
                    "page_right": page_num,
                    "row_id": head.get(id_field),
                    "dt_posted": head.get("dt_posted"),
                    "same_uuid": prev_tail.get(id_field) == head.get(id_field),
                }
            )
        prev_tail = tail

    return boundaries


def repair_filings(
    year: int,
    *,
    max_attempts: int = 8,
) -> dict[str, Any]:
    endpoint = "filings"
    base_dir = lda_year_raw_dir(year) / endpoint
    manifest = _read_json(base_dir / "manifest.json")
    client = LDAClient()
    rows_by_id = _load_rows_by_id(base_dir, endpoint, include_supplemental=False)
    initial_unique_count = len(rows_by_id)

    boundaries = _tied_timestamp_boundaries(year, endpoint)
    boundary_pages = sorted({item["page_left"] for item in boundaries} | {item["page_right"] for item in boundaries})
    supplemental_rows: dict[str, dict[str, Any]] = {}

    target_live_count = client.get("filings/", filing_year=year, ordering="dt_posted", page_size=1).get("count")
    attempts_run = 0

    for attempt in range(1, max_attempts + 1):
        attempts_run = attempt
        for page in boundary_pages:
            payload = client.get(
                "filings/",
                filing_year=year,
                ordering="dt_posted",
                page=page,
                page_size=100,
            )
            for row in payload.get("results", []):
                row_id = row.get("filing_uuid")
                if row_id and row_id not in rows_by_id and row_id not in supplemental_rows:
                    supplemental_rows[row_id] = row

        combined_unique = len(rows_by_id) + len(supplemental_rows)
        target_live_count = client.get("filings/", filing_year=year, ordering="dt_posted", page_size=1).get("count")
        if combined_unique >= target_live_count:
            break

    if supplemental_rows:
        supplemental_path = _supplemental_path(base_dir)
        with supplemental_path.open("w", encoding="utf-8") as handle:
            for row_id in sorted(supplemental_rows):
                handle.write(json.dumps(supplemental_rows[row_id]) + "\n")

    snapshot_summary = build_snapshot(year, endpoint)
    repair_summary = {
        "endpoint": endpoint,
        "year": year,
        "repaired_at_utc": _iso_utc_now(),
        "tied_timestamp_boundaries": len(boundaries),
        "same_uuid_boundaries": sum(1 for row in boundaries if row["same_uuid"]),
        "boundary_pages": boundary_pages,
        "attempts_run": attempts_run,
        "initial_unique_count": initial_unique_count,
        "supplemental_rows_recovered": len(supplemental_rows),
        "snapshot_unique_id_count": snapshot_summary["snapshot_unique_id_count"],
        "live_api_count": snapshot_summary["live_api_count"],
        "complete_as_of_repair": snapshot_summary["complete_as_of_snapshot"],
    }
    _write_json(_repair_manifest_path(base_dir), repair_summary)
    return repair_summary


def reconcile_year(year: int, *, repair_filings_endpoint: bool = True) -> dict[str, Any]:
    results: dict[str, Any] = {
        "year": year,
        "reconciled_at_utc": _iso_utc_now(),
        "snapshots": {},
    }

    for endpoint in ["filings", "contributions"]:
        results["snapshots"][endpoint] = build_snapshot(year, endpoint)

    if repair_filings_endpoint:
        results["filings_repair"] = repair_filings(year)
        results["snapshots"]["filings"] = build_snapshot(year, "filings")

    for endpoint in ["filings", "contributions"]:
        if not results["snapshots"][endpoint]["complete_as_of_snapshot"]:
            results[f"{endpoint}_tail_top_up"] = top_up_tail_pages(year, endpoint)
            results["snapshots"][endpoint] = build_snapshot(year, endpoint)

    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build deduped raw snapshots and repair ambiguous filings page boundaries.",
    )
    parser.add_argument("years", nargs="+", type=int, help="Years to reconcile")
    parser.add_argument(
        "--skip-filings-repair",
        action="store_true",
        help="Build snapshots only, without boundary-page repair for filings.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    for year in args.years:
        result = reconcile_year(year, repair_filings_endpoint=not args.skip_filings_repair)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
