from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.common.paths import lda_lookup_raw_dir, lda_year_raw_dir
from pipeline.lda.client import LDAClient


LOOKUP_ENDPOINTS: dict[str, str] = {
    "filing_types": "constants/filing/filingtypes/",
    "lobbying_activity_issues": "constants/filing/lobbyingactivityissues/",
    "government_entities": "constants/filing/governmententities/",
    "contribution_item_types": "constants/contribution/itemtypes/",
    "states": "constants/general/states/",
    "countries": "constants/general/countries/",
}


@dataclass(frozen=True)
class YearEndpointSpec:
    key: str
    path: str
    default_params: dict[str, Any]


YEAR_ENDPOINTS = [
    YearEndpointSpec(
        key="filings",
        path="filings/",
        default_params={"ordering": "dt_posted"},
    ),
    YearEndpointSpec(
        key="contributions",
        path="contributions/",
        default_params={"ordering": "dt_posted"},
    ),
]


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _page_path(base_dir: Path, page: int) -> Path:
    return base_dir / f"page_{page:05d}.json"


def _manifest_path(base_dir: Path) -> Path:
    return base_dir / "manifest.json"


def fetch_lookup_snapshots(client: LDAClient | None = None, *, force: bool = False) -> dict[str, Any]:
    client = client or LDAClient()
    out_dir = lda_lookup_raw_dir()
    manifest_rows: list[dict[str, Any]] = []

    for key, path in LOOKUP_ENDPOINTS.items():
        out_path = out_dir / f"{key}.json"
        if force or not out_path.exists():
            payload = client.get(path)
            _write_json(out_path, payload)
        else:
            payload = json.loads(out_path.read_text(encoding="utf-8"))

        size = len(payload) if isinstance(payload, list) else None
        manifest_rows.append(
            {
                "key": key,
                "path": path,
                "file": str(out_path),
                "row_count": size,
            }
        )

    manifest = {
        "fetched_at_utc": _iso_utc_now(),
        "lookups": manifest_rows,
    }
    _write_json(out_dir / "manifest.json", manifest)
    return manifest


def ingest_year_endpoint(
    year: int,
    spec: YearEndpointSpec,
    client: LDAClient | None = None,
    *,
    page_size: int = 100,
    max_pages: int | None = None,
) -> dict[str, Any]:
    client = client or LDAClient()
    base_dir = lda_year_raw_dir(year) / spec.key
    base_dir.mkdir(parents=True, exist_ok=True)
    existing_manifest_path = _manifest_path(base_dir)
    if existing_manifest_path.exists():
        existing_manifest = _read_json(existing_manifest_path)
        if existing_manifest.get("complete") is True:
            return existing_manifest

    page = 1
    page_count = 0
    row_count = 0
    first_posted = None
    last_posted = None
    api_reported_count = None
    stop_reason = "no_results"

    while True:
        params = {
            "page": page,
            "page_size": page_size,
            "filing_year": year,
            **spec.default_params,
        }
        payload = client.get(spec.path, **params)
        if api_reported_count is None:
            api_reported_count = payload.get("count")
        _write_json(_page_path(base_dir, page), payload)

        results = payload.get("results", [])
        if not results:
            stop_reason = "empty_results"
            break

        page_count += 1
        row_count += len(results)

        posted_values = [
            row.get("dt_posted")
            for row in results
            if isinstance(row, dict) and row.get("dt_posted")
        ]
        if posted_values:
            first_page_min = min(posted_values)
            first_page_max = max(posted_values)
            first_posted = first_page_min if first_posted is None else min(first_posted, first_page_min)
            last_posted = first_page_max if last_posted is None else max(last_posted, first_page_max)

        if payload.get("next") is None:
            stop_reason = "pagination_exhausted"
            break
        if max_pages is not None and page_count >= max_pages:
            stop_reason = "max_pages_reached"
            break
        page += 1

    complete = (
        stop_reason == "pagination_exhausted"
        and api_reported_count is not None
        and row_count == api_reported_count
    )

    manifest = {
        "endpoint": spec.key,
        "path": spec.path,
        "year": year,
        "page_size": page_size,
        "api_reported_count": api_reported_count,
        "page_count": page_count,
        "row_count": row_count,
        "complete": complete,
        "stop_reason": stop_reason,
        "first_dt_posted": first_posted,
        "last_dt_posted": last_posted,
        "fetched_at_utc": _iso_utc_now(),
        "params": {
            "filing_year": year,
            **spec.default_params,
        },
    }
    _write_json(_manifest_path(base_dir), manifest)
    return manifest


def ingest_year(
    year: int,
    *,
    page_size: int = 100,
    max_pages: int | None = None,
    include_lookups: bool = True,
) -> dict[str, Any]:
    client = LDAClient()
    year_dir = lda_year_raw_dir(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    endpoint_manifests = [
        ingest_year_endpoint(
            year,
            spec,
            client,
            page_size=page_size,
            max_pages=max_pages,
        )
        for spec in YEAR_ENDPOINTS
    ]

    run_manifest = {
        "year": year,
        "fetched_at_utc": _iso_utc_now(),
        "page_size": page_size,
        "max_pages": max_pages,
        "endpoints": endpoint_manifests,
        "complete": all(row["complete"] for row in endpoint_manifests),
    }
    if include_lookups:
        run_manifest["lookups"] = fetch_lookup_snapshots(client)

    _write_json(year_dir / "run_manifest.json", run_manifest)
    return run_manifest


def verify_year(year: int) -> dict[str, Any]:
    client = LDAClient()
    year_dir = lda_year_raw_dir(year)
    endpoint_results: list[dict[str, Any]] = []

    for spec in YEAR_ENDPOINTS:
        manifest_path = year_dir / spec.key / "manifest.json"
        manifest = _read_json(manifest_path)
        counted_rows = 0
        ids: list[str | None] = []
        for page in range(1, manifest["page_count"] + 1):
            payload = _read_json(year_dir / spec.key / f"page_{page:05d}.json")
            results = payload.get("results", [])
            counted_rows += len(results)
            ids.extend(row.get("filing_uuid") for row in results if isinstance(row, dict))

        id_counts = Counter(ids)
        duplicate_ids = sum(1 for _, count in id_counts.items() if count > 1)
        unique_ids = len([item for item in id_counts.keys() if item is not None])
        live_api_count = client.get(spec.path, filing_year=year, page_size=1).get("count")
        complete_as_of_verification = (
            duplicate_ids == 0
            and unique_ids == counted_rows
            and counted_rows == live_api_count
        )

        endpoint_results.append(
            {
                "endpoint": spec.key,
                "api_reported_count": manifest.get("api_reported_count"),
                "manifest_row_count": manifest.get("row_count"),
                "counted_rows": counted_rows,
                "unique_ids": unique_ids,
                "duplicate_ids": duplicate_ids,
                "live_api_count": live_api_count,
                "complete": (
                    manifest.get("complete") is True
                    and counted_rows == manifest.get("row_count")
                    and counted_rows == manifest.get("api_reported_count")
                ),
                "complete_as_of_verification": complete_as_of_verification,
                "stop_reason": manifest.get("stop_reason"),
                "page_count": manifest.get("page_count"),
            }
        )

    verification = {
        "year": year,
        "verified_at_utc": _iso_utc_now(),
        "complete": all(row["complete"] for row in endpoint_results),
        "complete_as_of_verification": all(
            row["complete_as_of_verification"] for row in endpoint_results
        ),
        "endpoints": endpoint_results,
    }
    _write_json(year_dir / "verification.json", verification)
    return verification


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch yearly LDA raw pages into data/lda/raw/<year>/.",
    )
    parser.add_argument("years", nargs="+", type=int, help="Filing years to fetch")
    parser.add_argument("--page-size", type=int, default=100, help="API page size")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap for sample runs",
    )
    parser.add_argument(
        "--skip-lookups",
        action="store_true",
        help="Skip refreshing lookup snapshots",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify previously downloaded yearly raw pages instead of fetching",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    for year in args.years:
        if args.verify:
            manifest = verify_year(year)
            print(json.dumps(manifest, indent=2))
            continue

        manifest = ingest_year(
            year,
            page_size=args.page_size,
            max_pages=args.max_pages,
            include_lookups=not args.skip_lookups,
        )
        print(
            json.dumps(
                {
                    "year": manifest["year"],
                    "page_size": manifest["page_size"],
                    "max_pages": manifest["max_pages"],
                    "complete": manifest["complete"],
                    "endpoints": [
                        {
                            "endpoint": row["endpoint"],
                            "api_reported_count": row["api_reported_count"],
                            "rows": row["row_count"],
                            "pages": row["page_count"],
                            "complete": row["complete"],
                            "stop_reason": row["stop_reason"],
                        }
                        for row in manifest["endpoints"]
                    ],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
