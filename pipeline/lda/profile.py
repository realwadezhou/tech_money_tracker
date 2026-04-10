from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pandas.errors import EmptyDataError
import pandas as pd

from pipeline.common.paths import lda_year_derived_dir, lda_year_interim_dir, lda_year_raw_dir


RAW_ENDPOINT_TABLES = {
    "filings": [
        {
            "table_name": "filings.csv",
            "grain": "one row per filing",
            "role": "main filing table with client and registrant keys",
        },
        {
            "table_name": "filing_activities.csv",
            "grain": "one row per lobbying activity within a filing",
            "role": "issue-level bridge exploded from lobbying_activities[]",
        },
        {
            "table_name": "filing_activity_lobbyists.csv",
            "grain": "one row per lobbyist attached to an activity",
            "role": "person bridge exploded from lobbying_activities[].lobbyists[]",
        },
        {
            "table_name": "filing_activity_government_entities.csv",
            "grain": "one row per government entity attached to an activity",
            "role": "target-agency bridge exploded from lobbying_activities[].government_entities[]",
        },
        {
            "table_name": "filing_foreign_entities.csv",
            "grain": "one row per foreign entity attached to a filing",
            "role": "foreign ownership/disclosure bridge exploded from foreign_entities[]",
        },
        {
            "table_name": "filing_affiliated_organizations.csv",
            "grain": "one row per affiliated organization attached to a filing",
            "role": "affiliation bridge exploded from affiliated_organizations[]",
        },
        {
            "table_name": "filing_conviction_disclosures.csv",
            "grain": "one row per conviction disclosure attached to a filing",
            "role": "conviction disclosure bridge exploded from conviction_disclosures[]",
        },
        {
            "table_name": "clients.csv",
            "grain": "one row per unique client API id observed in the year",
            "role": "dimension table for client metadata",
        },
        {
            "table_name": "registrants.csv",
            "grain": "one row per unique registrant API id observed in the year",
            "role": "dimension table for lobbying firms / in-house filers",
        },
        {
            "table_name": "lobbyists.csv",
            "grain": "one row per unique lobbyist API id observed in the year",
            "role": "dimension table for individual lobbyists",
        },
    ],
    "contributions": [
        {
            "table_name": "contributions.csv",
            "grain": "one row per LD-203 filing",
            "role": "main contribution filing table",
        },
        {
            "table_name": "contribution_items.csv",
            "grain": "one row per contribution item within a filing",
            "role": "item-level bridge exploded from contribution_items[]",
        },
        {
            "table_name": "registrants.csv",
            "grain": "one row per unique registrant API id observed in the year",
            "role": "dimension table reused by contribution filings",
        },
        {
            "table_name": "lobbyists.csv",
            "grain": "one row per unique lobbyist API id observed in the year",
            "role": "dimension table reused by contribution filings",
        },
    ],
}


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _inspect_value(
    value: Any,
    path: str,
    scalars: Counter[str],
    objects: Counter[str],
    arrays: Counter[str],
    nested_object_fields: dict[str, Counter[str]],
) -> None:
    if isinstance(value, dict):
        objects[path] += 1
        for key, child in value.items():
            nested_object_fields[path][key] += 1
            child_path = f"{path}.{key}" if path else key
            _inspect_value(child, child_path, scalars, objects, arrays, nested_object_fields)
        return

    if isinstance(value, list):
        arrays[path] += 1
        for child in value:
            child_path = f"{path}[]"
            _inspect_value(child, child_path, scalars, objects, arrays, nested_object_fields)
        return

    scalars[path] += 1


def profile_raw_endpoint(year: int, endpoint: str) -> dict[str, Any]:
    raw_dir = lda_year_raw_dir(year) / endpoint
    manifest = _read_json(raw_dir / "manifest.json")
    scalars: Counter[str] = Counter()
    objects: Counter[str] = Counter()
    arrays: Counter[str] = Counter()
    nested_object_fields: dict[str, Counter[str]] = defaultdict(Counter)
    scanned_rows = 0

    for page in range(1, manifest["page_count"] + 1):
        payload = _read_json(raw_dir / f"page_{page:05d}.json")
        for row in payload.get("results", []):
            scanned_rows += 1
            for key, value in row.items():
                _inspect_value(value, key, scalars, objects, arrays, nested_object_fields)

    return {
        "endpoint": endpoint,
        "row_count": manifest["row_count"],
        "page_count": manifest["page_count"],
        "scanned_rows": scanned_rows,
        "scalar_fields": sorted(scalars.keys()),
        "object_fields": sorted(objects.keys()),
        "array_fields": sorted(arrays.keys()),
        "object_field_children": {
            key: sorted(counter.keys())
            for key, counter in sorted(nested_object_fields.items())
        },
        "flattened_tables": RAW_ENDPOINT_TABLES[endpoint],
    }


def profile_flat_tables(year: int) -> list[dict[str, Any]]:
    interim_dir = lda_year_interim_dir(year)
    tables: list[dict[str, Any]] = []
    for path in sorted(interim_dir.glob("*.csv")):
        frame = _safe_read_csv(path)
        tables.append(
            {
                "table_name": path.name,
                "rows": len(frame),
                "columns": list(frame.columns),
                "column_count": len(frame.columns),
                "path": str(path),
            }
        )
    return tables


def write_table_shapes_csv(path: Path, flat_tables: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["table_name", "rows", "column_count", "columns", "path"],
        )
        writer.writeheader()
        for row in flat_tables:
            writer.writerow(
                {
                    "table_name": row["table_name"],
                    "rows": row["rows"],
                    "column_count": row["column_count"],
                    "columns": "; ".join(row["columns"]),
                    "path": row["path"],
                }
            )


def write_flattening_guide_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["endpoint", "table_name", "grain", "role"],
        )
        writer.writeheader()
        for endpoint, rows in RAW_ENDPOINT_TABLES.items():
            for row in rows:
                writer.writerow({"endpoint": endpoint, **row})


def profile_year(year: int) -> dict[str, Any]:
    out_dir = lda_year_derived_dir(year)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_profiles = {
        endpoint: profile_raw_endpoint(year, endpoint)
        for endpoint in ["filings", "contributions"]
    }
    flat_tables = profile_flat_tables(year)

    profile = {
        "year": year,
        "profiled_at_utc": _iso_utc_now(),
        "raw_endpoint_profiles": raw_profiles,
        "flat_tables": flat_tables,
        "flattening_takeaway": (
            "Yes: the source is nested JSON, but it breaks cleanly into a main filing table, "
            "bridge tables for repeated arrays, and reusable dimension tables."
        ),
    }

    _write_json(out_dir / "structure_profile.json", profile)
    write_table_shapes_csv(out_dir / "table_shapes.csv", flat_tables)
    write_flattening_guide_csv(out_dir / "flattening_guide.csv")
    return profile


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile LDA raw endpoint nesting and resulting flat interim tables.",
    )
    parser.add_argument("years", nargs="+", type=int, help="Years to profile")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    for year in args.years:
        profile = profile_year(year)
        print(
            json.dumps(
                {
                    "year": year,
                    "raw_endpoints": list(profile["raw_endpoint_profiles"].keys()),
                    "flat_tables": [row["table_name"] for row in profile["flat_tables"]],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
