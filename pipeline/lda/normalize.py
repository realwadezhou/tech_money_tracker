from __future__ import annotations

import argparse
import csv
import json
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pipeline.common.paths import lda_year_interim_dir, lda_year_raw_dir


TABLE_SCHEMAS: dict[str, list[str]] = {
    "filings.csv": [
        "filing_uuid",
        "filing_year",
        "filing_period",
        "filing_period_display",
        "filing_type",
        "filing_type_display",
        "dt_posted",
        "posted_by_name",
        "income",
        "expenses",
        "expenses_method",
        "expenses_method_display",
        "termination_date",
        "filing_document_url",
        "filing_document_content_type",
        "url",
        "client_api_id",
        "client_source_id",
        "client_name",
        "registrant_api_id",
        "registrant_house_id",
        "registrant_name",
        "registrant_address_1",
        "registrant_address_2",
        "registrant_city",
        "registrant_state",
        "registrant_zip",
        "registrant_country",
        "registrant_ppb_country",
        "registrant_different_address",
        "n_activities",
        "n_foreign_entities",
        "n_affiliated_organizations",
        "n_conviction_disclosures",
    ],
    "filing_activities.csv": [
        "activity_id",
        "filing_uuid",
        "activity_index",
        "general_issue_code",
        "general_issue_code_display",
        "description",
        "foreign_entity_issues",
        "n_lobbyists",
        "n_government_entities",
    ],
    "filing_activity_lobbyists.csv": [
        "activity_id",
        "filing_uuid",
        "activity_index",
        "activity_lobbyist_index",
        "lobbyist_api_id",
        "lobbyist_first_name",
        "lobbyist_middle_name",
        "lobbyist_last_name",
        "lobbyist_nickname",
        "prefix",
        "prefix_display",
        "suffix",
        "suffix_display",
        "covered_position",
        "is_new",
    ],
    "filing_activity_government_entities.csv": [
        "activity_id",
        "filing_uuid",
        "activity_index",
        "activity_government_entity_index",
        "government_entity_id",
        "government_entity_name",
    ],
    "filing_foreign_entities.csv": [
        "filing_uuid",
        "foreign_entity_index",
        "name",
        "contribution",
        "ownership_percentage",
        "address",
        "city",
        "state",
        "state_display",
        "country",
        "country_display",
        "ppb_city",
        "ppb_state",
        "ppb_state_display",
        "ppb_country",
        "ppb_country_display",
    ],
    "filing_affiliated_organizations.csv": [
        "filing_uuid",
        "affiliated_organization_index",
    ],
    "filing_conviction_disclosures.csv": [
        "filing_uuid",
        "conviction_disclosure_index",
    ],
    "contributions.csv": [
        "filing_uuid",
        "filing_year",
        "filing_period",
        "filing_period_display",
        "filing_type",
        "filing_type_display",
        "filer_type",
        "filer_type_display",
        "dt_posted",
        "comments",
        "contact_name",
        "address_1",
        "address_2",
        "city",
        "state",
        "state_display",
        "zip",
        "country",
        "country_display",
        "no_contributions",
        "pacs",
        "n_contribution_items",
        "registrant_api_id",
        "registrant_house_id",
        "registrant_name",
        "lobbyist_api_id",
        "lobbyist_name",
        "filing_document_url",
        "filing_document_content_type",
        "url",
    ],
    "contribution_items.csv": [
        "contribution_item_id",
        "filing_uuid",
        "item_index",
        "contribution_type",
        "contribution_type_display",
        "contributor_name",
        "payee_name",
        "honoree_name",
        "amount",
        "date",
    ],
    "clients.csv": [
        "client_api_id",
        "client_source_id",
        "client_name",
        "general_description",
        "client_self_select",
        "client_government_entity",
        "state",
        "state_display",
        "country",
        "country_display",
        "ppb_state",
        "ppb_state_display",
        "ppb_country",
        "ppb_country_display",
        "effective_date",
        "registrant_api_id",
    ],
    "registrants.csv": [
        "registrant_api_id",
        "house_registrant_id",
        "registrant_name",
        "description",
        "address_1",
        "address_2",
        "address_3",
        "address_4",
        "city",
        "state",
        "state_display",
        "zip",
        "country",
        "country_display",
        "ppb_country",
        "ppb_country_display",
        "contact_name",
        "contact_telephone",
        "dt_updated",
    ],
    "lobbyists.csv": [
        "lobbyist_api_id",
        "first_name",
        "middle_name",
        "last_name",
        "nickname",
        "prefix",
        "prefix_display",
        "suffix",
        "suffix_display",
        "registrant_api_id",
    ],
}


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _to_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return bool(value)


def _join_list(values: Any) -> str:
    if not values:
        return ""
    if isinstance(values, list):
        return "; ".join(str(value) for value in values)
    return str(values)


def _collect_fieldnames(rows: Iterable[dict[str, Any]]) -> list[str]:
    ordered: OrderedDict[str, None] = OrderedDict()
    for row in rows:
        for key in row.keys():
            ordered.setdefault(key, None)
    return list(ordered.keys())


def _write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    fieldnames: list[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    observed = _collect_fieldnames(rows)
    if fieldnames:
        fieldnames = list(OrderedDict.fromkeys([*fieldnames, *observed]))
    else:
        fieldnames = observed
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _endpoint_manifest(raw_year_dir: Path, endpoint: str) -> dict[str, Any]:
    return _read_json(raw_year_dir / endpoint / "manifest.json")


def _iter_endpoint_results(raw_year_dir: Path, endpoint: str) -> Iterable[dict[str, Any]]:
    snapshot_path = raw_year_dir / endpoint / "snapshot.jsonl"
    if snapshot_path.exists():
        for raw_line in snapshot_path.read_text(encoding="utf-8").splitlines():
            if raw_line.strip():
                yield json.loads(raw_line)
        return

    manifest = _endpoint_manifest(raw_year_dir, endpoint)
    seen_ids: set[str] = set()
    for page in range(1, manifest["page_count"] + 1):
        payload = _read_json(raw_year_dir / endpoint / f"page_{page:05d}.json")
        for row in payload.get("results", []):
            row_id = row.get("filing_uuid")
            if row_id and row_id in seen_ids:
                continue
            if row_id:
                seen_ids.add(row_id)
            yield row


def _upsert_dimension(
    store: OrderedDict[Any, dict[str, Any]],
    key: Any,
    values: dict[str, Any],
) -> None:
    if key in (None, ""):
        return
    current = store.get(key, {})
    for field, value in values.items():
        if value not in (None, "", []):
            current[field] = value
        else:
            current.setdefault(field, value)
    store[key] = current


def normalize_year(year: int) -> dict[str, Any]:
    raw_year_dir = lda_year_raw_dir(year)
    out_dir = lda_year_interim_dir(year)
    out_dir.mkdir(parents=True, exist_ok=True)

    filings_rows: list[dict[str, Any]] = []
    filing_activity_rows: list[dict[str, Any]] = []
    filing_activity_lobbyist_rows: list[dict[str, Any]] = []
    filing_activity_entity_rows: list[dict[str, Any]] = []
    filing_foreign_entity_rows: list[dict[str, Any]] = []
    filing_affiliate_rows: list[dict[str, Any]] = []
    filing_conviction_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    contribution_item_rows: list[dict[str, Any]] = []

    clients: OrderedDict[Any, dict[str, Any]] = OrderedDict()
    registrants: OrderedDict[Any, dict[str, Any]] = OrderedDict()
    lobbyists: OrderedDict[Any, dict[str, Any]] = OrderedDict()

    for filing in _iter_endpoint_results(raw_year_dir, "filings"):
        filing_uuid = filing["filing_uuid"]
        client = filing.get("client") or {}
        registrant = filing.get("registrant") or {}

        filings_rows.append(
            {
                "filing_uuid": filing_uuid,
                "filing_year": filing.get("filing_year"),
                "filing_period": filing.get("filing_period"),
                "filing_period_display": filing.get("filing_period_display"),
                "filing_type": filing.get("filing_type"),
                "filing_type_display": filing.get("filing_type_display"),
                "dt_posted": filing.get("dt_posted"),
                "posted_by_name": filing.get("posted_by_name"),
                "income": _to_number(filing.get("income")),
                "expenses": _to_number(filing.get("expenses")),
                "expenses_method": filing.get("expenses_method"),
                "expenses_method_display": filing.get("expenses_method_display"),
                "termination_date": filing.get("termination_date"),
                "filing_document_url": filing.get("filing_document_url"),
                "filing_document_content_type": filing.get("filing_document_content_type"),
                "url": filing.get("url"),
                "client_api_id": client.get("id"),
                "client_source_id": client.get("client_id"),
                "client_name": client.get("name"),
                "registrant_api_id": registrant.get("id"),
                "registrant_house_id": registrant.get("house_registrant_id"),
                "registrant_name": registrant.get("name"),
                "registrant_address_1": filing.get("registrant_address_1"),
                "registrant_address_2": filing.get("registrant_address_2"),
                "registrant_city": filing.get("registrant_city"),
                "registrant_state": filing.get("registrant_state"),
                "registrant_zip": filing.get("registrant_zip"),
                "registrant_country": filing.get("registrant_country"),
                "registrant_ppb_country": filing.get("registrant_ppb_country"),
                "registrant_different_address": _bool_or_none(
                    filing.get("registrant_different_address")
                ),
                "n_activities": len(filing.get("lobbying_activities") or []),
                "n_foreign_entities": len(filing.get("foreign_entities") or []),
                "n_affiliated_organizations": len(filing.get("affiliated_organizations") or []),
                "n_conviction_disclosures": len(filing.get("conviction_disclosures") or []),
            }
        )

        _upsert_dimension(
            clients,
            client.get("id"),
            {
                "client_api_id": client.get("id"),
                "client_source_id": client.get("client_id"),
                "client_name": client.get("name"),
                "general_description": client.get("general_description"),
                "client_self_select": _bool_or_none(client.get("client_self_select")),
                "client_government_entity": _bool_or_none(client.get("client_government_entity")),
                "state": client.get("state"),
                "state_display": client.get("state_display"),
                "country": client.get("country"),
                "country_display": client.get("country_display"),
                "ppb_state": client.get("ppb_state"),
                "ppb_state_display": client.get("ppb_state_display"),
                "ppb_country": client.get("ppb_country"),
                "ppb_country_display": client.get("ppb_country_display"),
                "effective_date": client.get("effective_date"),
                "registrant_api_id": registrant.get("id"),
            },
        )

        _upsert_dimension(
            registrants,
            registrant.get("id"),
            {
                "registrant_api_id": registrant.get("id"),
                "house_registrant_id": registrant.get("house_registrant_id"),
                "registrant_name": registrant.get("name"),
                "description": registrant.get("description"),
                "address_1": registrant.get("address_1"),
                "address_2": registrant.get("address_2"),
                "address_3": registrant.get("address_3"),
                "address_4": registrant.get("address_4"),
                "city": registrant.get("city"),
                "state": registrant.get("state"),
                "state_display": registrant.get("state_display"),
                "zip": registrant.get("zip"),
                "country": registrant.get("country"),
                "country_display": registrant.get("country_display"),
                "ppb_country": registrant.get("ppb_country"),
                "ppb_country_display": registrant.get("ppb_country_display"),
                "contact_name": registrant.get("contact_name"),
                "contact_telephone": registrant.get("contact_telephone"),
                "dt_updated": registrant.get("dt_updated"),
            },
        )

        for activity_index, activity in enumerate(filing.get("lobbying_activities") or [], start=1):
            activity_id = f"{filing_uuid}:activity:{activity_index}"
            filing_activity_rows.append(
                {
                    "activity_id": activity_id,
                    "filing_uuid": filing_uuid,
                    "activity_index": activity_index,
                    "general_issue_code": activity.get("general_issue_code"),
                    "general_issue_code_display": activity.get("general_issue_code_display"),
                    "description": activity.get("description"),
                    "foreign_entity_issues": activity.get("foreign_entity_issues"),
                    "n_lobbyists": len(activity.get("lobbyists") or []),
                    "n_government_entities": len(activity.get("government_entities") or []),
                }
            )

            for lobbyist_index, lobbyist_link in enumerate(activity.get("lobbyists") or [], start=1):
                lobbyist = (lobbyist_link or {}).get("lobbyist") or {}
                filing_activity_lobbyist_rows.append(
                    {
                        "activity_id": activity_id,
                        "filing_uuid": filing_uuid,
                        "activity_index": activity_index,
                        "activity_lobbyist_index": lobbyist_index,
                        "lobbyist_api_id": lobbyist.get("id"),
                        "lobbyist_first_name": lobbyist.get("first_name"),
                        "lobbyist_middle_name": lobbyist.get("middle_name"),
                        "lobbyist_last_name": lobbyist.get("last_name"),
                        "lobbyist_nickname": lobbyist.get("nickname"),
                        "prefix": lobbyist.get("prefix"),
                        "prefix_display": lobbyist.get("prefix_display"),
                        "suffix": lobbyist.get("suffix"),
                        "suffix_display": lobbyist.get("suffix_display"),
                        "covered_position": lobbyist_link.get("covered_position"),
                        "is_new": _bool_or_none(lobbyist_link.get("new")),
                    }
                )
                _upsert_dimension(
                    lobbyists,
                    lobbyist.get("id"),
                    {
                        "lobbyist_api_id": lobbyist.get("id"),
                        "first_name": lobbyist.get("first_name"),
                        "middle_name": lobbyist.get("middle_name"),
                        "last_name": lobbyist.get("last_name"),
                        "nickname": lobbyist.get("nickname"),
                        "prefix": lobbyist.get("prefix"),
                        "prefix_display": lobbyist.get("prefix_display"),
                        "suffix": lobbyist.get("suffix"),
                        "suffix_display": lobbyist.get("suffix_display"),
                    },
                )

            for entity_index, entity in enumerate(activity.get("government_entities") or [], start=1):
                filing_activity_entity_rows.append(
                    {
                        "activity_id": activity_id,
                        "filing_uuid": filing_uuid,
                        "activity_index": activity_index,
                        "activity_government_entity_index": entity_index,
                        "government_entity_id": entity.get("id"),
                        "government_entity_name": entity.get("name"),
                    }
                )

        for foreign_entity_index, foreign_entity in enumerate(
            filing.get("foreign_entities") or [],
            start=1,
        ):
            filing_foreign_entity_rows.append(
                {
                    "filing_uuid": filing_uuid,
                    "foreign_entity_index": foreign_entity_index,
                    "name": foreign_entity.get("name"),
                    "contribution": _to_number(foreign_entity.get("contribution")),
                    "ownership_percentage": _to_number(foreign_entity.get("ownership_percentage")),
                    "address": foreign_entity.get("address"),
                    "city": foreign_entity.get("city"),
                    "state": foreign_entity.get("state"),
                    "state_display": foreign_entity.get("state_display"),
                    "country": foreign_entity.get("country"),
                    "country_display": foreign_entity.get("country_display"),
                    "ppb_city": foreign_entity.get("ppb_city"),
                    "ppb_state": foreign_entity.get("ppb_state"),
                    "ppb_state_display": foreign_entity.get("ppb_state_display"),
                    "ppb_country": foreign_entity.get("ppb_country"),
                    "ppb_country_display": foreign_entity.get("ppb_country_display"),
                }
            )

        for affiliate_index, affiliate in enumerate(
            filing.get("affiliated_organizations") or [],
            start=1,
        ):
            row = {
                "filing_uuid": filing_uuid,
                "affiliated_organization_index": affiliate_index,
            }
            row.update(affiliate or {})
            filing_affiliate_rows.append(row)

        for conviction_index, disclosure in enumerate(
            filing.get("conviction_disclosures") or [],
            start=1,
        ):
            row = {
                "filing_uuid": filing_uuid,
                "conviction_disclosure_index": conviction_index,
            }
            row.update(disclosure or {})
            filing_conviction_rows.append(row)

    for contribution in _iter_endpoint_results(raw_year_dir, "contributions"):
        filing_uuid = contribution["filing_uuid"]
        registrant = contribution.get("registrant") or {}
        lobbyist = contribution.get("lobbyist") or {}

        contribution_rows.append(
            {
                "filing_uuid": filing_uuid,
                "filing_year": contribution.get("filing_year"),
                "filing_period": contribution.get("filing_period"),
                "filing_period_display": contribution.get("filing_period_display"),
                "filing_type": contribution.get("filing_type"),
                "filing_type_display": contribution.get("filing_type_display"),
                "filer_type": contribution.get("filer_type"),
                "filer_type_display": contribution.get("filer_type_display"),
                "dt_posted": contribution.get("dt_posted"),
                "comments": contribution.get("comments"),
                "contact_name": contribution.get("contact_name"),
                "address_1": contribution.get("address_1"),
                "address_2": contribution.get("address_2"),
                "city": contribution.get("city"),
                "state": contribution.get("state"),
                "state_display": contribution.get("state_display"),
                "zip": contribution.get("zip"),
                "country": contribution.get("country"),
                "country_display": contribution.get("country_display"),
                "no_contributions": _bool_or_none(contribution.get("no_contributions")),
                "pacs": _join_list(contribution.get("pacs")),
                "n_contribution_items": len(contribution.get("contribution_items") or []),
                "registrant_api_id": registrant.get("id"),
                "registrant_house_id": registrant.get("house_registrant_id"),
                "registrant_name": registrant.get("name"),
                "lobbyist_api_id": lobbyist.get("id"),
                "lobbyist_name": " ".join(
                    part
                    for part in [
                        lobbyist.get("first_name"),
                        lobbyist.get("middle_name"),
                        lobbyist.get("last_name"),
                    ]
                    if part
                ),
                "filing_document_url": contribution.get("filing_document_url"),
                "filing_document_content_type": contribution.get("filing_document_content_type"),
                "url": contribution.get("url"),
            }
        )

        _upsert_dimension(
            registrants,
            registrant.get("id"),
            {
                "registrant_api_id": registrant.get("id"),
                "house_registrant_id": registrant.get("house_registrant_id"),
                "registrant_name": registrant.get("name"),
                "description": registrant.get("description"),
                "address_1": registrant.get("address_1"),
                "address_2": registrant.get("address_2"),
                "address_3": registrant.get("address_3"),
                "address_4": registrant.get("address_4"),
                "city": registrant.get("city"),
                "state": registrant.get("state"),
                "state_display": registrant.get("state_display"),
                "zip": registrant.get("zip"),
                "country": registrant.get("country"),
                "country_display": registrant.get("country_display"),
                "ppb_country": registrant.get("ppb_country"),
                "ppb_country_display": registrant.get("ppb_country_display"),
                "contact_name": registrant.get("contact_name"),
                "contact_telephone": registrant.get("contact_telephone"),
                "dt_updated": registrant.get("dt_updated"),
            },
        )

        _upsert_dimension(
            lobbyists,
            lobbyist.get("id"),
            {
                "lobbyist_api_id": lobbyist.get("id"),
                "first_name": lobbyist.get("first_name"),
                "middle_name": lobbyist.get("middle_name"),
                "last_name": lobbyist.get("last_name"),
                "nickname": lobbyist.get("nickname"),
                "prefix": lobbyist.get("prefix"),
                "prefix_display": lobbyist.get("prefix_display"),
                "suffix": lobbyist.get("suffix"),
                "suffix_display": lobbyist.get("suffix_display"),
                "registrant_api_id": registrant.get("id"),
            },
        )

        for item_index, item in enumerate(contribution.get("contribution_items") or [], start=1):
            contribution_item_rows.append(
                {
                    "contribution_item_id": f"{filing_uuid}:item:{item_index}",
                    "filing_uuid": filing_uuid,
                    "item_index": item_index,
                    "contribution_type": item.get("contribution_type"),
                    "contribution_type_display": item.get("contribution_type_display"),
                    "contributor_name": item.get("contributor_name"),
                    "payee_name": item.get("payee_name"),
                    "honoree_name": item.get("honoree_name"),
                    "amount": _to_number(item.get("amount")),
                    "date": item.get("date"),
                }
            )

    tables = {
        "filings.csv": filings_rows,
        "filing_activities.csv": filing_activity_rows,
        "filing_activity_lobbyists.csv": filing_activity_lobbyist_rows,
        "filing_activity_government_entities.csv": filing_activity_entity_rows,
        "filing_foreign_entities.csv": filing_foreign_entity_rows,
        "filing_affiliated_organizations.csv": filing_affiliate_rows,
        "filing_conviction_disclosures.csv": filing_conviction_rows,
        "contributions.csv": contribution_rows,
        "contribution_items.csv": contribution_item_rows,
        "clients.csv": list(clients.values()),
        "registrants.csv": list(registrants.values()),
        "lobbyists.csv": list(lobbyists.values()),
    }

    for filename, rows in tables.items():
        _write_csv(out_dir / filename, rows, fieldnames=TABLE_SCHEMAS.get(filename))

    manifest = {
        "year": year,
        "normalized_at_utc": _iso_utc_now(),
        "source_endpoints": {
            endpoint: _endpoint_manifest(raw_year_dir, endpoint)
            for endpoint in ["filings", "contributions"]
        },
        "tables": {
            filename: {
                "rows": len(rows),
                "path": str(out_dir / filename),
            }
            for filename, rows in tables.items()
        },
    }
    _write_json(out_dir / "normalization_manifest.json", manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize raw LDA yearly pages into interim flat CSV tables.",
    )
    parser.add_argument("years", nargs="+", type=int, help="Filing years to normalize")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    for year in args.years:
        manifest = normalize_year(year)
        print(
            json.dumps(
                {
                    "year": manifest["year"],
                    "tables": {
                        name: spec["rows"]
                        for name, spec in manifest["tables"].items()
                    },
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
