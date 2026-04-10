from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import pandas as pd
from pandas.errors import EmptyDataError

from pipeline.common.paths import lda_year_derived_dir, lda_year_interim_dir


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_csv(
    year: int,
    filename: str,
    *,
    empty_columns: list[str] | None = None,
    **kwargs,
) -> pd.DataFrame:
    path = lda_year_interim_dir(year) / filename
    try:
        return pd.read_csv(path, **kwargs)
    except EmptyDataError:
        return pd.DataFrame(columns=empty_columns or [])


def _period_sort_key(period: str) -> int:
    return {
        "first_quarter": 1,
        "second_quarter": 2,
        "mid_year": 2,
        "third_quarter": 3,
        "fourth_quarter": 4,
        "year_end": 4,
    }.get(period, 99)


def _with_period_order(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["period_order"] = result["filing_period"].map(_period_sort_key)
    return result


def build_client_quarter_summary(filings: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        filings.groupby(
            ["filing_year", "filing_period", "filing_period_display", "client_api_id", "client_name"],
            dropna=False,
        )
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_registrants=("registrant_api_id", "nunique"),
            total_income=("income", "sum"),
            total_expenses=("expenses", "sum"),
            total_activities=("n_activities", "sum"),
            total_foreign_entities=("n_foreign_entities", "sum"),
            total_affiliated_organizations=("n_affiliated_organizations", "sum"),
        )
        .reset_index()
    )
    grouped["total_income"] = grouped["total_income"].fillna(0.0)
    grouped["total_expenses"] = grouped["total_expenses"].fillna(0.0)
    grouped["total_reported_spend"] = grouped["total_income"] + grouped["total_expenses"]
    return _with_period_order(grouped).sort_values(
        ["filing_year", "period_order", "total_reported_spend", "client_name"],
        ascending=[True, True, False, True],
    )


def build_client_issue_summary(
    filings: pd.DataFrame,
    filing_activities: pd.DataFrame,
) -> pd.DataFrame:
    merged = filing_activities.merge(
        filings[
            [
                "filing_uuid",
                "filing_year",
                "filing_period",
                "filing_period_display",
                "client_api_id",
                "client_name",
                "income",
                "expenses",
            ]
        ],
        on="filing_uuid",
        how="left",
    )
    grouped = (
        merged.groupby(
            [
                "filing_year",
                "filing_period",
                "filing_period_display",
                "client_api_id",
                "client_name",
                "general_issue_code",
                "general_issue_code_display",
            ],
            dropna=False,
        )
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_activities=("activity_id", "nunique"),
            income_sum=("income", "sum"),
            expenses_sum=("expenses", "sum"),
        )
        .reset_index()
    )
    grouped["income_sum"] = grouped["income_sum"].fillna(0.0)
    grouped["expenses_sum"] = grouped["expenses_sum"].fillna(0.0)
    grouped["total_reported_spend"] = grouped["income_sum"] + grouped["expenses_sum"]
    return _with_period_order(grouped).sort_values(
        ["filing_year", "period_order", "total_reported_spend", "client_name", "general_issue_code"],
        ascending=[True, True, False, True, True],
    )


def build_issue_quarter_summary(
    filings: pd.DataFrame,
    filing_activities: pd.DataFrame,
) -> pd.DataFrame:
    merged = filing_activities.merge(
        filings[
            [
                "filing_uuid",
                "filing_year",
                "filing_period",
                "filing_period_display",
                "income",
                "expenses",
                "client_api_id",
            ]
        ],
        on="filing_uuid",
        how="left",
    )
    grouped = (
        merged.groupby(
            [
                "filing_year",
                "filing_period",
                "filing_period_display",
                "general_issue_code",
                "general_issue_code_display",
            ],
            dropna=False,
        )
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_clients=("client_api_id", "nunique"),
            n_activities=("activity_id", "nunique"),
            income_sum=("income", "sum"),
            expenses_sum=("expenses", "sum"),
        )
        .reset_index()
    )
    grouped["income_sum"] = grouped["income_sum"].fillna(0.0)
    grouped["expenses_sum"] = grouped["expenses_sum"].fillna(0.0)
    grouped["total_reported_spend"] = grouped["income_sum"] + grouped["expenses_sum"]
    return _with_period_order(grouped).sort_values(
        ["filing_year", "period_order", "total_reported_spend", "general_issue_code"],
        ascending=[True, True, False, True],
    )


def build_client_lobbyist_summary(
    filings: pd.DataFrame,
    filing_activity_lobbyists: pd.DataFrame,
) -> pd.DataFrame:
    merged = filing_activity_lobbyists.merge(
        filings[
            [
                "filing_uuid",
                "filing_year",
                "filing_period",
                "filing_period_display",
                "client_api_id",
                "client_name",
                "registrant_api_id",
                "registrant_name",
            ]
        ],
        on="filing_uuid",
        how="left",
    )
    grouped = (
        merged.groupby(
            [
                "filing_year",
                "filing_period",
                "filing_period_display",
                "client_api_id",
                "client_name",
                "lobbyist_api_id",
                "lobbyist_first_name",
                "lobbyist_last_name",
            ],
            dropna=False,
        )
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_activities=("activity_id", "nunique"),
            n_registrants=("registrant_api_id", "nunique"),
            n_covered_positions=("covered_position", lambda x: x.notna().sum()),
            ever_marked_new=("is_new", "max"),
        )
        .reset_index()
    )
    return _with_period_order(grouped).sort_values(
        ["filing_year", "period_order", "n_activities", "client_name", "lobbyist_last_name"],
        ascending=[True, True, False, True, True],
    )


def build_government_entity_issue_summary(
    filings: pd.DataFrame,
    filing_activities: pd.DataFrame,
    filing_activity_government_entities: pd.DataFrame,
) -> pd.DataFrame:
    if filing_activity_government_entities.empty:
        return filing_activity_government_entities.copy()

    merged = (
        filing_activity_government_entities.merge(
            filing_activities[
                [
                    "activity_id",
                    "filing_uuid",
                    "general_issue_code",
                    "general_issue_code_display",
                ]
            ],
            on=["activity_id", "filing_uuid"],
            how="left",
        )
        .merge(
            filings[
                [
                    "filing_uuid",
                    "filing_year",
                    "filing_period",
                    "filing_period_display",
                    "client_api_id",
                    "client_name",
                ]
            ],
            on="filing_uuid",
            how="left",
        )
    )
    grouped = (
        merged.groupby(
            [
                "filing_year",
                "filing_period",
                "filing_period_display",
                "government_entity_id",
                "government_entity_name",
                "general_issue_code",
                "general_issue_code_display",
            ],
            dropna=False,
        )
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_clients=("client_api_id", "nunique"),
            n_activities=("activity_id", "nunique"),
        )
        .reset_index()
    )
    return _with_period_order(grouped).sort_values(
        ["filing_year", "period_order", "n_activities", "government_entity_name", "general_issue_code"],
        ascending=[True, True, False, True, True],
    )


def build_contribution_summary(
    contributions: pd.DataFrame,
    contribution_items: pd.DataFrame,
) -> pd.DataFrame:
    merged = contributions.merge(
        contribution_items[["filing_uuid", "amount"]],
        on="filing_uuid",
        how="left",
    )
    grouped = (
        merged.groupby(
            [
                "filing_year",
                "filing_period",
                "filing_period_display",
                "registrant_api_id",
                "registrant_name",
                "lobbyist_api_id",
                "lobbyist_name",
            ],
            dropna=False,
        )
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_items=("amount", "count"),
            contribution_amount=("amount", "sum"),
            filings_marked_no_contributions=("no_contributions", "sum"),
        )
        .reset_index()
    )
    grouped["contribution_amount"] = grouped["contribution_amount"].fillna(0.0)
    return _with_period_order(grouped).sort_values(
        ["filing_year", "period_order", "contribution_amount", "registrant_name", "lobbyist_name"],
        ascending=[True, True, False, True, True],
    )


def build_year_summaries(year: int) -> dict[str, dict[str, str | int]]:
    filings = _read_csv(year, "filings.csv")
    filing_activities = _read_csv(year, "filing_activities.csv")
    filing_activity_lobbyists = _read_csv(year, "filing_activity_lobbyists.csv")
    filing_activity_government_entities = _read_csv(
        year,
        "filing_activity_government_entities.csv",
        empty_columns=[
            "activity_id",
            "filing_uuid",
            "activity_index",
            "activity_government_entity_index",
            "government_entity_id",
            "government_entity_name",
        ],
    )
    contributions = _read_csv(year, "contributions.csv")
    contribution_items = _read_csv(
        year,
        "contribution_items.csv",
        empty_columns=[
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
    )

    out_dir = lda_year_derived_dir(year)
    out_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "client_quarter_summary.csv": build_client_quarter_summary(filings),
        "client_issue_summary.csv": build_client_issue_summary(filings, filing_activities),
        "issue_quarter_summary.csv": build_issue_quarter_summary(filings, filing_activities),
        "client_lobbyist_summary.csv": build_client_lobbyist_summary(filings, filing_activity_lobbyists),
        "government_entity_issue_summary.csv": build_government_entity_issue_summary(
            filings,
            filing_activities,
            filing_activity_government_entities,
        ),
        "contribution_summary.csv": build_contribution_summary(contributions, contribution_items),
    }

    manifest_tables: dict[str, dict[str, str | int]] = {}
    for filename, frame in tables.items():
        path = out_dir / filename
        frame.to_csv(path, index=False)
        manifest_tables[filename] = {"rows": len(frame), "path": str(path)}

    manifest = {
        "year": year,
        "built_at_utc": _iso_utc_now(),
        "tables": manifest_tables,
    }
    (out_dir / "summary_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_tables


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build derived exploratory summary tables from interim LDA tables.",
    )
    parser.add_argument("years", nargs="+", type=int, help="Years to summarize")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    for year in args.years:
        manifest = build_year_summaries(year)
        print(json.dumps({"year": year, "tables": manifest}, indent=2))


if __name__ == "__main__":
    main()
