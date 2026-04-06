"""
Build frontend-ready exports from the validated tech-money pipeline.

This is a thin presentation layer on top of the validated counting logic.
It does not introduce new transaction-type rules or new raw-data ingestion.

Usage:
    python -m pipeline.build_frontend_exports

Outputs:
    exports/site/<cycle>/
        site_metadata.json
        source_manifest.json
        homepage_summary.json
        companies.json
        committees.json
        major_donors.json
        charts/home_weekly_totals.json
        charts/companies/<company-slug>.json
        weekly_totals.csv
        weekly_by_company.csv
        weekly_by_recipient_bucket.csv
        weekly_by_recipient_party.csv
        candidate_race_summary.csv
        candidate_state_summary.csv
        candidate_house_district_summary.csv
        candidate_senate_summary.csv
        companies/<company-slug>.json
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.build_summaries import (
    build_candidate_house_district_summary,
    build_candidate_race_summary,
    build_candidate_senate_summary,
    build_candidate_state_summary,
    build_committee_tech_receipts,
    build_entity_party_lean,
    build_tech_company_summary,
    build_tech_donor_summary,
)
from pipeline.classify_partisan import (
    classify_committees_from_party_field,
    classify_donors,
    classify_ie_committees,
    load_candidate_parties,
)
from pipeline.fec_sources import write_source_manifest
from pipeline.load_fec import load_cycle, tag_tech_donors
from pipeline.paths import fec_cycle_derived_dir, site_export_cycle_dir


MAJOR_DONOR_THRESHOLD = 100_000
FEATURED_COMMITTEE_RECEIPTS = 100_000
FEATURED_COMMITTEE_PCT = 10.0
TOP_DONORS_PER_COMPANY = 50
TOP_COMMITTEES_PER_COMPANY = 50


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def records(df: pd.DataFrame) -> list[dict]:
    """Return strict JSON-safe records."""
    if len(df) == 0:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


def recipient_bucket(cmte_tp: str) -> str:
    if cmte_tp in {"H", "S", "P"}:
        return "candidate"
    if cmte_tp in {"X", "Y"}:
        return "party"
    if cmte_tp in {"O", "U", "W"}:
        return "outside_spending"
    if cmte_tp in {"N", "Q", "V"}:
        return "pac"
    if cmte_tp == "C":
        return "communication_cost"
    return "other"


def build_company_partisan(
    tagged: pd.DataFrame,
    committee_party_classification: pd.DataFrame,
) -> pd.DataFrame:
    """Rebuild company partisan summary if the saved table is missing."""
    donor_class = classify_donors(tagged, committee_party_classification)

    tech_donors = tagged[tagged["is_tech_employer"]].copy()
    tech_donor_parties = tech_donors.merge(
        donor_class[["name", "donor_party", "pct_d", "classified_total"]],
        on="name", how="left",
    )

    company_partisan = (
        tech_donor_parties.groupby(["tech_canonical_name", "donor_party"])
        .agg(
            donor_amt=("net_amt", "sum"),
            n_donors=("name", "nunique"),
        )
        .reset_index()
    )

    company_pivot = (
        company_partisan.pivot_table(
            index="tech_canonical_name",
            columns="donor_party",
            values=["donor_amt", "n_donors"],
            fill_value=0,
            aggfunc="sum",
        )
    )

    company_pivot.columns = [
        f"{stat}_{party}" for stat, party in company_pivot.columns
    ]
    company_pivot = company_pivot.reset_index()

    for party in ["D", "Mixed", "R", "Unknown"]:
        for stat in ["donor_amt", "n_donors"]:
            col = f"{stat}_{party}"
            if col not in company_pivot.columns:
                company_pivot[col] = 0.0

    for stat in ["donor_amt", "n_donors"]:
        cols = [c for c in company_pivot.columns if c.startswith(stat)]
        company_pivot[f"{stat}_total"] = company_pivot[cols].sum(axis=1)

    if "donor_amt_D" in company_pivot.columns and "donor_amt_R" in company_pivot.columns:
        dr_total = company_pivot["donor_amt_D"] + company_pivot["donor_amt_R"]
        company_pivot["pct_dem_by_donor"] = (
            company_pivot["donor_amt_D"] /
            dr_total.replace(0, float("nan")) * 100
        )

    return company_pivot.sort_values("donor_amt_total", ascending=False)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["transaction_date"] = pd.to_datetime(
        df["transaction_dt"],
        format="%m%d%Y",
        errors="coerce",
    )
    df = df[df["transaction_date"].notna()].copy()
    df["week_end"] = df["transaction_date"].dt.to_period("W-SUN").dt.end_time.dt.date
    return df


def build_weekly_totals(tech: pd.DataFrame, cycle: int) -> pd.DataFrame:
    weekly = (
        tech.groupby("week_end")
        .agg(
            net_total=("net_amt", "sum"),
            gross_positive=("transaction_amt", lambda x: x[x > 0].sum()),
            n_contributions=("net_amt", "size"),
            n_donors=("name", "nunique"),
            n_committees=("cmte_id", "nunique"),
        )
        .reset_index()
        .sort_values("week_end")
    )
    weekly["cycle"] = cycle
    weekly["cumulative_net_total"] = weekly["net_total"].cumsum()
    weekly["week_end"] = weekly["week_end"].astype(str)
    return weekly[
        [
            "cycle",
            "week_end",
            "net_total",
            "cumulative_net_total",
            "gross_positive",
            "n_contributions",
            "n_donors",
            "n_committees",
        ]
    ]


def build_weekly_by_company(tech: pd.DataFrame, cycle: int) -> pd.DataFrame:
    weekly = (
        tech.groupby(["week_end", "tech_canonical_name"])
        .agg(
            net_total=("net_amt", "sum"),
            gross_positive=("transaction_amt", lambda x: x[x > 0].sum()),
            n_contributions=("net_amt", "size"),
            n_donors=("name", "nunique"),
        )
        .reset_index()
        .sort_values(["tech_canonical_name", "week_end"])
    )
    weekly["cycle"] = cycle
    weekly["company_slug"] = weekly["tech_canonical_name"].map(slugify)
    weekly["cumulative_net_total"] = weekly.groupby("tech_canonical_name")["net_total"].cumsum()
    weekly["week_end"] = weekly["week_end"].astype(str)
    return weekly[
        [
            "cycle",
            "week_end",
            "tech_canonical_name",
            "company_slug",
            "net_total",
            "cumulative_net_total",
            "gross_positive",
            "n_contributions",
            "n_donors",
        ]
    ]


def build_weekly_by_recipient_bucket(tech: pd.DataFrame, cycle: int) -> pd.DataFrame:
    bucketed = tech.copy()
    bucketed["recipient_bucket"] = bucketed["cmte_tp"].fillna("").map(recipient_bucket)

    weekly = (
        bucketed.groupby(["week_end", "recipient_bucket"])
        .agg(
            net_total=("net_amt", "sum"),
            n_contributions=("net_amt", "size"),
            n_donors=("name", "nunique"),
            n_committees=("cmte_id", "nunique"),
        )
        .reset_index()
        .sort_values(["recipient_bucket", "week_end"])
    )
    weekly["cycle"] = cycle
    weekly["week_end"] = weekly["week_end"].astype(str)
    return weekly[
        [
            "cycle",
            "week_end",
            "recipient_bucket",
            "net_total",
            "n_contributions",
            "n_donors",
            "n_committees",
        ]
    ]


def build_weekly_by_recipient_party(tech: pd.DataFrame, cycle: int) -> pd.DataFrame:
    weekly = (
        tech.groupby(["week_end", "cmte_party_simple"])
        .agg(
            net_total=("net_amt", "sum"),
            n_contributions=("net_amt", "size"),
            n_donors=("name", "nunique"),
            n_committees=("cmte_id", "nunique"),
        )
        .reset_index()
        .sort_values(["cmte_party_simple", "week_end"])
    )
    weekly["cycle"] = cycle
    weekly["week_end"] = weekly["week_end"].astype(str)
    return weekly.rename(columns={"cmte_party_simple": "recipient_party"})[
        [
            "cycle",
            "week_end",
            "recipient_party",
            "net_total",
            "n_contributions",
            "n_donors",
            "n_committees",
        ]
    ]


def build_company_payloads(
    tech: pd.DataFrame,
    companies: pd.DataFrame,
    company_partisan: pd.DataFrame,
    committee_party_classification: pd.DataFrame,
) -> tuple[pd.DataFrame, list[tuple[str, dict]]]:
    company_summary = companies.merge(
        company_partisan,
        on="tech_canonical_name",
        how="left",
    )
    company_summary["slug"] = company_summary["tech_canonical_name"].map(slugify)

    cmte_lookup = committee_party_classification[
        ["cmte_id", "party_dr", "classification_source"]
    ].drop_duplicates(subset=["cmte_id"])

    payloads: list[tuple[str, dict]] = []

    for _, row in company_summary.sort_values("net_total", ascending=False).iterrows():
        company = row["tech_canonical_name"]
        slug = row["slug"]

        company_rows = tech[tech["tech_canonical_name"] == company].copy()
        if "party_dr" not in company_rows.columns and "classification_source" not in company_rows.columns:
            company_rows = company_rows.merge(cmte_lookup, on="cmte_id", how="left")
        elif "classification_source" not in company_rows.columns:
            company_rows = company_rows.merge(
                cmte_lookup[["cmte_id", "classification_source"]],
                on="cmte_id",
                how="left",
            )
        elif "party_dr" not in company_rows.columns:
            company_rows = company_rows.merge(
                cmte_lookup[["cmte_id", "party_dr"]],
                on="cmte_id",
                how="left",
            )
        company_rows["recipient_bucket"] = company_rows["cmte_tp"].fillna("").map(recipient_bucket)
        company_rows["party_dr"] = company_rows["party_dr"].fillna("Unknown")

        weekly = build_weekly_by_company(company_rows, int(company_rows["cycle"].iloc[0]))
        weekly = weekly.drop(columns=["tech_canonical_name", "company_slug"])

        top_donors = (
            company_rows.groupby("name")
            .agg(
                net_total=("net_amt", "sum"),
                gross_positive=("transaction_amt", lambda x: x[x > 0].sum()),
                n_contributions=("net_amt", "size"),
                n_committees=("cmte_id", "nunique"),
                employers=("employer", lambda x: "; ".join(sorted(x.dropna().unique()))),
                states=("state", lambda x: "; ".join(sorted(x.dropna().unique()))),
            )
            .reset_index()
            .sort_values("net_total", ascending=False)
            .head(TOP_DONORS_PER_COMPANY)
        )

        top_donor_committee = (
            company_rows.groupby(["name", "cmte_nm", "cmte_tp"])
            .agg(top_committee_amt=("net_amt", "sum"))
            .reset_index()
            .sort_values("top_committee_amt", ascending=False)
            .drop_duplicates(subset=["name"], keep="first")
            .rename(columns={"cmte_nm": "top_committee", "cmte_tp": "top_committee_type"})
        )
        top_donors = top_donors.merge(top_donor_committee, on="name", how="left")

        top_committees = (
            company_rows.groupby(
                [
                    "cmte_id",
                    "cmte_nm",
                    "cmte_tp",
                    "party_dr",
                    "classification_source",
                    "recipient_bucket",
                ]
            )
            .agg(
                net_total=("net_amt", "sum"),
                n_donors=("name", "nunique"),
                n_contributions=("net_amt", "size"),
            )
            .reset_index()
            .sort_values("net_total", ascending=False)
            .head(TOP_COMMITTEES_PER_COMPANY)
        )

        payload = {
            "company": company,
            "slug": slug,
            "summary": json.loads(
                row.drop(labels=[]).to_json(date_format="iso")
            ),
            "weekly_series": records(weekly),
            "top_donors": records(top_donors),
            "top_committees": records(top_committees),
        }
        payloads.append((slug, payload))

    return company_summary, payloads


def ensure_summary_tables(
    tagged: pd.DataFrame,
    committee_spending: pd.DataFrame,
    committees: pd.DataFrame,
    cycle: int,
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame,
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame,
    pd.DataFrame, pd.DataFrame,
]:
    table_dir = fec_cycle_derived_dir(cycle)
    table_dir.mkdir(parents=True, exist_ok=True)

    cand_party = load_candidate_parties(committees, cycle)
    committee_party_classification = classify_committees_from_party_field(committees)
    committee_party_classification = classify_ie_committees(
        committee_party_classification, cycle, cand_party
    )
    donor_party_classification = classify_donors(tagged, committee_party_classification)

    donor_summary = build_tech_donor_summary(tagged, donor_party_classification)
    company_summary = build_tech_company_summary(tagged, committee_party_classification)
    committee_tech_receipts = build_committee_tech_receipts(
        tagged,
        committees,
        committee_party_classification,
    )
    company_partisan = build_company_partisan(tagged, committee_party_classification)
    entity_party_lean = build_entity_party_lean(
        cycle,
        committee_party_classification,
        donor_summary,
        company_summary,
    )
    candidate_race_summary = build_candidate_race_summary(
        cycle,
        tagged,
        committee_spending,
        committee_tech_receipts,
        committees,
    )
    candidate_state_summary = build_candidate_state_summary(candidate_race_summary)
    candidate_house_district_summary = build_candidate_house_district_summary(
        candidate_race_summary
    )
    candidate_senate_summary = build_candidate_senate_summary(candidate_race_summary)

    donor_summary.to_csv(table_dir / "tech_donor_summary.csv", index=False)
    company_summary.to_csv(table_dir / "tech_company_summary.csv", index=False)
    committee_tech_receipts.to_csv(table_dir / "committee_tech_receipts.csv", index=False)
    committee_party_classification.to_csv(
        table_dir / "committee_party_classification.csv",
        index=False,
    )
    donor_party_classification.to_csv(
        table_dir / "donor_party_classification.csv",
        index=False,
    )
    company_partisan.to_csv(table_dir / "tech_company_partisan.csv", index=False)
    entity_party_lean.to_csv(table_dir / "entity_party_lean.csv", index=False)
    candidate_race_summary.to_csv(table_dir / "candidate_race_summary.csv", index=False)
    candidate_state_summary.to_csv(table_dir / "candidate_state_summary.csv", index=False)
    candidate_house_district_summary.to_csv(
        table_dir / "candidate_house_district_summary.csv", index=False
    )
    candidate_senate_summary.to_csv(table_dir / "candidate_senate_summary.csv", index=False)
    for entity_type, suffix in [
        ("company", "companies"),
        ("committee", "committees"),
        ("donor", "donors"),
    ]:
        entity_party_lean[
            entity_party_lean["entity_type"] == entity_type
        ].to_csv(table_dir / f"entity_party_lean_{suffix}.csv", index=False)

    return (
        donor_summary,
        company_summary,
        committee_tech_receipts,
        committee_party_classification,
        company_partisan,
        entity_party_lean,
        candidate_race_summary,
        candidate_state_summary,
        candidate_house_district_summary,
        candidate_senate_summary,
    )


def build_frontend_exports(cycle: int = 2024) -> Path:
    cycle_out_dir = site_export_cycle_dir(cycle)
    company_out_dir = cycle_out_dir / "companies"
    chart_out_dir = cycle_out_dir / "charts"
    company_chart_out_dir = chart_out_dir / "companies"
    company_out_dir.mkdir(parents=True, exist_ok=True)
    company_chart_out_dir.mkdir(parents=True, exist_ok=True)
    source_manifest = write_source_manifest(cycle, cycle_out_dir)

    print(f"Loading and tagging cycle {cycle}...")
    data = load_cycle(cycle)
    tagged_all = tag_tech_donors(data.donor_contributions, data.tech_employers)
    tagged_all["cycle"] = cycle
    tagged_all = parse_dates(tagged_all)
    tagged = tagged_all[tagged_all["is_tech_employer"]].copy()

    (
        donor_summary,
        company_summary,
        committee_tech_receipts,
        committee_party_classification,
        company_partisan,
        entity_party_lean,
        candidate_race_summary,
        candidate_state_summary,
        candidate_house_district_summary,
        candidate_senate_summary,
    ) = ensure_summary_tables(tagged_all, data.committee_spending, data.committees, cycle)

    cmte_party_lookup = committee_party_classification[
        ["cmte_id", "party_dr"]
    ].drop_duplicates(subset=["cmte_id"])
    tagged = tagged.merge(cmte_party_lookup, on="cmte_id", how="left")
    tagged["cmte_party_simple"] = tagged["party_dr"].fillna("Unknown")

    print("\nBuilding weekly exports...")
    weekly_totals = build_weekly_totals(tagged, cycle)
    weekly_by_company = build_weekly_by_company(tagged, cycle)
    weekly_by_recipient_bucket = build_weekly_by_recipient_bucket(tagged, cycle)
    weekly_by_recipient_party = build_weekly_by_recipient_party(tagged, cycle)

    print("Building company payloads...")
    companies, company_payloads = build_company_payloads(
        tagged,
        company_summary,
        company_partisan,
        committee_party_classification,
    )

    committees = committee_tech_receipts[committee_tech_receipts["tech_receipts"] > 0].copy()
    committees["recipient_bucket"] = committees["cmte_tp"].fillna("").map(recipient_bucket)
    committees["is_featured"] = (
        (committees["tech_receipts"] >= FEATURED_COMMITTEE_RECEIPTS) |
        (committees["tech_pct"] >= FEATURED_COMMITTEE_PCT)
    )
    committees["is_tech_dominated"] = committees["tech_pct"] >= 50
    committees = committees.sort_values("tech_receipts", ascending=False)
    candidate_committees = committees[committees["recipient_bucket"] == "candidate"].copy()
    political_bodies = committees[committees["recipient_bucket"] != "candidate"].copy()

    major_donors = donor_summary[donor_summary["net_total"] >= MAJOR_DONOR_THRESHOLD].copy()
    major_donors = major_donors.sort_values("net_total", ascending=False)

    max_txn = tagged["transaction_date"].max()
    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    site_metadata = {
        "cycle": cycle,
        "built_at_utc": built_at,
        "data_as_of": max_txn.date().isoformat() if pd.notna(max_txn) else None,
        "latest_bulk_release_utc": source_manifest["latest_bulk_release_utc"],
        "stale_bulk_sources": source_manifest["sources_with_remote_newer_than_local"],
        "total_tech_linked_giving": float(tagged["net_amt"].sum()),
        "tech_donor_count": int(donor_summary["name"].nunique()),
        "tracked_company_count": int(companies["tech_canonical_name"].nunique()),
        "committees_receiving_tech_money": int((committees["tech_receipts"] > 0).sum()),
        "tech_dominated_committees": int((committees["tech_pct"] >= 50).sum()),
        "notes": [
            f"All exports in this folder reflect the validated {cycle} pipeline output.",
            "Committee tech_pct is based on itemized individual contribution receipts, not all committee money.",
            "Recipient committee lean is inferred from candidate-facing spending when direct committee party is absent.",
            "Candidate navigation now includes national, state, Senate, and House district pages.",
        ],
    }

    homepage_summary = {
        "headline_numbers": {
            "total_tech_linked_giving": float(tagged["net_amt"].sum()),
            "tech_donor_count": int(donor_summary["name"].nunique()),
            "tracked_company_count": int(companies["tech_canonical_name"].nunique()),
            "committees_receiving_tech_money": int((committees["tech_receipts"] > 0).sum()),
        },
        "top_companies": records(
            companies[
                [
                    "tech_canonical_name",
                    "slug",
                    "net_total",
                    "n_donors",
                    "n_contributions",
                    "n_committees",
                    "pct_dem",
                    "pct_classified_recipients",
                    "pct_dem_by_donor",
                ]
            ].head(10)
        ),
        "top_candidates": records(
            candidate_committees[
                [
                    "cmte_id",
                    "cmte_nm",
                    "cmte_tp",
                    "party_dr",
                    "tech_receipts",
                    "tech_pct",
                    "tech_donors",
                    "is_featured",
                    "is_tech_dominated",
                    "recipient_bucket",
                ]
            ].head(10)
        ),
        "top_political_bodies": records(
            political_bodies[
                [
                    "cmte_id",
                    "cmte_nm",
                    "cmte_tp",
                    "party_dr",
                    "tech_receipts",
                    "tech_pct",
                    "tech_donors",
                    "is_featured",
                    "is_tech_dominated",
                    "recipient_bucket",
                ]
            ].head(10)
        ),
        "top_donors": records(
            major_donors[
                [
                    "name",
                    "D",
                    "R",
                    "net_total",
                    "donor_party",
                    "pct_d",
                    "pct_r",
                    "n_contributions",
                    "n_committees",
                    "tech_companies",
                    "top_committee",
                    "top_committee_type",
                    "top_committee_amt",
                ]
            ].head(20)
        ),
    }

    home_chart = records(
        weekly_totals[
            ["week_end", "net_total", "cumulative_net_total"]
        ]
    )

    print("Writing files...")
    (cycle_out_dir / "site_metadata.json").write_text(
        json.dumps(site_metadata, indent=2) + "\n",
        encoding="utf-8",
    )
    (cycle_out_dir / "homepage_summary.json").write_text(
        json.dumps(homepage_summary, indent=2) + "\n",
        encoding="utf-8",
    )
    (cycle_out_dir / "companies.json").write_text(
        json.dumps(
            records(
                companies[
                    [
                        "tech_canonical_name",
                        "slug",
                        "net_total",
                        "n_donors",
                        "n_contributions",
                        "n_committees",
                        "sectors",
                        "amt_dem",
                        "amt_rep",
                        "amt_mixed",
                        "amt_unknown",
                        "amt_other",
                        "pct_dem",
                        "pct_classified_recipients",
                        "donor_amt_D",
                        "donor_amt_Mixed",
                        "donor_amt_R",
                        "donor_amt_Unknown",
                        "pct_dem_by_donor",
                    ]
                ]
            ),
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    (cycle_out_dir / "committees.json").write_text(
        json.dumps(
            records(
                committees[
                    [
                        "cmte_id",
                        "cmte_nm",
                        "cmte_tp",
                        "cmte_dsgn",
                        "cmte_pty_affiliation",
                        "connected_org_nm",
                        "cand_id",
                        "party_dr",
                        "classification_source",
                        "evidence_sources",
                        "dem_evidence_amt",
                        "rep_evidence_amt",
                        "evidence_total",
                        "evidence_pct_dem",
                        "tech_receipts",
                        "tech_donors",
                        "tech_contributions",
                        "total_receipts",
                        "total_donors",
                        "tech_pct",
                        "tech_companies",
                        "recipient_bucket",
                        "is_featured",
                        "is_tech_dominated",
                    ]
                ]
            ),
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    (cycle_out_dir / "major_donors.json").write_text(
        json.dumps(records(major_donors), indent=2) + "\n",
        encoding="utf-8",
    )
    (chart_out_dir / "home_weekly_totals.json").write_text(
        json.dumps(home_chart, indent=2) + "\n",
        encoding="utf-8",
    )

    weekly_totals.to_csv(cycle_out_dir / "weekly_totals.csv", index=False)
    weekly_by_company.to_csv(cycle_out_dir / "weekly_by_company.csv", index=False)
    weekly_by_recipient_bucket.to_csv(
        cycle_out_dir / "weekly_by_recipient_bucket.csv", index=False
    )
    weekly_by_recipient_party.to_csv(
        cycle_out_dir / "weekly_by_recipient_party.csv", index=False
    )
    entity_party_lean.to_csv(cycle_out_dir / "entity_party_lean.csv", index=False)
    candidate_race_summary.to_csv(cycle_out_dir / "candidate_race_summary.csv", index=False)
    candidate_state_summary.to_csv(cycle_out_dir / "candidate_state_summary.csv", index=False)
    candidate_house_district_summary.to_csv(
        cycle_out_dir / "candidate_house_district_summary.csv", index=False
    )
    candidate_senate_summary.to_csv(cycle_out_dir / "candidate_senate_summary.csv", index=False)
    for entity_type, suffix in [
        ("company", "companies"),
        ("committee", "committees"),
        ("donor", "donors"),
    ]:
        entity_party_lean[
            entity_party_lean["entity_type"] == entity_type
        ].to_csv(cycle_out_dir / f"entity_party_lean_{suffix}.csv", index=False)

    for slug, payload in company_payloads:
        (company_out_dir / f"{slug}.json").write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
        (company_chart_out_dir / f"{slug}.json").write_text(
            json.dumps(payload["weekly_series"], indent=2) + "\n",
            encoding="utf-8",
        )

    print(f"\nFrontend exports written to {cycle_out_dir}")
    print(f"  Company payloads: {len(company_payloads)}")
    print(f"  Weekly rows: {len(weekly_totals)} totals, {len(weekly_by_company)} by-company")

    return cycle_out_dir


def main(args: list[str] | None = None):
    cycle_args = args if args is not None else sys.argv[1:]
    cycles = [int(arg) for arg in cycle_args] if cycle_args else [2024]
    for cycle in cycles:
        build_frontend_exports(cycle)


if __name__ == "__main__":
    main()
