"""
Step 4: Build summary tables from filtered FEC data.

Takes the output of load_fec and produces aggregated tables that all
downstream pages/charts draw from. These are the intermediate data
products — the pipeline's main output.

Usage:
    python -m pipeline.build_summaries

Outputs (as CSV files in outputs/tables/):
    - tech_donor_summary.csv
    - tech_company_summary.csv
    - committee_tech_receipts.csv
    - committee_outbound_spending.csv
    - tech_sankey_edges.csv
    - entity_party_lean.csv
"""

from pathlib import Path

import pandas as pd

from pipeline.classify_partisan import (
    PARTISAN_LEAN_THRESHOLD,
    classify_committees_from_party_field,
    classify_donors,
    classify_ie_committees,
    load_candidate_linkage_table,
    load_candidate_master_table,
    load_candidate_parties,
    PARTY_MAP,
)
from pipeline.load_fec import load_cycle, tag_tech_donors


OUT_DIR = Path(__file__).resolve().parent.parent / "outputs" / "tables"


def build_tech_donor_summary(
    tagged: pd.DataFrame,
    donor_classification: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """One row per tech-linked donor with their total giving and where it went.

    Columns: name, employer(s), tech_canonical_name, tech_sector,
             net_total, n_contributions, n_committees,
             top_committee, top_committee_type, top_committee_amt
    """
    tech = tagged[tagged["is_tech_employer"]].copy()

    # Per-donor summary
    donors = tech.groupby("name").agg(
        net_total=("net_amt", "sum"),
        gross_positive=("transaction_amt", lambda x: x[x > 0].sum()),
        n_contributions=("net_amt", "size"),
        n_committees=("cmte_id", "nunique"),
        employers=("employer", lambda x: "; ".join(sorted(x.unique()))),
        occupations=("occupation", lambda x: "; ".join(sorted(x.unique()))),
        states=("state", lambda x: "; ".join(sorted(x.unique()))),
        tech_companies=("tech_canonical_name",
                        lambda x: "; ".join(sorted(x.unique()))),
        tech_sectors=("tech_sector",
                      lambda x: "; ".join(sorted(x.dropna().unique()))),
    ).reset_index().sort_values("net_total", ascending=False)

    # Find each donor's top committee by amount
    top_cmte = (
        tech.groupby(["name", "cmte_id", "cmte_nm", "cmte_tp"])
        .agg(cmte_amt=("net_amt", "sum"))
        .reset_index()
        .sort_values("cmte_amt", ascending=False)
        .drop_duplicates(subset=["name"], keep="first")
        [["name", "cmte_nm", "cmte_tp", "cmte_amt"]]
        .rename(columns={
            "cmte_nm": "top_committee",
            "cmte_tp": "top_committee_type",
            "cmte_amt": "top_committee_amt",
        })
    )

    donors = donors.merge(top_cmte, on="name", how="left")

    if donor_classification is not None:
        donors = donors.merge(
            donor_classification[
                [
                    "name",
                    "donor_party",
                    "D",
                    "R",
                    "pct_d",
                    "pct_r",
                    "pct_classified",
                    "classified_total",
                    "overall_total",
                    "Mixed",
                    "Unknown",
                ]
            ],
            on="name",
            how="left",
        )

    print(f"  Tech donor summary: {len(donors):,} donors, "
          f"${donors['net_total'].sum():,.0f} total")

    return donors


def build_tech_company_summary(
    tagged: pd.DataFrame,
    committee_party_classification: pd.DataFrame,
) -> pd.DataFrame:
    """One row per tech company with employee giving totals and partisan split.

    Uses inferred committee partisan lean, not just raw filed party.
    """
    tech = tagged[tagged["is_tech_employer"]].copy()
    cmte_party = committee_party_classification[
        ["cmte_id", "party_dr"]
    ].drop_duplicates(subset=["cmte_id"])
    tech = tech.merge(cmte_party, on="cmte_id", how="left")
    tech["party_simple"] = tech["party_dr"].fillna("Unknown")

    # Company-level summary
    companies = tech.groupby("tech_canonical_name").agg(
        net_total=("net_amt", "sum"),
        n_donors=("name", "nunique"),
        n_contributions=("net_amt", "size"),
        n_committees=("cmte_id", "nunique"),
        sectors=("tech_sector",
                 lambda x: "; ".join(sorted(x.dropna().unique()))),
    ).reset_index()

    # Partisan split by dollar amount
    party_split = (
        tech.groupby(["tech_canonical_name", "party_simple"])
        .agg(party_amt=("net_amt", "sum"))
        .reset_index()
        .pivot(index="tech_canonical_name", columns="party_simple",
               values="party_amt")
        .fillna(0)
        .reset_index()
    )

    for col in ["D", "R", "Mixed", "Unknown"]:
        if col not in party_split.columns:
            party_split[col] = 0.0

    party_split = party_split.rename(columns={
        "D": "amt_dem",
        "R": "amt_rep",
        "Mixed": "amt_mixed",
        "Unknown": "amt_unknown",
    })
    party_split["amt_other"] = (
        party_split["amt_mixed"] + party_split["amt_unknown"]
    )

    companies = companies.merge(party_split, on="tech_canonical_name", how="left")

    for col in ["amt_dem", "amt_rep", "amt_mixed", "amt_unknown", "amt_other"]:
        if col not in companies.columns:
            companies[col] = 0.0

    companies["classified_recipient_total"] = companies["amt_dem"] + companies["amt_rep"]
    companies["pct_dem"] = (
        companies["amt_dem"] /
        companies["classified_recipient_total"].replace(0, float("nan"))
        * 100
    )
    companies["pct_classified_recipients"] = (
        companies["classified_recipient_total"] /
        companies["net_total"].replace(0, float("nan"))
        * 100
    )

    companies = companies.sort_values("net_total", ascending=False)

    print(f"  Tech company summary: {len(companies)} companies")

    return companies


def build_entity_party_lean(
    cycle: int,
    committee_party_classification: pd.DataFrame,
    tech_donor_summary: pd.DataFrame,
    tech_company_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Build a common lean dataset for committees, tech donors, and tech companies."""

    committee_rows = committee_party_classification.copy()
    committee_rows["entity_type"] = "committee"
    committee_rows["entity_id"] = committee_rows["cmte_id"]
    committee_rows["entity_name"] = committee_rows["cmte_nm"]
    committee_rows["entity_subtype"] = committee_rows["cmte_tp"].fillna("")
    committee_rows["party_label"] = committee_rows["party_dr"].fillna("Unknown")
    committee_rows["party_score_dem"] = float("nan")
    committee_rows.loc[committee_rows["party_label"] == "D", "party_score_dem"] = 1.0
    committee_rows.loc[committee_rows["party_label"] == "R", "party_score_dem"] = 0.0
    mixed_mask = (
        (committee_rows["party_label"] == "Mixed") &
        committee_rows["evidence_pct_dem"].notna()
    )
    committee_rows.loc[mixed_mask, "party_score_dem"] = committee_rows.loc[
        mixed_mask, "evidence_pct_dem"
    ]
    committee_rows["party_score_rep"] = 1 - pd.to_numeric(
        committee_rows["party_score_dem"], errors="coerce"
    )
    committee_rows.loc[committee_rows["party_score_dem"].isna(), "party_score_rep"] = float("nan")
    committee_rows["lean_dem_amt"] = committee_rows["dem_evidence_amt"]
    committee_rows["lean_rep_amt"] = committee_rows["rep_evidence_amt"]
    committee_rows["mixed_amt"] = 0.0
    committee_rows["unknown_amt"] = 0.0
    committee_rows["lean_measure_total"] = committee_rows["evidence_total"]
    committee_rows["coverage_total"] = committee_rows["evidence_total"]
    committee_rows["coverage_ratio"] = (
        committee_rows["evidence_total"] / committee_rows["evidence_total"]
    )
    committee_rows.loc[committee_rows["evidence_total"] <= 0, "coverage_ratio"] = float("nan")
    committee_rows["classification_method"] = "unknown"
    committee_rows.loc[
        committee_rows["classification_source"] == "party_field",
        "classification_method",
    ] = "filed_committee_party"
    committee_rows.loc[
        committee_rows["classification_source"].isin(
            ["ie_spending", "24k_contributions", "24k_contributions+ie_spending"]
        ),
        "classification_method",
    ] = "candidate_facing_committee_activity"
    committee_rows["scope"] = "all_committees"

    donor_rows = tech_donor_summary.copy()
    donor_rows["entity_type"] = "donor"
    donor_rows["entity_id"] = donor_rows["name"]
    donor_rows["entity_name"] = donor_rows["name"]
    donor_rows["entity_subtype"] = "tech_donor"
    donor_rows["party_label"] = donor_rows["donor_party"].fillna("Unknown")
    donor_rows["party_score_dem"] = donor_rows["pct_d"]
    donor_rows["party_score_rep"] = 1 - donor_rows["pct_d"]
    donor_rows.loc[donor_rows["pct_d"].isna(), "party_score_rep"] = float("nan")
    donor_rows["lean_dem_amt"] = donor_rows["D"].fillna(0.0)
    donor_rows["lean_rep_amt"] = donor_rows["R"].fillna(0.0)
    donor_rows["mixed_amt"] = donor_rows["Mixed"].fillna(0.0)
    donor_rows["unknown_amt"] = donor_rows["Unknown"].fillna(0.0)
    donor_rows["lean_measure_total"] = donor_rows["classified_total"]
    donor_rows["coverage_total"] = donor_rows["overall_total"]
    donor_rows["coverage_ratio"] = donor_rows["pct_classified"]
    donor_rows["classification_method"] = "recipient_committee_lean"
    donor_rows["classification_source"] = "donor_recipient_committee_lean"
    donor_rows["scope"] = "tech_donors"

    company_rows = tech_company_summary.copy()
    company_rows["entity_type"] = "company"
    company_rows["entity_id"] = company_rows["tech_canonical_name"]
    company_rows["entity_name"] = company_rows["tech_canonical_name"]
    company_rows["entity_subtype"] = "tech_company"
    company_rows["party_label"] = "Unknown"
    company_rows.loc[
        company_rows["classified_recipient_total"] > 0, "party_label"
    ] = "Mixed"
    company_rows["pct_rep"] = 100.0 - company_rows["pct_dem"]
    company_rows.loc[
        company_rows["pct_dem"] >= PARTISAN_LEAN_THRESHOLD * 100.0,
        "party_label",
    ] = "D"
    company_rows.loc[
        company_rows["pct_rep"] >= PARTISAN_LEAN_THRESHOLD * 100.0,
        "party_label",
    ] = "R"
    company_rows["party_score_dem"] = (
        company_rows["pct_dem"] / 100.0
    ).where(company_rows["classified_recipient_total"] > 0, float("nan"))
    company_rows["party_score_rep"] = 1 - company_rows["party_score_dem"]
    company_rows.loc[company_rows["party_score_dem"].isna(), "party_score_rep"] = float("nan")
    company_rows["lean_dem_amt"] = company_rows["amt_dem"]
    company_rows["lean_rep_amt"] = company_rows["amt_rep"]
    company_rows["mixed_amt"] = company_rows["amt_mixed"]
    company_rows["unknown_amt"] = company_rows["amt_unknown"]
    company_rows["lean_measure_total"] = company_rows["classified_recipient_total"]
    company_rows["coverage_total"] = company_rows["net_total"]
    company_rows["coverage_ratio"] = company_rows["pct_classified_recipients"] / 100.0
    company_rows["classification_method"] = "employee_recipient_committee_lean"
    company_rows["classification_source"] = "company_employee_giving"
    company_rows["scope"] = "tech_companies"

    columns = [
        "entity_type",
        "entity_id",
        "entity_name",
        "entity_subtype",
        "party_label",
        "party_score_dem",
        "party_score_rep",
        "lean_dem_amt",
        "lean_rep_amt",
        "mixed_amt",
        "unknown_amt",
        "lean_measure_total",
        "coverage_total",
        "coverage_ratio",
        "classification_method",
        "classification_source",
        "scope",
    ]

    entity_lean = pd.concat(
        [
            committee_rows[columns],
            donor_rows[columns],
            company_rows[columns],
        ],
        ignore_index=True,
    )
    entity_lean.insert(0, "cycle", cycle)

    type_order = pd.Categorical(
        entity_lean["entity_type"],
        categories=["company", "committee", "donor"],
        ordered=True,
    )
    entity_lean = entity_lean.assign(_type_order=type_order)
    entity_lean = entity_lean.sort_values(
        ["_type_order", "lean_measure_total", "coverage_total", "entity_name"],
        ascending=[True, False, False, True],
        na_position="last",
    ).drop(columns=["_type_order"])

    print(
        "  Entity lean dataset: "
        f"{len(entity_lean):,} rows "
        f"({(entity_lean['entity_type'] == 'company').sum():,} companies, "
        f"{(entity_lean['entity_type'] == 'committee').sum():,} committees, "
        f"{(entity_lean['entity_type'] == 'donor').sum():,} tech donors)"
    )

    return entity_lean


def _race_key(row: pd.Series) -> str:
    office = row["cand_office"]
    state = row["cand_office_st"]
    district = row["cand_office_district"]
    if office == "P":
        return "PRES"
    if office == "S":
        return f"SEN-{state}"
    if office == "H":
        return f"HOUSE-{state}-{district}"
    return f"{office}-{state}-{district}"


def _clean_code(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    if text in {"", "NAN", "NONE"}:
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _normalize_house_district(value: object) -> str:
    text = _clean_code(value)
    if not text:
        return ""
    try:
        district_num = int(float(text))
    except ValueError:
        return text
    if district_num == 0:
        return "AL"
    return f"{district_num:02d}"


HOUSE_SEAT_COUNT = {
    "AL": 7, "AK": 1, "AZ": 9, "AR": 4, "CA": 52, "CO": 8, "CT": 5, "DE": 1,
    "DC": 1, "FL": 28, "GA": 14, "HI": 2, "ID": 2, "IL": 17, "IN": 9, "IA": 4,
    "KS": 4, "KY": 6, "LA": 6, "ME": 2, "MD": 8, "MA": 9, "MI": 13, "MN": 8,
    "MS": 4, "MO": 8, "MT": 2, "NE": 3, "NV": 4, "NH": 2, "NJ": 12, "NM": 3,
    "NY": 26, "NC": 14, "ND": 1, "OH": 15, "OK": 5, "OR": 6, "PA": 17, "RI": 2,
    "SC": 7, "SD": 1, "TN": 9, "TX": 38, "UT": 4, "VT": 1, "VA": 11, "WA": 10,
    "WV": 2, "WI": 8, "WY": 1, "AS": 1, "GU": 1, "MP": 1, "PR": 1, "VI": 1,
}


def _is_valid_house_district(state_code: str, district_code: str) -> bool:
    seat_count = HOUSE_SEAT_COUNT.get(state_code)
    if seat_count is None:
        return district_code != ""
    if seat_count == 1:
        return district_code == "AL"
    if district_code in {"", "AL"}:
        return False
    try:
        district_num = int(district_code)
    except ValueError:
        return False
    return 1 <= district_num <= seat_count


def _district_sort_value(value: object) -> int:
    code = _normalize_house_district(value)
    if code == "AL":
        return 0
    try:
        return int(code)
    except ValueError:
        return 999


def _assign_party_label(
    df: pd.DataFrame,
    dem_col: str,
    rep_col: str,
    label_col: str = "party_label",
    pct_dem_col: str = "pct_dem",
    pct_rep_col: str = "pct_rep",
) -> pd.DataFrame:
    classified_total = df[dem_col].fillna(0.0) + df[rep_col].fillna(0.0)
    df[pct_dem_col] = (
        df[dem_col].fillna(0.0) /
        classified_total.replace(0, float("nan"))
        * 100
    )
    df[pct_rep_col] = (
        df[rep_col].fillna(0.0) /
        classified_total.replace(0, float("nan"))
        * 100
    )
    df[label_col] = "Unknown"
    df.loc[classified_total > 0, label_col] = "Mixed"
    df.loc[df[pct_dem_col] >= PARTISAN_LEAN_THRESHOLD * 100.0, label_col] = "D"
    df.loc[df[pct_rep_col] >= PARTISAN_LEAN_THRESHOLD * 100.0, label_col] = "R"
    return df


def _displayable_candidates(candidate_race: pd.DataFrame) -> pd.DataFrame:
    """Return candidates with enough finance signal to show in public navigation."""
    if "is_display_candidate" not in candidate_race.columns:
        return candidate_race.copy()
    return candidate_race[candidate_race["is_display_candidate"]].copy()


def build_candidate_race_summary(
    cycle: int,
    tagged: pd.DataFrame,
    committee_spending: pd.DataFrame,
    committee_tech_receipts: pd.DataFrame,
    committees: pd.DataFrame,
) -> pd.DataFrame:
    """Build one row per candidate with linked committees and race-level money touchpoints."""

    candidates = load_candidate_master_table(cycle).copy()
    candidates = candidates[candidates["cand_election_yr"] == str(cycle)].copy()
    candidates["party_dr"] = candidates["cand_pty_affiliation"].map(PARTY_MAP)
    candidates["is_major_party"] = candidates["party_dr"].isin(["D", "R"])
    candidates["race_key"] = candidates.apply(_race_key, axis=1)

    linkage = load_candidate_linkage_table(cycle).copy()
    linkage = linkage[
        (linkage["cand_election_yr"] == str(cycle)) |
        (linkage["fec_election_yr"] == str(cycle))
    ].copy()
    linkage = linkage.drop_duplicates(subset=["cand_id", "cmte_id"])
    linkage = linkage.merge(
        committees[["cmte_id", "cmte_nm"]].rename(columns={"cmte_nm": "linked_cmte_nm"}),
        on="cmte_id",
        how="left",
    )

    linkage_summary = (
        linkage.groupby("cand_id")
        .agg(
            linked_committee_count=("cmte_id", "nunique"),
            linked_principal_committee_count=("cmte_dsgn", lambda x: int((x == "P").sum())),
            linked_committee_ids=("cmte_id", lambda x: "; ".join(sorted(x.dropna().unique()))),
            linked_committee_names=("linked_cmte_nm", lambda x: "; ".join(sorted(x.dropna().unique()))),
            linked_committee_types=("cmte_tp", lambda x: "; ".join(sorted(x.dropna().unique()))),
        )
        .reset_index()
    )

    all_receipts = (
        tagged.groupby("cmte_id")
        .agg(
            total_itemized_receipts=("net_amt", "sum"),
            total_itemized_donors=("name", "nunique"),
        )
        .reset_index()
    )
    tech = tagged[tagged["is_tech_employer"]].copy()
    tech_receipts = (
        tech.groupby("cmte_id")
        .agg(
            tech_itemized_receipts=("net_amt", "sum"),
            tech_itemized_donors=("name", "nunique"),
            tech_itemized_contributions=("net_amt", "size"),
            tech_companies=("tech_canonical_name", lambda x: "; ".join(sorted(x.dropna().unique()))),
        )
        .reset_index()
    )

    linkage_finance = linkage[["cand_id", "cmte_id"]].drop_duplicates()
    linkage_finance = linkage_finance.merge(all_receipts, on="cmte_id", how="left")
    linkage_finance = linkage_finance.merge(tech_receipts, on="cmte_id", how="left")
    for col in [
        "total_itemized_receipts",
        "total_itemized_donors",
        "tech_itemized_receipts",
        "tech_itemized_donors",
        "tech_itemized_contributions",
    ]:
        linkage_finance[col] = linkage_finance[col].fillna(0)
    linkage_finance["tech_companies"] = linkage_finance["tech_companies"].fillna("")

    receipt_summary = (
        linkage_finance.groupby("cand_id")
        .agg(
            total_itemized_receipts=("total_itemized_receipts", "sum"),
            total_itemized_donors=("total_itemized_donors", "sum"),
            tech_itemized_receipts=("tech_itemized_receipts", "sum"),
            tech_itemized_donors=("tech_itemized_donors", "sum"),
            tech_itemized_contributions=("tech_itemized_contributions", "sum"),
            tech_company_tags=("tech_companies", lambda x: "; ".join(sorted({tag for tags in x for tag in tags.split("; ") if tag}))),
        )
        .reset_index()
    )
    receipt_summary["tech_pct_itemized_receipts"] = (
        receipt_summary["tech_itemized_receipts"] /
        receipt_summary["total_itemized_receipts"].replace(0, float("nan"))
        * 100
    )

    cmte_to_cand = linkage.set_index("cmte_id")["cand_id"].to_dict()
    ies = committee_spending[
        committee_spending["transaction_tp"].isin(["24E", "24A"])
    ].copy()
    ies["target_cand_id"] = ies["other_id"].map(cmte_to_cand)
    ies["target_cand_id"] = ies["target_cand_id"].where(
        ies["target_cand_id"].notna() & (ies["target_cand_id"] != ""),
        ies["other_id"],
    )
    ies = ies[ies["target_cand_id"] != ""].copy()
    tech_funded_cmtes = set(
        committee_tech_receipts.loc[
            committee_tech_receipts["tech_receipts"] > 0, "cmte_id"
        ]
    )
    ies["is_tech_funded_cmte"] = ies["cmte_id"].isin(tech_funded_cmtes)
    ies["support_amt"] = ies["net_amt"].where(ies["transaction_tp"] == "24E", 0.0)
    ies["oppose_amt"] = ies["net_amt"].where(ies["transaction_tp"] == "24A", 0.0)
    ies["support_amt_tech_funded"] = ies["support_amt"].where(ies["is_tech_funded_cmte"], 0.0)
    ies["oppose_amt_tech_funded"] = ies["oppose_amt"].where(ies["is_tech_funded_cmte"], 0.0)

    ie_summary = (
        ies.groupby("target_cand_id")
        .agg(
            ie_support_total=("support_amt", "sum"),
            ie_oppose_total=("oppose_amt", "sum"),
            tech_funded_ie_support_total=("support_amt_tech_funded", "sum"),
            tech_funded_ie_oppose_total=("oppose_amt_tech_funded", "sum"),
        )
        .reset_index()
        .rename(columns={"target_cand_id": "cand_id"})
    )
    ie_summary["ie_net_support"] = (
        ie_summary["ie_support_total"] - ie_summary["ie_oppose_total"]
    )
    ie_summary["tech_funded_ie_net_support"] = (
        ie_summary["tech_funded_ie_support_total"] -
        ie_summary["tech_funded_ie_oppose_total"]
    )

    result = candidates.merge(linkage_summary, on="cand_id", how="left")
    result = result.merge(receipt_summary, on="cand_id", how="left")
    result = result.merge(ie_summary, on="cand_id", how="left")

    fill_zero_cols = [
        "linked_committee_count",
        "linked_principal_committee_count",
        "total_itemized_receipts",
        "total_itemized_donors",
        "tech_itemized_receipts",
        "tech_itemized_donors",
        "tech_itemized_contributions",
        "ie_support_total",
        "ie_oppose_total",
        "ie_net_support",
        "tech_funded_ie_support_total",
        "tech_funded_ie_oppose_total",
        "tech_funded_ie_net_support",
    ]
    for col in fill_zero_cols:
        result[col] = result[col].fillna(0)
    for col in ["linked_committee_ids", "linked_committee_names", "linked_committee_types", "tech_company_tags"]:
        result[col] = result[col].fillna("")

    result["has_principal_candidate_committee"] = (
        result["linked_principal_committee_count"] > 0
    )
    result["has_finance_activity"] = (
        (result["total_itemized_receipts"] > 0) |
        (result["tech_itemized_receipts"] > 0) |
        (result["ie_support_total"] > 0) |
        (result["ie_oppose_total"] > 0) |
        (result["tech_funded_ie_support_total"] > 0) |
        (result["tech_funded_ie_oppose_total"] > 0)
    )
    result["is_display_candidate"] = (
        result["has_principal_candidate_committee"] |
        result["has_finance_activity"]
    )

    result = result.sort_values(
        [
            "is_display_candidate",
            "is_major_party",
            "tech_itemized_receipts",
            "ie_support_total",
            "linked_principal_committee_count",
            "cand_name",
        ],
        ascending=[False, False, False, False, False, True],
    )

    print(
        "  Candidate race summary: "
        f"{len(result):,} candidates, "
        f"{int(result['is_major_party'].sum()):,} major-party"
    )

    return result


def build_candidate_state_summary(candidate_race: pd.DataFrame) -> pd.DataFrame:
    """Build one row per state/jurisdiction for candidate navigation."""
    candidates = candidate_race.copy()
    candidates["state_code"] = candidates["cand_office_st"].map(_clean_code)
    candidates = candidates[
        candidates["state_code"].ne("") &
        candidates["state_code"].ne("US") &
        candidates["cand_office"].isin(["H", "S"])
    ].copy()
    candidates["district_code"] = candidates["cand_office_district"].map(_normalize_house_district)
    candidates = candidates[
        (candidates["cand_office"] != "H") |
        candidates.apply(
            lambda row: _is_valid_house_district(row["state_code"], row["district_code"]),
            axis=1,
        )
    ].copy()
    display_candidates = _displayable_candidates(candidates)
    candidates["dem_tech_itemized_receipts"] = candidates["tech_itemized_receipts"].where(
        candidates["party_dr"] == "D", 0.0
    )
    candidates["rep_tech_itemized_receipts"] = candidates["tech_itemized_receipts"].where(
        candidates["party_dr"] == "R", 0.0
    )

    state_summary = (
        candidates.groupby("state_code")
        .agg(
            candidate_count=("cand_id", "size"),
            major_party_candidate_count=("is_major_party", "sum"),
            house_candidate_count=("cand_office", lambda x: int((x == "H").sum())),
            senate_candidate_count=("cand_office", lambda x: int((x == "S").sum())),
            total_itemized_receipts=("total_itemized_receipts", "sum"),
            tech_itemized_receipts=("tech_itemized_receipts", "sum"),
            ie_support_total=("ie_support_total", "sum"),
            ie_oppose_total=("ie_oppose_total", "sum"),
            tech_funded_ie_support_total=("tech_funded_ie_support_total", "sum"),
            tech_funded_ie_oppose_total=("tech_funded_ie_oppose_total", "sum"),
            dem_tech_itemized_receipts=("dem_tech_itemized_receipts", "sum"),
            rep_tech_itemized_receipts=("rep_tech_itemized_receipts", "sum"),
        )
        .reset_index()
    )
    state_summary["ie_net_support"] = (
        state_summary["ie_support_total"] - state_summary["ie_oppose_total"]
    )
    state_summary["tech_funded_ie_net_support"] = (
        state_summary["tech_funded_ie_support_total"] -
        state_summary["tech_funded_ie_oppose_total"]
    )

    house_districts = (
        candidates[candidates["cand_office"] == "H"]
        .groupby("state_code")
        .agg(
            house_district_count=("district_code", "nunique"),
            major_party_house_district_count=(
                "district_code",
                lambda x: x[candidates.loc[x.index, "is_major_party"]].nunique(),
            ),
        )
        .reset_index()
    )
    senate_states = (
        candidates[candidates["cand_office"] == "S"][["state_code"]]
        .drop_duplicates()
        .assign(has_senate_race=1)
    )

    top_candidate = (
        display_candidates.sort_values(
            ["tech_itemized_receipts", "ie_support_total", "linked_committee_count", "cand_name"],
            ascending=[False, False, False, True],
        )
        .drop_duplicates(subset=["state_code"], keep="first")
        [["state_code", "cand_name", "party_dr", "tech_itemized_receipts"]]
        .rename(
            columns={
                "cand_name": "top_candidate_name",
                "party_dr": "top_candidate_party",
                "tech_itemized_receipts": "top_candidate_tech_itemized_receipts",
            }
        )
    )

    state_summary = state_summary.merge(house_districts, on="state_code", how="left")
    state_summary = state_summary.merge(senate_states, on="state_code", how="left")
    state_summary = state_summary.merge(top_candidate, on="state_code", how="left")
    state_summary["house_district_count"] = (
        state_summary["house_district_count"].fillna(0).astype(int)
    )
    state_summary["major_party_house_district_count"] = (
        state_summary["major_party_house_district_count"].fillna(0).astype(int)
    )
    state_summary["has_senate_race"] = state_summary["has_senate_race"].fillna(0).astype(int)
    state_summary = _assign_party_label(
        state_summary,
        "dem_tech_itemized_receipts",
        "rep_tech_itemized_receipts",
    )

    return state_summary.sort_values(
        ["tech_itemized_receipts", "house_district_count", "state_code"],
        ascending=[False, False, True],
    )


def build_candidate_house_district_summary(candidate_race: pd.DataFrame) -> pd.DataFrame:
    """Build one row per House district for candidate navigation."""
    house = candidate_race[candidate_race["cand_office"] == "H"].copy()
    house["state_code"] = house["cand_office_st"].map(_clean_code)
    house = house[house["state_code"] != ""].copy()
    house["district_code"] = house["cand_office_district"].map(_normalize_house_district)
    house = house[
        house.apply(
            lambda row: _is_valid_house_district(row["state_code"], row["district_code"]),
            axis=1,
        )
    ].copy()
    display_house = _displayable_candidates(house)
    house["district_sort"] = house["cand_office_district"].map(_district_sort_value)
    house["district_label"] = house["state_code"] + "-" + house["district_code"]
    house["dem_tech_itemized_receipts"] = house["tech_itemized_receipts"].where(
        house["party_dr"] == "D", 0.0
    )
    house["rep_tech_itemized_receipts"] = house["tech_itemized_receipts"].where(
        house["party_dr"] == "R", 0.0
    )

    district_summary = (
        house.groupby(["state_code", "district_code", "district_sort", "district_label"])
        .agg(
            candidate_count=("cand_id", "size"),
            major_party_candidate_count=("is_major_party", "sum"),
            total_itemized_receipts=("total_itemized_receipts", "sum"),
            tech_itemized_receipts=("tech_itemized_receipts", "sum"),
            tech_itemized_donors=("tech_itemized_donors", "sum"),
            ie_support_total=("ie_support_total", "sum"),
            ie_oppose_total=("ie_oppose_total", "sum"),
            tech_funded_ie_support_total=("tech_funded_ie_support_total", "sum"),
            tech_funded_ie_oppose_total=("tech_funded_ie_oppose_total", "sum"),
            dem_tech_itemized_receipts=("dem_tech_itemized_receipts", "sum"),
            rep_tech_itemized_receipts=("rep_tech_itemized_receipts", "sum"),
        )
        .reset_index()
    )
    district_summary["ie_net_support"] = (
        district_summary["ie_support_total"] - district_summary["ie_oppose_total"]
    )
    district_summary["tech_funded_ie_net_support"] = (
        district_summary["tech_funded_ie_support_total"] -
        district_summary["tech_funded_ie_oppose_total"]
    )

    ordered_house = display_house.sort_values(
        ["tech_itemized_receipts", "ie_support_total", "linked_committee_count", "cand_name"],
        ascending=[False, False, False, True],
    )
    top_candidate = (
        ordered_house.drop_duplicates(
            subset=["state_code", "district_code"], keep="first"
        )[["state_code", "district_code", "cand_name", "party_dr", "tech_itemized_receipts"]]
        .rename(
            columns={
                "cand_name": "top_candidate_name",
                "party_dr": "top_candidate_party",
                "tech_itemized_receipts": "top_candidate_tech_itemized_receipts",
            }
        )
    )
    dem_candidate = (
        ordered_house[ordered_house["party_dr"] == "D"]
        .drop_duplicates(subset=["state_code", "district_code"], keep="first")
        [["state_code", "district_code", "cand_name"]]
        .rename(columns={"cand_name": "dem_candidate_name"})
    )
    rep_candidate = (
        ordered_house[ordered_house["party_dr"] == "R"]
        .drop_duplicates(subset=["state_code", "district_code"], keep="first")
        [["state_code", "district_code", "cand_name"]]
        .rename(columns={"cand_name": "rep_candidate_name"})
    )
    other_candidate = (
        ordered_house[~ordered_house["party_dr"].isin(["D", "R"])]
        .groupby(["state_code", "district_code"])
        .agg(other_candidate_names=("cand_name", lambda x: "; ".join(sorted(x.unique()))))
        .reset_index()
    )

    district_summary = district_summary.merge(
        top_candidate, on=["state_code", "district_code"], how="left"
    )
    district_summary = district_summary.merge(
        dem_candidate, on=["state_code", "district_code"], how="left"
    )
    district_summary = district_summary.merge(
        rep_candidate, on=["state_code", "district_code"], how="left"
    )
    district_summary = district_summary.merge(
        other_candidate, on=["state_code", "district_code"], how="left"
    )
    district_summary["other_candidate_names"] = district_summary["other_candidate_names"].fillna("")
    district_summary = _assign_party_label(
        district_summary,
        "dem_tech_itemized_receipts",
        "rep_tech_itemized_receipts",
    )

    return district_summary.sort_values(
        ["state_code", "district_sort", "tech_itemized_receipts"],
        ascending=[True, True, False],
    )


def build_candidate_senate_summary(candidate_race: pd.DataFrame) -> pd.DataFrame:
    """Build one row per state Senate contest bucket for candidate navigation."""
    senate = candidate_race[candidate_race["cand_office"] == "S"].copy()
    senate["state_code"] = senate["cand_office_st"].map(_clean_code)
    senate = senate[senate["state_code"] != ""].copy()
    display_senate = _displayable_candidates(senate)
    senate["dem_tech_itemized_receipts"] = senate["tech_itemized_receipts"].where(
        senate["party_dr"] == "D", 0.0
    )
    senate["rep_tech_itemized_receipts"] = senate["tech_itemized_receipts"].where(
        senate["party_dr"] == "R", 0.0
    )

    senate_summary = (
        senate.groupby("state_code")
        .agg(
            candidate_count=("cand_id", "size"),
            major_party_candidate_count=("is_major_party", "sum"),
            total_itemized_receipts=("total_itemized_receipts", "sum"),
            tech_itemized_receipts=("tech_itemized_receipts", "sum"),
            tech_itemized_donors=("tech_itemized_donors", "sum"),
            ie_support_total=("ie_support_total", "sum"),
            ie_oppose_total=("ie_oppose_total", "sum"),
            tech_funded_ie_support_total=("tech_funded_ie_support_total", "sum"),
            tech_funded_ie_oppose_total=("tech_funded_ie_oppose_total", "sum"),
            dem_tech_itemized_receipts=("dem_tech_itemized_receipts", "sum"),
            rep_tech_itemized_receipts=("rep_tech_itemized_receipts", "sum"),
        )
        .reset_index()
    )
    senate_summary["ie_net_support"] = (
        senate_summary["ie_support_total"] - senate_summary["ie_oppose_total"]
    )
    senate_summary["tech_funded_ie_net_support"] = (
        senate_summary["tech_funded_ie_support_total"] -
        senate_summary["tech_funded_ie_oppose_total"]
    )

    ordered_senate = display_senate.sort_values(
        ["tech_itemized_receipts", "ie_support_total", "linked_committee_count", "cand_name"],
        ascending=[False, False, False, True],
    )
    top_candidate = (
        ordered_senate.drop_duplicates(subset=["state_code"], keep="first")
        [["state_code", "cand_name", "party_dr", "tech_itemized_receipts"]]
        .rename(
            columns={
                "cand_name": "top_candidate_name",
                "party_dr": "top_candidate_party",
                "tech_itemized_receipts": "top_candidate_tech_itemized_receipts",
            }
        )
    )
    dem_candidate = (
        ordered_senate[ordered_senate["party_dr"] == "D"]
        .drop_duplicates(subset=["state_code"], keep="first")
        [["state_code", "cand_name"]]
        .rename(columns={"cand_name": "dem_candidate_name"})
    )
    rep_candidate = (
        ordered_senate[ordered_senate["party_dr"] == "R"]
        .drop_duplicates(subset=["state_code"], keep="first")
        [["state_code", "cand_name"]]
        .rename(columns={"cand_name": "rep_candidate_name"})
    )
    other_candidate = (
        ordered_senate[~ordered_senate["party_dr"].isin(["D", "R"])]
        .groupby("state_code")
        .agg(other_candidate_names=("cand_name", lambda x: "; ".join(sorted(x.unique()))))
        .reset_index()
    )

    senate_summary = senate_summary.merge(top_candidate, on="state_code", how="left")
    senate_summary = senate_summary.merge(dem_candidate, on="state_code", how="left")
    senate_summary = senate_summary.merge(rep_candidate, on="state_code", how="left")
    senate_summary = senate_summary.merge(other_candidate, on="state_code", how="left")
    senate_summary["other_candidate_names"] = senate_summary["other_candidate_names"].fillna("")
    senate_summary = _assign_party_label(
        senate_summary,
        "dem_tech_itemized_receipts",
        "rep_tech_itemized_receipts",
    )

    return senate_summary.sort_values(
        ["tech_itemized_receipts", "state_code"],
        ascending=[False, True],
    )


def build_committee_tech_receipts(
    tagged: pd.DataFrame,
    committees: pd.DataFrame,
    committee_party_classification: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """For each committee, compute how much of their receipts came from tech.

    This identifies 'tech-dominated' committees — super PACs, PACs, etc.
    where tech money is a large share of total receipts.
    """
    # Total receipts per committee from all donors
    all_receipts = (
        tagged.groupby("cmte_id")
        .agg(
            total_receipts=("net_amt", "sum"),
            total_donors=("name", "nunique"),
            total_contributions=("net_amt", "size"),
        )
        .reset_index()
    )

    # Tech receipts per committee
    tech = tagged[tagged["is_tech_employer"]]
    tech_receipts = (
        tech.groupby("cmte_id")
        .agg(
            tech_receipts=("net_amt", "sum"),
            tech_donors=("name", "nunique"),
            tech_contributions=("net_amt", "size"),
            tech_companies=("tech_canonical_name",
                            lambda x: "; ".join(sorted(x.unique()))),
        )
        .reset_index()
    )

    # Merge
    result = all_receipts.merge(tech_receipts, on="cmte_id", how="left")
    result["tech_receipts"] = result["tech_receipts"].fillna(0)
    result["tech_donors"] = result["tech_donors"].fillna(0).astype(int)
    result["tech_contributions"] = result["tech_contributions"].fillna(0).astype(int)
    result["tech_companies"] = result["tech_companies"].fillna("")

    # Compute tech share
    result["tech_pct"] = (
        result["tech_receipts"] /
        result["total_receipts"].replace(0, float("nan"))
        * 100
    )

    # Join committee metadata
    result = result.merge(
        committees[["cmte_id", "cmte_nm", "cmte_tp", "cmte_dsgn",
                     "cmte_pty_affiliation", "connected_org_nm", "cand_id"]],
        on="cmte_id", how="left",
    )
    if committee_party_classification is not None:
        result = result.merge(
            committee_party_classification[
                [
                    "cmte_id",
                    "party_dr",
                    "classification_source",
                    "evidence_sources",
                    "dem_evidence_amt",
                    "rep_evidence_amt",
                    "evidence_total",
                    "evidence_pct_dem",
                ]
            ],
            on="cmte_id",
            how="left",
        )

    result = result.sort_values("tech_receipts", ascending=False)

    # Summary stats
    tech_dominated = result[result["tech_pct"] > 50]
    print(f"  Committee tech receipts: {(result['tech_receipts'] > 0).sum():,} "
          f"committees received tech money")
    print(f"  Tech-dominated (>50%): {len(tech_dominated)} committees, "
          f"${tech_dominated['tech_receipts'].sum():,.0f}")

    return result


def build_committee_outbound_spending(
    spending: pd.DataFrame,
    committee_tech_receipts: pd.DataFrame,
) -> pd.DataFrame:
    """Where do tech-funded committees spend their money?

    Filters committee_spending to committees that received tech money,
    and attaches the tech-receipts data so we know how 'tech-funded'
    each spending committee is.
    """
    # Committees that received any tech money
    tech_cmtes = committee_tech_receipts[
        committee_tech_receipts["tech_receipts"] > 0
    ][["cmte_id", "tech_receipts", "total_receipts", "tech_pct",
       "tech_companies"]].copy()

    # Filter spending to those committees
    result = spending.merge(
        tech_cmtes, on="cmte_id", how="inner",
    )

    print(f"  Outbound spending from tech-funded committees: "
          f"{len(result):,} rows, ${result['net_amt'].sum():,.0f}")

    return result


def build_sankey_edges(
    tagged: pd.DataFrame,
    committee_spending: pd.DataFrame,
    committee_tech_receipts: pd.DataFrame,
) -> pd.DataFrame:
    """Build edges for a Sankey diagram: donors -> committees -> races.

    Left edges: tech donors -> committees (aggregated by donor x committee)
    Right edges: committees -> candidates/races (aggregated by committee x candidate)
    """
    # LEFT EDGES: tech donors -> committees
    tech = tagged[tagged["is_tech_employer"]].copy()

    left = (
        tech.groupby(["name", "tech_canonical_name", "cmte_id", "cmte_nm", "cmte_tp"])
        .agg(amount=("net_amt", "sum"))
        .reset_index()
        .rename(columns={
            "name": "source",
            "tech_canonical_name": "source_company",
            "cmte_nm": "target",
            "cmte_tp": "target_type",
        })
    )
    left["edge_type"] = "donor_to_committee"
    left["source_type"] = "donor"

    # RIGHT EDGES: committees -> candidates/races
    # Only from committees that received tech money
    tech_cmte_ids = set(
        committee_tech_receipts[
            committee_tech_receipts["tech_receipts"] > 0
        ]["cmte_id"]
    )
    tech_spending = committee_spending[
        committee_spending["cmte_id"].isin(tech_cmte_ids)
    ].copy()

    right = (
        tech_spending.groupby([
            "cmte_id", "filer_cmte_nm", "filer_cmte_tp",
            "name", "transaction_tp",
        ])
        .agg(amount=("net_amt", "sum"))
        .reset_index()
        .rename(columns={
            "filer_cmte_nm": "source",
            "filer_cmte_tp": "source_type",
            "name": "target",
        })
    )
    right["edge_type"] = "committee_to_candidate"
    right["source_company"] = ""
    right["target_type"] = right["transaction_tp"]

    # Combine
    cols = ["source", "source_type", "source_company",
            "target", "target_type", "edge_type", "amount"]
    edges = pd.concat([left[cols], right[cols]], ignore_index=True)

    n_left = len(left)
    n_right = len(right)
    print(f"  Sankey edges: {n_left:,} donor->committee, "
          f"{n_right:,} committee->candidate")

    return edges


def main(cycle: int = 2024):
    """Build all summary tables and save to disk."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load filtered data
    data = load_cycle(cycle)

    # Tag tech donors
    print("\nTagging tech donors...")
    tagged = tag_tech_donors(data.donor_contributions, data.tech_employers)

    print("\nClassifying partisan lean...")
    cand_party = load_candidate_parties(data.committees, cycle)
    committee_party_classification = classify_committees_from_party_field(data.committees)
    committee_party_classification = classify_ie_committees(
        committee_party_classification,
        cycle,
        cand_party,
    )
    donor_party_classification = classify_donors(tagged, committee_party_classification)

    # Build summaries
    print("\nBuilding summaries...")

    donor_summary = build_tech_donor_summary(tagged, donor_party_classification)
    company_summary = build_tech_company_summary(tagged, committee_party_classification)
    cmte_tech = build_committee_tech_receipts(
        tagged,
        data.committees,
        committee_party_classification,
    )
    outbound = build_committee_outbound_spending(data.committee_spending, cmte_tech)
    sankey = build_sankey_edges(tagged, data.committee_spending, cmte_tech)
    entity_lean = build_entity_party_lean(
        cycle,
        committee_party_classification,
        donor_summary,
        company_summary,
    )
    candidate_race = build_candidate_race_summary(
        cycle,
        tagged,
        data.committee_spending,
        cmte_tech,
        data.committees,
    )
    candidate_state = build_candidate_state_summary(candidate_race)
    candidate_house_district = build_candidate_house_district_summary(candidate_race)
    candidate_senate = build_candidate_senate_summary(candidate_race)

    # Save as CSV (pyarrow not available for parquet)
    print("\nSaving...")
    donor_summary.to_csv(OUT_DIR / "tech_donor_summary.csv", index=False)
    company_summary.to_csv(OUT_DIR / "tech_company_summary.csv", index=False)
    cmte_tech.to_csv(OUT_DIR / "committee_tech_receipts.csv", index=False)
    outbound.to_csv(OUT_DIR / "committee_outbound_spending.csv", index=False)
    sankey.to_csv(OUT_DIR / "tech_sankey_edges.csv", index=False)
    entity_lean.to_csv(OUT_DIR / "entity_party_lean.csv", index=False)
    candidate_race.to_csv(OUT_DIR / "candidate_race_summary.csv", index=False)
    candidate_state.to_csv(OUT_DIR / "candidate_state_summary.csv", index=False)
    candidate_house_district.to_csv(
        OUT_DIR / "candidate_house_district_summary.csv", index=False
    )
    candidate_senate.to_csv(OUT_DIR / "candidate_senate_summary.csv", index=False)

    print(f"\nAll tables saved to {OUT_DIR}/")

    # Print headline numbers
    print("\n" + "=" * 60)
    print(f"HEADLINE NUMBERS — {cycle} CYCLE")
    print("=" * 60)
    print(f"  Total tech-linked contributions: ${tagged.loc[tagged['is_tech_employer'], 'net_amt'].sum():,.0f}")
    print(f"  Tech donors: {donor_summary['name'].nunique():,}")
    print(f"  Tech companies tracked: {company_summary['tech_canonical_name'].nunique()}")
    print(f"  Committees receiving tech money: {(cmte_tech['tech_receipts'] > 0).sum():,}")
    print(f"  Tech-dominated committees (>50%): {(cmte_tech['tech_pct'] > 50).sum()}")
    print()
    print("Top 10 tech companies by employee giving:")
    print(company_summary.head(10)[
        ["tech_canonical_name", "net_total", "n_donors", "pct_dem"]
    ].to_string(index=False))
    print()
    print("Top 10 tech-receiving committees:")
    print(cmte_tech.head(10)[
        ["cmte_nm", "cmte_tp", "tech_receipts", "tech_pct", "tech_companies"]
    ].to_string(index=False))


if __name__ == "__main__":
    main(2024)
