"""
Classify committees and donors as D/R/Mixed based on observed behavior.

Committee classification:
  1. Candidate committees (H, S, P) and party committees (X, Y) — use party field
  2. PACs / Super PACs / IE committees — infer from candidate-facing activity:
     - 24E / 24A independent expenditures
     - 24K contributions to candidates

Donor classification:
  Based on where they give. If >=70% of classified dollars go to D committees,
  donor is D. Same for R. Otherwise Mixed.

Usage:
    python -m pipeline.classify_partisan
"""

from pathlib import Path

import pandas as pd

from pipeline.common.paths import FEC_INTERIM_ROOT, fec_cycle_derived_dir
from pipeline.fec.load import CM_COLS, ITCONT_COLS, load_cycle, tag_tech_donors


PARTY_MAP = {"DEM": "D", "REP": "R"}
PARTISAN_LEAN_THRESHOLD = 0.70
CN_COLS = [
    "cand_id", "cand_name", "cand_pty_affiliation", "cand_election_yr",
    "cand_office_st", "cand_office", "cand_office_district", "cand_ici",
    "cand_status", "cand_pcc", "cand_st1", "cand_st2", "cand_city",
    "cand_st", "cand_zip",
]
CCL_COLS = [
    "cand_id", "cand_election_yr", "fec_election_yr", "cmte_id",
    "cmte_tp", "cmte_dsgn", "linkage_id",
]


def _candidate_master_path(cycle: int) -> Path:
    suffix = str(cycle)[2:]
    return FEC_INTERIM_ROOT / str(cycle) / f"cn{suffix}" / "cn.txt"


def _candidate_linkage_path(cycle: int) -> Path:
    suffix = str(cycle)[2:]
    return FEC_INTERIM_ROOT / str(cycle) / f"ccl{suffix}" / "ccl.txt"


def load_candidate_linkage_table(cycle: int) -> pd.DataFrame:
    """Load the FEC candidate-committee linkage table for a cycle."""
    ccl_path = _candidate_linkage_path(cycle)
    if not ccl_path.exists():
        raise FileNotFoundError(
            f"Required FEC candidate-committee linkage file missing: {ccl_path}. "
            "Download ccl.txt for this cycle before running partisan classification."
        )

    return pd.read_csv(
        ccl_path,
        sep="|",
        header=None,
        names=CCL_COLS,
        dtype="string",
        na_filter=False,
    )


def load_candidate_master_table(cycle: int) -> pd.DataFrame:
    """Load the full FEC candidate master table for a cycle."""
    cn_path = _candidate_master_path(cycle)
    if not cn_path.exists():
        raise FileNotFoundError(
            f"Required FEC candidate master file missing: {cn_path}. "
            "Download cn.txt for this cycle before running partisan classification."
        )

    return pd.read_csv(
        cn_path,
        sep="|",
        header=None,
        names=CN_COLS,
        dtype="string",
        na_filter=False,
    )


def load_candidate_party_table(cm: pd.DataFrame, cycle: int) -> pd.DataFrame:
    """Build a candidate_id -> party table from the FEC candidate master."""
    cn = load_candidate_master_table(cycle)
    candidates = cn[["cand_id", "cand_name", "cand_pcc", "cand_pty_affiliation"]].copy()
    candidates = cn[cn["cand_election_yr"] == str(cycle)].copy()
    candidates = candidates[["cand_id", "cand_name", "cand_pcc", "cand_pty_affiliation"]]
    candidates["party_dr"] = candidates["cand_pty_affiliation"].map(PARTY_MAP)
    candidates["candidate_source"] = "candidate_master"
    candidates = candidates[candidates["party_dr"].notna()].copy()

    print(
        f"  Candidate parties: {len(candidates):,} candidates "
        f"({(candidates['party_dr'] == 'D').sum():,} D, "
        f"{(candidates['party_dr'] == 'R').sum():,} R)"
    )

    return candidates


def load_candidate_parties(cm: pd.DataFrame, cycle: int) -> dict:
    """Build a mapping of candidate_id -> D/R party."""
    candidates = load_candidate_party_table(cm, cycle)
    return candidates.set_index("cand_id")["party_dr"].to_dict()


def classify_committees_from_party_field(cm: pd.DataFrame) -> pd.DataFrame:
    """Classify committees that have a direct party affiliation."""
    result = cm[["cmte_id", "cmte_nm", "cmte_tp", "cmte_pty_affiliation",
                 "cand_id"]].copy()
    result["party_dr"] = result["cmte_pty_affiliation"].map(PARTY_MAP)
    result["classification_source"] = ""
    result["evidence_sources"] = ""
    result["dem_evidence_amt"] = 0.0
    result["rep_evidence_amt"] = 0.0
    result["evidence_total"] = 0.0
    result["evidence_pct_dem"] = pd.NA

    # Candidate committees and party committees with D/R party
    has_party = result["party_dr"].notna()
    is_cand_or_party = result["cmte_tp"].isin(["H", "S", "P", "X", "Y"])
    direct = has_party & is_cand_or_party
    result.loc[direct, "classification_source"] = "party_field"

    n_classified = direct.sum()
    print(f"  Direct party field: {n_classified:,} committees classified")

    return result


def _load_itpas2(cycle: int) -> pd.DataFrame:
    suffix = str(cycle)[2:]
    base = FEC_INTERIM_ROOT / str(cycle)
    itpas2 = pd.read_csv(
        base / f"pas2{suffix}" / "itpas2.txt",
        sep="|", header=None, names=[
            "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
            "transaction_tp", "entity_tp", "name", "city", "state",
            "zip_code", "employer", "occupation", "transaction_dt",
            "transaction_amt", "other_id", "cand_id", "tran_id", "file_num",
            "memo_cd", "memo_text", "sub_id",
        ],
        dtype="string", na_filter=False,
    )
    itpas2["transaction_amt"] = pd.to_numeric(
        itpas2["transaction_amt"], errors="coerce"
    ).fillna(0.0)
    return itpas2


def build_behavioral_committee_classification(
    cycle: int,
    cand_party: dict,
) -> pd.DataFrame:
    """Build committee-level partisan evidence from candidate-facing activity."""
    suffix = str(cycle)[2:]
    base = FEC_INTERIM_ROOT / str(cycle)
    ccl = load_candidate_linkage_table(cycle)
    cmte_to_cand = (
        ccl.loc[ccl["cand_id"] != "", ["cmte_id", "cand_id"]]
        .drop_duplicates(subset=["cmte_id"])
        .set_index("cmte_id")["cand_id"]
        .to_dict()
    )

    print("  Loading itoth for IE classification...")
    itoth = pd.read_csv(
        base / f"oth{suffix}" / "itoth.txt",
        sep="|", header=None, names=ITCONT_COLS,
        dtype="string", na_filter=False,
    )
    itoth["transaction_amt"] = pd.to_numeric(
        itoth["transaction_amt"], errors="coerce"
    ).fillna(0.0)

    # 24E = support, 24A = oppose
    ies = itoth[itoth["transaction_tp"].isin(["24E", "24A"])].copy()
    ies["target_cand_id"] = ies["other_id"].map(cmte_to_cand)
    ies["target_cand_id"] = ies["target_cand_id"].where(
        ies["target_cand_id"].notna() & (ies["target_cand_id"] != ""),
        ies["other_id"],
    )

    # Look up candidate party
    ies["cand_party"] = ies["target_cand_id"].map(cand_party)

    # Determine the partisan effect of each IE
    # 24E (support) + D candidate = pro-D
    # 24E (support) + R candidate = pro-R
    # 24A (oppose) + D candidate = pro-R (opposing a D helps R)
    # 24A (oppose) + R candidate = pro-D (opposing an R helps D)
    conditions = [
        (ies["transaction_tp"] == "24E") & (ies["cand_party"] == "D"),
        (ies["transaction_tp"] == "24E") & (ies["cand_party"] == "R"),
        (ies["transaction_tp"] == "24A") & (ies["cand_party"] == "D"),
        (ies["transaction_tp"] == "24A") & (ies["cand_party"] == "R"),
    ]
    import numpy as np
    choices = ["D", "R", "R", "D"]
    ies["partisan_effect"] = np.select(conditions, choices, default="")

    # Drop rows where we couldn't determine partisan effect
    ies_classified = ies[ies["partisan_effect"] != ""].copy()

    print(f"  IE rows with partisan classification: {len(ies_classified):,} "
          f"of {len(ies):,}")

    cmte_ie = (
        ies_classified.groupby(["cmte_id", "partisan_effect"])
        .agg(ie_amt=("transaction_amt", "sum"))
        .reset_index()
        .pivot(index="cmte_id", columns="partisan_effect", values="ie_amt")
        .fillna(0)
        .reset_index()
    )
    for col in ["D", "R"]:
        if col not in cmte_ie.columns:
            cmte_ie[col] = 0.0
    cmte_ie = cmte_ie.rename(columns={"D": "dem_evidence_amt", "R": "rep_evidence_amt"})
    cmte_ie["evidence_sources"] = "ie_spending"

    # Also load itpas2 for 24K contribution patterns
    print("  Loading itpas2 for 24K classification...")
    itpas2 = _load_itpas2(cycle)

    # 24K contributions to candidates
    k24 = itpas2[
        (itpas2["transaction_tp"] == "24K") &
        (itpas2["memo_cd"] != "X")
    ].copy()
    k24["cand_party"] = k24["cand_id"].map(cand_party)
    k24_classified = k24[k24["cand_party"].notna()].copy()

    cmte_24k = (
        k24_classified.groupby(["cmte_id", "cand_party"])
        .agg(k24_amt=("transaction_amt", "sum"))
        .reset_index()
        .pivot(index="cmte_id", columns="cand_party", values="k24_amt")
        .fillna(0)
        .reset_index()
    )

    for col in ["D", "R"]:
        if col not in cmte_24k.columns:
            cmte_24k[col] = 0.0
    cmte_24k = cmte_24k.rename(columns={"D": "dem_evidence_amt", "R": "rep_evidence_amt"})
    cmte_24k["evidence_sources"] = "24k_contributions"

    evidence = pd.concat(
        [
            cmte_ie[["cmte_id", "dem_evidence_amt", "rep_evidence_amt", "evidence_sources"]],
            cmte_24k[["cmte_id", "dem_evidence_amt", "rep_evidence_amt", "evidence_sources"]],
        ],
        ignore_index=True,
    )
    if len(evidence) == 0:
        return pd.DataFrame(
            columns=[
                "cmte_id", "dem_evidence_amt", "rep_evidence_amt", "evidence_total",
                "evidence_pct_dem", "behavioral_party", "evidence_sources",
            ]
        )

    behavioral = (
        evidence.groupby("cmte_id")
        .agg(
            dem_evidence_amt=("dem_evidence_amt", "sum"),
            rep_evidence_amt=("rep_evidence_amt", "sum"),
            evidence_sources=("evidence_sources", lambda x: "+".join(sorted(set(x)))),
        )
        .reset_index()
    )
    behavioral["evidence_total"] = (
        behavioral["dem_evidence_amt"] + behavioral["rep_evidence_amt"]
    )
    behavioral["evidence_pct_dem"] = (
        behavioral["dem_evidence_amt"] /
        behavioral["evidence_total"].replace(0, float("nan"))
    )
    behavioral["evidence_pct_rep"] = (
        behavioral["rep_evidence_amt"] /
        behavioral["evidence_total"].replace(0, float("nan"))
    )
    behavioral["behavioral_party"] = "Mixed"
    behavioral.loc[
        behavioral["evidence_pct_dem"] >= PARTISAN_LEAN_THRESHOLD,
        "behavioral_party",
    ] = "D"
    behavioral.loc[
        behavioral["evidence_pct_rep"] >= PARTISAN_LEAN_THRESHOLD,
        "behavioral_party",
    ] = "R"

    print(
        f"  Behavioral committee evidence: {len(behavioral):,} committees "
        f"({(behavioral['behavioral_party'] == 'D').sum():,} D, "
        f"{(behavioral['behavioral_party'] == 'R').sum():,} R, "
        f"{(behavioral['behavioral_party'] == 'Mixed').sum():,} Mixed)"
    )

    return behavioral


def classify_ie_committees(
    result: pd.DataFrame,
    cycle: int,
    cand_party: dict,
) -> pd.DataFrame:
    """Attach behavioral committee classification to committees."""
    behavioral = build_behavioral_committee_classification(cycle, cand_party)

    result = result.merge(
        behavioral,
        on="cmte_id",
        how="left",
        suffixes=("", "_behavior"),
    )
    for col in ["dem_evidence_amt", "rep_evidence_amt", "evidence_total"]:
        behavior_col = f"{col}_behavior"
        result[col] = result[behavior_col].fillna(result[col]).fillna(0.0)
    result["evidence_pct_dem"] = result["evidence_pct_dem_behavior"].fillna(
        result["evidence_pct_dem"]
    )
    result["evidence_sources"] = result["evidence_sources_behavior"].fillna(
        result["evidence_sources"]
    )

    unclassified = result["classification_source"] == ""
    has_behavior = result["behavioral_party"].notna()
    result.loc[unclassified & has_behavior, "party_dr"] = result.loc[
        unclassified & has_behavior, "behavioral_party"
    ]
    result.loc[unclassified & has_behavior, "classification_source"] = result.loc[
        unclassified & has_behavior, "evidence_sources"
    ]
    result["evidence_sources"] = result["evidence_sources"].fillna("")

    n_behavioral = (result["classification_source"] == result["evidence_sources"]) & (
        result["classification_source"] != ""
    )
    print(f"  Newly classified from behavioral evidence: {n_behavioral.sum():,} committees")

    result = result.drop(
        columns=[
            "behavioral_party",
            "dem_evidence_amt_behavior",
            "rep_evidence_amt_behavior",
            "evidence_total_behavior",
            "evidence_pct_dem_behavior",
            "evidence_sources_behavior",
        ]
    )
    return result


def classify_donors(
    tagged: pd.DataFrame,
    cmte_classification: pd.DataFrame,
) -> pd.DataFrame:
    """Classify each donor as D/R/Mixed based on where they give.

    A donor is D if >=70% of their dollars go to D-classified committees.
    Same for R. Otherwise Mixed.
    """
    cmte_lookup = cmte_classification.set_index("cmte_id")["party_dr"].to_dict()
    tagged = tagged.copy()
    tagged["cmte_party"] = tagged["cmte_id"].map(cmte_lookup)
    tagged["cmte_party_bucket"] = tagged["cmte_party"].fillna("Unknown")

    donor_split = (
        tagged.groupby(["name", "cmte_party_bucket"])
        .agg(party_amt=("net_amt", "sum"))
        .reset_index()
        .pivot(index="name", columns="cmte_party_bucket", values="party_amt")
        .fillna(0)
        .reset_index()
    )

    for col in ["D", "R", "Mixed", "Unknown"]:
        if col not in donor_split.columns:
            donor_split[col] = 0.0

    donor_split["classified_total"] = donor_split["D"] + donor_split["R"]
    donor_split["overall_total"] = (
        donor_split["classified_total"] + donor_split["Mixed"] + donor_split["Unknown"]
    )
    donor_split["pct_d"] = (
        donor_split["D"] /
        donor_split["classified_total"].replace(0, float("nan"))
    )
    donor_split["pct_classified"] = (
        donor_split["classified_total"] /
        donor_split["overall_total"].replace(0, float("nan"))
    )

    donor_split["pct_r"] = (
        donor_split["R"] /
        donor_split["classified_total"].replace(0, float("nan"))
    )

    donor_split["donor_party"] = "Mixed"
    donor_split.loc[
        donor_split["pct_d"] >= PARTISAN_LEAN_THRESHOLD,
        "donor_party",
    ] = "D"
    donor_split.loc[
        donor_split["pct_r"] >= PARTISAN_LEAN_THRESHOLD,
        "donor_party",
    ] = "R"
    donor_split.loc[donor_split["classified_total"] <= 0, "donor_party"] = "Unknown"

    all_donors = pd.DataFrame({"name": tagged["name"].unique()})
    donor_result = all_donors.merge(
        donor_split[
            [
                "name", "D", "R", "Mixed", "Unknown", "overall_total",
                "classified_total", "pct_d", "pct_r", "pct_classified", "donor_party",
            ]
        ],
        on="name", how="left",
    )
    donor_result["donor_party"] = donor_result["donor_party"].fillna("Unknown")

    print(f"\n  Donor classification:")
    print(f"    D: {(donor_result['donor_party'] == 'D').sum():,}")
    print(f"    R: {(donor_result['donor_party'] == 'R').sum():,}")
    print(f"    Mixed: {(donor_result['donor_party'] == 'Mixed').sum():,}")
    print(f"    Unknown: {(donor_result['donor_party'] == 'Unknown').sum():,}")

    return donor_result


def main(cycle: int = 2024):
    out_dir = fec_cycle_derived_dir(cycle)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = load_cycle(cycle)

    print("\nClassifying committees...")
    cand_party = load_candidate_parties(data.committees, cycle)
    cmte_class = classify_committees_from_party_field(data.committees)
    cmte_class = classify_ie_committees(cmte_class, cycle, cand_party)

    # Summary
    classified = cmte_class[cmte_class["classification_source"] != ""]
    print(f"\n  Total classified: {len(classified):,} of {len(cmte_class):,}")
    print(f"  By source:")
    print(classified["classification_source"].value_counts().to_string())

    # Save committee classification
    cmte_class.to_csv(out_dir / "committee_party_classification.csv", index=False)

    # Tag tech donors and classify them
    print("\nTagging tech donors...")
    tagged = tag_tech_donors(data.donor_contributions, data.tech_employers)

    print("\nClassifying donors...")
    donor_class = classify_donors(tagged, cmte_class)

    # Save donor classification
    donor_class.to_csv(out_dir / "donor_party_classification.csv", index=False)

    # Merge donor classification with tech donor summary
    tech_donors = tagged[tagged["is_tech_employer"]].copy()
    tech_donor_parties = tech_donors.merge(
        donor_class[["name", "donor_party", "pct_d", "classified_total"]],
        on="name", how="left",
    )

    # Tech company summary with proper partisan breakdown
    print("\n" + "=" * 60)
    print("TECH COMPANY PARTISAN BREAKDOWN (by donor classification)")
    print("=" * 60)

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

    # Flatten column names
    company_pivot.columns = [
        f"{stat}_{party}" for stat, party in company_pivot.columns
    ]
    company_pivot = company_pivot.reset_index()

    # Compute total and pct
    for stat in ["donor_amt", "n_donors"]:
        cols = [c for c in company_pivot.columns if c.startswith(stat)]
        company_pivot[f"{stat}_total"] = company_pivot[cols].sum(axis=1)

    if "donor_amt_D" in company_pivot.columns and "donor_amt_R" in company_pivot.columns:
        dr_total = company_pivot["donor_amt_D"] + company_pivot["donor_amt_R"]
        company_pivot["pct_dem_by_donor"] = (
            company_pivot["donor_amt_D"] / dr_total.replace(0, float("nan")) * 100
        )

    company_pivot = company_pivot.sort_values("donor_amt_total", ascending=False)

    print(company_pivot.head(20).to_string(index=False))

    company_pivot.to_csv(out_dir / "tech_company_partisan.csv", index=False)

    # Spot-check known donors
    print("\n" + "=" * 60)
    print("SPOT CHECK — KNOWN DONOR CLASSIFICATIONS")
    print("=" * 60)
    for pattern in ["MUSK, ELON", "ANDREESSEN, MARC", "HOROWITZ, BEN",
                    "HOFFMAN, REID", "THIEL, PETER"]:
        rows = donor_class[donor_class["name"].str.contains(pattern, na=False)]
        if len(rows) > 0:
            for _, r in rows.iterrows():
                pct = f"{r['pct_d']*100:.0f}% D" if pd.notna(r["pct_d"]) else "n/a"
                print(f"  {r['name']}: {r['donor_party']} ({pct}, "
                      f"${r['classified_total']:,.0f} classified)")
        else:
            print(f"  {pattern}: not found")

    print(f"\nSaved to {out_dir}/")


if __name__ == "__main__":
    main(2024)
