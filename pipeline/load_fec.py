"""
Core data loading and filtering module for tech money analysis.

Loads raw FEC bulk files, applies validated transaction type rules,
and exposes clean dataframes for downstream analysis.

Usage:
    from pipeline.load_fec import load_cycle

    data = load_cycle(2024)
    data.donor_contributions   # individual contributions, filtered and cleaned
    data.committee_spending    # committee-to-committee and independent expenditures
    data.committees            # committee directory
    data.tech_employers        # employer lookup table (from manual tagging)
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.paths import FEC_INTERIM_ROOT, tech_employer_lookup_path


# ── Paths ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"


# ── Column definitions ──────────────────────────────────────────────
ITCONT_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

ITPAS2_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "cand_id", "tran_id", "file_num", "memo_cd", "memo_text",
    "sub_id",
]

CM_COLS = [
    "cmte_id", "cmte_nm", "tres_nm", "cmte_st1", "cmte_st2",
    "cmte_city", "cmte_st", "cmte_zip", "cmte_dsgn", "cmte_tp",
    "cmte_pty_affiliation", "cmte_filing_freq", "org_tp",
    "connected_org_nm", "cand_id",
]


# ── Transaction type rules ──────────────────────────────────────────
# See transaction_type_observations.md for full rationale on each.

# Types that represent real money entering the political system from donors.
INCLUDE_TYPES_ITCONT = {
    "10",   # contribution to super PAC / IE committee
    "15",   # standard contribution
    "15E",  # earmarked contribution (via conduit)
    "15C",  # candidate self-funding
    "11",   # tribal/organizational contribution
    "30",   # convention account contribution
    "31",   # headquarters account contribution
    "32",   # legal/recount account contribution
    "30E",  # earmarked convention account
    "31E",  # earmarked headquarters account
    "32E",  # earmarked legal/recount account
    "30T",  # conduit forward to convention account
    "31T",  # conduit forward to headquarters account
    "32T",  # conduit forward to legal/recount account
    "42Y",  # convention account contribution (alternate code)
    "41Y",  # headquarters account contribution (alternate code)
}

# Refund types — these represent money leaving the system back to donors.
# Positive amounts = refund issued, so we subtract them.
REFUND_TYPES_ITCONT = {"22Y", "21Y"}

# Memo X handling: only exclude on types where memo X means routing/double-count.
# Type 10 memo X = real money (in-kind, trust attribution). $180M in 2024.
# Type 15E memo X = earmark memo on conduit filing — the 24T is the real row.
MEMO_X_EXCLUDE_TYPES = {"15E"}

# itoth types for committee spending analysis
INCLUDE_TYPES_ITOTH_SPENDING = {
    "24E",  # independent expenditure supporting candidate
    "24A",  # independent expenditure opposing candidate
    "24K",  # contribution to nonaffiliated committee (PAC -> candidate)
    "24C",  # coordinated party expenditure
    "24Z",  # in-kind contribution
}


@dataclass
class FECData:
    """Container for loaded and filtered FEC data."""
    cycle: int
    donor_contributions: pd.DataFrame
    committee_spending: pd.DataFrame
    committees: pd.DataFrame
    tech_employers: pd.DataFrame


def _load_raw(cycle: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load raw FEC bulk files for a given election cycle."""
    suffix = str(cycle)[2:]  # e.g. 2024 -> "24"
    base = FEC_INTERIM_ROOT / str(cycle)

    print(f"Loading {cycle} committee master...")
    cm = pd.read_csv(
        base / f"cm{suffix}" / "cm.txt",
        sep="|", header=None, names=CM_COLS,
        dtype="string", na_filter=False,
    )

    print(f"Loading {cycle} itcont...")
    itcont = pd.read_csv(
        base / f"indiv{suffix}" / "itcont.txt",
        sep="|", header=None, names=ITCONT_COLS,
        dtype="string", na_filter=False,
    )
    itcont["transaction_amt"] = pd.to_numeric(
        itcont["transaction_amt"], errors="coerce"
    ).fillna(0.0)

    print(f"Loading {cycle} itoth...")
    itoth = pd.read_csv(
        base / f"oth{suffix}" / "itoth.txt",
        sep="|", header=None, names=ITCONT_COLS,
        dtype="string", na_filter=False,
    )
    itoth["transaction_amt"] = pd.to_numeric(
        itoth["transaction_amt"], errors="coerce"
    ).fillna(0.0)

    print(f"  itcont: {len(itcont):,} rows")
    print(f"  itoth:  {len(itoth):,} rows")
    print(f"  cm:     {len(cm):,} rows")

    return itcont, itoth, cm


def _load_tech_employers() -> pd.DataFrame:
    """Load the manually tagged employer lookup table."""
    path = tech_employer_lookup_path()
    df = pd.read_csv(path, dtype="string", na_filter=False)

    # Fix truncated column name from Excel
    if "canonical_me" in df.columns and "canonical_name" not in df.columns:
        df = df.rename(columns={"canonical_me": "canonical_name"})

    # Keep only included rows
    df = df[df["include"].str.upper() == "TRUE"].copy()

    # Fill missing canonical_name from matched_searches
    mask = (df["canonical_name"] == "") | df["canonical_name"].isna()
    df.loc[mask, "canonical_name"] = df.loc[mask, "matched_searches"]

    # Normalize
    df["employer_upper"] = df["employer"].str.strip().str.upper()

    print(f"  Tech employers: {len(df)} strings across "
          f"{df['canonical_name'].nunique()} companies")

    return df[["employer", "employer_upper", "canonical_name", "sector",
               "matched_searches"]].copy()


def _filter_donor_contributions(
    itcont: pd.DataFrame, cm: pd.DataFrame
) -> pd.DataFrame:
    """Apply transaction type rules to itcont and produce clean contributions."""

    all_types = INCLUDE_TYPES_ITCONT | REFUND_TYPES_ITCONT

    # Filter to relevant transaction types
    df = itcont[itcont["transaction_tp"].isin(all_types)].copy()

    # Exclude memo X only on types where it means routing
    df = df[
        (df["memo_cd"] != "X") | (~df["transaction_tp"].isin(MEMO_X_EXCLUDE_TYPES))
    ]

    # Compute net amount: refund rows get sign flipped
    # 22Y/21Y positive = refund issued = money left system -> subtract
    df["is_refund"] = df["transaction_tp"].isin(REFUND_TYPES_ITCONT)
    df["net_amt"] = df["transaction_amt"].where(
        ~df["is_refund"], -df["transaction_amt"]
    )

    # Join committee metadata
    df = df.merge(
        cm[["cmte_id", "cmte_nm", "cmte_tp", "cmte_dsgn",
            "cmte_pty_affiliation", "connected_org_nm", "cand_id"]],
        on="cmte_id", how="left",
    )

    return df


def _filter_committee_spending(
    itoth: pd.DataFrame, cm: pd.DataFrame
) -> pd.DataFrame:
    """Filter itoth to outbound committee spending (IEs, contributions)."""

    df = itoth[itoth["transaction_tp"].isin(INCLUDE_TYPES_ITOTH_SPENDING)].copy()

    # For spending, exclude memo X across the board (these are sub-line-item memos)
    df = df[df["memo_cd"] != "X"]

    # Net amount (spending is positive = money out)
    df["net_amt"] = df["transaction_amt"]

    # Join filer committee metadata
    df = df.merge(
        cm[["cmte_id", "cmte_nm", "cmte_tp", "cmte_dsgn",
            "cmte_pty_affiliation", "connected_org_nm", "cand_id"]].rename(
            columns={
                "cmte_nm": "filer_cmte_nm",
                "cmte_tp": "filer_cmte_tp",
                "cmte_dsgn": "filer_cmte_dsgn",
                "cmte_pty_affiliation": "filer_party",
                "connected_org_nm": "filer_connected_org",
                "cand_id": "filer_cand_id",
            }
        ),
        on="cmte_id", how="left",
    )

    return df


def load_cycle(cycle: int = 2024) -> FECData:
    """Load and filter FEC data for a given election cycle.

    Returns an FECData object with:
        - donor_contributions: filtered itcont with net amounts
        - committee_spending: filtered itoth (IEs, contributions out)
        - committees: full committee directory
        - tech_employers: manual employer lookup table
    """
    itcont, itoth, cm = _load_raw(cycle)
    tech_employers = _load_tech_employers()

    print("Filtering donor contributions...")
    donor_contributions = _filter_donor_contributions(itcont, cm)
    print(f"  {len(donor_contributions):,} contribution rows after filtering")

    print("Filtering committee spending...")
    committee_spending = _filter_committee_spending(itoth, cm)
    print(f"  {len(committee_spending):,} spending rows after filtering")

    return FECData(
        cycle=cycle,
        donor_contributions=donor_contributions,
        committee_spending=committee_spending,
        committees=cm,
        tech_employers=tech_employers,
    )


def tag_tech_donors(
    contributions: pd.DataFrame,
    tech_employers: pd.DataFrame,
) -> pd.DataFrame:
    """Add tech employer tags to a contributions dataframe.

    Adds columns: is_tech_employer, tech_canonical_name, tech_sector.
    """
    df = contributions.copy()
    df["employer_upper"] = df["employer"].str.strip().str.upper()

    # Join on exact employer string match
    tech_lookup = tech_employers[
        ["employer_upper", "canonical_name", "sector"]
    ].drop_duplicates(subset=["employer_upper"])

    df = df.merge(
        tech_lookup.rename(columns={
            "canonical_name": "tech_canonical_name",
            "sector": "tech_sector",
        }),
        on="employer_upper", how="left",
    )

    df["is_tech_employer"] = df["tech_canonical_name"].notna() & (
        df["tech_canonical_name"] != ""
    )

    df.drop(columns=["employer_upper"], inplace=True)

    n_tech = df["is_tech_employer"].sum()
    tech_total = df.loc[df["is_tech_employer"], "net_amt"].sum()
    print(f"  Tech-tagged: {n_tech:,} rows, ${tech_total:,.0f}")

    return df


if __name__ == "__main__":
    data = load_cycle(2024)

    print("\n" + "=" * 60)
    print("PIPELINE VALIDATION")
    print("=" * 60)

    dc = data.donor_contributions
    cs = data.committee_spending
    te = data.tech_employers

    print(f"\nDonor contributions: {len(dc):,} rows")
    print(f"  Net total: ${dc['net_amt'].sum():,.0f}")
    print(f"  By transaction type:")
    tt_summary = dc.groupby("transaction_tp").agg(
        rows=("net_amt", "size"),
        net_total=("net_amt", "sum"),
    ).reset_index().sort_values("net_total", ascending=False)
    print(tt_summary.to_string(index=False))

    print(f"\nCommittee spending: {len(cs):,} rows")
    print(f"  Net total: ${cs['net_amt'].sum():,.0f}")

    # Tag and check tech donors
    tagged = tag_tech_donors(dc, te)

    # Validate against known donors
    print("\n" + "=" * 60)
    print("KNOWN DONOR VALIDATION")
    print("=" * 60)

    for donor_pattern in ["MUSK, ELON", "ANDREESSEN, MARC", "HOROWITZ, BEN",
                          "GRIFFIN, KENNETH", "SOROS, GEORGE", "THIEL, PETER"]:
        rows = tagged[tagged["name"].str.contains(donor_pattern, na=False)]
        if len(rows) > 0:
            net = rows["net_amt"].sum()
            is_tech = rows["is_tech_employer"].any()
            tech_co = rows.loc[rows["is_tech_employer"], "tech_canonical_name"].unique()
            tech_str = ", ".join(tech_co) if len(tech_co) > 0 else "not matched"
            print(f"  {donor_pattern}: ${net:,.0f} ({len(rows)} rows) "
                  f"tech={is_tech} ({tech_str})")

    # Top tech companies by employee giving
    print("\n" + "=" * 60)
    print("TOP TECH COMPANIES BY EMPLOYEE GIVING")
    print("=" * 60)
    tech_rows = tagged[tagged["is_tech_employer"]]
    company_summary = tech_rows.groupby("tech_canonical_name").agg(
        net_total=("net_amt", "sum"),
        n_donors=("name", "nunique"),
        n_rows=("net_amt", "size"),
    ).reset_index().sort_values("net_total", ascending=False)
    print(company_summary.to_string(index=False))
