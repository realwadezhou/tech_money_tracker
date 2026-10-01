"""Rebuild only candidate CSVs after changes to candidate attribution.

Run after the full pipeline has produced committee_tech_receipts.csv from the
same FEC source snapshot. This command does not refresh other exports or source
metadata. It reads narrow chunks of the large transaction files and keeps only
candidate-linked receipts and independent-expenditure rows.

Usage:
    python -m pipeline.rebuild_candidate_exports 2024 2026
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pipeline.fec.transaction_reviews import SOURCE_FIELDS, ReviewedExclusionDependencies, reviewed_exclusion_mask

from pipeline.build_summaries import (
    build_candidate_house_district_summary,
    build_candidate_race_summary,
    build_candidate_senate_summary,
    build_candidate_state_summary,
)
from pipeline.classify_partisan import (
    load_candidate_linkage_table,
    load_candidate_master_table,
)
from pipeline.common.paths import (
    fec_cycle_derived_dir,
    fec_cycle_interim_dir,
    site_export_cycle_dir,
)
from pipeline.fec.load import (
    CM_COLS,
    INCLUDE_TYPES_ITCONT,
    ITCONT_COLS,
    MEMO_X_EXCLUDE_TYPES,
    REFUND_TYPES_ITCONT,
    _load_tech_employers,
    validated_tech_lookup,
)


CONTRIBUTION_INPUT_COLUMNS = [
    *SOURCE_FIELDS,
]
CONTRIBUTION_COLUMNS = [
    "cmte_id", "name", "net_amt", "tech_canonical_name", "is_tech_employer", "memo_cd",
]
IE_INPUT_COLUMNS = [
    "cmte_id", "other_id", "transaction_tp", "memo_cd", "transaction_amt",
]
IE_COLUMNS = ["cmte_id", "other_id", "transaction_tp", "net_amt"]


def filter_candidate_contribution_chunk(
    chunk: pd.DataFrame,
    committee_ids: set[str],
    employer_lookup: pd.Series,
    *, cycle: int | None = None,
) -> pd.DataFrame:
    """Apply the core receipt and exact-employer rules to a narrow chunk."""
    included = (
        chunk["cmte_id"].isin(committee_ids)
        & chunk["transaction_tp"].isin(INCLUDE_TYPES_ITCONT | REFUND_TYPES_ITCONT)
        & (
            chunk["memo_cd"].ne("X")
            | ~chunk["transaction_tp"].isin(MEMO_X_EXCLUDE_TYPES)
        )
    )
    receipts = chunk.loc[included].copy()
    excluded = reviewed_exclusion_mask(receipts, cycle)
    if excluded.any():
        receipts = receipts.loc[~excluded].copy()
    amounts = pd.to_numeric(receipts["transaction_amt"], errors="coerce").fillna(0.0)
    receipts["net_amt"] = amounts.where(
        ~receipts["transaction_tp"].isin(REFUND_TYPES_ITCONT), -amounts
    )
    receipts["tech_canonical_name"] = (
        receipts["employer"].str.strip().str.upper().map(employer_lookup)
    )
    receipts["is_tech_employer"] = (
        receipts["tech_canonical_name"].notna()
        & receipts["tech_canonical_name"].ne("")
    )
    return receipts[CONTRIBUTION_COLUMNS]


def filter_ie_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Keep the same non-memo support/opposition rows as the core loader."""
    spending = chunk.loc[
        chunk["transaction_tp"].isin(["24E", "24A"]) & chunk["memo_cd"].ne("X")
    ].copy()
    spending["net_amt"] = pd.to_numeric(
        spending["transaction_amt"], errors="coerce"
    ).fillna(0.0)
    return spending[IE_COLUMNS]


def load_candidate_contributions(
    path: Path,
    committee_ids: set[str],
    tech_employers: pd.DataFrame,
    chunksize: int = 500_000,
    *, cycle: int | None = None,
) -> pd.DataFrame:
    employer_lookup = (
        validated_tech_lookup(tech_employers)
        .set_index("employer_upper")["canonical_name"]
    )
    parts = []
    scanned = 0
    review_dependencies = ReviewedExclusionDependencies(cycle, source_file=path.name)
    with pd.read_csv(
        path, sep="|", header=None, names=ITCONT_COLS,
        usecols=CONTRIBUTION_INPUT_COLUMNS, dtype="string", na_filter=False,
        chunksize=chunksize,
    ) as reader:
        for chunk in reader:
            scanned += len(chunk)
            review_dependencies.observe(chunk)
            selected = filter_candidate_contribution_chunk(chunk, committee_ids, employer_lookup, cycle=cycle)
            if not selected.empty:
                parts.append(selected)
            if scanned % 5_000_000 == 0:
                print(f"  Scanned {scanned:,} individual contribution rows...", flush=True)
    review_dependencies.finalize()
    if parts:
        result = pd.concat(parts, ignore_index=True)
    else:
        result = pd.DataFrame({
            "cmte_id": pd.Series(dtype="string"),
            "name": pd.Series(dtype="string"),
            "net_amt": pd.Series(dtype="float64"),
            "tech_canonical_name": pd.Series(dtype="string"),
            "is_tech_employer": pd.Series(dtype="bool"),
            "memo_cd": pd.Series(dtype="string"),
        })
    print(f"  Retained {len(result):,} linked committee receipts from {scanned:,} rows", flush=True)
    return result


def load_candidate_spending(path: Path, chunksize: int = 500_000) -> pd.DataFrame:
    parts = []
    with pd.read_csv(
        path, sep="|", header=None, names=ITCONT_COLS,
        usecols=IE_INPUT_COLUMNS, dtype="string", na_filter=False,
        chunksize=chunksize,
    ) as reader:
        for chunk in reader:
            selected = filter_ie_chunk(chunk)
            if not selected.empty:
                parts.append(selected)
    if parts:
        result = pd.concat(parts, ignore_index=True)
    else:
        result = pd.DataFrame({
            "cmte_id": pd.Series(dtype="string"),
            "other_id": pd.Series(dtype="string"),
            "transaction_tp": pd.Series(dtype="string"),
            "net_amt": pd.Series(dtype="float64"),
        })
    print(f"  Retained {len(result):,} independent-expenditure rows", flush=True)
    return result


def rebuild_candidate_exports(cycle: int, chunksize: int = 500_000) -> Path:
    if chunksize <= 0:
        raise ValueError("chunksize must be positive")
    base = fec_cycle_interim_dir(cycle)
    suffix = str(cycle)[2:]
    derived = fec_cycle_derived_dir(cycle)
    export = site_export_cycle_dir(cycle)
    metadata_path = export / "site_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    committees = pd.read_csv(
        base / f"cm{suffix}" / "cm.txt", sep="|", header=None, names=CM_COLS,
        dtype="string", na_filter=False,
    )
    candidates = load_candidate_master_table(cycle)
    candidates = candidates[candidates["cand_election_yr"].eq(str(cycle))]
    linkage = load_candidate_linkage_table(cycle)
    relevant_links = linkage[
        linkage["cand_id"].isin(candidates["cand_id"])
        & (
            linkage["cand_election_yr"].eq(str(cycle))
            | linkage["fec_election_yr"].eq(str(cycle))
        )
    ]
    committee_ids = set(relevant_links["cmte_id"])
    committee_tech_receipts = pd.read_csv(
        derived / "committee_tech_receipts.csv",
        usecols=["cmte_id", "tech_receipts"], dtype={"cmte_id": "string"},
    )
    tech_employers = _load_tech_employers()
    print(f"Rebuilding candidate exports for {cycle}...", flush=True)
    tagged = load_candidate_contributions(
        base / f"indiv{suffix}" / "itcont.txt", committee_ids, tech_employers, chunksize, cycle=cycle
    )
    spending = load_candidate_spending(base / f"oth{suffix}" / "itoth.txt", chunksize)
    candidate_race = build_candidate_race_summary(
        cycle, tagged, spending, committee_tech_receipts, committees
    )
    tables = {
        "candidate_race_summary.csv": candidate_race,
        "candidate_state_summary.csv": build_candidate_state_summary(candidate_race),
        "candidate_house_district_summary.csv": build_candidate_house_district_summary(candidate_race),
        "candidate_senate_summary.csv": build_candidate_senate_summary(candidate_race),
    }
    for destination in (derived, export):
        destination.mkdir(parents=True, exist_ok=True)
        for filename, table in tables.items():
            table.to_csv(destination / filename, index=False)
    metadata["candidate_exports_generated_at_utc"] = (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote four candidate CSVs to {derived} and {export}", flush=True)
    return export


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cycles", type=int, nargs="+", help="Election cycles, e.g. 2024 2026")
    parser.add_argument("--chunksize", type=int, default=500_000)
    options = parser.parse_args(args)
    for cycle in options.cycles:
        rebuild_candidate_exports(cycle, options.chunksize)


if __name__ == "__main__":
    main()
