"""
One-time migration: data/reference/tech_employers/ → data/reference/companies/

Reads the legacy employer lookup CSV, drops Excel artifacts and generator-only
columns, bakes in the canonical_name fallback, keeps only decided rows
(include in {TRUE, FALSE}), and writes a clean curated.csv.

After running this once and verifying, the old directory can be deleted.

Idempotent: re-running produces the same curated.csv.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OLD_PATH = PROJECT_ROOT / "data" / "reference" / "tech_employers" / "employer_matches_for_review_manual_mar31temp.csv"
NEW_DIR = PROJECT_ROOT / "data" / "reference" / "companies"
NEW_PATH = NEW_DIR / "curated.csv"


CURATED_COLUMNS = [
    "employer",
    "include",
    "canonical_name",
    "sector",
    "notes",
    "matched_searches",
]


def main() -> int:
    if not OLD_PATH.exists():
        print(f"ERROR: source file not found: {OLD_PATH}", file=sys.stderr)
        return 1

    df = pd.read_csv(OLD_PATH, dtype="string", na_filter=False)

    print(f"Loaded {len(df)} rows, {len(df.columns)} columns from legacy file")

    # Excel left behind unnamed trailing columns and a truncated header.
    if "canonical_me" in df.columns and "canonical_name" not in df.columns:
        df = df.rename(columns={"canonical_me": "canonical_name"})

    for col in CURATED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Normalize include: empty or non-boolean values are "undecided".
    df["include"] = df["include"].str.strip().str.upper()
    decided_mask = df["include"].isin(["TRUE", "FALSE"])

    dropped = len(df) - decided_mask.sum()
    print(f"Dropping {dropped} undecided rows (include blank or malformed)")
    print(f"  Malformed include values found: "
          f"{sorted(set(df.loc[~decided_mask, 'include']))}")

    df = df.loc[decided_mask].copy()

    # Bake in the canonical_name fallback: if include=TRUE and blank, use matched_searches.
    fill_mask = (df["include"] == "TRUE") & (
        (df["canonical_name"] == "") | df["canonical_name"].isna()
    )
    n_filled = int(fill_mask.sum())
    df.loc[fill_mask, "canonical_name"] = df.loc[fill_mask, "matched_searches"]
    print(f"Filled canonical_name from matched_searches on {n_filled} rows")

    # Guardrail: every include=TRUE row should now have a canonical_name.
    still_blank = ((df["include"] == "TRUE") & (df["canonical_name"] == "")).sum()
    if still_blank:
        print(f"WARNING: {still_blank} include=TRUE rows still have blank canonical_name",
              file=sys.stderr)

    # Strip surrounding whitespace on string columns.
    for col in CURATED_COLUMNS:
        df[col] = df[col].fillna("").astype("string").str.strip()

    out = df[CURATED_COLUMNS].copy()
    out = out.sort_values(["include", "canonical_name", "employer"],
                          ascending=[False, True, True]).reset_index(drop=True)

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(NEW_PATH, index=False)

    print()
    print(f"Wrote {len(out)} rows to {NEW_PATH.relative_to(PROJECT_ROOT)}")
    print(f"  include=TRUE:  {(out['include'] == 'TRUE').sum()}")
    print(f"  include=FALSE: {(out['include'] == 'FALSE').sum()}")
    print(f"  distinct canonical_name (TRUE rows): "
          f"{out.loc[out['include'] == 'TRUE', 'canonical_name'].nunique()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
