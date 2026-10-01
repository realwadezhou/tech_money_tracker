"""
Generate company-tagging candidates from LDA client names.

The lobbying twin of pipeline.tagging.companies. It surfaces every client name
in the installed LDA quarterly reports that matches a broad pattern for a
tracked tech company, along with stats, and writes two files:

    data/reference/companies/
        lda_candidates.csv     — every surfaced client name + stats (overwritten)
        lda_review_queue.csv   — candidates NOT yet in lda_clients.csv (overwritten)

It NEVER touches lda_clients.csv. See data/reference/companies/README.md.

Usage:
    python -m pipeline.tagging.lda_clients
"""

from __future__ import annotations

import re

import pandas as pd

from pipeline.common.paths import (
    LDA_INTERIM_ROOT,
    company_registry_path,
    lda_clients_candidates_path,
    lda_clients_curated_path,
    lda_clients_review_queue_path,
)
from pipeline.lda.build_explorer import PERIODS, normalized_name, rows, select_current_reports
from pipeline.tagging.companies import TECH_SEARCHES

# Search labels in TECH_SEARCHES that belong to a differently named company.
SEARCH_TO_COMPANY = {
    "twitter": "x_twitter_spacex",
    "spacex": "x_twitter_spacex",
    "xai": "x_twitter_spacex",
    "tiktok": "bytedance",
    "scale": "scale_ai",
}

CANDIDATE_COLUMNS = [
    "client_name", "n_reports", "n_registrants", "self_filed", "first_quarter", "last_quarter",
    "income_usd", "expenses_usd", "matched_searches",
]


def load_current_reports(years: list[int] | None = None) -> pd.DataFrame:
    """Latest version of every quarterly report, using the explorer's selection rule."""
    years = years or sorted(int(p.name) for p in LDA_INTERIM_ROOT.iterdir()
                            if p.name.isdigit() and (p / "filings.csv").exists())
    records = []
    for year in years:
        current, _, _ = select_current_reports(list(rows(LDA_INTERIM_ROOT / str(year) / "filings.csv")))
        for r in current.values():
            records.append({
                "client_name": r["client_name"],
                "registrant_name": r["registrant_name"],
                "quarter": f'{r["filing_year"]} Q{PERIODS[r["filing_period"]]}',
                "income": pd.to_numeric(r["income"] or None),
                "expenses": pd.to_numeric(r["expenses"] or None),
            })
    return pd.DataFrame(records)


def client_stats(reports: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct client name (case and spacing ignored)."""
    reports = reports.assign(
        client_key=reports.client_name.map(normalized_name),
        self_report=reports.client_name.map(normalized_name) == reports.registrant_name.map(normalized_name),
    )
    return (reports.groupby("client_key", sort=False)
            .agg(client_name=("client_name", "first"), n_reports=("quarter", "size"),
                 n_registrants=("registrant_name", "nunique"), self_filed=("self_report", "any"),
                 first_quarter=("quarter", "min"), last_quarter=("quarter", "max"),
                 income_usd=("income", "sum"), expenses_usd=("expenses", "sum"))
            .reset_index(drop=True))


def build_candidates(reports: pd.DataFrame) -> pd.DataFrame:
    """Client names matching any broad company search, biggest spenders first."""
    stats = client_stats(reports)
    hits: dict[int, set[str]] = {}
    for slug, patterns in TECH_SEARCHES.items():
        combined = re.compile("|".join(patterns), re.IGNORECASE)
        for position in stats.index[stats.client_name.str.contains(combined, regex=True, na=False)]:
            hits.setdefault(position, set()).add(SEARCH_TO_COMPANY.get(slug, slug))
    candidates = stats.loc[sorted(hits)].copy()
    candidates["matched_searches"] = ["; ".join(sorted(hits[i])) for i in candidates.index]
    candidates["total"] = candidates.income_usd + candidates.expenses_usd
    return (candidates.sort_values("total", ascending=False)
            .reset_index(drop=True)[CANDIDATE_COLUMNS])


def load_curated() -> pd.DataFrame:
    path = lda_clients_curated_path()
    if not path.exists():
        return pd.DataFrame(columns=["client_name", "include", "canonical_name", "notes", "client_key"])
    curated = pd.read_csv(path, dtype="string", na_filter=False)
    curated["client_key"] = curated.client_name.map(normalized_name)
    if curated.client_key.duplicated().any():
        raise ValueError(f"Duplicate client names in {path.name}: "
                         f"{sorted(curated.client_key[curated.client_key.duplicated()])}")
    if not curated.include.isin(["TRUE", "FALSE"]).all():
        raise ValueError(f"{path.name}: include must be TRUE or FALSE")
    known = set(pd.read_csv(company_registry_path(), dtype="string", na_filter=False).canonical_name)
    unknown = set(curated.canonical_name[curated.include == "TRUE"]) - known
    if unknown:
        raise ValueError(f"{path.name} uses companies missing from companies.csv: {sorted(unknown)}")
    return curated


def client_to_company() -> dict[str, str]:
    """Normalized client name → canonical company, for included rows only."""
    curated = load_curated()
    included = curated[curated.include == "TRUE"]
    return dict(zip(included.client_key, included.canonical_name))


def build_review_queue(candidates: pd.DataFrame) -> pd.DataFrame:
    """Candidates whose client name is NOT present in lda_clients.csv."""
    known = set(load_curated().client_key)
    unreviewed = candidates[~candidates.client_name.map(normalized_name).isin(known)].copy()
    # Empty decision columns for the reviewer to fill in.
    for column in ["include", "canonical_name", "notes"]:
        unreviewed[column] = ""
    return unreviewed


def main() -> int:
    print("Reading installed LDA reports...")
    candidates = build_candidates(load_current_reports())
    candidates.to_csv(lda_clients_candidates_path(), index=False)
    print(f"Wrote {len(candidates)} candidates to {lda_clients_candidates_path().name}")
    review = build_review_queue(candidates)
    review.to_csv(lda_clients_review_queue_path(), index=False)
    print(f"Wrote {len(review)} unreviewed rows to {lda_clients_review_queue_path().name}")
    if len(review) == 0:
        print("Review queue is empty — every candidate is already in lda_clients.csv.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
