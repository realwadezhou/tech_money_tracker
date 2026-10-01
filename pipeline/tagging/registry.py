"""The one list of tracked companies, shared by FEC and lobbying.

data/reference/companies/companies.csv is edited by hand. Both alias lookups
(curated.csv for FEC employers, lda_clients.csv for lobbying clients) point at
its canonical_name column.
"""

from __future__ import annotations

import pandas as pd

from pipeline.common.paths import company_registry_path


def load_companies() -> pd.DataFrame:
    companies = pd.read_csv(company_registry_path(), dtype="string", na_filter=False)
    if companies.canonical_name.duplicated().any() or (companies.canonical_name == "").any():
        raise ValueError("companies.csv needs one non-empty row per canonical_name")
    return companies


def company_labels() -> dict[str, str]:
    """canonical_name → display name."""
    companies = load_companies()
    return dict(zip(companies.canonical_name, companies.display_name))
