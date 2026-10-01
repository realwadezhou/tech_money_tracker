"""Build quarterly lobbying spending by tracked company from normalized LDA reports.

Reads data/lda/interim/<year>/filings.csv and the shared company list
(data/reference/companies/). Writes exports/lobbying/:

    spending.json           company x quarter amounts and method notes
    spending.csv            the same, one row per company and quarter
    spending_reports.csv    every report behind the numbers, with source links

No network requests. See data/reference/companies/DECISIONS.md (Decision 9)
for the counting rule.

Usage:
    python -m pipeline.lda.build_spending
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd

from pipeline.common.paths import company_registry_path, lda_clients_curated_path
from pipeline.lda.build_explorer import EXPORT, digest, file_digest, normalized_name, save_csv, save_json
from pipeline.tagging.lda_clients import client_to_company, load_current_reports
from pipeline.tagging.registry import load_companies

# LD-2 quarterly reports are due 20 days after the quarter ends.
FILING_DEADLINE_DAYS = 20
METHOD = (
    "For each company and quarter: add up the expenses on every in-house report (a company "
    "reporting its own lobbying), add up the income on every outside-firm report (a lobbying "
    "firm reporting what the company paid it), and use the larger of the two sums. In-house "
    "expenses normally already include payments to outside firms, so adding both would double count."
)
CSV_FIELDS = ["company_id", "company", "sector", "year", "quarter", "quarter_complete", "total", "basis",
              "in_house_expenses", "outside_firm_income", "in_house_reports", "outside_firm_reports"]
REPORT_FIELDS = ["company_id", "company", "year", "quarter", "kind", "amount", "client_name",
                 "registrant_name", "filing_type", "dt_posted", "filing_uuid", "filing_url"]


def quarter_complete(year: int, quarter: int, cutoff: date) -> bool:
    """True when the quarter's filing deadline had passed at the newest posting we hold."""
    end = (date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)) - timedelta(days=1)
    return cutoff >= end + timedelta(days=FILING_DEADLINE_DAYS)


def tag_reports(reports: pd.DataFrame, company_map: dict[str, str]) -> pd.DataFrame:
    """Keep reports whose client name is reviewed as a tracked company."""
    tagged = reports.assign(company_id=reports.client_name.map(normalized_name).map(company_map))
    tagged = tagged[tagged.company_id.notna()].copy()
    tagged["year"] = tagged.quarter.str.slice(0, 4).astype(int)
    tagged["q"] = tagged.quarter.str.slice(-1).astype(int)
    # A report carries expenses (in-house) or income (outside firm), never both.
    tagged["kind"] = tagged.expenses.notna().map({True: "in_house", False: "outside_firm"})
    tagged["amount"] = tagged.expenses.fillna(tagged.income).fillna(0.0)
    return tagged


def company_quarters(tagged: pd.DataFrame) -> pd.DataFrame:
    """One row per company and quarter with both sums and the headline total."""
    keys = ["company_id", "year", "q"]
    out = tagged.groupby(keys).size().rename("reports").reset_index()[keys]
    for kind, amount_column, count_column in (("in_house", "in_house_expenses", "in_house_reports"),
                                              ("outside_firm", "outside_firm_income", "outside_firm_reports")):
        part = tagged[tagged.kind == kind].groupby(keys).amount.agg(["sum", "size"]).reset_index()
        part = part.rename(columns={"sum": amount_column, "size": count_column})
        out = out.merge(part, on=keys, how="left")
    out = out.fillna({"in_house_expenses": 0.0, "outside_firm_income": 0.0,
                      "in_house_reports": 0, "outside_firm_reports": 0})
    out[["in_house_reports", "outside_firm_reports"]] = out[["in_house_reports", "outside_firm_reports"]].astype(int)
    out["total"] = out[["in_house_expenses", "outside_firm_income"]].max(axis=1)
    out["basis"] = (out.in_house_expenses >= out.outside_firm_income).map({True: "in_house", False: "outside_firm"})
    out.loc[out.total == 0, "basis"] = "none_reported"
    return out


def build_spending(output: Path = EXPORT, reports: pd.DataFrame | None = None,
                   company_map: dict[str, str] | None = None) -> dict:
    reports = load_current_reports() if reports is None else reports
    cutoff = pd.to_datetime(reports.dt_posted, utc=True, errors="coerce", format="ISO8601").max().date()
    tagged = tag_reports(reports, client_to_company() if company_map is None else company_map)
    table = company_quarters(tagged)
    companies = load_companies().set_index("canonical_name")

    periods = sorted({(int(label[:4]), int(label[-1])) for label in reports.quarter.unique()})
    quarters = [{"id": f"{y} Q{q}", "year": y, "quarter": q, "complete": quarter_complete(y, q, cutoff)}
                for y, q in periods]
    cells = {(r.company_id, r.year, r.q): r for r in table.itertuples()}
    names = tagged.groupby("company_id").client_name.unique()

    def cell(company_id: str, y: int, q: int) -> dict | None:
        r = cells.get((company_id, y, q))
        return None if r is None else {
            "total": float(r.total), "basis": r.basis,
            "in_house_expenses": float(r.in_house_expenses), "outside_firm_income": float(r.outside_firm_income),
            "in_house_reports": int(r.in_house_reports), "outside_firm_reports": int(r.outside_firm_reports)}

    company_rows = [{
        "id": cid, "name": companies.display_name[cid], "sector": companies.sector[cid],
        "client_names": sorted(names[cid]),
        "quarters": [cell(cid, y, q) for y, q in periods],
    } for cid in sorted(names.index, key=lambda c: companies.display_name[c].lower())]

    metadata = {
        "schema_version": 1,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_cutoff": cutoff.isoformat(),
        "method": METHOD,
        "company_list_version": "companies-" + digest(
            file_digest(company_registry_path()) + file_digest(lda_clients_curated_path()))[:12],
        "company_count": len(company_rows),
        "report_count": int(len(tagged)),
        "notes": [
            "Amounts are as reported; filers round to the nearest $10,000 and may leave amounts under $5,000 blank.",
            "Latest posted version of each report in the saved snapshot; later amendments can change past quarters.",
            "Dollars cover a whole report and are not assigned to issues.",
            "Company grouping follows data/reference/companies/lda_clients.csv: subsidiaries roll up to the "
            "parent; subcontractor reports are excluded.",
        ],
    }
    save_json(output / "spending.json", {"metadata": metadata, "quarters": quarters, "companies": company_rows})
    complete = {(p["year"], p["quarter"]): p["complete"] for p in quarters}
    save_csv(output / "spending.csv", [{
        "company_id": r.company_id, "company": companies.display_name[r.company_id],
        "sector": companies.sector[r.company_id], "year": r.year, "quarter": r.q,
        "quarter_complete": complete[(r.year, r.q)], "total": r.total, "basis": r.basis,
        "in_house_expenses": r.in_house_expenses, "outside_firm_income": r.outside_firm_income,
        "in_house_reports": r.in_house_reports, "outside_firm_reports": r.outside_firm_reports,
    } for r in table.sort_values(["company_id", "year", "q"]).itertuples()], CSV_FIELDS)
    evidence = tagged.sort_values(["company_id", "year", "q", "kind", "registrant_name"])
    save_csv(output / "spending_reports.csv", [{
        "company_id": r.company_id, "company": companies.display_name[r.company_id], "year": r.year,
        "quarter": r.q, "kind": r.kind, "amount": r.amount, "client_name": r.client_name,
        "registrant_name": r.registrant_name, "filing_type": r.filing_type, "dt_posted": r.dt_posted,
        "filing_uuid": r.filing_uuid, "filing_url": r.filing_url,
    } for r in evidence.itertuples()], REPORT_FIELDS)
    return metadata


def main() -> None:
    print(json.dumps(build_spending(), indent=2))


if __name__ == "__main__":
    main()
