"""Count phrase mentions in lobbying issue descriptions, by quarter and tracked company.

Reads data/lda/interim/<year>/ (filings and issue descriptions), the shared
company list, and data/reference/lobbying/phrase_topics.json. Writes
exports/lobbying/phrase_topics.json: everything the public topics page needs,
so the page itself does no searching.

A topic counts an issue entry when one of its phrases appears in the entry's
description. That says the subject was named, not what position was taken.

Usage:
    python -m pipeline.lda.build_topics
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re

import pandas as pd

from pipeline.common.paths import LDA_INTERIM_ROOT, company_registry_path, lda_clients_curated_path
from pipeline.lda.build_explorer import EXPORT, REFERENCE, digest, file_digest, normalized_name, save_json
from pipeline.lda.build_spending import quarter_complete
from pipeline.tagging.lda_clients import client_to_company, load_current_reports
from pipeline.tagging.registry import load_companies

TOPICS_PATH = REFERENCE / "phrase_topics.json"
EXAMPLE_LENGTH = 320


def compile_topic(topic: dict) -> list[re.Pattern]:
    return [re.compile(p["pattern"], 0 if p.get("case_sensitive") else re.IGNORECASE) for p in topic["patterns"]]


def topic_mask(descriptions: pd.Series, topic: dict) -> pd.Series:
    """True where any of the topic's patterns appears in the description."""
    mask = pd.Series(False, index=descriptions.index)
    for pattern in compile_topic(topic):
        mask |= descriptions.str.contains(pattern, na=False)
    return mask


def example_text(description: str, topic: dict) -> str:
    """A short passage around the first matching phrase."""
    spans = [m.span() for m in (p.search(description) for p in compile_topic(topic)) if m]
    start = min(s for s, _ in spans) if spans else 0
    lo = max(0, start - EXAMPLE_LENGTH // 3)
    hi = min(len(description), lo + EXAMPLE_LENGTH)
    text = " ".join(description[lo:hi].split())
    return ("…" if lo > 0 else "") + text + ("…" if hi < len(description) else "")


def load_entries(reports: pd.DataFrame) -> pd.DataFrame:
    """Issue entries of the current reports, with each report's quarter and client."""
    years = sorted({label[:4] for label in reports.quarter.unique()})
    activities = pd.concat([
        pd.read_csv(LDA_INTERIM_ROOT / year / "filing_activities.csv", dtype=str, keep_default_na=False,
                    usecols=["filing_uuid", "description"]) for year in years], ignore_index=True)
    columns = ["filing_uuid", "quarter", "client_name", "registrant_name", "filing_url"]
    return activities.merge(reports[columns], on="filing_uuid", how="inner")


def build_topics(output: Path = EXPORT, reports: pd.DataFrame | None = None, entries: pd.DataFrame | None = None,
                 company_map: dict[str, str] | None = None, topics_path: Path = TOPICS_PATH) -> dict:
    config = json.loads(topics_path.read_text(encoding="utf-8"))
    reports = load_current_reports() if reports is None else reports
    entries = load_entries(reports) if entries is None else entries
    company_map = client_to_company() if company_map is None else company_map
    companies = load_companies().set_index("canonical_name")

    cutoff = pd.to_datetime(reports.dt_posted, utc=True, errors="coerce", format="ISO8601").max().date()
    labels = sorted(reports.quarter.unique(), key=lambda label: (int(label[:4]), int(label[-1])))
    quarters = [{"id": label, "year": int(label[:4]), "quarter": int(label[-1]),
                 "complete": quarter_complete(int(label[:4]), int(label[-1]), cutoff)} for label in labels]

    entries = entries.assign(client_key=entries.client_name.map(normalized_name))
    entries["company_id"] = entries.client_key.map(company_map).fillna("")
    all_clients = entries.groupby("quarter").client_key.nunique()
    tracked = entries[entries.company_id != ""]
    active = tracked.groupby("quarter").company_id.nunique()

    topics = []
    for topic in config["topics"]:
        hits = entries[topic_mask(entries.description, topic)]
        mentioning = hits.groupby("quarter").client_key.nunique()
        tracked_hits = hits[hits.company_id != ""]
        counts = tracked_hits.groupby(["company_id", "quarter"]).size()
        company_rows = []
        for company_id, group in tracked_hits.groupby("company_id"):
            latest = group.assign(order=group.quarter.map(labels.index)).sort_values("order").iloc[-1]
            company_rows.append({
                "id": company_id, "name": companies.display_name[company_id], "sector": companies.sector[company_id],
                "entries": [int(counts.get((company_id, label), 0)) for label in labels],
                "example": {"quarter": latest.quarter, "filer": latest.registrant_name,
                            "text": example_text(latest.description, topic), "url": latest.filing_url}})
        company_rows.sort(key=lambda c: (-sum(1 for n in c["entries"] if n), c["name"].lower()))
        topics.append({
            "id": topic["id"], "label": topic["label"], "phrases": topic["phrases"],
            "all_clients_mentioning": [int(mentioning.get(label, 0)) for label in labels],
            "tracked_companies_mentioning": [sum(1 for c in company_rows if c["entries"][i])
                                             for i in range(len(labels))],
            "companies": company_rows})

    metadata = {
        "schema_version": 1, "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_cutoff": cutoff.isoformat(), "topics_version": config["version"],
        "company_list_version": "companies-" + digest(
            file_digest(company_registry_path()) + file_digest(lda_clients_curated_path()))[:12],
        "issue_entry_count": int(len(entries)),
        "method": "An issue entry is counted for a topic when one of the topic's phrases appears in its "
                  "description. A company is counted in a quarter when at least one of its reports has such "
                  "an entry. Only the latest version of each report is used.",
    }
    payload = {"metadata": metadata, "quarters": quarters,
               "all_clients": [int(all_clients.get(label, 0)) for label in labels],
               "tracked_companies_active": [int(active.get(label, 0)) for label in labels],
               "topics": topics}
    save_json(output / "phrase_topics.json", payload)
    return metadata


def main() -> None:
    print(json.dumps(build_topics(), indent=2))


if __name__ == "__main__":
    main()
