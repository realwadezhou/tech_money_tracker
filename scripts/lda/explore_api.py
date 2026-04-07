from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.lda.client import LDAClient


def summarize_filings(year: int, *, pages: int = 4) -> dict:
    client = LDAClient()
    filings = list(
        client.iter_results(
            "filings/",
            filing_year=year,
            ordering="-dt_posted",
            max_pages=pages,
        )
    )
    issue_counts: Counter[str] = Counter()
    client_counts: Counter[str] = Counter()
    registrant_counts: Counter[str] = Counter()

    for filing in filings:
        client_name = (filing.get("client") or {}).get("name")
        registrant_name = (filing.get("registrant") or {}).get("name")
        if client_name:
            client_counts[client_name] += 1
        if registrant_name:
            registrant_counts[registrant_name] += 1
        for activity in filing.get("lobbying_activities") or []:
            code = activity.get("general_issue_code")
            if code:
                issue_counts[code] += 1

    latest_posted = filings[0]["dt_posted"] if filings else None
    return {
        "year": year,
        "sample_size": len(filings),
        "sample_pages": pages,
        "latest_posted": latest_posted,
        "top_issue_codes": issue_counts.most_common(10),
        "top_clients": client_counts.most_common(10),
        "top_registrants": registrant_counts.most_common(10),
    }


def main(args: list[str] | None = None) -> None:
    argv = args if args is not None else sys.argv[1:]
    year = int(argv[0]) if argv else 2026
    pages = int(argv[1]) if len(argv) > 1 else 4
    summary = summarize_filings(year, pages=pages)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
