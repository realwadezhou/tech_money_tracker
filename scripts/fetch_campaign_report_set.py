"""Acquire a source report inventory for review, without publishing any totals.

The processed API supplies the inventory. Raw electronic report headers select
replacement versions independently within that inventory. No account ownership
or economic interpretation is certified by a successful download.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
from urllib.request import Request, urlopen

from pipeline.fec.filings import latest_filings, parse_filing
from pipeline.fec.openfec import OpenFECClient


def acquire(committee: str, cycle: int, output: Path, source_dirs: list[Path]) -> dict:
    if not re.fullmatch(r"C\d{8}", committee) or cycle < 1980 or cycle % 2:
        raise ValueError("An exact committee ID and even two-year cycle are required")
    output.mkdir(parents=True, exist_ok=True)
    raw_dir = output / "source_filings"
    raw_dir.mkdir(exist_ok=True)
    catalog = list(OpenFECClient().iter_results(f"committee/{committee}/reports/", cycle=cycle))
    observed = datetime.now(timezone.utc).isoformat()
    (output / "reports_catalog.json").write_text(json.dumps({
        "checked_at": observed, "endpoint": f"committee/{committee}/reports/", "cycle": cycle,
        "results": catalog,
    }, indent=2) + "\n", encoding="utf-8")
    ids = [r["file_number"] for r in catalog]
    if not ids or any(type(n) is not int or n <= 0 for n in ids) or len(set(ids)) != len(ids):
        raise ValueError("Missing, paper, or duplicate source IDs require review")
    filings, errors, sources = [], [], []
    for number in sorted(ids):
        path = raw_dir / f"{number}.fec"
        if not path.exists():
            existing = next((d / path.name for d in source_dirs if (d / path.name).is_file()), None)
            if existing:
                shutil.copy2(existing, path)
            else:
                url = f"https://docquery.fec.gov/dcdev/posted/{number}.fec"
                with urlopen(Request(url, headers={"User-Agent": "tech-money/1.0"}), timeout=60) as response:
                    raw = response.read()
                temporary = path.with_suffix(".fec.part")
                temporary.write_bytes(raw)
                temporary.replace(path)
        sources.append({"file_number": number, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "bytes": path.stat().st_size})
        try:
            filing = parse_filing(path, number)
            if filing.metadata["committee_id"] != committee:
                raise ValueError("Filing committee disagrees with requested inventory")
            filings.append(filing)
        except ValueError as exc:
            errors.append({"file_number": number, "error": str(exc)})
        print(f"Reviewed source {number}", flush=True)
    selected = []
    if not errors:
        try:
            selected = latest_filings(filings)
            if {f.metadata["file_number"] for f in selected} != {r["file_number"] for r in catalog if r.get("most_recent")}:
                raise ValueError("Source-header version selection disagrees with processed catalog")
        except ValueError as exc:
            errors.append({"error": str(exc)})
            selected = []
    summary = {"committee_id": committee, "cycle": cycle, "source_catalog_as_of": observed,
               "sources": sources, "source_metadata": [f.metadata for f in filings],
               "latest_reports": [f.metadata for f in selected], "errors": errors,
               "limitation": "Processed report inventory only; not all unprocessed filings or a reviewed attribution result."}
    (output / "source_inventory.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (output / "latest_records.json").write_text(json.dumps([r for f in selected for r in f.records], indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"reports": len(catalog), "latest": len(selected), "errors": errors}))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--committee", required=True)
    parser.add_argument("--cycle", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, action="append", default=[])
    args = parser.parse_args()
    result = acquire(args.committee, args.cycle, args.output, args.source_dir)
    raise SystemExit(bool(result["errors"]))


if __name__ == "__main__":
    main()
