"""Build a bounded source-based report from a reviewed, hash-pinned manifest.

python -m scripts.build_candidate_attribution --manifest data/reference/attribution/nvidia_el_sayed.json
Add --check-current to compare the full committee report inventory with OpenFEC.
New filings require a new reviewed manifest; they are never silently appended.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen

from pipeline.common.paths import PROJECT_ROOT
from pipeline.fec.campaign_reports import build_report
from pipeline.fec.filings import parse_filing
from pipeline.fec.openfec import OpenFECClient


def load_sources(manifest: dict, cache: Path, search_dirs: list[Path]):
    cache.mkdir(parents=True, exist_ok=True)
    filings = []
    for source in manifest["sources"]:
        number = source["file_number"]
        if type(number) is not int or number <= 0:
            raise ValueError("Filing IDs must be positive integers")
        filename = f"{number}.fec"
        path = next((d / filename for d in [cache, *search_dirs] if (d / filename).is_file()), None)
        if path is None:
            url = f"https://docquery.fec.gov/dcdev/posted/{number}.fec"
            with urlopen(Request(url, headers={"User-Agent": "tech-money/1.0"}), timeout=60) as response:
                raw = response.read()
            if hashlib.sha256(raw).hexdigest() != source["sha256"]:
                raise ValueError(f"Downloaded source {number} does not match reviewed hash")
            path = cache / filename
            path.write_bytes(raw)
        filings.append(parse_filing(path, number, expected_sha256=source["sha256"]))
    return filings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cache", type=Path, default=PROJECT_ROOT / "data/fec/raw/filings")
    parser.add_argument("--source-dir", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check-current", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    catalog_check = None
    if args.check_current:
        if manifest["scope"]["kind"] != "campaign_reports":
            raise ValueError("--check-current requires a full campaign_reports inventory; an excerpt cannot establish whole-account coverage")
        client = OpenFECClient()
        catalog = [r for committee in manifest["scope"]["committee_ids"]
                   for r in client.iter_results(f"committee/{committee}/reports/", cycle=manifest["cycle"])]
        if len({int(r["file_number"]) for r in catalog}) != len(catalog):
            raise ValueError("OpenFEC catalog repeated a filing ID; refusing uncertain pagination")
        if {int(r["file_number"]) for r in catalog} != {s["file_number"] for s in manifest["sources"]}:
            raise ValueError("OpenFEC report inventory changed; review the new sources and amendment chains before publishing an updated amount")
        if {int(r["file_number"]) for r in catalog if r.get("most_recent")} != set(manifest["latest_file_numbers"]):
            raise ValueError("OpenFEC latest report selection changed; source review is required")
        catalog_check = {"checked_at": datetime.now(timezone.utc).isoformat(), "reports": len(catalog), "status": "unchanged"}
    result = build_report(manifest, load_sources(manifest, args.cache, args.source_dir))
    result["manifest_sha256"] = hashlib.sha256(args.manifest.read_bytes()).hexdigest()
    result["current_catalog_check"] = catalog_check
    output = args.output or PROJECT_ROOT / "exports/site" / str(manifest["cycle"]) / "attribution" / f'{manifest["report_id"]}.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    # Write only after every source and review dependency has passed.
    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(output)
    print(json.dumps({"report": str(output), "attributed_receipts": result["combined_amount"],
                      "net": result["net_amount"], "scope": result["scope"]["description"],
                      "unresolved": len(result["unresolved"]), "current_catalog_check": catalog_check}, indent=2))


if __name__ == "__main__":
    main()
