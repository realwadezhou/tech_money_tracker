"""Cache FEC-confirmed campaign-to-PAC conversions for historical attribution.

Refresh with: python -m pipeline.fec.committee_history 2024 2026
Normal summary builds read the committed supplement and make no API requests.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html import unescape
import json
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd

from pipeline.classify_partisan import load_candidate_linkage_table, load_candidate_master_table
from pipeline.common.paths import FEC_INTERIM_ROOT, REFERENCE_ROOT
from pipeline.fec.load import CM_COLS
from pipeline.fec.openfec import OpenFECClient


def converted_campaigns_path(cycle: int):
    return REFERENCE_ROOT / "committees" / f"converted_campaigns_{cycle}.json"


def load_converted_campaigns(cycle: int) -> pd.DataFrame:
    """Read explicit former candidate IDs, restricted to their election cycle."""
    path = converted_campaigns_path(cycle)
    if not path.exists():
        return pd.DataFrame(columns=["cmte_id", "cand_id"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("cycle") != cycle:
        raise ValueError(f"Conversion supplement cycle does not match {cycle}: {path}")
    rows = [
        {"cmte_id": row["cmte_id"], "cand_id": row["cand_id"]}
        for row in payload.get("committees", [])
        if row.get("converted_to_pac") is True
        and str(row.get("former_candidate_election_year")) == str(cycle)
    ]
    result = pd.DataFrame(rows, columns=["cmte_id", "cand_id"]).drop_duplicates()
    if result["cmte_id"].duplicated().any():
        raise ValueError(f"Ambiguous former candidate IDs in conversion supplement: {path}")
    return result


def parse_public_conversion(page: str, cycle: int) -> dict | None:
    """Read the FEC's historical conversion notice, with its rendered cycle.

    Out-of-range URLs can silently display a different cycle. The public page's
    context must match before its conversion notice is evidence for this cycle.
    """
    context_match = re.search(r"window\.context\s*=\s*(\{[^\n]*?\});", page)
    if not context_match:
        raise ValueError("FEC committee page has no recognizable cycle context")
    context = json.loads(context_match.group(1))
    if context.get("cycle") != cycle or context.get("cycleOutOfRange") is not False:
        return None
    notice_match = re.search(r"Financial data for this committee.*?</p>", page, re.DOTALL)
    if not notice_match:
        return None
    notice = notice_match.group(0)
    candidate_match = re.search(r'href="/data/candidate/([HSP][A-Z0-9]{8})/?"', notice)
    name_match = re.search(r"former name\s*<strong>(.*?)</strong>", notice, re.DOTALL)
    if not candidate_match or not name_match or "before it was converted" not in notice:
        raise ValueError("FEC conversion notice has an unrecognized format")
    return {
        "cand_id": candidate_match.group(1),
        "converted_to_pac": True,
        "history_cycle": cycle,
        # This compatibility field is derived only after the caller verifies
        # the former ID against the cycle's candidate master and linkage.
        "former_candidate_election_year": cycle,
        "candidate_election_year": cycle,
        "election_year_source": "FEC candidate master and cycle linkage",
        "conversion_source": "FEC public historical committee conversion notice",
        "former_committee_name": unescape(name_match.group(1)).strip(),
        "current_committee_name": context.get("name"),
    }


def refresh_converted_campaigns(cycle: int, *, workers: int = 4, source: str = "public") -> None:
    """Probe only noncampaign committees linked to candidates in this cycle."""
    candidates = load_candidate_master_table(cycle)
    candidates = candidates[candidates["cand_election_yr"] == str(cycle)]
    linkage = load_candidate_linkage_table(cycle)
    linkage = linkage[
        ((linkage["cand_election_yr"] == str(cycle)) |
         (linkage["fec_election_yr"] == str(cycle))) &
        linkage["cand_id"].isin(candidates["cand_id"])
    ].copy()
    suffix = str(cycle)[-2:]
    committees = pd.read_csv(
        FEC_INTERIM_ROOT / str(cycle) / f"cm{suffix}" / "cm.txt",
        sep="|", names=CM_COLS, dtype="string", na_filter=False,
    )
    linked = linkage[["cand_id", "cmte_id"]].merge(
        committees[["cmte_id", "cmte_tp", "cmte_dsgn"]], on="cmte_id", how="left",
    )
    noncampaign = ~(
        linked["cmte_tp"].isin(["H", "S", "P"]) & linked["cmte_dsgn"].isin(["P", "A"])
    )
    committee_ids = sorted(linked.loc[noncampaign, "cmte_id"].dropna().unique())
    allowed_links = set(linked[["cmte_id", "cand_id"]].itertuples(index=False, name=None))
    client = OpenFECClient() if source == "api" else None

    def fetch(committee_id):
        endpoint = f"committee/{committee_id}/history/{cycle}/"
        public_url = f"https://www.fec.gov/data/committee/{committee_id}/?cycle={cycle}"
        if source == "public":
            request = Request(public_url, headers={"User-Agent": "tech-money/1.0"})
            try:
                with urlopen(request, timeout=30) as response:
                    page = response.read().decode("utf-8")
            except HTTPError as exc:
                if exc.code == 404:
                    return []
                raise
            conversion = parse_public_conversion(page, cycle)
            if conversion is None or (committee_id, conversion["cand_id"]) not in allowed_links:
                return []
            return [{
                "cmte_id": committee_id,
                **conversion,
                "source_url": public_url,
                "public_source_url": public_url,
            }]
        result = client.get(endpoint, per_page=100)
        rows = []
        for row in result.get("results", []):
            former_candidate = row.get("former_candidate_id")
            if (
                row.get("convert_to_pac_flag") is True
                and str(row.get("former_candidate_election_year")) == str(cycle)
                and (committee_id, former_candidate) in allowed_links
            ):
                rows.append({
                    "cmte_id": committee_id,
                    "cand_id": former_candidate,
                    "converted_to_pac": True,
                    "former_candidate_election_year": cycle,
                    "former_committee_name": row.get("former_committee_name"),
                    "current_committee_name": row.get("name"),
                    "current_committee_type": row.get("committee_type"),
                    "current_designation": row.get("designation"),
                    "source_url": f"https://api.open.fec.gov/v1/{endpoint}",
                    "public_source_url": public_url,
                })
        return rows

    print(f"{cycle}: checking {len(committee_ids)} linked noncampaign committees...", flush=True)
    records = []
    errors = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch, committee_id): committee_id for committee_id in committee_ids}
        for count, future in enumerate(as_completed(futures), start=1):
            try:
                records.extend(future.result())
            except Exception as exc:
                errors.append(f"{futures[future]} ({type(exc).__name__})")
            if count % 25 == 0:
                print(f"  {count}/{len(committee_ids)} checked", flush=True)
    if errors:
        raise RuntimeError("Conversion refresh incomplete; existing cache preserved: " + ", ".join(errors))
    payload = {
        "schema_version": 1,
        "cycle": cycle,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "probed_committee_count": len(committee_ids),
        "source": source,
        "rule": (
            "FEC historical conversion notice identifies the former candidate; rendered page cycle matches requested cycle and cycleOutOfRange=false; former candidate appears in cycle linkage."
            if source == "public" else
            "FEC convert_to_pac_flag=true; former_candidate_election_year matches cycle; former_candidate_id appears in cycle candidate linkage."
        ),
        "committees": sorted(records, key=lambda row: row["cmte_id"]),
    }
    if source == "public":
        payload["public_source_provenance"] = (
            "Former candidate ID and former committee name are extracted from the official FEC conversion notice for the rendered history cycle. "
            "The compatibility former_candidate_election_year field is derived from the matching candidate master and cycle linkage, not extracted from the public HTML."
        )
    path = converted_campaigns_path(cycle)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    print(f"  Saved {len(records)} confirmed conversions to {path}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cycles", type=int, nargs="+", help="Election cycles to refresh")
    parser.add_argument("--source", choices=["public", "api"], default="public", help="Official FEC source; API requires an OpenFEC key")
    args = parser.parse_args()
    for cycle in args.cycles:
        refresh_converted_campaigns(cycle, source=args.source)


if __name__ == "__main__":
    main()
