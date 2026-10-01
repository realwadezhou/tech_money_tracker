"""Build money-in and money-out profiles for the committees in the registry.

Reads data/reference/committees/registry.csv and the FEC bulk files, and writes
exports/committees/:

    profiles.json                 everything below, per committee and cycle
    receipts.csv                  money in, by kind of source
    contributors.csv              named contributors (and who stands behind a gift)
    transfers.csv                 money passed to or received from other committees
    independent_expenditures.csv  spending for or against candidates

Unlike the employer-matched pages, this counts every reported source: people
with any employer, companies and other organizations, and other committees.

Counting rules (see notes/transaction_type_observations.md):
- Receipts are read from the receiving committee's own records only. A transfer
  also appears in the sender's records; the two are never added together.
- Memo rows say who stands behind another row (for example the partners behind
  a firm's gift). They are listed, never summed.

Usage:
    python -m pipeline.fec.committee_profiles 2024 2026
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd

from pipeline.classify_partisan import CN_COLS
from pipeline.common.paths import EXPORTS_ROOT, FEC_INTERIM_ROOT, REFERENCE_ROOT
from pipeline.fec.load import CM_COLS, INCLUDE_TYPES_ITCONT, ITCONT_COLS, ITPAS2_COLS, REFUND_TYPES_ITCONT
from pipeline.lda.build_explorer import save_csv, save_json

REGISTRY_PATH = REFERENCE_ROOT / "committees" / "registry.csv"
EXPORT = EXPORTS_ROOT / "committees"
# "general" marks reviewed recipients of tech money that are not tech vehicles (include=FALSE).
KINDS = {"issue_vehicle", "tech_funded", "corporate_pac", "trade_association", "general"}
TOPICS = {"ai", "crypto", "tech", "general"}
# How a contributor's FEC entity type is described to readers.
SOURCE_KINDS = {"IND": "individuals", "ORG": "organizations", "PAC": "committees", "COM": "committees",
                "CCM": "committees", "PTY": "committees", "CAN": "candidates"}
RECEIVED_FROM_COMMITTEE = {"18G", "18K"}   # receiver's own record of a transfer or contribution
GIVEN_TO_COMMITTEE = {"24G", "24K"}        # giver's own record
INDEPENDENT_EXPENDITURES = {"24E": "support", "24A": "oppose"}
TOP_CONTRIBUTORS = 25
READ = dict(sep="|", header=None, dtype="string", na_filter=False, quoting=3,
            encoding_errors="replace", on_bad_lines="skip")


def load_registry(path: Path = REGISTRY_PATH) -> pd.DataFrame:
    registry = pd.read_csv(path, dtype="string", na_filter=False)
    if registry.cmte_id.duplicated().any():
        raise ValueError(f"Duplicate committee IDs in {path.name}")
    if not registry.include.isin(["TRUE", "FALSE"]).all():
        raise ValueError(f"{path.name}: include must be TRUE or FALSE")
    if set(registry.kind) - KINDS or set(registry.topic) - TOPICS:
        raise ValueError(f"{path.name}: unknown kind or topic")
    return registry[registry.include == "TRUE"].reset_index(drop=True)


def read_receipts(cycle: int, ids: set[str]) -> pd.DataFrame:
    """Rows of the contributions file whose receiving committee is in the registry."""
    path = FEC_INTERIM_ROOT / str(cycle) / f"indiv{str(cycle)[2:]}" / "itcont.txt"
    columns = ["cmte_id", "transaction_tp", "entity_tp", "name", "employer", "occupation",
               "transaction_dt", "transaction_amt", "other_id", "memo_cd", "sub_id"]
    parts = [chunk[chunk.cmte_id.isin(ids)] for chunk in pd.read_csv(
        path, names=ITCONT_COLS, usecols=columns, chunksize=2_000_000, **READ)]
    return pd.concat(parts, ignore_index=True)


def read_committee_rows(cycle: int, ids: set[str]) -> pd.DataFrame:
    """Committee-to-committee rows filed by a registry committee, with candidate IDs where given."""
    base = FEC_INTERIM_ROOT / str(cycle)
    yy = str(cycle)[2:]
    other = pd.read_csv(base / f"oth{yy}" / "itoth.txt", names=ITCONT_COLS, **READ)
    other = other[other.cmte_id.isin(ids)]
    # itpas2 is a subset of itoth that adds the candidate ID.
    to_candidates = pd.read_csv(base / f"pas2{yy}" / "itpas2.txt", names=ITPAS2_COLS,
                                usecols=["cmte_id", "sub_id", "cand_id"], **READ)
    to_candidates = to_candidates[to_candidates.cmte_id.isin(ids)]
    return other.merge(to_candidates[["sub_id", "cand_id"]], on="sub_id", how="left").fillna({"cand_id": ""})


def summarize_receipts(rows: pd.DataFrame) -> tuple[dict, list[dict], list[dict]]:
    """Money in from the contributions file: totals by source, top contributors, and memo attributions."""
    rows = rows.assign(amount=pd.to_numeric(rows.transaction_amt, errors="coerce").fillna(0.0))
    refunds = rows.transaction_tp.isin(REFUND_TYPES_ITCONT)
    rows.loc[refunds, "amount"] = -rows.loc[refunds, "amount"]
    counted = rows[(rows.transaction_tp.isin(INCLUDE_TYPES_ITCONT) | refunds) & (rows.memo_cd != "X")]
    counted = counted.assign(source=counted.entity_tp.map(SOURCE_KINDS).fillna("other"))
    totals = {kind: float(counted.amount[counted.source == kind].sum())
              for kind in ("individuals", "organizations", "committees", "candidates", "other")}

    def top(frame: pd.DataFrame) -> list[dict]:
        grouped = (frame.groupby(["name", "source"], sort=False)
                   .agg(amount=("amount", "sum"), gifts=("amount", "size"),
                        employers=("employer", lambda s: "; ".join(sorted({e for e in s if e})[:3])))
                   .reset_index().sort_values("amount", ascending=False).head(TOP_CONTRIBUTORS))
        return [{"name": r.name, "source": r.source, "employers": r.employers,
                 "amount": float(r.amount), "gifts": int(r.gifts)} for r in grouped.itertuples(index=False)]

    memos = rows[rows.transaction_tp.isin(INCLUDE_TYPES_ITCONT) & (rows.memo_cd == "X")]
    memos = memos.assign(source=memos.entity_tp.map(SOURCE_KINDS).fillna("other"))
    return totals, top(counted), top(memos)


def summarize_committee_rows(rows: pd.DataFrame, names: pd.Series, candidates: pd.DataFrame,
                             registry_ids: set[str]) -> dict:
    rows = rows.assign(amount=pd.to_numeric(rows.transaction_amt, errors="coerce").fillna(0.0))
    rows = rows[rows.memo_cd != "X"]

    def partners(types: set[str]) -> list[dict]:
        part = rows[rows.transaction_tp.isin(types)]
        grouped = part.groupby("other_id").agg(amount=("amount", "sum"), name=("name", "first")).reset_index()
        grouped = grouped.sort_values("amount", ascending=False)
        return [{"cmte_id": r.other_id, "name": names.get(r.other_id, r.name), "amount": float(r.amount),
                 "in_registry": r.other_id in registry_ids} for r in grouped.itertuples(index=False)]

    spending = rows[rows.transaction_tp.isin(INDEPENDENT_EXPENDITURES)]
    spending = spending.assign(position=spending.transaction_tp.map(INDEPENDENT_EXPENDITURES))
    by_candidate = []
    for (cand_id, position), group in spending.groupby(["cand_id", "position"]):
        info = candidates.loc[cand_id] if cand_id in candidates.index else None
        by_candidate.append({
            "cand_id": cand_id, "position": position, "amount": float(group.amount.sum()),
            # Some rows carry no candidate ID; the name field then holds the payee, not a candidate.
            "candidate": info.cand_name if info is not None
            else f"Candidate not identified in the FEC bulk file (payee: {group.name.iloc[0]})",
            "party": info.cand_pty_affiliation if info is not None else "",
            "office": f"{info.cand_office}-{info.cand_office_st}-{info.cand_office_district}" if info is not None else ""})
    by_candidate.sort(key=lambda r: -r["amount"])
    return {
        "received_from_committees": partners(RECEIVED_FROM_COMMITTEE),
        "given_to_committees": partners(GIVEN_TO_COMMITTEE),
        "independent_expenditures": {
            "support": float(spending.amount[spending.position == "support"].sum()),
            "oppose": float(spending.amount[spending.position == "oppose"].sum()),
            "by_candidate": by_candidate},
    }


def build_cycle(cycle: int, registry: pd.DataFrame) -> dict[str, dict]:
    ids = set(registry.cmte_id)
    base = FEC_INTERIM_ROOT / str(cycle)
    yy = str(cycle)[2:]
    names = pd.read_csv(base / f"cm{yy}" / "cm.txt", names=CM_COLS, **READ).set_index("cmte_id").cmte_nm
    candidates = pd.read_csv(base / f"cn{yy}" / "cn.txt", names=CN_COLS, **READ).drop_duplicates("cand_id").set_index("cand_id")
    print(f"{cycle}: reading contributions...", flush=True)
    receipts = read_receipts(cycle, ids)
    print(f"{cycle}: reading committee-to-committee rows...", flush=True)
    committee_rows = read_committee_rows(cycle, ids)

    profiles = {}
    for cmte_id in registry.cmte_id:
        totals, contributors, attributions = summarize_receipts(receipts[receipts.cmte_id == cmte_id])
        flows = summarize_committee_rows(committee_rows[committee_rows.cmte_id == cmte_id], names, candidates, ids)
        transfers_in = sum(p["amount"] for p in flows["received_from_committees"])
        totals["committee_transfers"] = float(transfers_in)
        totals["total"] = float(sum(totals.values()))
        if totals["total"] == 0 and not contributors and not any(flows[k] for k in ("received_from_committees", "given_to_committees")) \
                and not flows["independent_expenditures"]["by_candidate"]:
            continue
        profiles[cmte_id] = {"receipts": totals, "top_contributors": contributors,
                             "attributions": attributions, **flows}
    return profiles


def build(cycles: list[int], output: Path = EXPORT) -> dict:
    registry = load_registry()
    by_cycle = {cycle: build_cycle(cycle, registry) for cycle in sorted(cycles)}
    committees = []
    for row in registry.itertuples(index=False):
        cycles_present = {str(c): by_cycle[c][row.cmte_id] for c in sorted(by_cycle) if row.cmte_id in by_cycle[c]}
        committees.append({"cmte_id": row.cmte_id, "name": row.name, "kind": row.kind, "topic": row.topic,
                           "network": row.network, "company": row.company, "notes": row.notes,
                           "cycles": cycles_present})
    metadata = {
        "schema_version": 1, "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cycles": sorted(cycles), "committee_count": len(committees),
        "method": "All reported receipts of each registry committee, read from the receiving committee's own "
                  "records. Memo rows are listed as attributions and never summed. Transfers between "
                  "committees are shown separately from money from people and organizations.",
    }
    save_json(output / "profiles.json", {"metadata": metadata, "committees": committees})

    receipts, contributors, transfers, spending = [], [], [], []
    for committee in committees:
        key = {"cmte_id": committee["cmte_id"], "committee": committee["name"], "kind": committee["kind"],
               "topic": committee["topic"], "network": committee["network"]}
        for cycle, profile in committee["cycles"].items():
            receipts.append({**key, "cycle": cycle, **profile["receipts"]})
            for role, items in (("contributor", profile["top_contributors"]), ("attribution", profile["attributions"])):
                contributors.extend({**key, "cycle": cycle, "role": role, **item} for item in items)
            for direction, items in (("received_from", profile["received_from_committees"]),
                                     ("given_to", profile["given_to_committees"])):
                transfers.extend({**key, "cycle": cycle, "direction": direction, "other_cmte_id": item["cmte_id"],
                                  "other_committee": item["name"], "amount": item["amount"],
                                  "other_in_registry": item["in_registry"]} for item in items)
            spending.extend({**key, "cycle": cycle, **item}
                            for item in profile["independent_expenditures"]["by_candidate"])
    base = ["cmte_id", "committee", "kind", "topic", "network", "cycle"]
    save_csv(output / "receipts.csv", receipts, base + ["individuals", "organizations", "committees", "candidates",
                                                        "other", "committee_transfers", "total"])
    save_csv(output / "contributors.csv", contributors, base + ["role", "name", "source", "employers", "amount", "gifts"])
    save_csv(output / "transfers.csv", transfers, base + ["direction", "other_cmte_id", "other_committee", "amount",
                                                          "other_in_registry"])
    save_csv(output / "independent_expenditures.csv", spending, base + ["position", "cand_id", "candidate", "party",
                                                                        "office", "amount"])
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("cycles", nargs="+", type=int)
    print(json.dumps(build(parser.parse_args().cycles), indent=2))


if __name__ == "__main__":
    main()
