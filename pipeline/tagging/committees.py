"""
Find committees that may belong in the committee registry.

The committee twin of pipeline.tagging.companies. It sweeps the FEC bulk files
five different ways and writes two files:

    data/reference/committees/
        candidates.csv      — every committee any sweep surfaced, with evidence (overwritten)
        review_queue.csv    — candidates NOT yet in registry.csv (overwritten)

It NEVER touches registry.csv. See data/reference/committees/README.md.

The five sweeps (a committee can be found by several):

    name          tech, AI or crypto words in the committee's name
    employer      at least $250,000 from donors whose employer is a tracked company
    organization  at least $250,000 given directly by tech or crypto organizations
    transfers     at least $100,000 passed to or from a committee already in the registry
    connected     a PAC whose connected organization or name matches a tracked company

Candidate, party and joint-fundraising committees are left out: they are
recipients of tech money, not vehicles for it.

Usage:
    python -m pipeline.tagging.committees            # all available cycles
"""

from __future__ import annotations

import re

import pandas as pd

from pipeline.common.paths import FEC_DERIVED_ROOT, FEC_INTERIM_ROOT, REFERENCE_ROOT
from pipeline.fec.load import CM_COLS, INCLUDE_TYPES_ITCONT, ITCONT_COLS
from pipeline.tagging.companies import TECH_SEARCHES

COMMITTEE_DIR = REFERENCE_ROOT / "committees"
REGISTRY_PATH = COMMITTEE_DIR / "registry.csv"
CANDIDATES_PATH = COMMITTEE_DIR / "candidates.csv"
REVIEW_QUEUE_PATH = COMMITTEE_DIR / "review_queue.csv"

EMPLOYER_MONEY_MIN = 250_000
ORGANIZATION_MONEY_MIN = 250_000
TRANSFER_MIN = 100_000
# House, Senate, presidential, party, and joint-fundraising committees are recipients, not vehicles.
RECIPIENT_TYPES = {"H", "S", "P", "X", "Y", "Z"}

NAME_SEARCH = re.compile(
    r"ARTIFICIAL INTELLIGENCE|(?<![A-Z])A\.?I\.?(?![A-Z.])|\bAGI\b|CRYPTO|BLOCKCHAIN|BITCOIN|DIGITAL ASSET|"
    r"DIGITAL FREEDOM|RESPONSIBLE INNOVATION|TECHNOLOGY|\bTECH\b|TECHNET|VENTURE CAPITAL|SEMICONDUCTOR|"
    r"SOFTWARE|INTERNET|SILICON VALLEY|STARTUP", re.IGNORECASE)
# Organizations that give directly and are not tracked employers.
EXTRA_ORGANIZATIONS = [
    "AH CAPITAL", "A16Z", "ANDREESSEN", "JUMP CRYPTO", "JUMP TRADING", "PARADIGM OPERATIONS", "PAYWARD",
    "KRAKEN", "CIRCLE INTERNET", "UNISWAP", "SOLANA", "TETHER", "BLOCKCHAIN", "CRYPTO", "GEMINI TRUST",
    "WINKLEVOSS", "MULTICOIN", "DIGITAL CURRENCY GROUP", "FOUNDERS FUND", "SV ANGEL", "PERPLEX",
]
ORGANIZATION_SEARCH = re.compile(
    "|".join([p for group in TECH_SEARCHES.values() for p in group] + EXTRA_ORGANIZATIONS), re.IGNORECASE)
COMPANY_SEARCH = re.compile("|".join(p for group in TECH_SEARCHES.values() for p in group), re.IGNORECASE)

READ = dict(sep="|", header=None, dtype="string", na_filter=False, quoting=3,
            encoding_errors="replace", on_bad_lines="skip")
CANDIDATE_COLUMNS = ["cmte_id", "name", "cmte_tp", "connected_org_nm", "cycles", "found_by",
                     "employer_matched_usd", "organization_gifts_usd", "top_organizations",
                     "registry_transfers_usd", "registry_partners"]


def available_cycles() -> list[int]:
    return sorted(int(p.name) for p in FEC_INTERIM_ROOT.iterdir() if p.is_dir() and p.name.isdigit())


def load_registry() -> pd.DataFrame:
    if not REGISTRY_PATH.exists():
        return pd.DataFrame(columns=["cmte_id", "include"])
    return pd.read_csv(REGISTRY_PATH, dtype="string", na_filter=False)


def sweep_cycle(cycle: int, registry_ids: set[str]) -> pd.DataFrame:
    """One row per (committee, sweep) hit in a cycle."""
    yy = str(cycle)[2:]
    base = FEC_INTERIM_ROOT / str(cycle)
    cm = pd.read_csv(base / f"cm{yy}" / "cm.txt", names=CM_COLS, **READ)
    vehicles = cm[~cm.cmte_tp.isin(RECIPIENT_TYPES) & (cm.cmte_dsgn != "J")]
    hits = []

    def add(frame: pd.DataFrame, sweep: str, **columns) -> None:
        hits.append(frame.assign(sweep=sweep, **columns)[["cmte_id", "sweep", *columns]])

    add(vehicles[vehicles.cmte_nm.str.contains(NAME_SEARCH, na=False)], "name")
    add(vehicles[vehicles.cmte_tp.isin(["Q", "N"]) & (
        vehicles.connected_org_nm.str.contains(COMPANY_SEARCH, na=False)
        | vehicles.cmte_nm.str.contains(COMPANY_SEARCH, na=False))], "connected")

    derived = FEC_DERIVED_ROOT / str(cycle) / "committee_tech_receipts.csv"
    if derived.exists():
        matched = pd.read_csv(derived, usecols=["cmte_id", "tech_receipts"], dtype={"cmte_id": "string"})
        matched = matched[matched.tech_receipts >= EMPLOYER_MONEY_MIN]
        add(matched, "employer", employer_matched_usd=matched.tech_receipts)

    print(f"{cycle}: reading contributions for organization gifts...", flush=True)
    columns = ["cmte_id", "transaction_tp", "entity_tp", "name", "transaction_amt", "memo_cd"]
    parts = []
    for chunk in pd.read_csv(base / f"indiv{yy}" / "itcont.txt", names=ITCONT_COLS, usecols=columns,
                             chunksize=2_000_000, **READ):
        organizations = chunk[(chunk.entity_tp == "ORG") & (chunk.memo_cd != "X")
                              & chunk.transaction_tp.isin(INCLUDE_TYPES_ITCONT)]
        parts.append(organizations[organizations.name.str.contains(ORGANIZATION_SEARCH, na=False)])
    gifts = pd.concat(parts)
    gifts["amount"] = pd.to_numeric(gifts.transaction_amt, errors="coerce").fillna(0.0)
    by_committee = gifts.groupby("cmte_id").amount.sum()
    by_committee = by_committee[by_committee >= ORGANIZATION_MONEY_MIN]
    givers = (gifts[gifts.cmte_id.isin(by_committee.index)].groupby(["cmte_id", "name"]).amount.sum()
              .sort_values(ascending=False).reset_index().groupby("cmte_id").name
              .agg(lambda s: "; ".join(s.head(4))))
    organization_hits = by_committee.rename("organization_gifts_usd").reset_index()
    add(organization_hits, "organization", organization_gifts_usd=organization_hits.organization_gifts_usd,
        top_organizations=organization_hits.cmte_id.map(givers))

    print(f"{cycle}: reading committee-to-committee rows...", flush=True)
    other = pd.read_csv(base / f"oth{yy}" / "itoth.txt", names=ITCONT_COLS,
                        usecols=["cmte_id", "transaction_tp", "other_id", "transaction_amt", "memo_cd"], **READ)
    other = other[other.cmte_id.isin(registry_ids) & other.transaction_tp.isin(["24G", "24K", "18G", "18K"])
                  & (other.memo_cd != "X") & (other.other_id != "")]
    other["amount"] = pd.to_numeric(other.transaction_amt, errors="coerce").fillna(0.0)
    pairs = other.groupby(["other_id", "cmte_id"]).amount.sum().reset_index()
    pairs = pairs[pairs.amount >= TRANSFER_MIN]
    names = cm.set_index("cmte_id").cmte_nm
    transfers = pairs.groupby("other_id").agg(
        registry_transfers_usd=("amount", "sum"),
        registry_partners=("cmte_id", lambda s: "; ".join(names.get(i, i) for i in s))).reset_index()
    transfers = transfers.rename(columns={"other_id": "cmte_id"})
    add(transfers, "transfers", registry_transfers_usd=transfers.registry_transfers_usd,
        registry_partners=transfers.registry_partners)

    found = pd.concat(hits, ignore_index=True)
    found = found[found.cmte_id.isin(set(vehicles.cmte_id))]
    info = cm.set_index("cmte_id")[["cmte_nm", "cmte_tp", "connected_org_nm"]]
    return found.join(info, on="cmte_id").assign(cycle=cycle)


def build_candidates(cycles: list[int]) -> pd.DataFrame:
    registry_ids = set(load_registry().query("include == 'TRUE'").cmte_id)
    found = pd.concat([sweep_cycle(cycle, registry_ids) for cycle in cycles], ignore_index=True)

    def joined(values: pd.Series) -> str:
        return "; ".join(sorted({str(v) for v in values if isinstance(v, str) and v}))

    numeric = ["employer_matched_usd", "organization_gifts_usd", "registry_transfers_usd"]
    for column in numeric + ["top_organizations", "registry_partners"]:
        if column not in found:
            found[column] = pd.NA
    # Sum each sweep's dollars across cycles (one row per committee, sweep and cycle).
    out = found.sort_values("cycle").groupby("cmte_id").agg(
        name=("cmte_nm", "last"), cmte_tp=("cmte_tp", "last"), connected_org_nm=("connected_org_nm", "last"),
        cycles=("cycle", lambda s: "; ".join(str(c) for c in sorted(set(s)))),
        found_by=("sweep", joined),
        **{column: (column, "sum") for column in numeric},
        top_organizations=("top_organizations", joined), registry_partners=("registry_partners", joined),
    ).reset_index()
    out["evidence"] = out[numeric].fillna(0).max(axis=1)
    return out.sort_values("evidence", ascending=False)[CANDIDATE_COLUMNS].reset_index(drop=True)


def build_review_queue(candidates: pd.DataFrame) -> pd.DataFrame:
    """Candidates whose committee ID is NOT present in registry.csv."""
    known = set(load_registry().cmte_id)
    unreviewed = candidates[~candidates.cmte_id.isin(known)].copy()
    # Empty decision columns for the reviewer to fill in.
    for column in ["kind", "topic", "network", "company", "include", "notes"]:
        unreviewed[column] = ""
    return unreviewed


def main() -> int:
    candidates = build_candidates(available_cycles())
    candidates.to_csv(CANDIDATES_PATH, index=False)
    print(f"Wrote {len(candidates)} candidates to {CANDIDATES_PATH.name}")
    review = build_review_queue(candidates)
    review.to_csv(REVIEW_QUEUE_PATH, index=False)
    print(f"Wrote {len(review)} unreviewed rows to {REVIEW_QUEUE_PATH.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
