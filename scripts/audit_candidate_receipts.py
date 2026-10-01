"""Audit a company/candidate query without rebuilding or changing site exports.

Example: python -m scripts.audit_candidate_receipts --cycle 2026 \
    --candidate S6MI00418 --company nvidia --output outputs/audit_20260918/nvidia_el_sayed

Only current principal/authorized campaign committees supply the headline
comparison. Former campaign accounts, memo allocations, intermediary forwards,
and possible name-matched refunds are separate evidence, never added blindly.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline.classify_partisan import CCL_COLS, CN_COLS
from pipeline.common.paths import PROJECT_ROOT, company_curated_path, fec_cycle_interim_dir
from pipeline.fec.committee_history import load_converted_campaigns
from pipeline.fec.load import (
    CM_COLS, ITCONT_COLS, INCLUDE_TYPES_ITCONT, MEMO_X_EXCLUDE_TYPES, REFUND_TYPES_ITCONT,
)
from pipeline.fec.transaction_reviews import SOURCE_FIELDS, ReviewedExclusionDependencies, reviewed_exclusion_mask

AUDIT_COLUMNS = [
    *SOURCE_FIELDS,
]
ROUTING_TYPES = {"24T", "24I", "15I", "15T"}
ALLOCATION_TYPES = {"10J", "11J", "15J", "30J", "31J", "32J"}


def read_table(path: Path, columns: list[str]) -> pd.DataFrame:
    return pd.read_csv(path, sep="|", names=columns, dtype="string", na_filter=False)


def current_authorized_links(linkage: pd.DataFrame, committees: pd.DataFrame,
                             candidate_id: str, cycle: int) -> pd.DataFrame:
    """Resolve current campaign ownership independently of historical overlays."""
    cycle_links = linkage.loc[
        linkage.fec_election_yr.eq(str(cycle)) | linkage.cand_election_yr.eq(str(cycle)),
        ["cand_id", "cmte_id"],
    ].drop_duplicates()
    links = cycle_links.merge(
        committees[["cmte_id", "cmte_tp", "cmte_dsgn", "cand_id", "cmte_nm"]],
        on="cmte_id", suffixes=("", "_master"), validate="many_to_one",
    )
    links = links.loc[
        links.cmte_tp.isin(["H", "S", "P"]) & links.cmte_dsgn.isin(["P", "A"])
    ].copy()
    links = links.loc[links.cand_id_master.eq("") | links.cand_id.eq(links.cand_id_master)]
    unambiguous = links.groupby("cmte_id").cand_id.transform("nunique").eq(1)
    return links.loc[unambiguous & links.cand_id.eq(candidate_id)].copy()


def select_evidence(chunk: pd.DataFrame, aliases: set[str], current_ids: set[str],
                    former_ids: set[str], candidate_id: str, search: str, *,
                    cycle: int | None = None, source_file: str = "itcont.txt") -> pd.DataFrame:
    """Keep small review sets; do not assign unmatched refunds by name."""
    employer = chunk.employer.str.strip().str.upper()
    exact = employer.isin(aliases)
    receiving = chunk.cmte_id.isin(current_ids | former_ids)
    route_target = chunk.other_id.isin(current_ids | former_ids | {candidate_id})
    possible_alias = employer.str.contains(search.upper(), regex=False) if search else exact
    keep = (
        (receiving & (exact | possible_alias | chunk.transaction_tp.isin(REFUND_TYPES_ITCONT)))
        | (route_target & exact)
    )
    rows = chunk.loc[keep].copy()
    rows["employer_exact_match"] = exact.loc[keep]
    rows["account_scope"] = "intermediary_or_other_filer"
    rows.loc[rows.cmte_id.isin(former_ids), "account_scope"] = "former_campaign_account"
    rows.loc[rows.cmte_id.isin(current_ids), "account_scope"] = "current_authorized_campaign"
    rows["numeric_amount"] = pd.to_numeric(rows.transaction_amt, errors="coerce")
    rows["invalid_amount"] = rows.numeric_amount.isna()
    rows["is_refund"] = rows.transaction_tp.isin(REFUND_TYPES_ITCONT)
    rows["signed_amount"] = rows.numeric_amount.where(~rows.is_refund, -rows.numeric_amount)
    rows["core_included"] = (
        rows.transaction_tp.isin(INCLUDE_TYPES_ITCONT | REFUND_TYPES_ITCONT)
        & (~rows.memo_cd.eq("X") | ~rows.transaction_tp.isin(MEMO_X_EXCLUDE_TYPES))
    )
    rows["exclusion_reason"] = "not_a_selected_receipt_type"
    rows.loc[rows.transaction_tp.isin(ROUTING_TYPES), "exclusion_reason"] = "intermediary_routing"
    rows.loc[rows.transaction_tp.isin(ALLOCATION_TYPES), "exclusion_reason"] = "allocation_memo"
    rows.loc[rows.memo_cd.eq("X") & rows.transaction_tp.isin(MEMO_X_EXCLUDE_TYPES), "exclusion_reason"] = "unresolved_15E_memo"
    rows.loc[rows.core_included, "exclusion_reason"] = ""
    reviewed = reviewed_exclusion_mask(rows, cycle, source_file=source_file)
    rows.loc[reviewed, "core_included"] = False
    rows.loc[reviewed, "exclusion_reason"] = "source_reviewed_repeated_original"
    return rows


def scan(path: Path, aliases: set[str], current_ids: set[str], former_ids: set[str],
         candidate_id: str, search: str, chunksize: int, *, cycle: int | None = None) -> tuple[pd.DataFrame, int, dict]:
    before = path.stat()
    parts, scanned = [], 0
    review_dependencies = ReviewedExclusionDependencies(cycle, source_file=path.name)
    with pd.read_csv(path, sep="|", names=ITCONT_COLS, usecols=AUDIT_COLUMNS,
                     dtype="string", na_filter=False, chunksize=chunksize) as reader:
        for chunk in reader:
            scanned += len(chunk)
            review_dependencies.observe(chunk)
            selected = select_evidence(chunk, aliases, current_ids, former_ids, candidate_id, search,
                                       cycle=cycle, source_file=path.name)
            if not selected.empty:
                parts.append(selected)
            if scanned % 5_000_000 == 0:
                print(f"{path.name}: scanned {scanned:,} rows", flush=True)
    review_dependency_status = review_dependencies.finalize()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"Input changed during audit: {path}")
    empty = pd.DataFrame({column: pd.Series(dtype="string") for column in AUDIT_COLUMNS})
    frame = pd.concat(parts, ignore_index=True) if parts else select_evidence(
        empty, aliases, current_ids, former_ids, candidate_id, search, cycle=cycle, source_file=path.name)
    frame["source_file"] = path.name
    return frame, scanned, {
        "path": str(path.resolve()), "bytes": after.st_size,
        "modified_at_utc": datetime.fromtimestamp(after.st_mtime, timezone.utc).isoformat(),
        "review_dependencies": review_dependency_status,
    }


def summarize(evidence: pd.DataFrame) -> dict:
    primary = evidence.loc[
        evidence.source_file.eq("itcont.txt")
        & evidence.account_scope.eq("current_authorized_campaign")
        & evidence.employer_exact_match & evidence.core_included
    ].copy()
    if primary.invalid_amount.any():
        raise ValueError("Selected campaign receipts contain invalid amounts; review before totaling")
    names = set(primary.name)
    possible_refunds = evidence.loc[
        evidence.source_file.eq("itcont.txt")
        & evidence.account_scope.eq("current_authorized_campaign")
        & evidence.is_refund & ~evidence.employer_exact_match & evidence.name.isin(names)
    ]
    dates = pd.to_datetime(primary.transaction_dt, format="%m%d%Y", errors="coerce")
    exact = evidence.loc[evidence.employer_exact_match]
    breakdown = exact.groupby(
        ["source_file", "account_scope", "transaction_tp", "memo_cd", "core_included"], dropna=False,
    ).agg(rows=("sub_id", "size"), raw_amount=("numeric_amount", "sum"),
          signed_amount=("signed_amount", "sum")).reset_index()
    return {
        "current_authorized_net": float(primary.signed_amount.sum()),
        "current_authorized_rows": len(primary),
        "current_authorized_distinct_name_strings": int(primary.name.nunique()),
        "current_authorized_entity_IND_net": float(primary.loc[primary.entity_tp.eq("IND"), "signed_amount"].sum()),
        "positive_receipts": float(primary.loc[~primary.is_refund & primary.numeric_amount.gt(0), "numeric_amount"].sum()),
        "negative_receipt_adjustments": float(primary.loc[~primary.is_refund & primary.numeric_amount.lt(0), "numeric_amount"].sum()),
        "matched_refunds_signed": float(primary.loc[primary.is_refund, "signed_amount"].sum()),
        "earliest_included_receipt_date": None if dates.isna().all() else str(dates.min().date()),
        "latest_included_receipt_date": None if dates.isna().all() else str(dates.max().date()),
        "undated_included_rows": int(dates.isna().sum()),
        "potential_same_name_unmatched_refund_rows": len(possible_refunds),
        "potential_same_name_unmatched_refunds_signed": float(possible_refunds.signed_amount.sum()),
        "duplicate_sub_id_rows": int(primary.loc[primary.sub_id.ne("")].sub_id.duplicated(keep=False).sum()),
        "reused_transaction_ids_across_filings": int(primary.groupby(["cmte_id", "tran_id"]).file_num.nunique().gt(1).sum()),
        "breakdown": json.loads(breakdown.to_json(orient="records")),
    }


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycle", type=int, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--company", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--chunksize", type=int, default=250_000)
    options = parser.parse_args(args)
    if options.chunksize <= 0:
        parser.error("chunksize must be positive")
    base = fec_cycle_interim_dir(options.cycle)
    suffix = str(options.cycle)[-2:]
    committees = read_table(base / f"cm{suffix}/cm.txt", CM_COLS)
    linkage = read_table(base / f"ccl{suffix}/ccl.txt", CCL_COLS)
    candidates = read_table(base / f"cn{suffix}/cn.txt", CN_COLS)
    candidate = candidates.loc[candidates.cand_id.eq(options.candidate)]
    if len(candidate) != 1:
        raise ValueError("Candidate ID must identify one row in the selected cycle master")
    current = current_authorized_links(linkage, committees, options.candidate, options.cycle)
    current_ids = set(current.cmte_id)
    if not current_ids:
        raise ValueError("No unambiguous current authorized campaign committees; do not infer from former accounts")
    former = load_converted_campaigns(options.cycle)
    former_ids = set(former.loc[former.cand_id.eq(options.candidate), "cmte_id"]) - current_ids
    aliases_table = pd.read_csv(company_curated_path(), dtype="string", na_filter=False)
    aliases = set(aliases_table.loc[
        aliases_table.include.str.upper().eq("TRUE")
        & aliases_table.canonical_name.eq(options.company), "employer",
    ].str.strip().str.upper())
    if not aliases:
        raise ValueError("Company has no included employer aliases")
    frames, provenance = [], []
    for folder, filename in [(f"indiv{suffix}", "itcont.txt"), (f"oth{suffix}", "itoth.txt")]:
        frame, count, source = scan(base / folder / filename, aliases, current_ids, former_ids,
                                    options.candidate, options.company, options.chunksize, cycle=options.cycle)
        frames.append(frame)
        provenance.append(source | {"scanned_rows": count})
    evidence = pd.concat(frames, ignore_index=True)
    result = summarize(evidence)
    export_path = PROJECT_ROOT / f"exports/site/{options.cycle}/companies/{options.company}.json"
    comparison = {"available": False}
    if export_path.exists():
        payload = json.loads(export_path.read_text(encoding="utf-8"))
        matched = [row for row in payload.get("top_committees", []) if row["cmte_id"] in current_ids]
        comparison = {"available": True, "path": str(export_path),
                      "complete_for_current_accounts": {row["cmte_id"] for row in matched} == current_ids,
                      "listed_committee_net": sum(row["net_total"] for row in matched)}
    options.output.mkdir(parents=True, exist_ok=True)
    evidence = evidence.sort_values(["source_file", "account_scope", "cmte_id", "sub_id"])
    evidence_bytes = evidence.to_csv(index=False).encode("utf-8")
    (options.output / "evidence.csv").write_bytes(evidence_bytes)
    result.update({
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "cycle": options.cycle, "candidate_id": options.candidate,
        "candidate_name": candidate.iloc[0].cand_name, "company": options.company,
        "current_authorized_committees": json.loads(current[["cmte_id", "cmte_nm"]].to_json(orient="records")),
        "former_campaign_account_ids": sorted(former_ids), "employer_aliases": sorted(aliases),
        "source_files": provenance, "company_export_comparison": comparison,
        "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
        "scope": "Two-year bulk snapshot; current authorized accounts only. Routing, allocation memos, former accounts, and name-only refund matches are not added to headline.",
        "limitation": "Self-reported employer aliases and distinct name strings do not verify employment or donor identity. No claim to lifetime giving, all candidate benefit, or exact cross-filer reconciliation.",
    })
    (options.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key not in {"breakdown", "employer_aliases"}}, indent=2))


if __name__ == "__main__":
    main()
