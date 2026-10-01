"""Read-only audit of installed bulk records against selected original filings.

The scan reads itcont once, preserving exact target-committee source rows in
local audit gzip files. They contain source location fields and must never be
copied to the public site. Public comparison tables omit those fields.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timezone
from decimal import Decimal
import csv
import gzip
import hashlib
import json
from pathlib import Path
import time

import pandas as pd

from pipeline.common.paths import PROJECT_ROOT, company_curated_path
from pipeline.fec.load import (
    CM_COLS, ITCONT_COLS, INCLUDE_TYPES_ITCONT, REFUND_TYPES_ITCONT,
    MEMO_X_EXCLUDE_TYPES, _load_tech_employers, validated_tech_lookup,
)
from pipeline.fec.transaction_reviews import (
    PUBLIC_SIGNATURE_FIELDS, ReviewedExclusionDependencies, reviewed_exclusion_mask,
    source_row_sha256, REFERENCE,
)


PUBLIC_COLUMNS = ["source_row_number", "source_row_sha256", *PUBLIC_SIGNATURE_FIELDS,
                  "canonical_company", "committee_type", "legacy_selected",
                  "legacy_exclusion_reason", "legacy_net_amount"]


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def dump_json(path: Path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def decimal_string(value):
    return format(value, ".2f")


def aliases_from_curated() -> dict[str, str]:
    lookup = validated_tech_lookup(_load_tech_employers())
    return dict(zip(lookup["employer_upper"], lookup["canonical_name"]))


def legacy_selection(row: dict, excluded_ids: set[str]) -> tuple[bool, str, Decimal]:
    """Use the actual imported bulk selector's type/memo/refund rules.

    pandas conversion mirrors the production loader's numeric coercion. Raw
    amount strings remain separately retained for all precision comparisons.
    """
    amount = pd.to_numeric(row["transaction_amt"], errors="coerce")
    amount = Decimal("0") if pd.isna(amount) else Decimal(str(amount))
    if not amount.is_finite():
        raise ValueError("Nonfinite bulk amount requires source review")
    net = -amount if row["transaction_tp"] in REFUND_TYPES_ITCONT else amount
    if row["sub_id"] in excluded_ids:
        return False, "reviewed_exact_source_exclusion", net
    if row["transaction_tp"] not in INCLUDE_TYPES_ITCONT | REFUND_TYPES_ITCONT:
        return False, "transaction_type_not_selected", net
    if row["memo_cd"] == "X" and row["transaction_tp"] in MEMO_X_EXCLUDE_TYPES:
        return False, "memo_type_exclusion", net
    return True, "", net


def public_bulk_row(row, row_number, aliases, committees, excluded_ids):
    selected, reason, net = legacy_selection(row, excluded_ids)
    return {
        "source_row_number": row_number, "source_row_sha256": source_row_sha256(row),
        **{key: row[key] for key in PUBLIC_SIGNATURE_FIELDS},
        "canonical_company": aliases.get(row["employer"].strip().upper(), ""),
        "committee_type": committees.get(row["cmte_id"], {}).get("cmte_tp", ""),
        "legacy_selected": selected, "legacy_exclusion_reason": reason,
        "legacy_net_amount": decimal_string(net),
    }


def scan_bulk(cycle: int, bulk_path: Path, output: Path, committee_ids: list[str]):
    output.mkdir(parents=True, exist_ok=True)
    suffix = str(cycle)[2:]
    cm_path = PROJECT_ROOT / f"data/fec/interim/{cycle}/cm{suffix}/cm.txt"
    with cm_path.open(encoding="utf-8", newline="") as stream:
        committees = {}
        for values in csv.reader(stream, delimiter="|"):
            row = dict(zip(CM_COLS, values))
            if row["cmte_id"] in committees:
                raise ValueError("Duplicate committee-master identity")
            committees[row["cmte_id"]] = row
    aliases = aliases_from_curated()
    review_dependencies = ReviewedExclusionDependencies(cycle)
    excluded_ids = set(review_dependencies.reviews)
    watched_ids = review_dependencies.watched_ids
    target_bytes = {value.encode("ascii"): value for value in committee_ids}
    start_stat = bulk_path.stat()
    start_time = time.monotonic()
    started_at = utcnow()
    digest = hashlib.sha256()
    matched_count = selected_count = scanned = 0
    target_counts = dict.fromkeys(committee_ids, 0)
    group_stats = defaultdict(lambda: {"record_count": 0, "raw_amount_sum": Decimal(0),
                                       "net_amount_sum": Decimal(0), "positive_records": 0,
                                       "negative_records": 0, "zero_records": 0})
    watched = []
    temp_paths = []
    with ExitStack() as stack:
        raw_handles, target_writers = {}, {}
        for committee in committee_ids:
            raw_path = output / f"{committee}.raw.psv.gz"
            index_path = output / f"{committee}.public.csv.gz"
            for final_path in (raw_path, index_path):
                temp_paths.append((final_path.with_name(final_path.name + ".tmp"), final_path))
            raw_handles[committee] = stack.enter_context(gzip.open(temp_paths[-2][0], "wb", compresslevel=1))
            handle = stack.enter_context(gzip.open(temp_paths[-1][0], "wt", encoding="utf-8", newline="", compresslevel=1))
            target_writers[committee] = csv.DictWriter(handle, fieldnames=PUBLIC_COLUMNS)
            target_writers[committee].writeheader()
        matched_path = output / "all_curated_employer_matches.public.csv.gz"
        matched_temp = matched_path.with_name(matched_path.name + ".tmp")
        temp_paths.append((matched_temp, matched_path))
        matched_stream = stack.enter_context(gzip.open(matched_temp, "wt", encoding="utf-8", newline="", compresslevel=1))
        matched_writer = csv.DictWriter(matched_stream, fieldnames=PUBLIC_COLUMNS)
        matched_writer.writeheader()
        with bulk_path.open("rb", buffering=4 * 1024 * 1024) as source:
            for scanned, raw in enumerate(source, 1):
                digest.update(raw)
                pieces = raw.rstrip(b"\r\n").split(b"|")
                # Match pandas' CSV quote handling when a quote occurs. The raw
                # bytes are still copied unchanged to target committee extracts.
                if b'"' in raw:
                    values = next(csv.reader([raw.decode("utf-8")], delimiter="|"))
                    pieces = [part.encode("utf-8") for part in values]
                if len(pieces) != 21:
                    raise ValueError(f"Expected 21 bulk fields at source row {scanned}; found {len(pieces)}")
                company = aliases.get(pieces[11].decode("utf-8").strip().upper())
                target = target_bytes.get(pieces[0])
                watched_id = pieces[20].decode("ascii")
                if company or target or watched_id in watched_ids:
                    row = dict(zip(ITCONT_COLS, (part.decode("utf-8") for part in pieces)))
                    if watched_id in watched_ids:
                        watched.append(row)
                    public = public_bulk_row(row, scanned, aliases, committees, excluded_ids)
                    if target:
                        raw_handles[target].write(raw)
                        target_writers[target].writerow(public)
                        target_counts[target] += 1
                    if company:
                        matched_writer.writerow(public)
                        matched_count += 1
                        selected_count += int(public["legacy_selected"])
                        key = (company, row["cmte_id"], public["committee_type"], row["transaction_tp"],
                               row["memo_cd"], row["entity_tp"], public["legacy_selected"], public["legacy_exclusion_reason"])
                        metrics = group_stats[key]
                        amount = Decimal(public["legacy_net_amount"])
                        raw_amount = pd.to_numeric(row["transaction_amt"], errors="coerce")
                        metrics["record_count"] += 1
                        metrics["raw_amount_sum"] += Decimal(0) if pd.isna(raw_amount) else Decimal(str(raw_amount))
                        metrics["net_amount_sum"] += amount
                        metrics["positive_records"] += amount > 0
                        metrics["negative_records"] += amount < 0
                        metrics["zero_records"] += amount == 0
                if scanned % 2_000_000 == 0:
                    progress = {"status": "scanning", "rows_scanned": scanned, "curated_matches": matched_count,
                                "targets": target_counts, "elapsed_seconds": round(time.monotonic() - start_time, 1)}
                    dump_json(output / "scan_progress.json", progress)
                    print(json.dumps(progress), flush=True)
        watched_frame = pd.DataFrame(watched, columns=ITCONT_COLS)
        review_dependencies.observe(watched_frame)
        verified = review_dependencies.finalize()
        if int(reviewed_exclusion_mask(watched_frame, cycle).sum()) != len(review_dependencies.seen_exclusions):
            raise ValueError("Unexpected duplicate reviewed exclusion identity")
        end_stat = bulk_path.stat()
        if (start_stat.st_size, start_stat.st_mtime_ns) != (end_stat.st_size, end_stat.st_mtime_ns):
            raise ValueError("Bulk source changed during the scan")
    for temporary, final in temp_paths:
        temporary.replace(final)
    dimensions = ["canonical_company", "committee_id", "committee_type", "transaction_type", "memo_cd",
                  "entity_type", "legacy_selected", "legacy_exclusion_reason"]
    metrics_keys = ["record_count", "raw_amount_sum", "net_amount_sum", "positive_records", "negative_records", "zero_records"]
    with (output / "curated_match_groups.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=dimensions + metrics_keys)
        writer.writeheader()
        for key, metrics in sorted(group_stats.items()):
            writer.writerow({**dict(zip(dimensions, key)), **{name: decimal_string(value) if isinstance(value, Decimal) else value
                                                            for name, value in metrics.items()}})
    manifest = {
        "status": "complete", "cycle": cycle, "started_at": started_at, "completed_at": utcnow(),
        "source_path": str(bulk_path.resolve()), "source_bytes": start_stat.st_size,
        "source_modified_utc": datetime.fromtimestamp(start_stat.st_mtime, timezone.utc).isoformat(),
        "source_sha256": digest.hexdigest(), "source_rows": scanned,
        "target_committee_rows": target_counts, "curated_employer_matched_rows": matched_count,
        "legacy_selected_curated_rows": selected_count, "review_dependencies": verified,
        "curated_aliases_sha256": hashlib.sha256(company_curated_path().read_bytes()).hexdigest(),
        "reviewed_exclusions_sha256": hashlib.sha256(REFERENCE.read_bytes()).hexdigest(),
        "selector_module_sha256": hashlib.sha256((PROJECT_ROOT / "pipeline/fec/load.py").read_bytes()).hexdigest(),
        "selector": {"include_types": sorted(INCLUDE_TYPES_ITCONT), "refund_types": sorted(REFUND_TYPES_ITCONT),
                     "memo_x_excluded_types": sorted(MEMO_X_EXCLUDE_TYPES), "employer_match": "strip then uppercase, exact included curated alias"},
        "raw_extract_warning": "Exact raw 21-field target extracts contain location fields. Local audit only; do not publish.",
        "public_fields": PUBLIC_COLUMNS,
    }
    dump_json(output / "scan_manifest.json", manifest)
    dump_json(output / "scan_progress.json", {"status": "complete", "rows_scanned": scanned, "targets": target_counts})
    print(json.dumps(manifest, indent=2), flush=True)
    return manifest


def bulk_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%m%d%Y").date().isoformat() if len(value) == 8 else value
    except ValueError:
        return value


def normalize_name(value: str) -> str:
    """Formatting comparison only; never used to join or identify a person."""
    return " ".join(value.upper().replace(",", " ").split())


def exact_source_differences(bulk: dict, source: dict) -> list[str]:
    """Describe facts at the same filing/transaction key, without name matching."""
    differences = []
    if bulk["employer"].strip().upper() != source["employer"].strip().upper():
        differences.append("employer_difference")
    if normalize_name(bulk["name"]) != normalize_name(source["donor_name"]):
        differences.append("reported_name_difference")
    if bulk["entity_tp"] != source["entity_type"]:
        differences.append("entity_type_difference")
    if bulk_date(bulk["transaction_dt"]) != source["date"]:
        differences.append("date_difference")
    if bulk["transaction_pgi"] != source["election"]:
        differences.append("election_designation_difference")
    if (bulk["memo_cd"] == "X") != source["memo"]:
        differences.append("memo_status_difference")
    raw_bulk = Decimal(bulk["transaction_amt"])
    original = Decimal(source["amount"])
    if raw_bulk != original:
        # This is a demonstrated difference between retained source strings,
        # not a general claim that FEC bulk amounts are always rounded.
        if (not differences and raw_bulk == raw_bulk.to_integral_value()
                and original != original.to_integral_value() and abs(raw_bulk - original) < 1):
            differences.append("bulk_precision_difference")
        else:
            differences.append("amount_difference")
    expected_net = -raw_bulk if bulk["transaction_tp"] in REFUND_TYPES_ITCONT else raw_bulk
    if Decimal(bulk["legacy_net_amount"]) != expected_net:
        differences.append("legacy_numeric_conversion_difference")
    return differences


def compare_sources(manifest_path: Path, bulk_index: Path, source_dirs: list[Path], output: Path):
    # Imports occur in the comparison process, after the scan, so this uses the
    # currently installed attribution engine rather than a scan-time snapshot.
    from pipeline.fec.filings import parse_filing, latest_filings
    from pipeline.fec.campaign_reports import build_report, classify_records
    from pipeline.fec.attribution import attribute_campaign

    engine_sha256 = hashlib.sha256((PROJECT_ROOT / "pipeline/fec/attribution.py").read_bytes()).hexdigest()
    report_builder_sha256 = hashlib.sha256((PROJECT_ROOT / "pipeline/fec/campaign_reports.py").read_bytes()).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    aliases = aliases_from_curated()
    filings = []
    for source in manifest["sources"]:
        number = source["file_number"]
        path = next((folder / f"{number}.fec" for folder in source_dirs if (folder / f"{number}.fec").is_file()), None)
        if path is None:
            raise ValueError(f"Missing original filing {number}")
        filings.append(parse_filing(path, number, expected_sha256=source["sha256"]))
    latest = latest_filings(filings)
    if manifest.get("selected_record_ids") is not None:
        raise ValueError("Full campaign comparison requires all latest report records, not a selected excerpt")
    records = [record for filing in latest for record in filing.records]
    normalized = classify_records(records, manifest.get("reviews", []))
    with gzip.open(bulk_index, "rt", encoding="utf-8", newline="") as stream:
        bulk_rows = list(csv.DictReader(stream))
    committees = set(manifest["scope"]["committee_ids"])
    bulk_rows = [row for row in bulk_rows if row["cmte_id"] in committees]
    by_bulk, by_source = defaultdict(list), {}
    for row in bulk_rows:
        by_bulk[(row["cmte_id"], row["file_num"], row["tran_id"])].append(row)
    for row in records:
        key = (row["committee_id"], str(row["file_number"]), row["transaction_id"])
        if key in by_source:
            raise ValueError(f"Ambiguous original source identity: {key}")
        by_source[key] = row
    company_keys = {row["canonical_company"] for row in bulk_rows if row["canonical_company"]}
    company_keys |= {aliases.get(row["employer"].strip().upper()) for row in records}
    company_keys.discard(None)
    company_keys.discard("")
    results, reports = {}, {}
    for company in sorted(company_keys):
        candidate_manifest = {**manifest, "company": {"key": company, "label": company}, "employer_aliases": aliases}
        reports[company] = build_report(candidate_manifest, filings)
        complete_refunds = all(Decimal(f.metadata["refunds_unitemized_or_unreconciled"]) == 0 for f in latest)
        results[company] = attribute_campaign(normalized, aliases, target_company=company, refund_scope_complete=complete_refunds)
    first_result = next(iter(results.values()), None)
    resolved_company = {d.record_id: d.canonical_company for d in first_result.decisions} if first_result else {}
    decisions = {company: {d.record_id: d for d in result.decisions} for company, result in results.items()}
    row_comparisons, counters = [], defaultdict(int)
    latest_ids = {str(f.metadata["file_number"]) for f in latest}
    all_ids = {str(f.metadata["file_number"]) for f in filings}
    for key in sorted(set(by_bulk) | set(by_source)):
        old = by_bulk.get(key, [])
        original = by_source.get(key)
        companies = {row["canonical_company"] for row in old if row["canonical_company"]}
        original_company = resolved_company.get(original["record_id"]) if original else None
        if original_company:
            companies.add(original_company)
        if not companies:
            continue
        facts = []
        if len(old) > 1:
            facts.append("ambiguous_bulk_join_multiple_sub_ids")
        if not old:
            facts.append("original_only")
        if not original:
            facts.append("bulk_only")
            facts.append("superseded_file_version" if key[1] in all_ids - latest_ids else "absent_from_latest_original_scope")
        if len(old) == 1 and original:
            facts.extend(exact_source_differences(old[0], original))
        if not facts:
            facts.append("same_source_facts")
        for company in sorted(companies):
            decision = decisions[company].get(original["record_id"]) if original else None
            old_selected = [row for row in old if row["canonical_company"] == company and row["legacy_selected"] == "True"]
            old_amount = sum((Decimal(row["legacy_net_amount"]) for row in old_selected), Decimal(0))
            supported = (decision.amount if decision and decision.status == "included" and decision.component != "refunds" else Decimal(0))
            treatment = []
            if old_selected and decision and decision.status == "unresolved":
                treatment.append("legacy_selected_now_unresolved")
            elif decision and decision.status == "included" and not old_selected:
                treatment.append("new_supported_record_not_in_legacy_selection")
            elif old_selected and decision and decision.status != "included":
                treatment.append("legacy_selected_new_" + decision.status)
            if decision and decision.component == "refunds":
                treatment.append("refund_basis_separate_from_pre_refund_receipts")
            if original_company and any(row["canonical_company"] != original_company for row in old):
                treatment.append("company_attribution_difference")
            flags = facts + treatment
            for flag in flags:
                counters[flag] += 1
            row_comparisons.append({
                "company": company, "committee_id": key[0], "file_number": key[1], "transaction_id": key[2],
                "categories": " | ".join(flags), "bulk_sub_ids": " | ".join(row["sub_id"] for row in old),
                "bulk_row_count": len(old), "bulk_source_row_numbers": " | ".join(row["source_row_number"] for row in old),
                "bulk_raw_amounts": " | ".join(row["transaction_amt"] for row in old),
                "bulk_employers": " | ".join(row["employer"] for row in old),
                "bulk_names": " | ".join(row["name"] for row in old),
                "bulk_transaction_types": " | ".join(row["transaction_tp"] for row in old),
                "bulk_memo_codes": " | ".join(row["memo_cd"] for row in old),
                "bulk_exclusion_reasons": " | ".join(row["legacy_exclusion_reason"] for row in old),
                "legacy_selected_count": len(old_selected), "legacy_selected_record_amount": decimal_string(old_amount),
                "original_record_id": original["record_id"] if original else "",
                "original_raw_amount": original["amount"] if original else "",
                "original_employer": original["employer"] if original else "",
                "original_name": original["donor_name"] if original else "",
                "original_schedule": original["schedule"] if original else "",
                "original_memo": original["memo"] if original else "",
                "original_source_row_sha256": original["source_row_sha256"] if original else "",
                "original_source_url": original["source_url"] if original else "",
                "new_role": decision.role if decision else "outside_supplied_originals",
                "new_disposition": decision.status if decision else "outside_supplied_originals",
                "new_component": decision.component or "" if decision else "",
                "new_supported_before_refunds_amount": decimal_string(supported),
                "component_difference_not_total_delta": decimal_string(supported - old_amount),
            })
    with (output / "record_comparison.csv").open("w", encoding="utf-8", newline="") as stream:
        if row_comparisons:
            writer = csv.DictWriter(stream, fieldnames=list(row_comparisons[0]))
            writer.writeheader()
            writer.writerows(row_comparisons)
    company_summaries = []
    for company in sorted(company_keys):
        report = reports[company]
        result = results[company]
        selected = [row for row in bulk_rows if row["canonical_company"] == company and row["legacy_selected"] == "True"]
        old_amount = sum((Decimal(row["legacy_net_amount"]) for row in selected), Decimal(0))
        supported = sum((value for component, value in result.components.items() if component != "refunds"), Decimal(0))
        unresolved_target = [d for d in result.decisions if d.status == "unresolved" and d.canonical_company == company]
        source_amounts = {row["record_id"]: Decimal(row["amount"]) for row in records}
        company_summaries.append({
            "company": company, "legacy_selected_records": len(selected), "legacy_selected_record_sum": decimal_string(old_amount),
            "supported_components_before_refunds_sum": decimal_string(supported),
            "supported_component_difference_not_total_delta": decimal_string(supported - old_amount),
            "combined_amount": report["combined_amount"], "combined_status": report["combined_status"],
            "net_amount": report["net_amount"], "net_status": report["net_status"],
            "components": report["components"], "unresolved_receipt_count": sum(r["scope"] == "receipts" for r in report["unresolved"]),
            "unresolved_refund_count": sum(r["scope"] == "refunds" for r in report["unresolved"]),
            "unresolved_matched_record_count": len(unresolved_target),
            "unresolved_matched_raw_signed_amount": decimal_string(sum((source_amounts[d.record_id] for d in unresolved_target), Decimal(0))),
            "unresolved_matched_raw_absolute_amount": decimal_string(sum((abs(source_amounts[d.record_id]) for d in unresolved_target), Decimal(0))),
            "receipt_issues": [row for row in report["unresolved"] if row["scope"] == "receipts"],
        })
    comparison = {
        "created_at": utcnow(), "manifest_path": str(manifest_path), "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "bulk_index_path": str(bulk_index), "join": "Exact committee_id + file_number + transaction_id; never donor names or amounts",
        "scope": manifest["scope"], "source_filing_count": len(filings), "latest_source_filing_count": len(latest),
        "latest_file_numbers": sorted(latest_ids), "bulk_committee_record_count": len(bulk_rows), "original_schedule_record_count": len(records),
        "company_count": len(company_summaries), "comparison_row_count": len(row_comparisons), "category_counts": dict(counters),
        "company_results": company_summaries,
        "engine_sha256": engine_sha256, "report_builder_sha256": report_builder_sha256,
        "precision_note": "Raw bulk and original amount strings are retained. bulk_precision_difference means an exact-key raw whole-dollar bulk amount differs from source cents by less than $1 while compared identities agree; it is not a generalized claim about FEC rounding. The official bulk schema permits cents.",
        "official_bulk_schema": "https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/",
        "limitations": ["Supported component differences are diagnostics, not changes in a certified total when combined_status is unresolved.",
                        "Missing original/bulk records are classified by exact source identity; donor-name guesses are never used to fill gaps.",
                        "No unknown refund is assigned to an employer. Net remains unavailable when refund coverage or identity is unresolved."],
    }
    dump_json(output / "comparison_summary.json", comparison)
    print(json.dumps({k: comparison[k] for k in ("source_filing_count", "latest_source_filing_count", "company_count", "comparison_row_count", "category_counts")}, indent=2), flush=True)
    return comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("scan")
    scan.add_argument("--cycle", type=int, default=2026)
    scan.add_argument("--bulk", type=Path)
    scan.add_argument("--output", type=Path, required=True)
    scan.add_argument("--committee", action="append", required=True)
    compare = sub.add_parser("compare")
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--bulk-index", type=Path, required=True)
    compare.add_argument("--source-dir", type=Path, action="append", required=True)
    compare.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "scan":
        bulk = args.bulk or PROJECT_ROOT / f"data/fec/interim/{args.cycle}/indiv{str(args.cycle)[2:]}/itcont.txt"
        scan_bulk(args.cycle, bulk, args.output, args.committee)
    elif args.command == "compare":
        compare_sources(args.manifest, args.bulk_index, args.source_dir, args.output)


if __name__ == "__main__":
    main()
