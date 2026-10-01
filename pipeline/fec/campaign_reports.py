"""Build bounded, reproducible employer-to-campaign attribution reports.

The reviewed manifest defines the question. It never expands a partial example
into a complete candidate total, and it never updates the global bulk selector.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import re

from pipeline.fec.attribution import AttributionRecord, attribute_campaign
from pipeline.fec.filings import Filing, latest_filings


DESCRIPTIONS = {
    "direct_receipts": "Matched individual nonmemo receipts on the campaign's supported contribution line; may include in-kind contributions.",
    "jfc_allocations": "Matched donor allocations attached to campaign joint-fundraising transfers; the parent transfer is not added again.",
    "partnership_attributions": "Matched individual shares of a fully represented partnership receipt; the organizational parent is not added again.",
    "signed_adjustments": "Source-reviewed changes retained with their filed signs. Balanced election redesignations add zero overall.",
    "refunds": "Only refunds with a supported donor relationship can adjust the matched employer amount. Unknown is not zero.",
}


def classify_records(records: list[dict], reviews: list[dict]) -> list[AttributionRecord]:
    """Infer only explicit structural relationships; verify every review dependency.

    All supplied review rows must be present with unchanged hashes. Thus a
    reviewed balanced family cannot silently lose one of its required siblings.
    """
    by_id = {r["record_id"]: r for r in records}
    if len(by_id) != len(records):
        raise ValueError("Repeated source identity")
    annotations = {}
    for review in reviews:
        key = review["record_id"]
        if set(review) - {"record_id", "source_row_sha256", "role", "related_record_id", "identity_key", "evidence", "rationale"}:
            raise ValueError(f"Reviews may not overwrite filed source fields: {key}")
        if key in annotations:
            raise ValueError(f"Repeated review annotation: {key}")
        if key not in by_id or by_id[key]["source_row_sha256"] != review["source_row_sha256"]:
            raise ValueError(f"Required reviewed source row missing or changed: {key}")
        if not review.get("evidence") or not review.get("rationale"):
            raise ValueError(f"Review needs evidence and rationale: {key}")
        if review.get("related_record_id") and review["related_record_id"] not in by_id:
            raise ValueError(f"Required reviewed parent missing: {key}")
        annotations[key] = review
    allowed = {f.name for f in fields(AttributionRecord)}
    output = []
    for source in records:
        row = {k: v for k, v in source.items() if k in allowed}
        key = row["record_id"]
        parent_id = f'{row["file_number"]}:{row["back_reference_transaction_id"]}'
        parent = by_id.get(parent_id)
        description = " ".join([source.get("memo_text", ""), source.get("purpose", "")]).upper()
        if key in annotations:
            row.update({k: v for k, v in annotations[key].items()
                        if k in {"role", "related_record_id", "identity_key", "evidence"}})
        elif (row["memo"] and row["entity_type"] == "IND" and parent and not parent["memo"]
              and parent["entity_type"] in {"ORG", "PART"}
              and ("PARTNERSHIP" in description or "PARTNER ATTRIBUTION" in description)):
            row.update(role="partnership_attribution", related_record_id=parent_id,
                       evidence=[source["source_url"], "Filed partnership description and explicit organizational parent"])
        elif any(word in description for word in ("REDESIGNAT", "REATTRIBUT", "RE-ATTRIBUT", "RE-DESIGNAT", "ADJUSTMENT", "ORIGINAL CONTRIBUTION")):
            # A positive adjustment is not automatically a second new gift.
            row["role"] = "unresolved"
        output.append(AttributionRecord(**row))
    return output


def build_report(manifest: dict, filings: list[Filing]) -> dict:
    if manifest.get("schema_version") != 1:
        raise ValueError("Unsupported attribution manifest schema")
    cycle = manifest.get("cycle")
    if type(cycle) is not int or not 1980 <= cycle <= 9998 or cycle % 2:
        raise ValueError("Cycle must be an even integer year from 1980 through 9998")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", manifest.get("report_id", "")):
        raise ValueError("A safe report_id is required")
    expected_sources = {s["file_number"]: s for s in manifest["sources"]}
    if len(expected_sources) != len(manifest["sources"]) or not expected_sources:
        raise ValueError("Source inventory must be nonempty and unique")
    if {f.metadata["file_number"] for f in filings} != set(expected_sources):
        raise ValueError("Supplied files differ from the reviewed source inventory")
    for filing in filings:
        if filing.metadata["sha256"] != expected_sources[filing.metadata["file_number"]]["sha256"]:
            raise ValueError("Source bytes differ from reviewed manifest")
    latest = latest_filings(filings)
    cycle_start, cycle_end = f"{cycle - 1}-01-01", f"{cycle}-12-31"
    if any(not (cycle_start <= f.metadata["coverage_start"] <= f.metadata["coverage_end"] <= cycle_end)
           for f in latest):
        raise ValueError("Report coverage falls outside the stated two-year cycle; review the reporting scope")
    expected_latest = set(manifest["latest_file_numbers"])
    if {f.metadata["file_number"] for f in latest} != expected_latest:
        raise ValueError("Source amendment selection disagrees with reviewed latest report inventory")
    scope = manifest["scope"]
    if {f.metadata["committee_id"] for f in latest} != set(scope["committee_ids"]):
        raise ValueError("Filing committees differ from the stated account scope")
    start = min(f.metadata["coverage_start"] for f in latest)
    end = max(f.metadata["coverage_end"] for f in latest)
    if (scope["coverage_start"], scope["coverage_end"]) != (start, end):
        raise ValueError("Stated coverage does not match original report headers")
    all_records = [r for filing in latest for r in filing.records]
    selected_ids = manifest.get("selected_record_ids")
    if selected_ids is not None:
        if scope.get("kind") != "reviewed_record_group" or not selected_ids or len(set(selected_ids)) != len(selected_ids):
            raise ValueError("A record subset requires an explicit nonempty reviewed_record_group scope")
        records = [r for r in all_records if r["record_id"] in set(selected_ids)]
        if {r["record_id"] for r in records} != set(selected_ids):
            raise ValueError("Required record in the reviewed group is missing")
    else:
        if scope.get("kind") != "campaign_reports":
            raise ValueError("Full reports require campaign_reports scope")
        records = all_records
    normalized = classify_records(records, manifest.get("reviews", []))
    scope_issues = []
    if len(scope["committee_ids"]) > 1:
        scope_issues.append({"record_id": "cross_account_reconciliation", "scope": "receipts",
                             "reason": "This workflow does not reconcile the same donor money across multiple campaign accounts. Build separate account reports; a combined candidate amount is unavailable."})
    if selected_ids is None:
        for committee in scope["committee_ids"]:
            periods = sorted((f.metadata["coverage_start"], f.metadata["coverage_end"])
                             for f in latest if f.metadata["committee_id"] == committee)
            # A catalog can omit an entire report. Matching its IDs alone cannot
            # convert a gap in the actual source periods into zero activity.
            gaps = any(date.fromisoformat(next_start) != date.fromisoformat(previous_end) + timedelta(days=1)
                       for (_, previous_end), (next_start, _) in zip(periods, periods[1:]))
            if gaps or periods[0][0] != start or periods[-1][1] != end:
                scope_issues.append({"record_id": committee, "scope": "receipts",
                                     "reason": "The listed reports do not continuously cover this account over the stated reporting period."})
    ownership = manifest.get("committee_scope_review", {})
    if ownership.get("status") != "resolved_for_scope" or not ownership.get("evidence"):
        scope_issues.append({"record_id": "account_scope", "reason": "Candidate/account ownership has not been reviewed for this reporting period.", "scope": "receipts"})
    for filing in latest:
        if not filing.metadata["individual_itemized_reconciles"]:
            scope_issues.append({"record_id": str(filing.metadata["file_number"]),
                                 "reason": "Itemized individual receipt rows do not reconcile to the cover summary.", "scope": "receipts"})
    refund_complete = selected_ids is None and all(
        Decimal(f.metadata["refunds_unitemized_or_unreconciled"]) == 0 for f in latest)
    result = attribute_campaign(normalized, manifest["employer_aliases"],
                                target_company=manifest["company"]["key"],
                                refund_scope_complete=refund_complete)
    unresolved = list(scope_issues)
    source_by_id = {r["record_id"]: r for r in records}
    for issue in result.issues:
        if issue.blocks_attributed_total or issue.blocks_net_total:
            unresolved.append({"record_id": issue.record_id or issue.code, "reason": issue.message,
                               "amount": source_by_id.get(issue.record_id, {}).get("amount"),
                               "scope": "receipts" if issue.blocks_attributed_total else "refunds"})
    if not refund_complete:
        unresolved.append({"record_id": "refund_coverage", "scope": "refunds",
                           "reason": "The supplied scope omits refunds or the itemized refunds do not cover the report-summary refund amount."})
    attributed = None if scope_issues else result.attributed_total
    net = None if attributed is None or not refund_complete else result.net_total
    components = {}
    for key, value in result.components.items():
        unknown = key == "refunds" and net is None
        components[key] = {
            "amount": None if unknown else format(value, ".2f"),
            "count": sum(d.component == key and d.status == "included" for d in result.decisions),
            "status": "unresolved" if unknown else "supported_component",
            "description": DESCRIPTIONS[key],
        }
    # Include target records, their explicit context, reviewed records, and
    # unresolved refunds. Never export private addresses or all other donors.
    by_id = {r["record_id"]: r for r in records}
    decisions = {d.record_id: d for d in result.decisions}
    evidence_ids = {d.record_id for d in result.decisions if d.canonical_company == manifest["company"]["key"]}
    evidence_ids |= {r["record_id"] for r in manifest.get("reviews", [])}
    evidence_ids |= {i["record_id"] for i in unresolved if i["record_id"] in by_id}
    todo = list(evidence_ids)
    while todo:
        key = todo.pop()
        source, decision = by_id[key], decisions[key]
        for related in (decision.related_record_id, f'{source["file_number"]}:{source["back_reference_transaction_id"]}'):
            if related in by_id and related not in evidence_ids:
                evidence_ids.add(related)
                todo.append(related)
    evidence = []
    for key in sorted(evidence_ids):
        source, decision = by_id[key], decisions[key]
        evidence.append({**source, "component": decision.component, "disposition": decision.status,
                         "role": decision.role, "related_record_id": decision.related_record_id,
                         "counted_amount": None if decision.amount is None else format(decision.amount, ".2f"),
                         "reason": next((r["rationale"] for r in manifest.get("reviews", []) if r["record_id"] == key), decision.role)})
    return {
        "schema_version": 1, "report_id": manifest["report_id"], "cycle": manifest["cycle"],
        "candidate": manifest["candidate"], "company": manifest["company"], "scope": scope,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_catalog_as_of": manifest["source_catalog_as_of"],
        "source_inventory": [f.metadata for f in filings],
        "sources": [f.metadata for f in latest], "components": components,
        "combined_amount": None if attributed is None else format(attributed, ".2f"),
        "combined_status": "unresolved" if attributed is None else "resolved_for_scope",
        "combined_reason": "Unresolved source or attribution issues prevent a combined amount." if attributed is None else
                           "Supported attributed receipts within the stated source scope, before refunds; not a complete employee-giving estimate.",
        "net_amount": None if net is None else format(net, ".2f"),
        "net_status": "unresolved" if net is None else "resolved_for_scope",
        "net_reason": "Refund coverage or donor relationships remain unresolved." if net is None else "Supported attribution plus linked signed refunds within this scope.",
        "unresolved": unresolved, "evidence": evidence,
        "employer_aliases": manifest["employer_aliases"],
        "limitations": manifest["limitations"],
        "diagnostics": {"source_records": len(records), "issue_counts": dict(Counter(i.code for i in result.issues)),
                        "undated_source_records": sum(not r["date"] for r in records),
                        "campaign_nonmemo_receipts": format(result.campaign_nonmemo_receipts, ".2f"),
                        "note": "Nonmemo receipt lines can include in-kind contributions; this is not a cash-flow measure."},
    }
