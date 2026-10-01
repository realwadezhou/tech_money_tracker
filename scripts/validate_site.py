"""Check generated local links, strict JSON, and finance total reconciliation.

Usage: python scripts/validate_site.py [docs]
"""
from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
import hashlib
from html.parser import HTMLParser
import json
import math
from pathlib import Path
import re
import sys
from urllib.parse import unquote, urlsplit


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        for name, value in attrs:
            if name in {"href", "src"} and value:
                self.urls.append(value)


def read_json(path):
    def invalid(value):
        raise ValueError(f"Invalid JSON number: {value}")
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=invalid)


def validate_fec_freshness(metadata: dict, manifest: dict, errors: list) -> None:
    cycle = metadata["cycle"]
    if not manifest["all_local_bulk_files_present"]:
        errors.append(f"{cycle} source files missing")
    status = manifest.get("source_check_status")
    if status is not None and status != "current":
        errors.append(f"{cycle} source freshness is not current: {status}")
    for key in ("source_check_status", "latest_local_bulk_release_utc"):
        if key in metadata and metadata[key] != manifest.get(key):
            errors.append(f"{cycle} metadata {key} disagrees with source manifest")
    for row in manifest["bulk_sources"]:
        if row["remote_is_newer"] or row["remote_error"]:
            errors.append(f"{cycle} source freshness unverified: {row['key']}")


def validate_candidate_components(cycle: int, row: dict, errors: list) -> None:
    if not any(key.startswith("former_campaign_account_") for key in row):
        return  # Older snapshots predate the explicit account components.
    for metric in ("total_itemized_receipts", "tech_itemized_receipts", "tech_itemized_contributions"):
        try:
            total = float(row[metric])
            current = float(row[f"current_campaign_account_{metric}"])
            former = float(row[f"former_campaign_account_{metric}"])
            valid = all(math.isfinite(value) for value in (total, current, former))
            valid = valid and abs(total - current - former) <= 0.01
        except (KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            errors.append(f"{cycle} {row['cand_id']} account components do not reconcile: {metric}")


def validate_receipt_context(label: str, row: dict, errors: list, *,
                             total_key: str, share_key: str | None = None,
                             prefix: str = "") -> None:
    """Catch lost memo safeguards or damaged diagnostics in serialized exports."""
    if prefix + "selected_record_net_total" not in row:
        return  # Older snapshots predate receipt-context fields.
    try:
        selected, nonmemo, memo, count, legacy = (
            float(row[prefix + key]) for key in (
                "selected_record_net_total", "nonmemo_receipt_net_total",
                "memo_receipt_net_total", "memo_receipt_record_count", total_key))
        risk_value = row[prefix + "has_unreconciled_memo_attributions"]
        if str(risk_value).lower() not in {"true", "false", "1", "0", "1.0", "0.0"}:
            raise ValueError("Invalid memo flag")
        risk = str(risk_value).lower() in {"true", "1", "1.0"}
        valid = all(math.isfinite(value) for value in (selected, nonmemo, memo, count, legacy))
        valid = valid and abs(selected - legacy) <= .01 and abs(selected - nonmemo - memo) <= .01
        valid = valid and count >= 0 and count.is_integer()
        valid = valid and (not risk or count > 0) and (memo == 0 or risk)
        if not valid:
            raise ValueError("Inconsistent receipt diagnostics")
        if risk and share_key and row.get(share_key) not in (None, ""):
            errors.append(f"{label} unreconciled memo records have a displayed Tech Share")
        if risk and str(row.get("is_tech_dominated", False)).lower() in {"true", "1"}:
            errors.append(f"{label} unreconciled memo records are classified as tech dominated")
    except (KeyError, TypeError, ValueError):
        errors.append(f"{label} invalid receipt diagnostics: {prefix or 'combined'}")


ATTRIBUTION_COMPONENTS = ("direct_receipts", "jfc_allocations", "partnership_attributions",
                          "signed_adjustments", "refunds")


def validate_attribution_report(label: str, report: dict, errors: list) -> None:
    """Check serialized arithmetic/provenance, not certify FEC interpretations."""
    def fail(message):
        errors.append(f"{label} attribution: {message}")

    def amount(value):
        if not isinstance(value, str):
            raise ValueError("Dollar values must be decimal strings (or explicit null when unresolved)")
        try:
            number = Decimal(value)
            if not number.is_finite() or number != number.quantize(Decimal("0.01")):
                raise ValueError("Nonfinite or fractional-cent dollar value")
        except InvalidOperation as exc:
            raise ValueError("Invalid decimal dollar value") from exc
        return number

    def file_id(value):
        if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]*", str(value)):
            raise ValueError("Invalid source filing number")
        return int(value)

    def digest(value):
        return isinstance(value, str) and re.fullmatch(r"[a-fA-F0-9]{64}", value) is not None

    def source_map(values):
        result = {}
        for source in values:
            fid = file_id(source["file_number"])
            if fid in result:
                raise ValueError("Duplicate source filing ID")
            if not digest(source["sha256"]):
                raise ValueError("Missing or invalid source SHA-256")
            if source["url"] != f"https://docquery.fec.gov/dcdev/posted/{fid}.fec":
                raise ValueError("Source URL does not identify its official electronic filing")
            result[fid] = source
        if not result:
            raise ValueError("Missing source provenance")
        return result

    try:
        if report["schema_version"] != 1:
            raise ValueError("Unsupported report schema")
        sources = source_map(report["sources"])
        if "manifest_sha256" in report and not digest(report["manifest_sha256"]):
            raise ValueError("Invalid reviewed-manifest SHA-256")
        if "source_inventory" in report:
            inventory = source_map(report["source_inventory"])
            chains = {}
            for fid, source in inventory.items():
                root = file_id(source["original_file_number"])
                sequence = source["amendment_sequence"]
                if type(sequence) is not int or sequence < 0:
                    raise ValueError("Invalid source amendment sequence")
                chains.setdefault((source["committee_id"], root), []).append((sequence, fid))
            latest = set()
            for (_, root), chain in chains.items():
                chain.sort()
                if chain[0] != (0, root) or [x[0] for x in chain] != list(range(len(chain))):
                    raise ValueError("Incomplete or ambiguous source amendment inventory")
                latest.add(chain[-1][1])
            if set(sources) != latest:
                fail("latest source inventory disagrees with amendment chains")
            for fid, source in sources.items():
                original = inventory.get(fid)
                if original is None or any(source.get(key) != original.get(key) for key in (
                    "sha256", "committee_id", "original_file_number", "amendment_sequence",
                    "coverage_start", "coverage_end", "form_type", "report_type",
                )):
                    fail("latest source metadata disagrees with the frozen source inventory")

        components = report["components"]
        if set(components) != set(ATTRIBUTION_COMPONENTS):
            raise ValueError("Missing or unexpected attribution component")
        component_amounts = {}
        for key, component in components.items():
            if type(component["count"]) is not int or component["count"] < 0:
                raise ValueError("Invalid attribution component count")
            value = component["amount"]
            if component["status"] == "unresolved":
                if value is not None:
                    fail(f"unresolved {key} must use null, not a dollar amount")
                component_amounts[key] = None
            elif component["status"] in {"supported_component", "supported_within_scope"}:
                component_amounts[key] = amount(value)
            else:
                raise ValueError("Invalid component status")

        records = {}
        counted = {key: Decimal("0.00") for key in ATTRIBUTION_COMPONENTS}
        counts = dict.fromkeys(ATTRIBUTION_COMPONENTS, 0)
        for row in report["evidence"]:
            key = row["record_id"]
            if not isinstance(key, str) or not key or key in records:
                raise ValueError("Duplicate or invalid evidence record ID")
            records[key] = row
            fid = file_id(row["file_number"])
            if fid not in sources or row["source_url"] != sources[fid]["url"]:
                fail(f"evidence {key} has no matching latest source provenance")
            elif row.get("committee_id") != sources[fid].get("committee_id"):
                fail(f"evidence {key} committee differs from its source filing")
            if key != f'{fid}:{row["transaction_id"]}' or not digest(row["source_row_sha256"]):
                fail(f"evidence {key} has inconsistent source identity or row hash")
            if type(row["source_record_number"]) is not int or row["source_record_number"] < 3:
                fail(f"evidence {key} has invalid source record position")
            original_amount = amount(row["amount"])
            disposition = row["disposition"]
            if disposition == "included":
                component = row["component"]
                if component not in counted:
                    raise ValueError("Included evidence has no known component")
                value = amount(row["counted_amount"])
                if value != (-original_amount if component == "refunds" else original_amount):
                    fail(f"evidence {key} counted amount disagrees with its filed amount/sign")
                counted[component] += value
                counts[component] += 1
            elif disposition in {"context", "excluded_repeated_original", "unresolved", "outside_employer_scope"}:
                if row.get("counted_amount") is not None or row.get("component") is not None:
                    fail(f"uncounted evidence {key} carries a counted amount or component")
            else:
                raise ValueError("Unknown evidence disposition")
        for row in records.values():
            related = row.get("related_record_id")
            if row["disposition"] in {"included", "excluded_repeated_original"} and related and related not in records:
                fail(f"evidence {row['record_id']} is missing its required related source record")
        for key, component in components.items():
            if counts[key] != component["count"]:
                fail(f"{key} evidence count does not reconcile")
            if component_amounts[key] is not None and counted[key] != component_amounts[key]:
                fail(f"{key} evidence amounts do not reconcile")

        combined = report["combined_amount"]
        if report["combined_status"] == "resolved_for_scope":
            total = amount(combined)
            before_refunds = [component_amounts[key] for key in ATTRIBUTION_COMPONENTS if key != "refunds"]
            if any(value is None for value in before_refunds) or total != sum(before_refunds, Decimal(0)):
                fail("combined amount does not reconcile to supported receipt components")
            if any(issue.get("scope") == "receipts" for issue in report["unresolved"]):
                fail("combined amount is exposed despite unresolved receipt scope")
        elif report["combined_status"] == "unresolved":
            total = None
            if combined is not None:
                fail("unresolved combined amount must be null, not zero or a component sum")
        else:
            raise ValueError("Invalid combined status")
        net = report["net_amount"]
        if report["net_status"] == "resolved_for_scope":
            net_value = amount(net)
            refund = component_amounts["refunds"]
            if total is None or refund is None or net_value != total + refund:
                fail("net amount does not reconcile to supported receipts and refunds")
            if any(issue.get("scope") in {"receipts", "refunds"} for issue in report["unresolved"]):
                fail("net amount is exposed despite unresolved receipt/refund scope")
        elif report["net_status"] == "unresolved":
            if net is not None:
                fail("unresolved net amount must be null, not zero or a partial sum")
        else:
            raise ValueError("Invalid net status")
    except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation) as exc:
        fail(f"invalid report structure: {exc}")


def validate_attribution(root: Path, errors: list) -> dict | None:
    paths = sorted(root.glob("*/data/attribution/*.json"))
    if not paths:
        return None  # Source-based reports are optional in older site exports.
    seen = set()
    for path in paths:
        label = str(path.relative_to(root))
        try:
            report = read_json(path)
            key = (report["cycle"], report["report_id"])
            if key in seen or path.stem != report["report_id"]:
                errors.append(f"{label} duplicate attribution report ID or mismatched filename")
            seen.add(key)
            if str(report["cycle"]) != path.parents[2].name:
                errors.append(f"{label} attribution report cycle differs from its directory")
            validate_attribution_report(label, report, errors)
        except (OSError, KeyError, TypeError, ValueError) as exc:
            errors.append(f"{label} invalid attribution export: {exc}")
    return {"reports": len(paths), "validation_scope": "Serialized arithmetic, identities, and provenance consistency; not certification of accounting interpretations."}


def validate_lobbying(root: Path, errors: list) -> dict | None:
    data = root / "lobbying/data"
    # The spending page can be published without the AI explorer.
    if not (data / "explorer-data.js").exists():
        return None
    try:
        script = (data / "explorer-data.js").read_text(encoding="utf-8")
        prefix = "window.TechMoneyLobbyingData="
        if not script.startswith(prefix) or not script.rstrip().endswith(";"):
            raise ValueError("Invalid lobbying data script wrapper")
        def invalid_number(value):
            raise ValueError(f"Invalid JSON number: {value}")
        payload = json.loads(script[len(prefix):].rstrip()[:-1], parse_constant=invalid_number)
        metadata = payload["metadata"]
        if metadata != read_json(data / "manifest.json"):
            errors.append("Lobbying page index and coverage manifest differ")
        activities = payload["activities"]
        ids = [row["activity_id"] for row in activities]
        if len(ids) != len(set(ids)):
            errors.append("Duplicate lobbying issue IDs")
        reports = {row["filing_uuid"] for row in activities}
        if metadata["activity_count"] != len(activities) or metadata["report_count"] != len(reports):
            errors.append("Lobbying index counts do not reconcile")
        expected = {}
        topic_ids = {topic["id"] for topic in payload["topics"]}
        for row in activities:
            if hashlib.sha256(row["description"].encode("utf-8")).hexdigest() != row["description_sha256"]:
                errors.append(f"Changed lobbying passage: {row['activity_id']}")
            if not row["filing_url"].startswith("https://lda.gov/") or row["quarter"] not in (1, 2, 3, 4):
                errors.append(f"Invalid lobbying source or period: {row['activity_id']}")
            if "income" in row or "expenses" in row or "total_reported_spend" in row:
                errors.append("Report dollar amounts leaked into the topic index")
            for match in row["matches"]:
                key = (row["activity_id"], match["topic_id"])
                if key in expected or match["topic_id"] not in topic_ids:
                    errors.append(f"Duplicate or unknown lobbying topic match: {key}")
                expected[key] = (match["decision"], bool(match.get("dependency_rejected")),
                                 row.get("government_entity_scope"))
                for evidence in match["evidence"]:
                    if row["description"][evidence["start"]:evidence["end"]] != evidence["text"]:
                        errors.append(f"Invalid keyword evidence span: {key}")
        actual = {}
        with (data / "matches.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                key = (row["activity_id"], row["topic_id"])
                if key in actual:
                    errors.append(f"Duplicate lobbying CSV match: {key}")
                actual[key] = (row["decision"], row.get("dependency_rejected", "").lower() == "true",
                               row.get("government_entity_scope"))
                if row["rules_version"] != metadata["rules_version"]:
                    errors.append("Lobbying CSV uses a different rules version")
        if expected != actual:
            errors.append("Lobbying evidence CSV and browser index disagree")
        for source in metadata["sources"]:
            year_count = sum(row["year"] == source["year"] for row in activities)
            if year_count != source["indexed_activity_count"]:
                errors.append(f"Lobbying {source['year']} activity counts disagree")
        return {"issue_entries": len(activities), "distinct_reports": len(reports),
                "topic_matches": len(expected), "rules_version": metadata["rules_version"]}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(f"Invalid lobbying export: {exc}")
        return None


def validate_lobbying_spending(root: Path, errors: list) -> dict | None:
    """Spending page data: the CSVs must reconcile with the JSON and the counting rule."""
    data = root / "lobbying/spending/data"
    if not data.exists():
        return None
    try:
        payload = read_json(data / "spending.json")
        quarters = payload["quarters"]
        expected = {}
        for company in payload["companies"]:
            for quarter, cell in zip(quarters, company["quarters"]):
                if cell is None:
                    continue
                if cell["total"] != max(cell["in_house_expenses"], cell["outside_firm_income"]):
                    errors.append(f"Lobbying spending total breaks the counting rule: {company['id']} {quarter['id']}")
                expected[(company["id"], quarter["year"], quarter["quarter"])] = (
                    cell["total"], cell["in_house_expenses"], cell["outside_firm_income"])
        actual = {}
        with (data / "spending.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                actual[(row["company_id"], int(row["year"]), int(row["quarter"]))] = (
                    float(row["total"]), float(row["in_house_expenses"]), float(row["outside_firm_income"]))
        if expected != actual:
            errors.append("Lobbying spending CSV and JSON disagree")
        sums, reports = {}, 0
        with (data / "spending_reports.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                reports += 1
                if not row["filing_url"].startswith("https://lda.gov/"):
                    errors.append(f"Invalid lobbying spending source link: {row['filing_uuid']}")
                key = (row["company_id"], int(row["year"]), int(row["quarter"]))
                pair = sums.setdefault(key, [0.0, 0.0])
                pair[0 if row["kind"] == "in_house" else 1] += float(row["amount"])
        if {key: (value[1], value[2]) for key, value in expected.items()} != {
                key: tuple(value) for key, value in sums.items()}:
            errors.append("Lobbying spending totals do not add up from the listed reports")
        if reports != payload["metadata"]["report_count"]:
            errors.append("Lobbying spending report count disagrees with the listed reports")
        return {"companies": len(payload["companies"]), "company_quarters": len(expected), "reports": reports,
                "source_cutoff": payload["metadata"]["source_cutoff"]}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(f"Invalid lobbying spending export: {exc}")
        return None


def validate(root: Path) -> dict:
    root = root.resolve()
    errors = []
    pages = list(root.rglob("*.html"))
    json_paths = list(root.rglob("*.json"))
    for path in pages:
        parser = Links()
        parser.feed(path.read_text(encoding="utf-8"))
        for url in parser.urls:
            parsed = urlsplit(url)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            target = ((root / unquote(parsed.path).lstrip("/")) if parsed.path.startswith("/")
                      else (path.parent / unquote(parsed.path))).resolve()
            if target.is_dir():
                target = target / "index.html"
            if not target.is_relative_to(root) or not target.is_file():
                errors.append(f"Broken local link: {path.relative_to(root)} -> {url}")
    for path in json_paths:
        try:
            read_json(path)
        except (ValueError, OSError) as exc:
            errors.append(f"Invalid JSON: {path.relative_to(root)}: {exc}")

    cycles = {}
    for metadata_path in sorted(root.glob("*/data/site_metadata.json")):
        data = metadata_path.parent
        metadata = read_json(metadata_path)
        total = metadata["total_tech_linked_giving"]
        companies = read_json(data / "companies.json")
        committees = read_json(data / "committees.json")
        headline = read_json(data / "homepage_summary.json")["headline_numbers"]
        with (data / "weekly_totals.csv").open(encoding="utf-8", newline="") as handle:
            weeks = list(csv.DictReader(handle))
        sums = {
            "company_total": sum(row["net_total"] for row in companies),
            "homepage_total": headline["total_tech_linked_giving"],
            "dated_plus_undated_total": sum(float(row["net_total"]) for row in weeks)
                + metadata.get("undated_tech_net_total", 0),
        }
        for label, value in sums.items():
            if abs(total - value) > 0.01:
                errors.append(f"{metadata['cycle']} {label} {value} differs from headline {total}")
        slugs = [row["slug"] for row in companies]
        if len(slugs) != len(set(slugs)):
            errors.append(f"{metadata['cycle']} duplicate company slugs")
        for company in companies:
            payload_path = data / "companies" / f"{company['slug']}.json"
            if not payload_path.is_file():
                errors.append(f"Missing company payload: {payload_path}")
                continue
            payload = read_json(payload_path)
            series = payload["weekly_series"]
            company_total = company["net_total"]
            series_total = sum(row["net_total"] for row in series)
            if abs(series_total + payload.get("undated_tech_net_total", 0) - company_total) > 0.01:
                errors.append(f"{metadata['cycle']} {company['slug']} chart does not reconcile")
            if abs(payload["summary"]["net_total"] - company_total) > 0.01:
                errors.append(f"{metadata['cycle']} {company['slug']} detail total differs")
            party_total = sum(company[key] for key in ["amt_dem", "amt_rep", "amt_mixed", "amt_unknown"])
            if abs(party_total - company_total) > 0.01:
                errors.append(f"{metadata['cycle']} {company['slug']} recipient party totals differ")
            for key in ["pct_dem", "pct_classified_recipients", "pct_dem_by_donor"]:
                value = company.get(key)
                if value is not None and not (0 <= value <= 100):
                    errors.append(f"{metadata['cycle']} {company['slug']} invalid {key}: {value}")
        # The public committee table intentionally lists only positive net
        # recipients. Its sum can exceed the headline by net refund outflows.
        committee_total = sum(row["tech_receipts"] for row in committees)
        for row in committees:
            validate_receipt_context(f"{metadata['cycle']} {row['cmte_id']}", row, errors,
                                     total_key="total_receipts", share_key="tech_pct")
            share = row.get("tech_pct")
            if share is not None and not (0 <= share <= 100):
                errors.append(f"{metadata['cycle']} {row['cmte_id']} invalid tech share: {share}")
        with (data / "candidate_race_summary.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                validate_candidate_components(metadata["cycle"], row, errors)
                for prefix in ("", "current_campaign_account_", "former_campaign_account_"):
                    validate_receipt_context(f"{metadata['cycle']} {row['cand_id']}", row, errors,
                        total_key="total_itemized_receipts", prefix=prefix,
                        share_key="tech_pct_itemized_receipts" if not prefix else None)
                share = row.get("tech_pct_itemized_receipts")
                if share and not (0 <= float(share) <= 100):
                    errors.append(f"{metadata['cycle']} {row['cand_id']} invalid tech share: {share}")
        running = 0
        for row in weeks:
            running += float(row["net_total"])
            if abs(running - float(row["cumulative_net_total"])) > 0.01:
                errors.append(f"{metadata['cycle']} incorrect cumulative total at {row['week_end']}")
                break
        manifest = read_json(data / "source_manifest.json")
        validate_fec_freshness(metadata, manifest, errors)
        cycles[str(metadata["cycle"])] = {
            "data_as_of": metadata["data_as_of"],
            "total_tech_linked_giving": total,
            "tech_donor_count": metadata["tech_donor_count"],
            "positive_recipient_committee_total": committee_total,
            **sums,
        }
    if not pages or not cycles:
        errors.append("No generated site pages or cycle data found")
    lobbying = validate_lobbying(root, errors)
    lobbying_spending = validate_lobbying_spending(root, errors)
    attribution = validate_attribution(root, errors)
    return {"html_pages": len(pages), "json_files": len(json_paths), "cycles": cycles,
            "lobbying": lobbying, "lobbying_spending": lobbying_spending,
            "attribution": attribution, "errors": errors}


if __name__ == "__main__":
    result = validate(Path(sys.argv[1] if len(sys.argv) > 1 else "docs"))
    print(json.dumps(result, indent=2))
    raise SystemExit(bool(result["errors"]))
