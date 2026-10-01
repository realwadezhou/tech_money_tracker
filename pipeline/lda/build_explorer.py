"""Build a reproducible, evidence-first topic index from normalized LDA reports.

No network requests and no use of the exploratory spending summaries. Run after
reconcile/normalize. Source CSVs and hand-edited reference decisions are read-only.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile
from typing import Iterable

from pipeline.common.paths import PROJECT_ROOT

REFERENCE = PROJECT_ROOT / "data/reference/lobbying"
EXPORT = PROJECT_ROOT / "exports/lobbying"
PERIODS = {f"{name}_quarter": index for index, name in enumerate(
    ("first", "second", "third", "fourth"), 1)}
REPORT_TYPE = re.compile(r"(?:Q([1-4])Y?|([1-4])(?:A|T|@)Y?)$")
REVIEW_FIELDS = ["activity_id", "topic_id", "rules_version", "description_sha256",
                 "decision", "reviewer", "reviewed_at", "notes"]


def rows(path: Path) -> Iterable[dict]:
    # Descriptions can exceed csv's default 128 KiB field limit.
    csv.field_size_limit(10_000_000)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        yield from csv.DictReader(handle)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def save_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=".lobbying-", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(value, handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            handle.write("\n")
        except BaseException:
            handle.close()
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def save_csv(path: Path, values: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent,
                                     prefix=".lobbying-", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in values:
                # A spreadsheet must not execute filing text beginning with a formula.
                writer.writerow({key: ("'" + value if isinstance(value, str) and
                    re.match(r"(?:\s*[=+@-]|[\t\r\n])", value) else value) for key, value in row.items()})
        except BaseException:
            handle.close()
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, AttributeError):
        return None


def government_entity_scope(posted_at: str) -> str:
    """Preserve the legacy API's filing-wide government-entity associations.

    The official API caveat dates the change to February 14, 2021, describing
    records before and after that date. Treat the transition date conservatively
    because the documentation does not supply a precise cutover instant.
    https://lda.gov/api/redoc/v1/#section/Implementation-Details/Limitations-Caveats
    """
    if not timestamp(posted_at):
        return "unknown"
    posted_date = date.fromisoformat(posted_at[:10])
    if posted_date < date(2021, 2, 14):
        return "filing"
    if posted_date == date(2021, 2, 14):
        return "unknown"
    return "issue_entry"


def select_current_reports(filings: list[dict]) -> tuple[dict, list[dict], dict]:
    """Latest posted report per source registrant/client/year/quarter.

    Do not combine distinct source clients by display name. Missing chronology
    or tied latest timestamps quarantine the entire family, rather than choosing
    an arbitrary original/amendment. Registrations never enter activity counts.
    """
    groups = defaultdict(list)
    ledger = []
    seen = set()
    for row in filings:
        uid = row["filing_uuid"]
        if not uid or uid in seen:
            raise ValueError("Missing or duplicate filing UUID in normalized filings")
        seen.add(uid)
        match = REPORT_TYPE.fullmatch(row.get("filing_type", ""))
        entry = {key: row.get(key, "") for key in (
            "filing_uuid", "filing_type", "filing_year", "filing_period", "dt_posted",
            "client_api_id", "client_name", "registrant_api_id", "registrant_name",
            "income", "expenses", "expenses_method", "filing_document_url")}
        entry.update(selection_status="excluded_registration" if row.get("filing_type") in {"RR", "RA"}
                     else "excluded_unknown_type", selected_filing_uuid="")
        ledger.append(entry)
        if not match:
            continue
        if int(next(x for x in match.groups() if x)) != PERIODS.get(row.get("filing_period")):
            entry["selection_status"] = "excluded_period_mismatch"
            continue
        if not all(row.get(key) for key in ("registrant_api_id", "client_api_id", "filing_year")):
            entry["selection_status"] = "excluded_missing_identity"
            continue
        key = tuple(row[key] for key in ("registrant_api_id", "client_api_id", "filing_year", "filing_period"))
        groups[key].append((row, entry))

    current = {}
    for family in groups.values():
        dates = [timestamp(row.get("dt_posted", "")) for row, _ in family]
        if None in dates or dates.count(max(dates)) != 1:
            for _, entry in family:
                entry["selection_status"] = "excluded_ambiguous_chronology"
            continue
        chosen = family[dates.index(max(dates))][0]
        uid = chosen["filing_uuid"]
        current[uid] = {**chosen, "revision_count": len(family)}
        for row, entry in family:
            entry["selection_status"] = "current" if row["filing_uuid"] == uid else "superseded"
            entry["selected_filing_uuid"] = uid
    return current, ledger, dict(Counter(row["selection_status"] for row in ledger))


class Topics:
    def __init__(self, config: dict):
        self.config = config
        self.version = config["version"]
        self.topics = config["topics"]
        self.compiled = {}
        known = set()
        rule_ids = set()
        for topic in self.topics:
            if topic["id"] in known:
                raise ValueError("Duplicate topic ID")
            if topic.get("requires") and topic["requires"] not in known:
                raise ValueError("Topic dependencies must precede dependent topics")
            known.add(topic["id"])
            for rule in topic["rules"]:
                if rule["id"] in rule_ids or rule["kind"] not in {"direct", "ambiguous", "context"}:
                    raise ValueError("Invalid or duplicate topic rule")
                rule_ids.add(rule["id"])
                self.compiled[rule["id"]] = re.compile(rule["pattern"], 0 if rule.get("case_sensitive") else re.I)

    def match(self, text: str) -> list[dict]:
        matches = {}
        for topic in self.topics:
            dependency = topic.get("requires")
            if dependency and dependency not in matches:
                continue
            evidence = []
            for rule in topic["rules"]:
                for hit in self.compiled[rule["id"]].finditer(text):
                    if hit.start() == hit.end():
                        raise ValueError("Keyword rules must not match empty text")
                    evidence.append({"rule_id": rule["id"], "start": hit.start(), "end": hit.end(),
                                     "text": hit.group(), "kind": rule["kind"]})
            if evidence:
                matches[topic["id"]] = {
                    "topic_id": topic["id"], "requires": dependency,
                    "kind": "context" if dependency else (
                        "direct" if any(e["kind"] == "direct" for e in evidence) else "ambiguous"),
                    "evidence": evidence, "decision": "unreviewed", "review": None,
                }
        return list(matches.values())


def normalized_name(name: str) -> str:
    # Only case and whitespace; no substring, suffix stripping, or fuzzy join.
    return " ".join(name.upper().split())


class Organizations:
    def __init__(self, config: dict, reviews: list[dict]):
        self.config = config
        self.organizations = {org["id"]: org for org in config["organizations"]}
        if len(self.organizations) != len(config["organizations"]):
            raise ValueError("Duplicate organization ID")
        self.aliases = {}
        self.reviews = {}
        for org in self.organizations.values():
            for alias in org["aliases"]:
                key = normalized_name(alias)
                if key in self.aliases and self.aliases[key] != org["id"]:
                    raise ValueError(f"Conflicting organization alias: {alias}")
                self.aliases[key] = org["id"]
        for review in reviews:
            key = (review["registrant_api_id"], review["client_api_id"])
            if key in self.reviews or review["decision"] not in {"accepted", "rejected"}:
                raise ValueError("Duplicate or invalid organization review")
            if not all(review.get(k) for k in ("registrant_api_id", "client_api_id", "reviewer", "reviewed_at")):
                raise ValueError("Organization reviews require both source IDs, reviewer, and date")
            if review["decision"] == "accepted" and review["organization_id"] not in self.organizations:
                raise ValueError("Organization review refers to an unknown organization")
            self.reviews[key] = review
        for watchlist in config["watchlists"]:
            if set(watchlist["organization_ids"]) - self.organizations.keys():
                raise ValueError("Unknown organization in watchlist")

    def identify(self, report: dict) -> dict:
        key = (report["registrant_api_id"], report["client_api_id"])
        review = self.reviews.get(key)
        org_id = self.aliases.get(normalized_name(report["client_name"]))
        status = "name_seed" if org_id else "unmapped"
        if review:
            org_id = review["organization_id"] if review["decision"] == "accepted" else None
            status = "reviewed" if org_id else "rejected_mapping"
        return {"organization_id": org_id,
                "organization_name": self.organizations[org_id]["name"] if org_id else None,
                "organization_status": status}


def load_topic_reviews(path: Path, topics: Topics) -> dict:
    reviews = {}
    known = {topic["id"] for topic in topics.topics}
    for row in rows(path):
        key = (row["activity_id"], row["topic_id"], row["rules_version"])
        if key in reviews or row["decision"] not in {"accepted", "rejected", "uncertain"}:
            raise ValueError("Duplicate or invalid topic review")
        if row["topic_id"] not in known or not re.fullmatch(r"[a-f0-9]{64}", row["description_sha256"]):
            raise ValueError("Topic reviews require a known topic and the exact description hash")
        if not row["reviewer"] or not row["reviewed_at"]:
            raise ValueError("Topic reviews require reviewer and date")
        reviews[key] = row
    return reviews


def apply_reviews(activity_id: str, text: str, matches: list[dict], topics: Topics, reviews: dict) -> list[dict]:
    result = {m["topic_id"]: m for m in matches}
    for topic in topics.topics:
        review = reviews.get((activity_id, topic["id"], topics.version))
        if not review:
            continue
        if review["description_sha256"] != digest(text):
            raise ValueError(f"Stale review for changed passage: {activity_id}")
        # Allows a reviewer to add a false negative without inventing a keyword hit.
        match = result.setdefault(topic["id"], {"topic_id": topic["id"], "kind": "manual",
                                                "requires": topic.get("requires"), "evidence": []})
        match["decision"] = review["decision"]
        match["review"] = {key: review[key] for key in ("reviewer", "reviewed_at", "notes")}
    # Rejected prerequisite cannot silently keep a co-occurrence classification.
    for match in result.values():
        dependency = result.get(match.get("requires"))
        match["dependency_rejected"] = bool(match.get("requires") and
            (not dependency or dependency["decision"] == "rejected"))
    return list(result.values())


def quarter_coverage(year: int, source_date: str) -> list[dict]:
    as_of = date.fromisoformat(source_date[:10])
    quarters = []
    for quarter in range(1, 5):
        start = date(year, 3 * quarter - 2, 1)
        next_start = date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)
        status = "not_started" if as_of < start else "in_progress" if as_of < next_start else "ended"
        quarters.append({"quarter": quarter, "period_start": start.isoformat(),
                         "period_end": (next_start - timedelta(days=1)).isoformat(), "status": status})
    return quarters


def build_year(year: int, root: Path, topics: Topics, orgs: Organizations, reviews: dict) -> tuple:
    interim = root / "data/lda/interim" / str(year)
    raw = root / "data/lda/raw" / str(year) / "filings"
    normalization = json.loads((interim / "normalization_manifest.json").read_text(encoding="utf-8"))
    snapshot_path = raw / "snapshot_manifest.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8")) if snapshot_path.exists() else {}
    # Reconciliation may only deduplicate already-saved pages and compare live
    # counts. Its build timestamp cannot establish fresh source contents.
    source_endpoint = normalization.get("source_endpoints", {}).get("filings", {})
    source_date = (snapshot.get("snapshot_cutoff_utc") or
                   source_endpoint.get("snapshot_cutoff_utc") or source_endpoint.get("fetched_at_utc"))
    normalized_at = normalization.get("normalized_at_utc")
    if not timestamp(source_date) or not timestamp(normalized_at):
        raise ValueError(f"{year}: missing or invalid source/normalization timestamp")
    if snapshot.get("snapshot_built_at_utc") and timestamp(snapshot["snapshot_built_at_utc"]) > timestamp(normalized_at):
        raise ValueError(f"{year}: reconcile snapshot is newer than normalized tables; normalize again")
    filings = list(rows(interim / "filings.csv"))
    if len(filings) != normalization["tables"]["filings.csv"]["rows"]:
        raise ValueError(f"{year}: normalized filing row count disagrees with manifest")
    if snapshot and len(filings) != snapshot["snapshot_unique_id_count"]:
        raise ValueError(f"{year}: snapshot and normalized filing counts differ")
    current, ledger, counts = select_current_reports(filings)
    activities = []
    review_queue = []
    nonmatch_sample = []
    identities = {}
    scanned = 0
    seen_activities = set()
    for activity in rows(interim / "filing_activities.csv"):
        report = current.get(activity["filing_uuid"])
        if not report:
            continue
        aid = activity["activity_id"]
        if aid in seen_activities:
            raise ValueError(f"Duplicate activity ID: {aid}")
        seen_activities.add(aid)
        # No-activity reports supersede earlier reports but supply no activity evidence.
        if report["filing_type"].endswith("Y"):
            continue
        scanned += 1
        text = activity.get("description") or ""
        matches = apply_reviews(aid, text, topics.match(text), topics, reviews)
        if not matches:
            # Deterministic ~0.2% audit sample, independent of keyword output.
            if int(digest(aid)[:8], 16) % 500 == 0:
                nonmatch_sample.append({"activity_id": aid, "client_name": report["client_name"],
                    "description": text, "description_sha256": digest(text), "rules_version": topics.version,
                    "filing_document_url": report["filing_document_url"]})
            continue
        source_key = f"{report['registrant_api_id']}:{report['client_api_id']}"
        identity = orgs.identify(report)
        identities[source_key] = {"registrant_api_id": report["registrant_api_id"],
            "client_api_id": report["client_api_id"], "client_name": report["client_name"],
            "registrant_name": report["registrant_name"], **identity,
            "filing_document_url": report["filing_document_url"]}
        public_url = report["filing_document_url"]
        if not public_url.startswith("https://lda.gov/"):
            raise ValueError(f"Unexpected filing source URL for {report['filing_uuid']}")
        item = {"activity_id": aid, "filing_uuid": report["filing_uuid"], "year": year,
                "quarter": PERIODS[report["filing_period"]], "posted_at": report["dt_posted"],
                "client_source_key": source_key, "client_name": report["client_name"], **identity,
                "registrant_name": report["registrant_name"], "registrant_api_id": report["registrant_api_id"],
                "client_api_id": report["client_api_id"], "issue_code": activity["general_issue_code"],
                "issue_label": activity["general_issue_code_display"], "description": text,
                "description_sha256": digest(text), "matches": matches,
                "filing_url": public_url, "revision_count": report["revision_count"],
                "lobbyists": [], "government_entities": [],
                "government_entity_scope": government_entity_scope(report["dt_posted"])}
        activities.append(item)
        for match in matches:
            if match["decision"] == "unreviewed":
                review_queue.append({"activity_id": aid, "topic_id": match["topic_id"],
                    "rules_version": topics.version, "description_sha256": digest(text),
                    "decision": "", "reviewer": "", "reviewed_at": "", "notes": "",
                    "match_kind": match["kind"], "client_name": report["client_name"], "description": text,
                    "filing_url": public_url})

    by_activity = {item["activity_id"]: item for item in activities}
    for row in rows(interim / "filing_activity_lobbyists.csv"):
        item = by_activity.get(row["activity_id"])
        if item is not None:
            person = {"id": row["lobbyist_api_id"], "name": " ".join(
                row[key] for key in ("lobbyist_first_name", "lobbyist_middle_name", "lobbyist_last_name") if row.get(key))}
            if person not in item["lobbyists"]:
                item["lobbyists"].append(person)
    for row in rows(interim / "filing_activity_government_entities.csv"):
        item = by_activity.get(row["activity_id"])
        if item is not None:
            entity = {"id": row["government_entity_id"], "name": row["government_entity_name"]}
            if entity not in item["government_entities"]:
                item["government_entities"].append(entity)
    return activities, ledger, review_queue, nonmatch_sample, list(identities.values()), {
        "year": year, "snapshot_at": source_date, "normalized_at": normalized_at,
        "source_filing_count": len(filings), "selection_counts": counts,
        "current_activity_count": scanned, "indexed_activity_count": len(activities),
        "government_entity_scope_counts": dict(Counter(
            item["government_entity_scope"] for item in activities)),
        "periods_with_reports": sorted({PERIODS[r["filing_period"]] for r in current.values()}),
        "complete_at_snapshot_by_count": snapshot.get("complete_as_of_snapshot", False),
        "complete_before_cutoff_by_count": snapshot.get("complete_before_cutoff_by_count", False),
        "verified_at": snapshot.get("verified_at_utc"),
        "source_count_at_verification": snapshot.get("live_api_count"),
        "quarters": quarter_coverage(year, source_date),
        "coverage_note": "Saved source data as of the stated date. Count checks do not establish that all required reports have been filed. Current and future quarters are incomplete; late filings and amendments may arrive later.",
        "input_sha256": {name: file_digest(interim / name) for name in (
            "filings.csv", "filing_activities.csv", "filing_activity_lobbyists.csv",
            "filing_activity_government_entities.csv", "normalization_manifest.json")},
    }


def build_explorer(years: list[int], *, root: Path = PROJECT_ROOT, reference: Path = REFERENCE,
                   output: Path = EXPORT) -> dict:
    topic_config = json.loads((reference / "topics.json").read_text(encoding="utf-8"))
    org_config = json.loads((reference / "organizations.json").read_text(encoding="utf-8"))
    topics = Topics(topic_config)
    orgs = Organizations(org_config, list(rows(reference / "organization_reviews.csv")))
    reviews = load_topic_reviews(reference / "topic_reviews.csv", topics)
    items, ledger, queue, nonmatches, identities, sources = [], [], [], [], [], []
    for year in sorted(set(years)):
        result = build_year(year, root, topics, orgs, reviews)
        for target, values in zip((items, ledger, queue, nonmatches, identities), result[:5]):
            target.extend(values)
        sources.append(result[5])
    items.sort(key=lambda r: (-r["year"], -r["quarter"], r["organization_name"] or r["client_name"], r["activity_id"]))
    indexed_ids = {item["activity_id"] for item in items}
    metadata = {
        "schema_version": 1, "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rules_version": topics.version, "organization_version": org_config["version"],
        "sources": sources, "activity_count": len(items),
        "report_count": len({item["filing_uuid"] for item in items}),
        "selection_method": "Latest dt_posted per source registrant/client/year/quarter; ambiguous families excluded. Registrations excluded. No-activity reports supersede earlier reports.",
        "reference_sha256": {name: file_digest(reference / name) for name in (
            "topics.json", "organizations.json", "topic_reviews.csv", "organization_reviews.csv")},
        "counts_note": "Issue entries and distinct reports are not meetings. Unmapped client IDs are not deduplicated organizations. Dollar amounts are not allocated to topics.",
        "government_entities_note": "For filings posted before February 14, 2021, the API lists government entities for the entire filing, not the individual issue entry. The transition date is treated as unknown scope. Later records link entities to issue entries; none establishes a meeting or policy position.",
        "unused_review_count": sum(1 for (aid, _, version) in reviews
            if version != topics.version or aid not in indexed_ids),
    }
    # Build everything before replacing the export files. Never rewrite reference decisions.
    save_json(output / "explorer.json", {"metadata": metadata, "topics": topic_config["topics"],
        "organizations": org_config["organizations"], "watchlists": org_config["watchlists"], "activities": items})
    save_json(output / "manifest.json", metadata)
    save_json(output / "topics.json", topic_config)
    save_json(output / "organizations.json", org_config)
    save_csv(output / "topic_review_queue.csv", queue, REVIEW_FIELDS + ["match_kind", "client_name", "description", "filing_url"])
    save_csv(output / "nonmatch_sample.csv", nonmatches, ["activity_id", "client_name", "description",
        "description_sha256", "rules_version", "filing_document_url"])
    unique_identities = {(r["registrant_api_id"], r["client_api_id"]): r for r in identities}
    save_csv(output / "organization_review_queue.csv", list(unique_identities.values()), [
        "registrant_api_id", "client_api_id", "organization_id", "decision", "reviewer", "reviewed_at", "notes",
        "client_name", "registrant_name", "organization_name", "organization_status", "filing_document_url"])
    save_csv(output / "report_versions.csv", ledger, list(ledger[0]) if ledger else ["filing_uuid", "selection_status"])
    evidence = []
    for item in items:
        for match in item["matches"]:
            evidence.append({**{k: item[k] for k in ("activity_id", "filing_uuid", "year", "quarter",
                "client_name", "registrant_name", "organization_name", "organization_status", "issue_code",
                "description", "description_sha256", "filing_url", "government_entity_scope")}, "topic_id": match["topic_id"],
                "match_kind": match["kind"], "decision": match["decision"],
                "dependency_rejected": match["dependency_rejected"], "rules_version": topics.version,
                "matched_phrases": " | ".join(dict.fromkeys(e["text"] for e in match["evidence"]))})
    fields = ["activity_id", "filing_uuid", "year", "quarter", "client_name", "registrant_name",
        "organization_name", "organization_status", "issue_code", "description", "description_sha256",
        "filing_url", "government_entity_scope", "topic_id", "match_kind", "decision", "dependency_rejected", "rules_version", "matched_phrases"]
    save_csv(output / "matches.csv", evidence, fields)
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("years", nargs="+", type=int)
    parser.add_argument("--output", type=Path, default=EXPORT)
    args = parser.parse_args()
    metadata = build_explorer(args.years, output=args.output)
    print(json.dumps({key: metadata[key] for key in ("activity_count", "report_count", "rules_version")}, indent=2))


if __name__ == "__main__":
    main()
