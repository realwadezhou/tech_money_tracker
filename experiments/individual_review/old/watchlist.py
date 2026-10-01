"""Evidence-scoped prominent-person review; never imported by the money pipeline.

python experiments/individual_review/watchlist.py scan --cycle 2024 --cycle 2026
python experiments/individual_review/watchlist.py build
python experiments/individual_review/watchlist.py serve --port 8766
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import threading
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PILOT = HERE / "pilot"
STATE = HERE / "state" / "pilot"
COLS = ["cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
        "transaction_tp", "entity_tp", "name", "city", "state", "zip_code",
        "employer", "occupation", "transaction_dt", "transaction_amt", "other_id",
        "tran_id", "file_num", "memo_cd", "memo_text", "sub_id"]
TUPLE_FIELDS = ["cycle", "name", "city", "state", "zip_code", "employer", "occupation", "entity_tp"]
TITLES = {"MR", "MRS", "MS", "MISS", "DR", "MD", "PHD", "ESQ"}
SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}
VERSION = "prominent-people-v1"
LOCK = threading.Lock()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(path)


def normalize(value):
    value = unicodedata.normalize("NFKD", str(value).upper())
    return " ".join(re.findall(r"[A-Z0-9]+", value))


def parse_name(value):
    # Honorifics are formatting noise; middle names and generations are evidence.
    parts = str(value).split(",", 1)
    if len(parts) == 2:
        last, given = normalize(parts[0]).split(), normalize(parts[1]).split()
    else:
        tokens = [t for t in normalize(value).split() if t not in TITLES]
        suffix = [t for t in tokens if t in SUFFIXES]
        tokens = [t for t in tokens if t not in SUFFIXES]
        last, given = tokens[-1:], tokens[:-1] + suffix
    suffixes = [t for t in last + given if t in SUFFIXES]
    last = [t for t in last if t not in TITLES | SUFFIXES]
    given = [t for t in given if t not in TITLES | SUFFIXES]
    return {"last": " ".join(last), "first": given[0] if given else "",
            "middle": given[1:], "suffixes": suffixes}


def candidate_reason(name, person):
    parsed = parse_name(name)
    if parsed["last"] not in person["search_surnames"]:
        return None
    first = parsed["first"]
    if first in person["search_given_names"]:
        return "surname_and_given_name"
    # Initial expansion is deliberately noisy and never confers acceptance.
    if len(first) == 1 and first in {x[0] for x in person["search_given_names"]}:
        return "surname_and_first_initial"
    if len(first) >= 4 and any(one_edit(first, given) for given in person["search_given_names"] if len(given) >= 4):
        return "surname_and_given_name_typo"
    return None


def one_edit(left, right):
    if abs(len(left) - len(right)) > 1:
        return False
    if len(left) == len(right):
        return sum(a != b for a, b in zip(left, right)) == 1
    short, long = sorted((left, right), key=len)
    return any(long[:i] + long[i + 1:] == short for i in range(len(long)))


def registry():
    data = read_json(PILOT / "people.json")
    ids = [p["person_id"] for p in data["people"]]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate person_id")
    return data


def evidence_bundle():
    """A fresh checkout can review/export the tracked snapshot without raw inputs."""
    path = STATE / "evidence.json"
    return read_json(path if path.exists() else PILOT / "evidence.json")


def archive_contexts(bundle):
    """Retain the actual reviewed context behind historical decision hashes."""
    path = PILOT / "context_history.json"
    history = read_json(path) if path.exists() else {"people": {}, "profiles": {}}
    for person in bundle["people"]:
        history["people"][digest(person)] = person
    profiles = read_json(PILOT / "review_profiles.json")
    for profile in profiles.values():
        history["profiles"][digest(profile)] = profile
    write_json(path, history)


def scan(cycles):
    people = registry()["people"]
    surnames = sorted({s for p in people for s in p["search_surnames"]})
    pattern = r"\b(?:" + "|".join(re.escape(s) for s in surnames) + r")\b"
    rg = shutil.which("rg")
    if not rg:
        raise RuntimeError("ripgrep is required for the bulk scan")
    STATE.mkdir(parents=True, exist_ok=True)
    manifests = []
    for cycle in cycles:
        source = ROOT / f"data/fec/interim/{cycle}/indiv{str(cycle)[-2:]}/itcont.txt"
        before = source.stat()  # Fail visibly; never silently omit a requested cycle.
        output = STATE / f"surname_records_{cycle}.txt"
        temporary = output.with_suffix(".tmp")
        print(f"Scanning {cycle}: {before.st_size:,} bytes", flush=True)
        with temporary.open("wb") as stream:
            result = subprocess.run([rg, "--text", "--no-heading", "--no-filename", "-i",
                                     pattern, str(source)], stdout=stream, stderr=subprocess.PIPE)
        if result.returncode not in (0, 1):
            raise RuntimeError(result.stderr.decode(errors="replace"))
        after = source.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RuntimeError(f"Source changed during scan: {source}")
        temporary.replace(output)
        manifest = {"cycle": cycle, "source_path": source.relative_to(ROOT).as_posix(),
                    "source_size": before.st_size, "source_mtime_ns": before.st_mtime_ns,
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "retrieval_surnames": surnames, "retrieval_version": VERSION,
                    "extract_sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
        write_json(STATE / f"scan_{cycle}.json", manifest)
        manifests.append(manifest)
        print(f"Extracted {output.stat().st_size:,} bytes", flush=True)
    # Only the explicitly requested cycles participate; leftover caches cannot leak in.
    write_json(STATE / "scan_manifest.json", {"cycles": cycles, "sources": manifests})


def date_iso(raw):
    try:
        return datetime.strptime(raw, "%m%d%Y").date().isoformat()
    except ValueError:
        return ""


def build_groups(records, people):
    groups = {}
    row_seen = {}
    duplicate_occurrences = 0
    for row in records:
        matches = [(p["person_id"], candidate_reason(row["name"], p)) for p in people]
        matches = [(pid, reason) for pid, reason in matches if reason]
        if not matches:
            continue
        if not row["sub_id"]:
            raise ValueError("Candidate source record is missing SUB_ID")
        record_id = f"{row['cycle']}:{row['sub_id']}"
        row_hash = digest({k: row[k] for k in COLS})
        if record_id in row_seen:
            if row_seen[record_id] != row_hash:
                raise ValueError(f"Conflicting contents for {record_id}")
            duplicate_occurrences += 1
            continue  # one identity link, not an accounting deletion
        row_seen[record_id] = row_hash
        tuple_value = {k: row[k] for k in TUPLE_FIELDS}
        group_id = "g_" + digest(tuple_value)[:24]
        group = groups.setdefault(group_id, {
            "group_id": group_id, **tuple_value, "candidate_people": {},
            "person_context_sha256": {p["person_id"]: digest(p) for p in people if p["person_id"] in dict(matches)},
            "records": []})
        group["candidate_people"].update(dict(matches))
        group["records"].append({**row, "record_id": record_id, "row_sha256": row_hash,
                                 "date": date_iso(row["transaction_dt"]),
                                 "filing_url": f"https://docquery.fec.gov/cgi-bin/fecimg/?{row['image_num']}"
                                 if row["image_num"] else ""})
    for group in groups.values():
        group["records"].sort(key=lambda r: r["record_id"])
        group["evidence_sha256"] = digest([(r["record_id"], r["row_sha256"]) for r in group["records"]])
        dates = sorted(r["date"] for r in group["records"] if r["date"])
        group["date_min"] = dates[0] if dates else ""
        group["date_max"] = dates[-1] if dates else ""
        group["record_count"] = len(group["records"])
        group["invalid_date_count"] = sum(not r["date"] for r in group["records"])
        group["raw_signed_amount"] = str(sum((Decimal(r["transaction_amt"] or "0") for r in group["records"]), Decimal(0)))
    return sorted(groups.values(), key=lambda g: g["group_id"]), duplicate_occurrences


def load_records(manifest):
    for source in manifest["sources"]:
        path = STATE / f"surname_records_{source['cycle']}.txt"
        if hashlib.sha256(path.read_bytes()).hexdigest() != source["extract_sha256"]:
            raise ValueError(f"Changed extract: {path}")
        with path.open(encoding="utf-8", errors="strict", newline="") as stream:
            for line_no, line in enumerate(stream, 1):
                fields = line.rstrip("\r\n").split("|")
                if len(fields) != len(COLS):
                    raise ValueError(f"Malformed extract {path}:{line_no}")
                yield {**dict(zip(COLS, fields)), "cycle": source["cycle"]}


def decision_history(path=None):
    path = path or PILOT / "decisions.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def latest_decisions(history):
    result = {}
    for decision in history:
        result[(decision["person_id"], decision["group_id"])] = decision
    return result


def decision_state(group, decision):
    if not decision:
        return "unreviewed"
    if decision["evidence_sha256"] != group["evidence_sha256"]:
        return "stale"
    if decision.get("person_context_sha256") != group["person_context_sha256"][decision["person_id"]]:
        return "stale"
    return decision["decision"]


def anchor_matches(group, anchor):
    if (len(group["zip_code"]) != 9 or not group["zip_code"].isdigit()
            or group["invalid_date_count"] or anchor["invalid_date_count"]
            or group["cycle"] != anchor["cycle"]):
        return False
    if any(normalize(group[k]) != normalize(anchor[k]) for k in ("name", "city", "state", "zip_code")):
        return False
    dates = [datetime.fromisoformat(r["date"]) for r in group["records"]]
    others = [datetime.fromisoformat(r["date"]) for r in anchor["records"]]
    return bool(dates and others) and all(min(abs((d - a).days) for a in others) <= 90 for d in dates)


def review_states(groups, history):
    latest = latest_decisions(history)
    by_id = {g["group_id"]: g for g in groups}
    result = {(pid, g["group_id"]): decision_state(g, latest.get((pid, g["group_id"])))
              for g in groups for pid in g["candidate_people"]}
    for key, state in list(result.items()):
        if state != "accepted":
            continue
        d = latest[key]
        if "N1_L1_T1" in d["criteria"] and not d.get("anchor_refs"):
            result[key] = "stale"
        for ref in d.get("anchor_refs", []):
            anchor = latest.get((key[0], ref["group_id"]))
            anchor_group = by_id.get(ref["group_id"])
            # Direct evidence only: do not let weak matches form transitive chains.
            if (not anchor or not anchor_group or decision_state(anchor_group, anchor) != "accepted"
                    or "N1_E1_O1" not in anchor["criteria"] or anchor.get("anchor_refs")
                    or anchor["decision_id"] != ref["decision_id"]
                    or anchor_group["evidence_sha256"] != ref["evidence_sha256"]
                    or not anchor_matches(by_id[key[1]], anchor_group)):
                result[key] = "stale"
    return result


def validate_decision(decision, groups, people):
    group = groups.get(decision.get("group_id"))
    if not group or decision.get("person_id") not in group["candidate_people"]:
        raise ValueError("Decision must reference an existing candidate person/group pair")
    if decision.get("decision") not in {"accepted", "rejected", "unresolved"}:
        raise ValueError("Invalid decision")
    if decision.get("evidence_sha256") != group["evidence_sha256"]:
        raise ValueError("Evidence changed; review the current records")
    if decision.get("person_context_sha256") != group["person_context_sha256"][decision["person_id"]]:
        raise ValueError("Public-person context changed; review again")
    for key in ("reviewer", "reviewed_at", "rationale", "criteria", "support", "limitations"):
        if not decision.get(key):
            raise ValueError(f"A decision requires {key}")
    if not isinstance(decision["criteria"], list) or not isinstance(decision["source_ids"], list):
        raise ValueError("criteria and source_ids must be lists")
    valid_sources = {s["source_id"] for p in people if p["person_id"] == decision["person_id"] for s in p["sources"]}
    if not set(decision["source_ids"]).issubset(valid_sources):
        raise ValueError("Unknown source citation")
    if decision["decision"] == "accepted":
        if group["entity_tp"] != "IND":
            raise ValueError("Only explicitly individual records may be accepted in this pilot")
        if not decision["source_ids"]:
            raise ValueError("Acceptance needs public-person evidence")
        if not set(decision["criteria"]) & {"N1_E1_O1", "N1_L1_T1"}:
            raise ValueError("Acceptance needs a documented corroboration criterion")


def resolved_links(groups, history):
    decisions = latest_decisions(history)
    states = review_states(groups, history)
    links = {}
    for group in groups:
        for pid in group["candidate_people"]:
            decision = decisions.get((pid, group["group_id"]))
            if states[(pid, group["group_id"])] != "accepted":
                continue
            for row in group["records"]:
                rid = row["record_id"]
                if rid in links and links[rid]["person_id"] != pid:
                    raise ValueError(f"Conflicting accepted people for record {rid}")
                links[rid] = {"record_id": rid, "person_id": pid, "group_id": group["group_id"],
                              "row_sha256": row["row_sha256"], "decision_id": decision["decision_id"],
                              "reviewer": decision["reviewer"], "reviewed_at": decision["reviewed_at"]}
    return list(sorted(links.values(), key=lambda r: r["record_id"]))


def annotate_records(records, links):
    """Pure, one-to-one annotation for validation/future integration; no filtering."""
    lookup = {}
    for link in links:
        if link["record_id"] in lookup:
            raise ValueError("Duplicate record-person mapping")
        lookup[link["record_id"]] = link
    output = []
    for row in records:
        link = lookup.get(f"{row['cycle']}:{row['sub_id']}")
        if link and digest({k: row[k] for k in COLS}) != link["row_sha256"]:
            raise ValueError("Mapped record contents changed")
        output.append({**row, "person_id": link["person_id"] if link else None,
                       "identity_decision_id": link["decision_id"] if link else None})
    return output


def append_decisions(incoming):
    with LOCK:
        bundle = evidence_bundle()
        groups = {g["group_id"]: g for g in bundle["groups"]}
        history = decision_history()
        for decision in incoming:
            validate_decision(decision, groups, bundle["people"])
            decision.pop("decision_id", None)
            decision["decision_id"] = "d_" + digest(decision)[:24]
        states = review_states(bundle["groups"], history + incoming)
        for decision in incoming:
            if decision["decision"] == "accepted" and states[(decision["person_id"], decision["group_id"])] != "accepted":
                raise ValueError("Accepted decision has missing, stale or indirect supporting anchors")
        resolved_links(bundle["groups"], history + incoming)  # Reject cross-person collisions before writing.
        path = PILOT / "decisions.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            for decision in incoming:
                stream.write(json.dumps(decision, ensure_ascii=False) + "\n")


def build():
    data = registry()
    manifest = read_json(STATE / "scan_manifest.json")
    expected = {s for p in data["people"] for s in p["search_surnames"]}
    for source in manifest["sources"]:
        if not expected.issubset(source["retrieval_surnames"]):
            raise ValueError("Watchlist changed; rescan before building")
    groups, duplicates = build_groups(load_records(manifest), data["people"])
    bundle = {"version": VERSION, "people": data["people"], "groups": groups,
              "manifest": manifest, "duplicate_source_occurrences": duplicates}
    write_json(STATE / "evidence.json", bundle)
    print(f"Built {len(groups):,} evidence groups / {sum(g['record_count'] for g in groups):,} records", flush=True)
    export()


def export():
    bundle = evidence_bundle()
    archive_contexts(bundle)
    groups, people = bundle["groups"], bundle["people"]
    history = decision_history()
    decisions = latest_decisions(history)
    states = review_states(groups, history)
    links = resolved_links(groups, history)
    original_records = [r for g in groups for r in g["records"]]
    annotated = annotate_records(original_records, links)
    unchanged = len(original_records) == len(annotated) and all(
        all(before[k] == after[k] for k in before) for before, after in zip(original_records, annotated))
    if not unchanged:
        raise ValueError("Identity annotation changed source rows")
    with (PILOT / "record_person_links.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["record_id", "person_id", "group_id", "row_sha256",
                                                   "decision_id", "reviewer", "reviewed_at"])
        writer.writeheader()
        writer.writerows(links)
    summary = []
    for person in people:
        selected = [g for g in groups if person["person_id"] in g["candidate_people"]]
        counts = Counter(states[(person["person_id"], g["group_id"])] for g in selected)
        summary.append({"person_id": person["person_id"], "display_name": person["display_name"],
                        "groups": len(selected), "records": sum(g["record_count"] for g in selected),
                        "accepted_records": sum(g["record_count"] for g in selected if states[(person["person_id"], g["group_id"])] == "accepted"),
                        **{s: counts[s] for s in ("accepted", "rejected", "unresolved", "unreviewed", "stale")}})
    validation = {"version": VERSION, "sources": bundle["manifest"]["sources"], "people": summary,
                  "record_person_links": len(links), "unique_linked_records": len({r['record_id'] for r in links}),
                  "candidate_records": sum(g["record_count"] for g in groups),
                  "candidate_groups": len(groups), "production_integration": False,
                  "annotation_preserves_all_source_fields_and_row_order": unchanged,
                  "original_raw_signed_amount": str(sum((Decimal(r['transaction_amt'] or '0') for r in original_records), Decimal(0))),
                  "annotated_raw_signed_amount": str(sum((Decimal(r['transaction_amt'] or '0') for r in annotated), Decimal(0))),
                  "duplicate_source_occurrences": bundle["duplicate_source_occurrences"],
                  "accounting": "No rows deleted and no money rules applied; this is an identity annotation only."}
    write_json(PILOT / "validation.json", validation)
    # Evidence snapshot is bounded to this watchlist; raw bulk/extracts stay in ignored state.
    write_json(PILOT / "evidence.json", bundle)
    bundle["review_states"] = {"|".join(k): v for k, v in states.items()}
    render_review(bundle, decisions, summary)
    render_dossiers(bundle, decisions, summary)
    print(f"Exported {len(links):,} unique record-person links", flush=True)


def render_review(bundle, decisions, summary):
    template = (HERE / "watchlist.html").read_text(encoding="utf-8")
    payload = {**bundle, "decisions": list(decisions.values()), "summary": summary,
               "profiles": read_json(PILOT / "review_profiles.json")}
    serialized = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")
    (PILOT / "review.html").write_text(template.replace("/*REVIEW_DATA*/{}", serialized), encoding="utf-8")


def render_dossiers(bundle, decisions, summary):
    directory = PILOT / "dossiers"
    directory.mkdir(exist_ok=True)
    index = ["# Prominent donor identity pilot", "", "Agent-assisted review dated 2026-09-20. Decisions describe evidence and criteria, not certainty about every filing. See [methodology](../METHODOLOGY.md).", "",
             f"Reviewed {sum(s['groups'] for s in summary):,} candidate groups: {sum(s['accepted'] for s in summary):,} accepted, {sum(s['rejected'] for s in summary):,} rejected, and {sum(s['unresolved'] + s['unreviewed'] + s['stale'] for s in summary):,} open. Accepted links cover {sum(s['accepted_records'] for s in summary):,} source records, not distinct gifts.", "",
             "| Person | Accepted groups | Unresolved groups | Rejected groups | Linked records |", "|---|---:|---:|---:|---:|"]
    profiles_path = PILOT / "review_profiles.json"
    profiles = read_json(profiles_path) if profiles_path.exists() else {}
    for p, s in zip(bundle["people"], summary):
        filename = p["person_id"] + "-" + re.sub(r"[^a-z0-9]+", "-", p["display_name"].lower()) + ".md"
        index.append(f"| [{p['display_name']}](dossiers/{filename}) | {s['accepted']} | {s['unresolved'] + s['unreviewed'] + s['stale']} | {s['rejected']} | {s['accepted_records']} |")
        lines = [f"# {p['display_name']} ({p['person_id']})", "", "## Review approach", "",
                 profiles.get(p["person_id"], {}).get("review_note", "No person-specific narrative recorded yet."), "",
                 f"Examined {s['groups']} candidate evidence groups containing {s['records']} source records. Accepted {s['accepted_records']} record links. Counts are not distinct gifts or dollar totals.", "",
                 "## Identity and collision sources", ""]
        for source in p["sources"]:
            lines += [f"- [{source['title']}]({source['url']}) — {source['claim']} Observed {source['accessed']}."]
        lines += ["", "## Scope and unresolved work", "", "Only the exact records and hashes listed below are covered. Known name variants and initials were retrieved across both installed cycles without a giving threshold. Unlisted surname typos, changed surnames and unitemized giving are not covered. Acceptance never changes reported employer or gift accounting. Initial decisions were prepared by Codex using explicitly documented person-specific criteria and then expanded to evidence groups; they are not human sign-offs.", ""]
        selected = [g for g in bundle["groups"] if p["person_id"] in g["candidate_people"]]
        for g in sorted(selected, key=lambda x: (bundle["review_states"][p["person_id"]+'|'+x["group_id"]], x["name"], x["group_id"])):
            d = decisions.get((p["person_id"], g["group_id"]))
            state = bundle["review_states"][p["person_id"]+'|'+g["group_id"]]
            lines += [f"## {state.upper()}: {g['name']} — {g['group_id']}", "",
                      f"Reported context: **{g['employer'] or '(missing employer)'} / {g['occupation'] or '(missing occupation)'}**; {g['city']}, {g['state']}, ZIP {g['zip_code']}. Cycle {g['cycle']}; {g['date_min']} through {g['date_max']}; {g['record_count']} records.", ""]
            if d:
                lines += [f"**Basis:** {d['rationale']}", "", f"**Support:** {d['support']}", "",
                          f"**Limits/conflicts:** {d['limitations']}", "",
                          f"Criteria: {', '.join(d['criteria'])}. Reviewer: {d['reviewer']}; {d['reviewed_at']}. Decision `{d['decision_id']}`.", "",
                          "Source IDs: " + ", ".join(d["source_ids"]) + ".", ""]
                if d.get("anchor_refs"):
                    lines += ["Direct supporting groups: " + ", ".join(f"`{a['group_id']}` / `{a['decision_id']}`" for a in d["anchor_refs"]) + ".", ""]
            lines += [f"Evidence SHA-256: `{g['evidence_sha256']}`", "", "| Record | Date | Filing | Raw amount | Type / memo |", "|---|---|---|---:|---|"]
            for r in g["records"]:
                link = f"[filing {r['file_num']}]({r['filing_url']})" if r["filing_url"] else f"{r['file_num']} (no image)"
                lines.append(f"| `{r['record_id']}` | {r['date'] or r['transaction_dt']} | {link}; `{r['tran_id']}` | {r['transaction_amt']} | {r['transaction_tp']} / {r['memo_cd']} |")
            lines += [""]
        (directory / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
    index += ["", "Open [the interactive review](review.html), [validation summary](validation.json), or [record-to-person mapping](record_person_links.csv).", "", "No changes have been made to production identities, financial filters, exports or published totals."]
    (PILOT / "README.md").write_text("\n".join(index) + "\n", encoding="utf-8")


def serve(port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if urlparse(self.path).path != "/":
                self.send_error(404)
                return
            content = (PILOT / "review.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(content)

        def do_POST(self):
            if self.path != "/api/decision":
                self.send_error(404)
                return
            # Local review mutations accept only same-origin JSON, never cross-site form posts.
            origin = self.headers.get("Origin")
            if origin != f"http://127.0.0.1:{port}" or self.headers.get("Content-Type") != "application/json":
                self.send_error(403)
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size < 100_000:
                    raise ValueError("Invalid request size")
                decision = json.loads(self.rfile.read(size))
                decision["reviewed_at"] = datetime.now(timezone.utc).isoformat()
                append_decisions([decision])
                export()
                result, status = {"ok": True}, 200
            except (ValueError, KeyError, TypeError) as exc:
                result, status = {"error": str(exc)}, 400
            content = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    # Serialize saves and export together; concurrent exports could overwrite one
    # another's temporary files or briefly expose mixed audit/mapping versions.
    class LocalServer(HTTPServer):
        # On Windows SO_REUSEADDR can silently share an existing server's port.
        allow_reuse_address = False

    server = LocalServer(("127.0.0.1", port), Handler)
    print(f"Donor review: http://127.0.0.1:{port}/", flush=True)
    server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scan_args = sub.add_parser("scan")
    scan_args.add_argument("--cycle", type=int, action="append", required=True)
    sub.add_parser("build")
    sub.add_parser("export")
    decide = sub.add_parser("decide")
    decide.add_argument("--file", type=Path, required=True)
    server = sub.add_parser("serve")
    server.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    if args.command == "scan":
        scan(args.cycle)
    elif args.command == "build":
        build()
    elif args.command == "export":
        export()
    elif args.command == "decide":
        append_decisions(read_json(args.file))
        export()
    elif args.command == "serve":
        serve(args.port)


if __name__ == "__main__":
    main()
