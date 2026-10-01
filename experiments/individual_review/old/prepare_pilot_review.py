"""Prepare (never apply) the documented first-pass review for the 20-person pilot.

The profiles were authored after inspecting per-person name/employer/role
inventories and public identity sources. This deterministic expansion is labeled
agent-assisted, not independent human review. Inspect the draft before using
watchlist.py decide. Ordinary scan/build/export never runs this script.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

if __package__:
    from .watchlist import PILOT, STATE, anchor_matches, digest, normalize, parse_name, read_json, write_json
else:
    from watchlist import PILOT, STATE, anchor_matches, digest, normalize, parse_name, read_json, write_json

NEUTRAL = {"", "SELF", "SELF EMPLOYED", "NOT EMPLOYED", "RETIRED", "NONE", "N A",
           "INFORMATION REQUESTED"}
NEUTRAL_ROLES = {"", "SELF", "SELF EMPLOYED", "NOT EMPLOYED", "RETIRED", "NONE", "N A",
                 "INFORMATION REQUESTED"}


def compatible_name(group, profile):
    parsed = parse_name(group["name"])
    if parsed["first"] not in profile["given_names"]:
        return False, "The given name is a retrieval hypothesis, not an approved variant of this person."
    if any(m[0] not in profile["middle_initials"] for m in parsed["middle"]):
        return False, "The reported middle name/initial conflicts with or extends beyond the reviewed target variants."
    if any(s not in profile["suffixes"] for s in parsed["suffixes"]):
        return False, "The generational suffix is not supported by the reviewed identity evidence."
    return True, ""


def make_decision(group, person, profile, stamp):
    okay, conflict = compatible_name(group, profile)
    employer, occupation = normalize(group["employer"]), normalize(group["occupation"])
    outcome, criteria = "unresolved", ["U1"]
    rationale = "The proposed identity is plausible from the name, but this group lacks a specifically corroborated affiliation and compatible role. No merge is established."
    limits = "A matching name or state alone cannot establish this identity. Obtain an original-filing clarification or stronger independent corroboration."
    collision = next((c for c in profile.get("reject_contexts", [])
                      if employer == c["employer"] and occupation == c["occupation"]), None)
    if group["entity_tp"] != "IND":
        outcome, criteria = "rejected", ["R2"]
        rationale = "The source does not explicitly classify this group as an individual. It is excluded from this person's mapping."
        limits = "This rejects the proposed person mapping; it does not adjudicate the economic beneficiary of an organization or correct the FEC entity type."
    elif collision:
        outcome = "rejected"
        criteria = ["R4" if collision.get("source_ids") else "R3"]
        rationale = collision["rationale"]
        limits = ("Reject this proposed link to the named public figure. This is an evidence-based identity judgment, "
                  "not a correction to the original filing. Reopen only if stronger filing-specific evidence explains the conflict. "
                  "An alternative biography supports exclusion but does not create a new donor identity in this pilot.")
    elif not okay:
        parsed = parse_name(group["name"])
        outcome = "unresolved" if len(parsed["first"]) == 1 or parsed["first"] in profile["given_names"] else "rejected"
        criteria = ["U2"] if outcome == "unresolved" else ["R1"]
        rationale = conflict + " No merge is established."
        limits = "A filing typo remains possible. Rejection means insufficient basis for this proposed merge, not proof of another real-world identity. Do not erase the conflict during name normalization."
    elif group["invalid_date_count"]:
        criteria = ["U2"]
        rationale = "At least one record has an invalid or missing transaction date; the group's time context needs review."
    elif employer in profile["employers"] and re.search(profile["roles"], occupation):
        outcome, criteria = "accepted", ["N1_E1_O1"]
        rationale = (f"Accept this bounded group into {person['display_name']}: the reviewed name variant, "
                     f"specific affiliation '{group['employer']}', and role '{group['occupation']}' jointly corroborate the public-person profile.")
        limits = ("Filer-reported data can contain errors. This establishes a reviewed identity link only; "
                  "it does not verify current employment, deduplicate gifts, or approve future records. "
                  "A historical role may be repeated in later filings; consult the dated source observations and person-level review note.")
    elif employer not in NEUTRAL or occupation not in NEUTRAL_ROLES:
        criteria = ["U2"]
        rationale = ("The name is compatible, but the reported employer/role has not met this person's documented acceptance criteria. "
                     "It may describe a different person, a stale role, a swapped field, or an unsourced affiliation; no merge is established.")
    source_ids = [s["source_id"] for s in person["sources"] if s.get("purpose") != "collision"]
    if collision:
        source_ids += collision.get("source_ids", [])
    support = (f"Observed {group['record_count']} record(s): name '{group['name']}', employer "
               f"'{group['employer'] or '(blank)'}', occupation '{group['occupation'] or '(blank)'}', "
               f"{group['city']}/{group['state']}, ZIP {group['zip_code']}; dates "
               f"{group['date_min']} to {group['date_max']}. Public identity sources: {', '.join(source_ids)}. "
               "Every included record ID and filing reference is preserved in the evidence snapshot.")
    return {"person_id": person["person_id"], "group_id": group["group_id"],
            "evidence_sha256": group["evidence_sha256"],
            "person_context_sha256": group["person_context_sha256"][person["person_id"]],
            "decision": outcome, "criteria": criteria, "rationale": rationale,
            "support": support, "limitations": limits, "source_ids": source_ids,
            "reviewer": "Codex — agent-assisted initial review", "reviewed_at": stamp,
            "assessment_mode": "documented_person_profile_applied_to_evidence_group",
            "review_profile_sha256": digest(profile), "anchor_refs": []}


def direct_anchor(group, candidates):
    # Exact full ZIP+4 and exact normalized name are deliberately required in v1.
    # A shared ZIP5 is useful for discovery but insufficient for this initial merge.
    for anchor, decision in candidates:
        if anchor_matches(group, anchor):
            return anchor, decision
    return None


def prepare():
    bundle = read_json(STATE / "evidence.json")
    profiles = read_json(PILOT / "review_profiles.json")
    people = {p["person_id"]: p for p in bundle["people"]}
    group_by_id = {g["group_id"]: g for g in bundle["groups"]}
    stamp = datetime.now(timezone.utc).isoformat()
    decisions = [make_decision(g, people[pid], profiles[pid], stamp)
                 for g in bundle["groups"] for pid in g["candidate_people"]]
    for d in decisions:
        d["decision_id"] = "d_" + digest(d)[:24]
    anchors = {}
    for d in decisions:
        if d["decision"] == "accepted":
            anchors.setdefault(d["person_id"], []).append((group_by_id[d["group_id"]], d))
    # One direct corroboration hop only, using specific frozen source groups.
    for d in decisions:
        if d["decision"] != "unresolved":
            continue
        g, profile = group_by_id[d["group_id"]], profiles[d["person_id"]]
        if not compatible_name(g, profile)[0] or g["entity_tp"] != "IND":
            continue
        emp, occ = normalize(g["employer"]), normalize(g["occupation"])
        if emp not in NEUTRAL | set(profile.get("anchor_extra_employers", [])):
            continue
        if occ not in NEUTRAL_ROLES and not re.search(r"INVESTOR|INVESTMENTS|PHILANTHROP|ENTREPRENEUR|VENTURE|FOUNDING PARTNER|CEO", occ):
            continue
        match = direct_anchor(g, anchors.get(d["person_id"], []))
        if not match:
            continue
        anchor, ad = match
        d.update(decision="accepted", criteria=["N1_L1_T1"],
                 rationale="Accept this bounded group using a direct affiliation-supported anchor: the normalized full reported name, city, state and full nine-digit ZIP match, and every record is within 90 days of an anchor record in the same cycle. No conflicting role was accepted.",
                 support=d["support"] + f" Direct anchor {anchor['group_id']} reports {anchor['employer']}/{anchor['occupation']} with decision {ad['decision_id']}.",
                 limitations="Location and time corroborate the name; neither alone proves identity. This is an agent-assisted inference, not a new employment claim. It does not propagate to another alias or location. Rejection, revision or source change of the direct anchor invalidates this link.",
                 anchor_refs=[{"group_id": anchor["group_id"], "evidence_sha256": anchor["evidence_sha256"], "decision_id": ad["decision_id"]}])
        d.pop("decision_id")
        d["decision_id"] = "d_" + digest(d)[:24]
    write_json(STATE / "draft_decisions.json", decisions)
    from collections import Counter
    for pid, p in people.items():
        subset = [d for d in decisions if d["person_id"] == pid]
        print(p["display_name"], dict(Counter(d["decision"] for d in subset)))
    print("Draft written only; no durable decisions or production outputs changed.")


if __name__ == "__main__":
    prepare()
