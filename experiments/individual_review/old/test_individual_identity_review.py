"""Identity mistakes must not silently merge people or alter campaign money."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from experiments.individual_review.prepare_pilot_review import make_decision
from experiments.individual_review.watchlist import (
    COLS, PILOT, anchor_matches, annotate_records, build_groups, candidate_reason,
    decision_history, decision_state, digest, evidence_bundle, latest_decisions,
    parse_name, read_json, resolved_links, review_states, validate_decision, write_json,
)


def person(pid="p1"):
    return {"person_id": pid, "display_name": "Alex Example", "search_surnames": ["EXAMPLE"],
            "search_given_names": ["ALEX", "ALEXANDER"],
            "sources": [{"source_id": "company-bio", "claim": "Founder"}]}


def row(sub="1", **changes):
    value = {key: "" for key in COLS}
    value.update(cycle=2024, sub_id=sub, name="EXAMPLE, ALEX C. III", employer="EXAMPLE TECH",
                 occupation="FOUNDER", state="CA", city="EXAMPLE CITY", zip_code="900001234",
                 transaction_dt="01012024", transaction_amt="100", entity_tp="IND", cmte_id="C1")
    return {**value, **changes}


def accepted(group, pid="p1", **changes):
    d = {"person_id": pid, "group_id": group["group_id"], "decision": "accepted",
         "evidence_sha256": group["evidence_sha256"], "person_context_sha256": group["person_context_sha256"][pid],
         "criteria": ["N1_E1_O1"], "source_ids": ["company-bio"], "reviewer": "Test reviewer",
         "reviewed_at": "2026-09-20T00:00:00Z", "rationale": "Name and founder/company evidence agree.",
         "support": "Source biography plus specific filing role.", "limitations": "Filer-reported evidence.",
         "anchor_refs": [], **changes}
    d["decision_id"] = "d_" + digest(d)[:24]
    return d


class IndividualIdentityReviewTests(unittest.TestCase):
    def test_middle_and_generational_suffix_survive_formatting(self):
        parsed = parse_name("EXAMPLE III, ALEX C. DR.")
        self.assertEqual(parsed, {"last": "EXAMPLE", "first": "ALEX", "middle": ["C"], "suffixes": ["III"]})
        self.assertEqual(parse_name("Dr Alex C Example III"), parsed)

    def test_initials_nicknames_typos_retrieve_without_amount_or_employer(self):
        p = person()
        for name in ["EXAMPLE, ALEX", "EXAMPLE, ALEXANDER", "EXAMPLE, A.", "EXAMPLE, ALEXX"]:
            self.assertTrue(candidate_reason(name, p), name)
        self.assertIsNone(candidate_reason("EXAMPLE, ANNE", p))
        groups, _ = build_groups([row(transaction_amt="1", employer="")], [p])
        self.assertEqual(len(groups), 1)
        self.assertEqual(resolved_links(groups, []), [])

    def test_same_name_and_state_keep_different_contexts_separate(self):
        groups, _ = build_groups([row(), row("2", employer="OTHER BUSINESS", occupation="NURSE")], [person()])
        self.assertEqual(len(groups), 2)
        chosen = next(g for g in groups if g["employer"] == "EXAMPLE TECH")
        links = resolved_links(groups, [accepted(chosen)])
        self.assertEqual([r["record_id"] for r in links], ["2024:1"])

    def test_source_change_and_new_record_in_same_group_require_review(self):
        original, _ = build_groups([row()], [person()])
        decision = accepted(original[0])
        for rows in [[row(transaction_amt="101")], [row(), row("2")]]:
            changed, _ = build_groups(rows, [person()])
            self.assertEqual(decision_state(changed[0], decision), "stale")
            self.assertEqual(resolved_links(changed, [decision]), [])

    def test_public_identity_source_change_invalidates_old_match(self):
        groups, _ = build_groups([row()], [person()])
        changed_person = person()
        changed_person["sources"][0]["claim"] = "Corrected biography"
        updated, _ = build_groups([row()], [changed_person])
        self.assertEqual(resolved_links(updated, [accepted(groups[0])]), [])

    def test_two_people_cannot_claim_the_same_record(self):
        groups, _ = build_groups([row()], [person("p1"), person("p2")])
        with self.assertRaisesRegex(ValueError, "Conflicting accepted"):
            resolved_links(groups, [accepted(groups[0]), accepted(groups[0], "p2")])

    def test_missing_identity_sources_or_rationale_prevents_acceptance(self):
        groups, _ = build_groups([row()], [person()])
        by_id = {g["group_id"]: g for g in groups}
        for changes in [{"source_ids": []}, {"rationale": ""}, {"source_ids": ["invented"]},
                        {"criteria": ["NAME_ONLY"]}, {"person_context_sha256": "old"}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_decision(accepted(groups[0], **changes), by_id, [person()])

    def test_organization_not_accepted_as_individual(self):
        groups, _ = build_groups([row(entity_tp="ORG")], [person()])
        with self.assertRaisesRegex(ValueError, "explicitly individual"):
            validate_decision(accepted(groups[0]), {groups[0]["group_id"]: groups[0]}, [person()])

    def test_anchor_revision_invalidates_dependents_and_disallows_chains(self):
        groups, _ = build_groups([row(), row("2", employer="SELF"), row("3", employer="")], [person()])
        direct = next(g for g in groups if g["employer"] == "EXAMPLE TECH")
        indirect = next(g for g in groups if g["employer"] == "SELF")
        third = next(g for g in groups if g["employer"] == "")
        a = accepted(direct)
        b = accepted(indirect, criteria=["N1_L1_T1"], anchor_refs=[{
            "group_id": direct["group_id"], "evidence_sha256": direct["evidence_sha256"], "decision_id": a["decision_id"]}])
        c = accepted(third, criteria=["N1_L1_T1"], anchor_refs=[{
            "group_id": indirect["group_id"], "evidence_sha256": indirect["evidence_sha256"], "decision_id": b["decision_id"]}])
        self.assertEqual(len(resolved_links(groups, [a, b, c])), 2)
        revised = accepted(direct, decision="unresolved", criteria=["U1"])
        self.assertEqual(resolved_links(groups, [a, b, revised]), [])
        self.assertEqual(review_states(groups, [a, b, revised])[("p1", indirect["group_id"])], "stale")

    def test_record_identifier_conflict_fails_and_repeated_occurrences_are_reported(self):
        with self.assertRaisesRegex(ValueError, "Conflicting contents"):
            build_groups([row(), row(employer="OTHER")], [person()])
        groups, duplicates = build_groups([row(), row()], [person()])
        self.assertEqual(duplicates, 1)
        self.assertEqual(groups[0]["record_count"], 1)

    def test_annotation_preserves_real_repeat_gifts_refunds_and_memos(self):
        rows = [row(), row("2"), row("3", transaction_amt="-50", memo_cd="X"),
                row("4", transaction_amt="25", transaction_tp="22Y"), row("5", name="OTHER, ALEX")]
        original = copy.deepcopy(rows)
        groups, _ = build_groups(rows, [person()])
        links = resolved_links(groups, [accepted(g) for g in groups])
        annotated = annotate_records(rows, links)
        self.assertEqual(rows, original)
        self.assertEqual(len(annotated), len(rows))
        for before, after in zip(rows, annotated):
            self.assertTrue(all(after[k] == v for k, v in before.items()))
        self.assertIsNone(annotated[-1]["person_id"])
        self.assertEqual(len(links), 4)
        with self.assertRaisesRegex(ValueError, "contents changed"):
            annotate_records([row(transaction_amt="200")], links)

    def test_anchor_requires_every_date_full_zip_name_and_cycle(self):
        base = build_groups([row()], [person()])[0][0]
        nearby = build_groups([row("2", employer="SELF", transaction_dt="03312024")], [person()])[0][0]
        self.assertTrue(anchor_matches(nearby, base))  # exactly 90 days in leap year
        for changes in [dict(transaction_dt="04012024"), dict(zip_code="90000"),
                        dict(cycle=2026), dict(name="EXAMPLE, ALEXANDER C. III"), dict(city="OTHER CITY")]:
            candidate = build_groups([row("2", employer="SELF", **changes)], [person()])[0][0]
            self.assertFalse(anchor_matches(candidate, base), changes)
        mixed_dates = build_groups([row("2", employer="SELF"), row("3", employer="SELF", transaction_dt="07012024")], [person()])[0][0]
        self.assertFalse(anchor_matches(mixed_dates, base))

    def test_journal_cannot_bypass_anchor_geographic_constraint(self):
        groups, _ = build_groups([row(), row("2", employer="SELF", zip_code="800001234")], [person()])
        direct = next(g for g in groups if g['employer'] == 'EXAMPLE TECH')
        other = next(g for g in groups if g['employer'] == 'SELF')
        a = accepted(direct)
        b = accepted(other, criteria=['N1_L1_T1'], anchor_refs=[{
            'group_id': direct['group_id'], 'evidence_sha256': direct['evidence_sha256'], 'decision_id': a['decision_id']}])
        self.assertEqual(len(resolved_links(groups, [a,b])), 1)

    def test_tracked_snapshot_can_be_reviewed_without_ignored_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root / 'pilot' / 'evidence.json', {'snapshot': True})
            with patch('experiments.individual_review.watchlist.STATE', root / 'empty'), \
                 patch('experiments.individual_review.watchlist.PILOT', root / 'pilot'):
                self.assertEqual(evidence_bundle(), {'snapshot': True})


class PilotEvidenceRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = read_json(PILOT / 'evidence.json')
        cls.profiles = read_json(PILOT / 'review_profiles.json')
        cls.history = decision_history()
        cls.latest = latest_decisions(cls.history)

    def test_attorney_namesake_is_rejected_with_explicit_corroboration(self):
        groups = [g for g in self.bundle['groups'] if 'p0003' in g['candidate_people'] and g['employer'] == 'VENABLE LLP']
        self.assertTrue(groups)
        self.assertEqual(sum(g['record_count'] for g in groups), 18)
        for group in groups:
            decision = self.latest['p0003', group['group_id']]
            self.assertEqual(decision['decision'], 'rejected')
            self.assertEqual(decision['criteria'], ['R4'])
            self.assertIn('collision-ben-venable', decision['source_ids'])
            self.assertIn('litigation attorney', decision['rationale'])
        links = resolved_links(self.bundle['groups'], self.history)
        self.assertFalse({r['record_id'] for g in groups for r in g['records']} & {l['record_id'] for l in links})

    def test_shared_company_does_not_override_conflicting_staff_role(self):
        cases = [('p0003', 'WORKDAY'), ('p0012', 'GOOGLE'), ('p0012', 'GOOGLE LLC')]
        for pid, employer in cases:
            groups = [g for g in self.bundle['groups'] if pid in g['candidate_people'] and g['employer'] == employer and 'ENGINEER' in g['occupation']]
            self.assertTrue(groups, (pid, employer))
            for group in groups:
                self.assertEqual(self.latest[pid, group['group_id']]['decision'], 'rejected')

    def test_unfamiliar_investment_vehicle_and_missing_employer_are_not_professional_rejections(self):
        for pid, employer in [('p0008','APOLLO PROJECTS'), ('p0019','LAWRENCE INVESTMENTS, LLC')]:
            groups = [g for g in self.bundle['groups'] if pid in g['candidate_people'] and g['employer'] == employer]
            self.assertTrue(groups)
            for group in groups:
                self.assertEqual(self.latest[pid, group['group_id']]['decision'], 'unresolved')

    def test_latest_judgments_match_authored_profiles_and_archive_has_historical_context(self):
        people = {p['person_id']:p for p in self.bundle['people']}
        contexts = read_json(PILOT / 'context_history.json')
        for decision in self.history:
            self.assertIn(decision['review_profile_sha256'], contexts['profiles'])
            self.assertIn(decision['person_context_sha256'], contexts['people'])
        for group in self.bundle['groups']:
            for pid in group['candidate_people']:
                decision = self.latest[pid, group['group_id']]
                self.assertEqual(decision['review_profile_sha256'], digest(self.profiles[pid]))
                self.assertEqual(decision_state(group, decision), decision['decision'])
                proposed = make_decision(group, people[pid], self.profiles[pid], decision['reviewed_at'])
                # Secondary matches use separately audited location/time anchors.
                if decision['criteria'] != ['N1_L1_T1']:
                    self.assertEqual(proposed['decision'], decision['decision'], group['group_id'])

    def test_frozen_evidence_rebuild_and_mapping_preserve_all_raw_fields(self):
        rows = [r for g in self.bundle['groups'] for r in g['records']]
        rebuilt, _ = build_groups(rows, self.bundle['people'])
        self.assertEqual(rebuilt, self.bundle['groups'])
        links = resolved_links(rebuilt, self.history)
        self.assertEqual(len(links), len({l['record_id'] for l in links}))
        annotated = annotate_records(rows, links)
        self.assertEqual(len(rows), len(annotated))
        for before, after in zip(rows, annotated):
            self.assertTrue(all(after[k] == v for k, v in before.items()))


if __name__ == "__main__":
    unittest.main()
