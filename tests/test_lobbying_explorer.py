import csv
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from pipeline.lda.build_explorer import (Organizations, Topics, apply_reviews, build_explorer,
    digest, government_entity_scope, load_topic_reviews, save_csv, select_current_reports, quarter_coverage, REFERENCE, REVIEW_FIELDS)


def report(uid, date="2026-01-20T12:00:00-05:00", kind="Q4", **changes):
    return {"filing_uuid": uid, "dt_posted": date, "filing_type": kind,
        "filing_year": "2025", "filing_period": "fourth_quarter", "client_api_id": "1",
        "registrant_api_id": "2", "client_name": "University Research", "registrant_name": "Example Firm",
        "income": "100000", "expenses": "", "expenses_method": "",
        "filing_document_url": f"https://lda.gov/filings/public/filing/{uid}/print/", **changes}


class ReportSelectionTests(unittest.TestCase):
    def test_legacy_government_entities_have_filing_scope_based_on_posting_date(self):
        self.assertEqual(government_entity_scope("2021-02-13T23:59:59-05:00"), "filing")
        self.assertEqual(government_entity_scope("2021-02-14T12:00:00-05:00"), "unknown")
        self.assertEqual(government_entity_scope("2021-02-15T00:00:00-05:00"), "issue_entry")
        self.assertEqual(government_entity_scope(""), "unknown")

    def test_quarter_coverage_separates_elapsed_current_and_future_periods(self):
        periods = quarter_coverage(2026, "2026-09-08T01:00:00+00:00")
        self.assertEqual([p["status"] for p in periods], ["ended", "ended", "in_progress", "not_started"])
        self.assertEqual(periods[3]["period_end"], "2026-12-31")
        self.assertEqual({p["status"] for p in quarter_coverage(2025, "2026-09-08")}, {"ended"})

    def test_amendment_replaces_original_but_preserves_version_ledger(self):
        current, ledger, counts = select_current_reports([
            report("original"), report("amendment", "2026-02-01T12:00:00-05:00", "4A", income="120000")])
        self.assertEqual(set(current), {"amendment"})
        self.assertEqual(current["amendment"]["revision_count"], 2)
        self.assertEqual(ledger[0]["selected_filing_uuid"], "amendment")
        self.assertEqual(counts, {"superseded": 1, "current": 1})

    def test_same_name_different_source_clients_remain_separate(self):
        current, _, _ = select_current_reports([report("a"), report("b", client_api_id="3")])
        self.assertEqual(len(current), 2)

    def test_registrations_are_excluded_and_no_activity_amendments_replace_reports(self):
        current, _, counts = select_current_reports([report("registration", kind="RR"), report("a"),
            report("none", "2026-02-01T12:00:00-05:00", "4AY")])
        self.assertEqual(set(current), {"none"})
        self.assertEqual(counts["excluded_registration"], 1)

    def test_missing_and_tied_dates_quarantine_whole_family(self):
        for date in ("", "2026-01-20T17:00:00+00:00"):
            with self.subTest(date=date):
                current, _, counts = select_current_reports([report("a"), report("b", date, "4A")])
                self.assertEqual(current, {})
                self.assertEqual(counts["excluded_ambiguous_chronology"], 2)

    def test_actual_instant_orders_across_timezones(self):
        current, _, _ = select_current_reports([report("a", "2026-01-20T17:30:00+00:00"),
            report("b", "2026-01-20T13:00:00-05:00", "4A")])
        self.assertEqual(set(current), {"b"})

    def test_missing_source_ids_and_wrong_quarter_do_not_enter_index(self):
        current, _, counts = select_current_reports([report("a", client_api_id=""), report("b", kind="Q1")])
        self.assertEqual(current, {})
        self.assertEqual(counts["excluded_missing_identity"], 1)
        self.assertEqual(counts["excluded_period_mismatch"], 1)

    def test_duplicate_uuid_is_not_silently_counted(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            select_current_reports([report("a"), report("a")])


class KeywordTests(unittest.TestCase):
    def setUp(self):
        self.topics = Topics(json.loads((REFERENCE / "topics.json").read_text()))

    def test_context_needs_ai_in_same_issue_and_generic_words_do_not_match_ai(self):
        self.assertEqual(self.topics.match("Paid copyright counsel to maintain data centers."), [])
        matches = self.topics.match("Artificial intelligence; copyright; export controls.")
        self.assertEqual({m["topic_id"] for m in matches}, {"ai", "copyright", "infrastructure"})

    def test_short_terms_have_boundaries_and_uncertainty(self):
        self.assertEqual(self.topics.match("PAID AID CHAIR EMAIL claiming rights"), [])
        match = self.topics.match("AI and LLM standards")[0]
        self.assertEqual(match["kind"], "ambiguous")
        self.assertEqual(self.topics.match("A.I. governance")[0]["kind"], "ambiguous")

    def test_spans_reproduce_exact_original_text_including_unicode(self):
        text = "🧠 Generative-AI; ARTIFICIAL\nINTELLIGENCE; copyright"
        for match in self.topics.match(text):
            for evidence in match["evidence"]:
                self.assertEqual(text[evidence["start"]:evidence["end"]], evidence["text"])

    def test_rejecting_parent_retains_audit_evidence_but_blocks_subtopic(self):
        text = "AI and copyright"
        review = {"description_sha256": digest(text), "decision": "rejected", "reviewer": "Analyst",
                  "reviewed_at": "2026-09-07", "notes": "Different meaning of AI"}
        matches = apply_reviews("a", text, self.topics.match(text), self.topics,
                                {("a", "ai", self.topics.version): review})
        self.assertEqual(matches[0]["decision"], "rejected")
        self.assertTrue(matches[1]["dependency_rejected"])

    def test_manual_false_negative_and_stale_text_review(self):
        text = "A new model policy"
        review = {"description_sha256": digest(text), "decision": "accepted", "reviewer": "Analyst",
                  "reviewed_at": "2026-09-07", "notes": "Verified AI context in filing"}
        reviews = {("a", "ai", self.topics.version): review}
        matches = apply_reviews("a", text, [], self.topics, reviews)
        self.assertEqual(matches[0]["kind"], "manual")
        self.assertEqual(matches[0]["evidence"], [])
        with self.assertRaisesRegex(ValueError, "Stale review"):
            apply_reviews("a", "Changed passage", [], self.topics, reviews)
        self.assertEqual(apply_reviews("a", text, [], self.topics,
            {("a", "ai", "older-version"): review}), [])


class IdentityTests(unittest.TestCase):
    def test_exact_seeds_do_not_repeat_false_positives(self):
        config = json.loads((REFERENCE / "organizations.json").read_text())
        orgs = Organizations(config, [])
        for name in ("APPLETON INTERNATIONAL AIRPORT", "U.S. APPLE ASSOCIATION", "RAPPLER INC.",
                     "COHERENT, INC", "MISTRAL GROUP (FORMERLY AS MISTRAL SECURITY, INC.)"):
            self.assertIsNone(orgs.identify(report("a", client_name=name))["organization_id"])
        match = orgs.identify(report("a", client_name="  apple   inc. "))
        self.assertEqual(match["organization_id"], "apple")
        self.assertEqual(match["organization_status"], "name_seed")

    def test_historical_exact_company_names_remain_in_watchlists(self):
        config = json.loads((REFERENCE / "organizations.json").read_text())
        orgs = Organizations(config, [])
        for name, expected in (("MICROSOFT CORP", "microsoft"),
                               ("GOOGLE CLIENT SERVICES LLC (FKA GOOGLE LLC)", "google"),
                               ("SCALE AI", "scale-ai"), ("COHERE INC.", "cohere")):
            with self.subTest(name=name):
                match = orgs.identify(report("a", client_name=name))
                self.assertEqual(match["organization_id"], expected)
                self.assertEqual(match["organization_status"], "name_seed")
        self.assertIsNone(orgs.identify(report("a",
            client_name="AQUIA GROUP ON BEHALF OF ANTHROPIC, PBC"))["organization_id"])

    def test_source_id_rejection_overrides_seed(self):
        config = json.loads((REFERENCE / "organizations.json").read_text())
        orgs = Organizations(config, [{"registrant_api_id": "2", "client_api_id": "1",
            "organization_id": "apple", "decision": "rejected", "reviewer": "Analyst", "reviewed_at": "2026-09-07"}])
        result = orgs.identify(report("a", client_name="APPLE INC."))
        self.assertIsNone(result["organization_id"])
        self.assertEqual(result["organization_status"], "rejected_mapping")


class ExportTests(unittest.TestCase):
    def test_pipeline_indexes_outside_watchlist_and_current_evidence_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            interim = root / "data/lda/interim/2025"
            filings = [report("old"), report("new", "2026-02-01T12:00:00-05:00", "4A"),
                report("registration", kind="RR"), report("inactive-original", client_api_id="5"),
                report("inactive", "2026-02-01T12:00:00-05:00", "4AY", client_api_id="5")]
            save_csv(interim / "filings.csv", filings, list(filings[0]))
            activities = [{"activity_id": r["filing_uuid"] + ":activity:1", "filing_uuid": r["filing_uuid"],
                "general_issue_code": "SCI", "general_issue_code_display": "Science/Technology",
                "description": "Artificial intelligence and copyright"} for r in filings]
            save_csv(interim / "filing_activities.csv", activities, list(activities[0]))
            save_csv(interim / "filing_activity_lobbyists.csv", [{"activity_id": "new:activity:1",
                "lobbyist_api_id": "3", "lobbyist_first_name": "Test", "lobbyist_middle_name": "",
                "lobbyist_last_name": "Person"}], ["activity_id", "lobbyist_api_id", "lobbyist_first_name",
                                                     "lobbyist_middle_name", "lobbyist_last_name"])
            save_csv(interim / "filing_activity_government_entities.csv", [{"activity_id": "new:activity:1",
                "government_entity_id": "4", "government_entity_name": "Example Agency"}],
                ["activity_id", "government_entity_id", "government_entity_name"])
            (interim / "normalization_manifest.json").write_text(json.dumps({
                "normalized_at_utc": "2026-04-10T00:00:00+00:00", "source_endpoints": {"filings": {
                    "fetched_at_utc": "2026-04-09T00:00:00+00:00"}}, "tables": {"filings.csv": {"rows": len(filings)}}}))
            output = root / "export"
            metadata = build_explorer([2025], root=root, output=output)
            self.assertEqual(metadata["activity_count"], 1)
            payload = json.loads((output / "explorer.json").read_text())
            item = payload["activities"][0]
            self.assertEqual(item["filing_uuid"], "new")
            self.assertEqual(item["client_name"], "University Research")
            self.assertIsNone(item["organization_id"])
            self.assertEqual(item["government_entities"][0]["name"], "Example Agency")
            self.assertEqual(item["government_entity_scope"], "issue_entry")
            self.assertEqual(item["lobbyists"][0]["name"], "Test Person")
            self.assertNotIn("income", item)
            self.assertNotIn("expenses", item)
            with (output / "matches.csv").open(newline="", encoding="utf-8") as handle:
                matches = list(csv.DictReader(handle))
            self.assertEqual(len(matches), 2)
            self.assertEqual({m["decision"] for m in matches}, {"unreviewed"})
            self.assertEqual({m["government_entity_scope"] for m in matches}, {"issue_entry"})
            self.assertEqual(len(metadata["sources"][0]["input_sha256"]), 5)
            # A later local reconciliation must not relabel old source data as
            # newly downloaded merely because its unique-ID count is unchanged.
            snapshot_dir = root / "data/lda/raw/2025/filings"
            snapshot_dir.mkdir(parents=True)
            (snapshot_dir / "snapshot_manifest.json").write_text(json.dumps({
                "snapshot_built_at_utc": "2026-04-09T18:00:00+00:00",
                "snapshot_unique_id_count": len(filings)}))
            rebuilt = build_explorer([2025], root=root, output=output)
            self.assertEqual(rebuilt["sources"][0]["snapshot_at"], "2026-04-09T00:00:00+00:00")
            from scripts.validate_site import validate_lobbying
            public = root / "site/lobbying/data"
            public.mkdir(parents=True)
            for name in ("manifest.json", "matches.csv"):
                shutil.copy2(output / name, public / name)
            script = public / "explorer-data.js"
            script.write_text("window.TechMoneyLobbyingData=" + json.dumps(payload) + ";\n")
            errors = []
            self.assertEqual(validate_lobbying(root / "site", errors)["topic_matches"], 2)
            self.assertEqual(errors, [])
            payload["activities"][0]["matches"][0]["evidence"][0]["text"] = "wrong evidence"
            script.write_text("window.TechMoneyLobbyingData=" + json.dumps(payload) + ";\n")
            validate_lobbying(root / "site", errors)
            self.assertTrue(any("Invalid keyword evidence span" in error for error in errors))

    def test_spreadsheet_formula_text_is_escaped(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "safe.csv"
            save_csv(path, [{"text": '=HYPERLINK("https://example.com")'}], ["text"])
            with path.open(newline="") as handle:
                self.assertTrue(next(csv.DictReader(handle))["text"].startswith("'="))

    def test_duplicate_review_decisions_fail_instead_of_last_row_winning(self):
        topics = Topics(json.loads((REFERENCE / "topics.json").read_text()))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviews.csv"
            review = {"activity_id": "a", "topic_id": "ai", "rules_version": topics.version,
                "description_sha256": digest("AI"), "decision": "accepted", "reviewer": "Analyst", "reviewed_at": "2026-09-07"}
            save_csv(path, [review, review], REVIEW_FIELDS)
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                load_topic_reviews(path, topics)


if __name__ == "__main__":
    unittest.main()
