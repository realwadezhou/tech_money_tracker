import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from frontend.lobbying_topics import page
from pipeline.lda.build_topics import TOPICS_PATH, build_topics, compile_topic, example_text, topic_mask
from scripts.validate_site import validate_lobbying_topics


def report(uid, client, quarter, posted="2025-07-25T10:00:00-04:00"):
    return {"filing_uuid": uid, "client_name": client, "registrant_name": "FIRM", "quarter": quarter,
            "dt_posted": posted, "filing_url": f"https://lda.gov/filings/public/filing/{uid}/print/"}


REPORTS = pd.DataFrame([
    report("a", "OPENAI OPCO, LLC", "2025 Q1"), report("b", "Open  AI", "2025 Q1"),
    report("c", "GOOGLE CLIENT SERVICES LLC", "2025 Q1"), report("d", "ACME STEEL", "2025 Q1"),
    report("e", "OPENAI OPCO, LLC", "2025 Q2"), report("f", "ACME STEEL", "2025 Q2"),
    report("g", "OPENAI OPCO, LLC", "2025 Q3", posted="2025-09-18T10:00:00-04:00"),
])
DESCRIPTIONS = {
    "a": ["AI safety and data centers", "Artificial intelligence research funding"],
    "b": ["He said the chain of custody matters"],          # "said" and "chain" are not "AI"
    "c": ["Data center permitting"],
    "d": ["Steel tariffs and AI in manufacturing"],
    "e": ["Export controls"],
    "f": ["Tariffs"],
    "g": ["AI policy"],
}
ENTRIES = pd.DataFrame([{**REPORTS.set_index("filing_uuid").loc[uid][["quarter", "client_name", "registrant_name", "filing_url"]],
                         "filing_uuid": uid, "description": text}
                        for uid, texts in DESCRIPTIONS.items() for text in texts])
COMPANY_MAP = {"OPENAI OPCO, LLC": "openai", "OPEN AI": "openai", "GOOGLE CLIENT SERVICES LLC": "google"}
TOPICS = {"version": "test", "topics": [
    {"id": "ai", "label": "Artificial intelligence", "phrases": "\"artificial intelligence\" or \"AI\"",
     "patterns": [{"pattern": "\\bartificial[\\s-]+intelligence\\b"},
                  {"pattern": "(?<![A-Za-z0-9])(?:AI|A\\.I\\.)(?![A-Za-z0-9])", "case_sensitive": True}]},
    {"id": "data_centers", "label": "Data centers", "phrases": "\"data center\"",
     "patterns": [{"pattern": "\\bdata[\\s-]?cent(?:er|re)s?\\b"}]},
]}


class TopicCountingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.output = Path(cls.directory.name)
        topics_path = cls.output / "topics.json"
        topics_path.write_text(json.dumps(TOPICS), encoding="utf-8")
        build_topics(cls.output, reports=REPORTS, entries=ENTRIES, company_map=COMPANY_MAP, topics_path=topics_path)
        cls.data = json.loads((cls.output / "phrase_topics.json").read_text(encoding="utf-8"))
        cls.ai, cls.centers = cls.data["topics"]

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_ai_abbreviation_is_case_sensitive_and_whole_word(self):
        mask = topic_mask(pd.Series(["AI policy", "He said so", "the chain", "A.I. rules", "ai", "FAIR Act"]), TOPICS["topics"][0])
        self.assertEqual(mask.tolist(), [True, False, False, True, False, False])

    def test_counts_by_quarter_for_companies_and_all_clients(self):
        self.assertEqual([q["id"] for q in self.data["quarters"]], ["2025 Q1", "2025 Q2", "2025 Q3"])
        self.assertEqual([q["complete"] for q in self.data["quarters"]], [True, True, False])
        # All clients: OpenAI (two spellings count separately as reported names), Google, Acme.
        self.assertEqual(self.data["all_clients"], [4, 2, 1])
        self.assertEqual(self.ai["all_clients_mentioning"], [2, 0, 1])        # OpenAI OpCo and Acme; then OpenAI
        self.assertEqual(self.ai["tracked_companies_mentioning"], [1, 0, 1])
        openai = self.ai["companies"][0]
        self.assertEqual((openai["id"], openai["entries"]), ("openai", [2, 0, 1]))
        self.assertEqual(self.centers["tracked_companies_mentioning"], [2, 0, 0])
        self.assertEqual({c["id"] for c in self.centers["companies"]}, {"openai", "google"})

    def test_example_is_the_latest_matching_passage_with_its_source(self):
        example = self.ai["companies"][0]["example"]
        self.assertEqual((example["quarter"], example["text"]), ("2025 Q3", "AI policy"))
        self.assertTrue(example["url"].startswith("https://lda.gov/"))
        long_text = "x " * 400 + "data centers and power " + "y " * 400
        text = example_text(long_text, TOPICS["topics"][1])
        self.assertIn("data centers", text)
        self.assertTrue(text.startswith("…") and text.endswith("…"))
        self.assertLess(len(text), 340)

    def test_published_topic_definitions_compile(self):
        config = json.loads(TOPICS_PATH.read_text(encoding="utf-8"))
        ids = [t["id"] for t in config["topics"]]
        self.assertEqual(len(ids), len(set(ids)))
        for topic in config["topics"]:
            self.assertTrue(compile_topic(topic))
            self.assertTrue(topic["label"] and topic["phrases"])

    def test_validator_accepts_the_export_and_catches_tampering(self):
        site = self.output / "site/lobbying/topics/data"
        site.mkdir(parents=True)
        (site / "phrase_topics.json").write_bytes((self.output / "phrase_topics.json").read_bytes())
        errors = []
        self.assertEqual(validate_lobbying_topics(self.output / "site", errors)["topics"], 2)
        self.assertEqual(errors, [])
        broken = json.loads(json.dumps(self.data))
        broken["topics"][0]["tracked_companies_mentioning"][0] = 9
        (site / "phrase_topics.json").write_text(json.dumps(broken), encoding="utf-8")
        validate_lobbying_topics(self.output / "site", errors)
        self.assertTrue(errors)

    def test_page_renders_summary_and_embedded_data(self):
        html = page(self.data, [2026, 2024])
        self.assertIn("What tech lobbies about", html)
        self.assertIn("Latest complete quarter: 2025 Q2", html)
        self.assertIn('<a href="?topic=data_centers#explore">Data centers</a>', html)
        self.assertIn('id="topic-data"', html)
        self.assertIn("not which side the company took", html)


if __name__ == "__main__":
    unittest.main()
