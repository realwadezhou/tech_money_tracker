from html.parser import HTMLParser
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urljoin

from frontend import lobbying
from frontend.layout import render_shell


class Elements(HTMLParser):
    """Read rendered semantics without coupling tests to indentation or CSS."""

    def __init__(self, markup):
        super().__init__()
        self.elements = []
        self.stack = []
        self.feed(markup)

    def handle_starttag(self, tag, attrs):
        node = {"tag": tag, "attrs": dict(attrs), "text": "", "parents": list(self.stack)}
        self.elements.append(node)
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            self.stack.append(node)

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index]["tag"] == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        for node in self.stack:
            node["text"] += data

    def find(self, tag, **attrs):
        return [node for node in self.elements if node["tag"] == tag
                and all(node["attrs"].get(key) == value for key, value in attrs.items())]


class ReferenceLayoutTests(unittest.TestCase):
    def render(self, body="", **options):
        return render_shell("Example - Tech Money", body,
                            stylesheet_url="static/site.css?v=example", **options)

    def test_main_skip_link_and_named_navigation_work_without_javascript(self):
        document = Elements(self.render('<h1>Acme &amp; partners</h1><p>Records</p>'))
        mains = document.find("main")
        self.assertEqual(len(mains), 1)
        self.assertEqual(mains[0]["attrs"]["id"], "main-content")
        self.assertEqual(len(document.find("h1")), 1)
        self.assertEqual(document.find("h1")[0]["text"], "Acme & partners")
        self.assertEqual(len(document.find("a", href="#main-content")), 1)
        self.assertTrue(all(node["attrs"].get("aria-label") for node in document.find("nav")))
        mobile_menu = document.find("details", **{"class": "mobile-navigation"})
        self.assertEqual(len(mobile_menu), 1)
        self.assertTrue(any(mobile_menu[0] in node["parents"] for node in document.find("summary")))

    def test_missing_page_heading_gets_one_escaped_heading(self):
        page = render_shell('Sources <2026> & "coverage" - Tech Money', "<p>Records</p>",
                            stylesheet_url="static/site.css")
        headings = Elements(page).find("h1")
        self.assertEqual(len(headings), 1)
        self.assertEqual(headings[0]["text"], 'Sources <2026> & "coverage"')
        self.assertIn("Sources &lt;2026&gt; &amp;", page)

    def test_nested_page_navigation_stays_within_cycle_and_shared_lobbying_root(self):
        page = self.render("<h1>California</h1>", navigation_prefix="../../../",
                           home_href="../../../index.html", current_section="candidates",
                           lobbying_href="../../../../lobbying/", cycle_label="2026 cycle")
        document = Elements(page)
        base = "https://example.test/tech-money/2026/candidates/states/california/"
        expected = {
            "Overview": "2026/index.html", "Employers": "2026/companies/",
            "Candidates": "2026/candidates/", "Lobbying": "lobbying/", "Sources": "2026/data/",
        }
        primary = document.find("nav", **{"aria-label": "Main navigation"})[0]
        primary_links = [node for node in document.find("a") if primary in node["parents"]]
        self.assertEqual({node["text"] for node in primary_links}, set(expected))
        for node in primary_links:
            self.assertEqual(urljoin(base, node["attrs"]["href"]),
                             "https://example.test/tech-money/" + expected[node["text"]])
        active_links = document.find("a", **{"aria-current": "page"})
        self.assertTrue(active_links)
        self.assertTrue(all(node["text"] == "Candidates" and
                            node["attrs"]["href"] == "../../../candidates/" for node in active_links))
        for node in document.find("a"):
            href = node["attrs"].get("href", "")
            if not href.startswith("#"):
                self.assertTrue(urljoin(base, href).startswith("https://example.test/tech-money/"))

    def test_heading_links_preserve_authored_ids_and_avoid_all_existing_body_ids(self):
        body = '''<h1>Records</h1><div id="section-results"></div>
        <h2>Results</h2><h2>Results</h2><h2 id="coverage">Coverage</h2>
        <h2>Research &amp; <em>sources</em> &lt;2026&gt;</h2>
        <h2 id="facts&amp;limits">Facts &amp; limits</h2>'''
        document = Elements(self.render(body))
        ids = [node["attrs"]["id"] for node in document.elements if "id" in node["attrs"]]
        self.assertEqual(len(ids), len(set(ids)))
        headings = document.find("h2")
        self.assertEqual(headings[2]["attrs"]["id"], "coverage")
        self.assertEqual(headings[4]["attrs"]["id"], "facts&limits")
        self.assertEqual(headings[3]["text"], "Research & sources <2026>")
        outline = document.find("nav", **{"aria-label": "On this page"})[0]
        links = [node for node in document.find("a") if outline in node["parents"]]
        self.assertEqual(len(links), len(headings))
        for link, heading in zip(links, headings):
            self.assertEqual(link["attrs"]["href"], "#" + heading["attrs"]["id"])
            self.assertEqual(link["text"], heading["text"])

    def test_source_note_keeps_authored_markup_and_is_available_without_outline(self):
        source_note = '<p class="meta"><strong>Latest matched transaction:</strong> 2026-09-01. <a href="data/">Source records</a></p>'
        page = self.render("<h1>Overview</h1><p>Records</p>", source_note=source_note)
        document = Elements(page)
        self.assertIn(source_note, page)
        self.assertEqual(document.find("nav", **{"aria-label": "On this page"}), [])
        details = document.find("details", **{"class": "source-details"})
        self.assertEqual(len(details), 1)
        self.assertIn("open", details[0]["attrs"])
        self.assertTrue(any(details[0] in node["parents"] for node in document.find("summary")))
        self.assertTrue(any(details[0] in node["parents"] for node in document.find("strong")))

    def test_lobbying_context_uses_calendar_data_and_its_own_methodology(self):
        document = Elements(self.render('<h1>Federal lobbying</h1><h2 id="methodology">Methodology</h2>',
                                        navigation_prefix="../2026/", home_href="../index.html",
                                        current_section="federal-lobbying", lobbying_href="./",
                                        main_id="lobbying-explorer", cycle_label="Calendar years"))
        self.assertEqual(len(document.find("a", href="#lobbying-explorer")), 1)
        footer = document.find("nav", **{"aria-label": "Footer"})[0]
        links = {node["text"]: node["attrs"]["href"] for node in document.find("a") if footer in node["parents"]}
        self.assertEqual(links["Methodology"], "#methodology")
        self.assertEqual(links["Data & sources"], "data/manifest.json")
        self.assertNotIn("Latest tech-matched transaction", document.find("body")[0]["text"])

    def test_real_lobbying_page_context_and_footer_fragments_resolve(self):
        metadata = {"sources": [], "rules_version": "test-rules", "built_at": "2026-09-19"}
        with tempfile.TemporaryDirectory() as directory:
            export = Path(directory)
            (export / "explorer.json").write_text("{}", encoding="utf-8")
            with patch.object(lobbying, "EXPORT", export):
                document = Elements(lobbying.page(metadata, [2026, 2024]))
        ids = {node["attrs"]["id"] for node in document.elements if "id" in node["attrs"]}
        for node in document.find("a"):
            href = node["attrs"].get("href", "")
            if href.startswith("#"):
                self.assertIn(href[1:], ids)
        context = document.find("aside", **{"aria-label": "Page context"})[0]
        footer = document.find("nav", **{"aria-label": "Footer"})[0]
        methodology_links = document.find("a", href="#methodology")
        self.assertTrue(any(context in node["parents"] for node in methodology_links))
        self.assertTrue(any(footer in node["parents"] for node in methodology_links))
        self.assertEqual(len(document.find("details", id="methodology")), 1)


if __name__ == "__main__":
    unittest.main()
