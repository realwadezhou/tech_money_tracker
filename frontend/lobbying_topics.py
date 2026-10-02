"""What tracked tech companies lobby about, by phrase: /lobbying/topics/.

Reads exports/lobbying/phrase_topics.json (python -m pipeline.lda.build_topics).
All counting is done there; the page only draws the saved numbers.
"""
from __future__ import annotations

from hashlib import sha256
from html import escape
import json
from pathlib import Path

from frontend.layout import render_shell
from frontend.lobbying import ASSETS, EXPORT, replace_file
from frontend.lobbying_spending import sector_label

DATA_FILE = "phrase_topics.json"


def latest_and_year_ago(quarters: list[dict]) -> tuple[int, int | None]:
    complete = [i for i, q in enumerate(quarters) if q["complete"]]
    latest = complete[-1]
    year_ago = next((i for i in complete if quarters[i]["year"] == quarters[latest]["year"] - 1
                     and quarters[i]["quarter"] == quarters[latest]["quarter"]), None)
    return latest, year_ago


def page(data: dict, cycles: list[int]) -> str:
    from frontend.build_site import table

    def asset(name):
        return f"../static/{name}?v={sha256((ASSETS / name).read_bytes()).hexdigest()[:12]}"

    metadata, quarters = data["metadata"], data["quarters"]
    latest, year_ago = latest_and_year_ago(quarters)
    first = next(i for i, q in enumerate(quarters) if q["complete"])
    latest_label = quarters[latest]["id"]
    incomplete = [q["id"] for q in quarters if not q["complete"]]
    active = data["tracked_companies_active"][latest]

    def share(topic: dict, index: int) -> str:
        total = data["all_clients"][index]
        return "—" if not total else "{:.1f}%".format(100 * topic["all_clients_mentioning"][index] / total)

    headers = ["Topic", f"Tracked companies, {latest_label}"]
    if year_ago is not None:
        headers.append(f"Tracked companies, {quarters[year_ago]['id']}")
    headers += [f"Tracked companies, {quarters[first]['id']}", f"All lobbying clients, {latest_label}",
                f"Share of all clients, {latest_label}"]
    rows = []
    for topic in sorted(data["topics"], key=lambda t: -t["tracked_companies_mentioning"][latest]):
        cells = [f'<td><a href="?topic={escape(topic["id"])}#explore">{escape(topic["label"])}</a></td>',
                 f'<td class="number">{topic["tracked_companies_mentioning"][latest]}</td>']
        if year_ago is not None:
            cells.append(f'<td class="number">{topic["tracked_companies_mentioning"][year_ago]}</td>')
        cells += [f'<td class="number">{topic["tracked_companies_mentioning"][first]}</td>',
                  f'<td class="number">{topic["all_clients_mentioning"][latest]:,}</td>',
                  f'<td class="number">{share(topic, latest)}</td>']
        rows.append(cells)
    summary = table(headers, rows, filterable=False)

    definitions = "".join(f'<li><strong>{escape(t["label"])}</strong>: {escape(t["phrases"])}</li>'
                          for t in data["topics"])
    links = ''.join(f'<a class="cycle-pill" href="../../{c}/">{c}</a>' for c in cycles)
    payload = {**data, "sector_labels": {c["sector"]: sector_label(c["sector"])
                                         for t in data["topics"] for c in t["companies"]}}
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    body = f'''
  <h1>What tech lobbies about</h1>
  <p class="lobbying-intro">How often subjects such as artificial intelligence and data centers are named in federal lobbying reports, and which tracked tech companies name them.</p>
  <aside class="lobbying-coverage" aria-label="Data coverage">
    <strong>Reports posted through {escape(metadata["source_cutoff"])}.</strong>
    Latest complete quarter: {escape(latest_label)}.
    {("Not shown because reports were not yet due: " + escape(", ".join(incomplete)) + ".") if incomplete else ""}
    A subject is counted when its words appear in a report's description of what was lobbied on. That shows the subject was raised, not which side the company took.
  </aside>
  <h2 id="explore">Explore a topic</h2>
  <div class="spending-chart-controls">
    <label>Topic <select id="topic-select"></select></label>
    <label>Chart shows <select id="topic-measure">
      <option value="tracked">Tracked tech companies naming it</option>
      <option value="share">Share of all lobbying clients naming it</option>
    </select></label>
  </div>
  <p class="lobbying-filter-note" id="topic-phrases"></p>
  <figure class="spending-chart"><svg id="topic-chart" role="img" aria-labelledby="topic-chart-caption"></svg>
    <figcaption id="topic-chart-caption"></figcaption></figure>
  <h3 id="topic-grid-title">By company</h3>
  <p class="lobbying-filter-note">Each square is one quarter. Darker means more separate issue entries naming the topic in that company's reports. Blank means none.</p>
  <div class="topic-grid-wrap"><table class="topic-grid" id="topic-grid"></table></div>
  <h3>What they wrote</h3>
  <p class="lobbying-filter-note">The most recent passage from each company, exactly as filed. Select a link to read the whole report.</p>
  <div id="topic-examples"></div>
  <noscript><p>The chart and company grid need JavaScript. The table below works without it.</p></noscript>
  <h2>All topics at a glance</h2>
  <p>Number of tracked tech companies with at least one report naming the topic. {active} tracked companies filed reports with issue descriptions in {escape(latest_label)}. Select a topic to open it above.</p>
  {summary}
  <p>For dollar amounts, see <a href="../spending/">lobbying spending by company and quarter</a>.</p>
  <details class="lobbying-methodology" id="methodology" open>
    <summary>How these numbers are counted</summary>
    <p>Every quarterly lobbying report lists the issues lobbied on, each with a short description written by the filer. {escape(metadata["method"])}</p>
    <p>The words searched for each topic are:</p>
    <ul>{definitions}</ul>
    <p>These are simple word searches, and nobody has read each passage. A match can be incidental: "energy" also matches the Energy and Commerce Committee, and "chips" covers both semiconductors and the CHIPS Act. A report can raise a subject without using these words, so the counts are a floor. Dollars are reported for a whole report and cannot be split by topic.</p>
    <p>Tracked companies are the {len({c["id"] for t in data["topics"] for c in t["companies"]})} companies on this site's lobbying list that named at least one topic; their reported names are listed on the <a href="../spending/#names">spending page</a>. "All lobbying clients" counts every client name in the data, in every industry, after ignoring capitalization and spacing.</p>
    <p>Topic definitions: <strong>{escape(metadata["topics_version"])}</strong>. <a href="https://lda.gov/api/">Official data source</a>. Senate Office of Public Records cannot vouch for the data or analyses derived from these data after the data have been retrieved from LDA.gov.</p>
  </details>
  <details class="lobbying-methodology" id="downloads" open>
    <summary>Download the data</summary>
    <ul><li><a href="data/{DATA_FILE}">All topic counts by quarter and company (JSON)</a></li></ul>
  </details>
  <script type="application/json" id="topic-data">{payload_json}</script>
'''
    return render_shell(
        "What tech lobbies about - Tech Money",
        body,
        stylesheet_url=asset("site.css"),
        tables_script_url=asset("tables.js"),
        extra_head=f'''<meta name="description" content="Which subjects tracked tech companies name in federal lobbying reports, by quarter: artificial intelligence, data centers, export controls and more.">
  <link rel="stylesheet" href="{asset('lobbying.css')}">
  <script src="{asset('lobbying_topics.js')}" defer></script>''',
        navigation_prefix=f"../../{max(cycles)}/" if cycles else "../../",
        home_href="../../",
        lobbying_href="../spending/",
        current_section="federal-lobbying",
        cycle_label="Calendar-year reporting",
        cycle_controls=(f'<nav class="cycle-toggle" aria-label="Election cycles">'
                        f'<span class="cycle-label">Election cycles</span>{links}</nav>') if links else "",
        source_note=(f"LDA reports posted through {escape(metadata['source_cutoff'])}. Reporting years are "
                     "calendar years, independent of election cycles."),
        main_class="lobbying-page",
        eyebrow="Federal lobbying",
        definition_href="#methodology",
        data_href="#downloads",
    )


def build_topics_page(site_root: Path, cycles: list[int]) -> bool:
    if not (EXPORT / DATA_FILE).exists():
        return False
    data = json.loads((EXPORT / DATA_FILE).read_text(encoding="utf-8"))
    target = site_root / "lobbying"
    (target / "static").mkdir(parents=True, exist_ok=True)
    (target / "topics/data").mkdir(parents=True, exist_ok=True)
    for name in ("site.css", "lobbying.css", "tables.js", "lobbying_topics.js"):
        replace_file(target / "static" / name, (ASSETS / name).read_bytes())
    replace_file(target / "topics/data" / DATA_FILE, (EXPORT / DATA_FILE).read_bytes())
    replace_file(target / "topics/index.html", page(data, cycles).encode("utf-8"))
    return True
