"""Quarterly lobbying spending by tracked company: /lobbying/spending/.

Reads exports/lobbying/spending.json (python -m pipeline.lda.build_spending).
Independent of the AI explorer export, so either page can be built alone.
"""
from __future__ import annotations

from hashlib import sha256
from html import escape
import json
from pathlib import Path

from frontend.layout import render_shell
from frontend.lobbying import ASSETS, EXPORT, explorer_published, replace_file

DATA_FILES = ("spending.json", "spending.csv", "spending_reports.csv")
# Reader-facing names for the sector tags in data/reference/companies/companies.csv.
SECTOR_LABELS = {
    "tech_giant": "Large tech companies", "ai": "AI", "semiconductors": "Semiconductors",
    "software": "Software", "cloud": "Cloud", "cybersecurity": "Cybersecurity", "hardware": "Hardware",
    "fintech": "Fintech", "social_media": "Social media", "media": "Media", "ecommerce": "E-commerce",
    "marketplace": "Marketplaces", "delivery": "Delivery", "rideshare": "Ride-hailing",
    "communications": "Communications", "defense": "Defense tech", "cars": "Automotive",
    "elon_empire": "Musk companies", "vc": "Venture capital", "other": "Other",
}


def sector_label(sector: str) -> str:
    return SECTOR_LABELS.get(sector, sector.replace("_", " ").capitalize() or "Other")
RECENT_QUARTERS = 5


def money(value: float | None) -> str:
    return "—" if value is None else "${:,.0f}".format(value)


def millions(value: float) -> str:
    return "${:,.1f}M".format(value / 1e6)


def summarize(data: dict) -> dict:
    """Totals the page needs, using complete quarters only."""
    quarters = data["quarters"]
    complete = [i for i, q in enumerate(quarters) if q["complete"]]
    years = sorted({quarters[i]["year"] for i in complete})

    def total(company: dict, index: int) -> float | None:
        cell = company["quarters"][index]
        return None if cell is None else cell["total"]

    rows = []
    for company in data["companies"]:
        by_year = {}
        for year in years:
            values = [total(company, i) for i in complete if quarters[i]["year"] == year]
            by_year[year] = sum(v for v in values if v is not None) if any(v is not None for v in values) else None
        rows.append({"company": company, "by_year": by_year,
                     "by_quarter": {i: total(company, i) for i in complete}})

    def year_label(year: int) -> str:
        done = [quarters[i]["quarter"] for i in complete if quarters[i]["year"] == year]
        return str(year) if len(done) == 4 else f"{year} Q{min(done)}–Q{max(done)}"

    latest = complete[-1]
    year_ago = next((i for i in complete if quarters[i]["year"] == quarters[latest]["year"] - 1
                     and quarters[i]["quarter"] == quarters[latest]["quarter"]), None)
    all_companies = {i: sum(r["by_quarter"][i] or 0 for r in rows) for i in complete}
    return {"rows": rows, "years": years, "year_labels": {y: year_label(y) for y in years},
            "complete": complete, "latest": latest, "year_ago": year_ago, "all_companies": all_companies}


def change(now: float | None, before: float | None) -> str:
    if not now or not before:
        return "—"
    return "{:+.0f}%".format(100 * (now - before) / before)


def page(data: dict, cycles: list[int], explorer_available: bool) -> str:
    from frontend.build_site import headline_stats, table

    def asset(name):
        return f"../static/{name}?v={sha256((ASSETS / name).read_bytes()).hexdigest()[:12]}"

    metadata, quarters = data["metadata"], data["quarters"]
    s = summarize(data)
    latest, year_ago = s["latest"], s["year_ago"]
    latest_label = quarters[latest]["id"]
    latest_total = s["all_companies"][latest]
    top = max(s["rows"], key=lambda r: r["by_quarter"][latest] or 0)
    full_years = [y for y in s["years"] if s["year_labels"][y] == str(y)]
    last_full = full_years[-1] if full_years else None
    incomplete = [q["id"] for q in quarters if not q["complete"]]

    stats = [(f"All tracked companies, {latest_label}", millions(latest_total),
              (f"{change(latest_total, s['all_companies'][year_ago])} vs. {quarters[year_ago]['id']}"
               if year_ago is not None else "Latest complete quarter")),
             (f"Largest in {latest_label}", escape(top["company"]["name"]),
              f"{millions(top['by_quarter'][latest])} reported")]
    if last_full:
        stats.insert(1, (f"All tracked companies, {last_full}",
                         millions(sum(r["by_year"][last_full] or 0 for r in s["rows"])),
                         f"{metadata['company_count']} companies; full calendar year"))

    annual_rows = sorted(s["rows"], key=lambda r: -(r["by_year"].get(last_full or s["years"][-1]) or 0))
    annual = table(["Company", "Sector"] + [s["year_labels"][y] for y in s["years"]], [
        [f'<td>{escape(r["company"]["name"])}</td>', f'<td>{escape(sector_label(r["company"]["sector"]))}</td>'] +
        [f'<td class="number">{money(r["by_year"][y])}</td>' for y in s["years"]]
        for r in annual_rows])

    recent = s["complete"][-RECENT_QUARTERS:]
    quarterly_rows = sorted(s["rows"], key=lambda r: -(r["by_quarter"][latest] or 0))
    quarterly = table(["Company", "Sector"] + [quarters[i]["id"] for i in recent] +
                      ([f"Change vs. {quarters[year_ago]['id']}"] if year_ago is not None else []), [
        [f'<td>{escape(r["company"]["name"])}</td>', f'<td>{escape(sector_label(r["company"]["sector"]))}</td>'] +
        [f'<td class="number">{money(r["by_quarter"][i])}</td>' for i in recent] +
        ([f'<td class="number">{change(r["by_quarter"][latest], r["by_quarter"][year_ago])}</td>']
         if year_ago is not None else [])
        for r in quarterly_rows])

    topics_link = ('<p>To see <em>what</em> companies lobbied on, see '
                   '<a href="../topics/">what tech lobbies about</a>.</p>') if (EXPORT / "phrase_topics.json").exists() else ""
    explorer_link = topics_link + ('<p>To search individual passages, use the '
                                   '<a href="../">AI lobbying explorer</a>.</p>' if explorer_available else "")
    links = ''.join(f'<a class="cycle-pill" href="../../{c}/">{c}</a>' for c in cycles)
    sectors = sorted({c["sector"] for c in data["companies"]}, key=sector_label)
    payload = json.dumps({**data, "sectors": [{"id": x, "label": sector_label(x)} for x in sectors]},
                         ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    names = "".join(
        f'<li><strong>{escape(c["name"])}</strong>: {escape("; ".join(c["client_names"]))}</li>'
        for c in data["companies"])
    body = f'''
  <h1>Tech lobbying spending</h1>
  <p class="lobbying-intro">What tracked tech companies reported spending on federal lobbying, by quarter, from Lobbying Disclosure Act reports.</p>
  <aside class="lobbying-coverage" aria-label="Data coverage">
    <strong>Reports posted through {escape(metadata["source_cutoff"])}.</strong>
    Latest complete quarter: {escape(latest_label)}.
    {("Not shown because reports were not yet due: " + escape(", ".join(incomplete)) + ".") if incomplete else ""}
    Late filings and amendments can change earlier quarters.
  </aside>
  {headline_stats(stats)}
  <h2>Spending by quarter</h2>
  <div class="spending-chart-controls"><label>Show
    <select id="spending-company"><option value="">All tracked companies</option></select></label></div>
  <figure class="spending-chart"><svg id="spending-chart" role="img" aria-labelledby="spending-chart-caption"></svg>
    <figcaption id="spending-chart-caption">Reported lobbying spending per quarter. The tables below hold the same numbers.</figcaption></figure>
  <noscript><p>The chart needs JavaScript; the tables below show the same figures.</p></noscript>
  <h2>Spending by year</h2>
  <p>Each year adds up that year's complete quarters. Select a column heading to sort, or type a company or sector in the search box.</p>
  {annual}
  <h2>Recent quarters</h2>
  {quarterly}
  {explorer_link}
  <details class="lobbying-methodology" id="methodology" open>
    <summary>How these numbers are counted</summary>
    <p>Lobbying reports carry one of two kinds of dollar amount. A company that lobbies with its own staff reports its <strong>expenses</strong>. A lobbying firm hired by the company reports its <strong>income</strong> from that company. {escape(metadata["method"])}</p>
    <p>Where the outside firms together reported more than the company itself, the outside-firm sum is used instead. The download says which was used for every company and quarter.</p>
    <p>A company includes its subsidiaries that file separately (for example Waymo with Google, LinkedIn with Microsoft). Reports by one lobbying firm working on behalf of another are left out, because that money is already inside the main firm's report. Companies were matched by exact reported name; names not on the list are not counted.</p>
    <ul>{"".join(f"<li>{escape(note)}</li>" for note in metadata["notes"])}</ul>
    <p>These are amounts companies and firms disclosed. They do not cover state lobbying, trade association dues, advertising, or political contributions, and they do not say what position a company took.</p>
    <p><a href="https://lobbyingdisclosure.house.gov/ldaguidance.pdf">Official LDA guidance</a> · <a href="https://lda.gov/api/">Official data source</a></p>
    <p>Senate Office of Public Records cannot vouch for the data or analyses derived from these data after the data have been retrieved from LDA.gov.</p>
  </details>
  <details class="lobbying-methodology" id="names">
    <summary>Reported names counted for each company</summary>
    <ul>{names}</ul>
  </details>
  <details class="lobbying-methodology" id="downloads" open>
    <summary>Download the data</summary>
    <ul>
      <li><a href="data/spending.csv">Spending by company and quarter (CSV)</a> — includes in-house and outside-firm sums and which one was used.</li>
      <li><a href="data/spending_reports.csv">Every report behind the numbers (CSV)</a> — with links to the original filings.</li>
      <li><a href="data/spending.json">The same data with method notes (JSON)</a></li>
    </ul>
  </details>
  <script type="application/json" id="spending-data">{payload}</script>
'''
    return render_shell(
        "Tech lobbying spending - Tech Money",
        body,
        stylesheet_url=asset("site.css"),
        tables_script_url=asset("tables.js"),
        extra_head=f'''<meta name="description" content="Quarterly federal lobbying spending reported by tracked tech companies, from Lobbying Disclosure Act reports.">
  <link rel="stylesheet" href="{asset('lobbying.css')}">
  <script src="{asset('lobbying_spending.js')}" defer></script>''',
        navigation_prefix=f"../../{max(cycles)}/" if cycles else "../../",
        home_href="../../",
        lobbying_href="./",
        lobbying_root="../",
        lobbying_page="spending",
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


def build_spending_page(site_root: Path, cycles: list[int]) -> bool:
    if not (EXPORT / "spending.json").exists():
        return False
    data = json.loads((EXPORT / "spending.json").read_text(encoding="utf-8"))
    target = site_root / "lobbying"
    (target / "static").mkdir(parents=True, exist_ok=True)
    (target / "spending/data").mkdir(parents=True, exist_ok=True)
    for name in ("site.css", "lobbying.css", "tables.js", "lobbying_spending.js"):
        replace_file(target / "static" / name, (ASSETS / name).read_bytes())
    for name in DATA_FILES:
        replace_file(target / "spending/data" / name, (EXPORT / name).read_bytes())
    replace_file(target / "spending/index.html", page(data, cycles, explorer_published()).encode("utf-8"))
    return True
