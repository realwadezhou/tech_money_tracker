"""Shared calendar-year lobbying explorer, separate from FEC cycle bundles."""
from __future__ import annotations

import argparse
from datetime import date
from hashlib import sha256
from html import escape
import json
from pathlib import Path
import tempfile

from frontend.layout import render_shell

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "frontend/assets"
EXPORT = ROOT / "exports/lobbying"


def replace_file(path: Path, content: bytes) -> None:
    # Replace the file rather than truncating a file mapped by a local browser
    # or indexer. A failed write leaves the previous complete file available.
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".lobbying-", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def page(metadata: dict, cycles: list[int]) -> str:
    def asset(name):
        return f"static/{name}?v={sha256((ASSETS / name).read_bytes()).hexdigest()[:12]}"

    sources = "".join(
        f'<li><strong>{s["year"]}</strong>: snapshot {escape((s.get("snapshot_at") or "unknown")[:10])}; '
        f'{s["source_filing_count"]:,} source filings; '
        f'{s["selection_counts"].get("current", 0):,} selected quarterly reports. '
        + (f'Full download counts checked {escape(s["verified_at"][:10])} UTC.'
           if s.get("complete_before_cutoff_by_count") and s.get("verified_at") else 'Saved collection; no recent full-download verification.')
        + '</li>'
        for s in metadata["sources"])
    early_years = [s["year"] for s in metadata["sources"] if s.get("snapshot_at") and
                   date.fromisoformat(s["snapshot_at"][:10]) < date(s["year"], 4, 20)]
    early_note = (" The " + ", ".join(map(str, early_years)) +
                  " snapshot predates the usual first-quarter filing deadline and is especially incomplete.") if early_years else ""
    source_dates = ", ".join(sorted({(s.get("snapshot_at") or "unknown")[:10] for s in metadata["sources"]}))
    exclusions = sum(sum(n for status, n in s["selection_counts"].items()
                         if status.startswith("excluded_") and status != "excluded_registration")
                     for s in metadata["sources"])
    links = ''.join(f'<a class="cycle-pill" href="../{c}/">{c}</a>' for c in cycles)
    data_version = sha256((EXPORT / "explorer.json").read_bytes()).hexdigest()[:12]
    spending_link = (' For dollar amounts, see <a href="spending/">lobbying spending by company and quarter</a>.'
                     if (EXPORT / "spending.json").exists() else "")
    body = f'''
  <h1>AI lobbying explorer</h1>
  <p class="lobbying-intro">Find AI references in reported lobbying issues, across all clients or a selected group of firms.{spending_link}</p>
  <aside class="lobbying-coverage" aria-label="Data coverage">
    <strong>Source data as of {escape(source_dates)} UTC.</strong> Current and future quarters are incomplete.{escape(early_note)}
    <details><summary>Reporting periods and source coverage</summary><ul>{sources}</ul>
      <p>Years here are calendar reporting years, independent of the election-cycle pages. Snapshot dates describe the saved data; rebuilding the explorer does not refresh the filings. Missing results are not evidence of no lobbying.</p>
      <p>{exclusions:,} reports with unsupported types, inconsistent periods, missing identities, or ambiguous chronology were excluded. Full-download checks compare unique filing counts with the API for the stated cutoff. Matching counts do not prove every required filing was submitted; late filings and amendments can arrive later.</p>
      <a href="data/manifest.json">Download coverage and build details</a>
    </details>
  </aside>
  <form class="lobbying-filters" id="lobbying-filters" aria-label="Filter lobbying passages">
    <label>Reporting year<select id="filter-year" name="year"></select></label>
    <label>Quarter<select id="filter-quarter" name="quarter"><option value="">All quarters</option><option value="1">Q1 · Jan–Mar</option><option value="2">Q2 · Apr–Jun</option><option value="3">Q3 · Jul–Sep</option><option value="4">Q4 · Oct–Dec</option></select></label>
    <label>Company watchlist<select id="filter-watchlist" name="watchlist"><option value="">All clients / sectors</option></select></label>
    <label>Topic<select id="filter-topic" name="topic"></select></label>
    <label>Client / company<input id="filter-client" name="client" type="search" list="client-suggestions" placeholder="Any reported or grouped name"></label>
    <datalist id="client-suggestions"></datalist>
    <label>Evidence<select id="filter-evidence" name="evidence"><option value="">All candidates except rejected</option><option value="explicit">Explicit AI phrase or accepted</option><option value="ambiguous">Abbreviation / uncertain</option><option value="accepted">Accepted by a reviewer</option><option value="rejected">Rejected matches · audit</option></select></label>
    <label class="lobbying-search">Search within indexed passages<input id="filter-query" name="query" type="search" placeholder="Words in passages, clients, firms, lobbyists, or agencies"></label>
    <div class="lobbying-actions"><button type="reset">Reset filters</button><button type="button" id="download-results" disabled>Download filtered CSV</button></div>
  </form>
  <p class="lobbying-filter-note" id="topic-note"></p>
  <p class="lobbying-filter-note">Agency searches also match filing-level government-entity lists on older reports; a result does not establish that an agency was lobbied on this issue. Each result identifies the list's scope.</p>
  <p class="lobbying-filter-note" id="watchlist-note">Watchlists use initial exact-name mappings. Other clients remain searchable under their reported names.</p>
  <p class="lobbying-counts" id="result-counts" role="status" aria-live="polite">Loading the saved topic index…</p>
  <p class="lobbying-filter-note">Reports across all quarters with the other filters applied:</p>
  <div class="lobbying-quarter-counts" id="quarter-counts" aria-label="Distinct matching reports by quarter"></div>
  <section id="lobbying-results" aria-label="Matching issue passages"></section>
  <nav class="lobbying-pagination" aria-label="Result pages"><button id="previous-page" type="button" disabled>Previous</button><span id="page-count"></span><button id="next-page" type="button" disabled>Next</button></nav>
  <noscript><p>Enable JavaScript to filter the index, or <a href="data/matches.csv">download the indexed matches</a>. The methodology and coverage below are available without JavaScript.</p></noscript>
  <details class="lobbying-methodology" id="methodology">
    <summary>How to interpret these results</summary>
    <p>A client is the organization represented. The filing organization (registrant) may be the client itself or a hired lobbying firm. An issue entry can describe several subjects and list several lobbyists. Government entities in reports posted before February 14, 2021 are linked to the whole filing, not individual issue entries; later reports link them to issue entries. Scope on the transition date is unverified. These associations are not meeting records or proof of a position on a policy.</p>
    <p>The search covers issue descriptions matched by our saved topic rules. Company business descriptions and names do not establish AI relevance. Explicit phrases and ambiguous abbreviations are distinguished. Subtopics mean language co-occurs in the same entry; the passage may discuss the subjects separately.</p>
    <p>Latest posted quarterly reports are selected within each source registrant/client/year/quarter. Older versions and registrations are excluded. No-activity reports can replace earlier reports. Ambiguous latest timestamps are excluded for review. These are latest versions within the saved snapshot, not necessarily the latest filings available today.</p>
    <p>Dollar amounts cover whole reports and can overlap between company expense reports and hired firms’ income reports. This explorer does not assign dollars to AI or any other topic. Distinct report counts are not unique lobbying efforts. Client source records are not deduplicated organizations.</p>
    <p>Watchlists are starting selections, not a complete inventory of AI companies. Exact name seeds are labeled separately from reviewed source-ID mappings. Subsidiaries, intermediaries, and associations are not automatically attributed to a parent or member company.</p>
    <p>Topic rules: <strong>{escape(metadata['rules_version'])}</strong>. Build: {escape(metadata['built_at'][:10])} UTC. Keywords and reviews are versioned so historical comparisons can use the same definition.</p>
    <p><a href="https://lobbyingdisclosure.house.gov/ldaguidance.pdf">Official LDA guidance</a> · <a href="https://lda.gov/api/">Official data source</a></p>
    <p>Senate Office of Public Records cannot vouch for the data or analyses derived from these data after the data have been retrieved from LDA.gov.</p>
  </details>
  <details class="lobbying-methodology" id="downloads">
    <summary>Download data, definitions, and review worksheets</summary>
    <ul>
      <li><a href="data/matches.csv">All indexed matches (CSV)</a> — one row per issue entry and topic, including review status. Count distinct filing IDs for report totals.</li>
      <li><a href="data/topics.json">Topic definitions and keyword rules</a></li>
      <li><a href="data/organizations.json">Company name mappings and watchlists</a></li>
      <li><a href="data/topic_review_queue.csv">Topic review worksheet</a></li>
      <li><a href="data/organization_review_queue.csv">Company identity review worksheet</a></li>
      <li><a href="data/nonmatch_sample.csv">Sample of nonmatching passages</a> — use to look for missed language.</li>
      <li><a href="data/README.md">Data dictionary and review instructions</a></li>
    </ul>
  </details>
'''
    return render_shell(
        "AI lobbying explorer - Tech Money",
        body,
        stylesheet_url=asset("site.css"),
        extra_head=f'''<meta name="description" content="Explore AI-related passages in federal lobbying reports, with company watchlists, topic rules, and original filings.">
  <link rel="stylesheet" href="{asset('lobbying.css')}">
  <script src="data/explorer-data.js?v={data_version}" defer></script>
  <script src="{asset('lobbying.js')}" defer></script>''',
        navigation_prefix=f"../{max(cycles)}/" if cycles else "../",
        home_href="../",
        lobbying_href="./",
        current_section="federal-lobbying",
        cycle_label="Calendar-year reporting",
        cycle_controls=(f'<nav class="cycle-toggle" aria-label="Election cycles">'
                        f'<span class="cycle-label">Election cycles</span>{links}</nav>') if links else "",
        source_note=(f"LDA source snapshots: {escape(source_dates)} UTC. Reporting years are calendar years, "
                     "independent of election cycles. Current and future quarters are incomplete."),
        main_id="lobbying-explorer",
        main_class="lobbying-page",
        eyebrow="Federal lobbying",
    )


def build_lobbying(site_root: Path, cycles: list[int]) -> bool:
    if not (EXPORT / "explorer.json").exists():
        return False
    payload = (EXPORT / "explorer.json").read_text(encoding="utf-8")
    metadata = json.loads(payload)["metadata"]
    target = site_root / "lobbying"
    (target / "static").mkdir(parents=True, exist_ok=True)
    (target / "data").mkdir(parents=True, exist_ok=True)
    for name in ("site.css", "lobbying.css", "lobbying.js"):
        replace_file(target / "static" / name, (ASSETS / name).read_bytes())
    for name in ("manifest.json", "matches.csv", "topics.json", "organizations.json",
                 "topic_review_queue.csv", "organization_review_queue.csv", "nonmatch_sample.csv"):
        replace_file(target / "data" / name, (EXPORT / name).read_bytes())
    replace_file(target / "data/README.md", (ROOT / "data/reference/lobbying/README.md").read_bytes())
    # External data script works both on GitHub Pages and in a local file preview.
    # Escape HTML-significant characters even though this script is not inline.
    safe = payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    replace_file(target / "data/explorer-data.js", ("window.TechMoneyLobbyingData=" + safe.strip() + ";\n").encode("utf-8"))
    replace_file(target / "index.html", page(metadata, cycles).encode("utf-8"))
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Build only lobbying pages into an existing static site.")
    parser.add_argument("--site-root", type=Path, default=ROOT / "frontend/site")
    args = parser.parse_args()
    cycles = sorted((int(p.name) for p in args.site_root.iterdir()
                     if p.is_dir() and p.name.isdigit()), reverse=True)
    from frontend.lobbying_spending import build_spending_page
    explorer = build_lobbying(args.site_root, cycles)
    spending = build_spending_page(args.site_root, cycles)
    if not explorer and not spending:
        raise SystemExit("Build a lobbying export first: python -m pipeline.lda.build_spending "
                         "and/or python -m pipeline.lda.build_explorer 2025 2026")
    # Keep existing cycle links useful without rewriting unrelated FEC pages.
    from frontend import build_site
    build_site.AVAILABLE_CYCLES = cycles
    for cycle in cycles:
        build_site.CURRENT_RENDER_CYCLE = cycle
        build_site.CURRENT_RENDER_REL_DIR = "federal-lobbying/"
        path = args.site_root / str(cycle) / "federal-lobbying/index.html"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(build_site.page_federal_lobbying({"cycle": cycle}), encoding="utf-8")
    if explorer:
        print(f"Built lobbying explorer: {args.site_root / 'lobbying/index.html'}")
    if spending:
        print(f"Built lobbying spending page: {args.site_root / 'lobbying/spending/index.html'}")


if __name__ == "__main__":
    main()
