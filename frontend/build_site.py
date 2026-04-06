from __future__ import annotations

import csv
import html
import json
import shutil
from pathlib import Path


FRONTEND_ROOT = Path(__file__).resolve().parent
ASSET_ROOT = FRONTEND_ROOT / "assets"
SITE_ROOT = FRONTEND_ROOT / "site"
EXPORT_ROOT = FRONTEND_ROOT.parent / "exports" / "site"

AVAILABLE_CYCLES: list[int] = []
CYCLE_PAGE_DIRS: dict[int, set[str]] = {}
CURRENT_RENDER_CYCLE: int | None = None
CURRENT_RENDER_REL_DIR = ""

COMPANY_LABELS = {
    "a16z": "a16z",
    "amd": "AMD",
    "anthropic": "Anthropic",
    "apple": "Apple",
    "amazon": "Amazon",
    "ebay": "eBay",
    "google": "Google",
    "greylock": "Greylock",
    "ibm": "IBM",
    "khosla": "Khosla Ventures",
    "meta": "Meta",
    "microsoft": "Microsoft",
    "netflix": "Netflix",
    "nvidia": "NVIDIA",
    "openai": "OpenAI",
    "oracle": "Oracle",
    "qualcomm": "Qualcomm",
    "ripple": "Ripple",
    "salesforce": "Salesforce",
    "sequoia": "Sequoia",
    "shopify": "Shopify",
    "stripe": "Stripe",
    "tesla": "Tesla",
    "uber": "Uber",
    "union_square_ventures": "Union Square Ventures",
    "x_twitter_spacex": "X / Twitter / SpaceX",
    "zoom": "Zoom",
}

STATE_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "AS": "American Samoa",
    "GU": "Guam",
    "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico",
    "VI": "U.S. Virgin Islands",
}

STATE_TILE_LAYOUT = {
    "AK": (0, 0),
    "ME": (11, 0),
    "VT": (10, 1),
    "NH": (11, 1),
    "WA": (1, 2),
    "ID": (2, 2),
    "MT": (3, 2),
    "ND": (4, 2),
    "MN": (5, 2),
    "WI": (6, 3),
    "MI": (7, 3),
    "OH": (8, 2),
    "NY": (9, 2),
    "MA": (11, 2),
    "OR": (1, 3),
    "UT": (2, 3),
    "WY": (3, 3),
    "SD": (4, 3),
    "IA": (5, 3),
    "IL": (6, 4),
    "IN": (7, 4),
    "WV": (8, 3),
    "PA": (9, 3),
    "CT": (10, 3),
    "RI": (11, 3),
    "CA": (1, 4),
    "NV": (2, 4),
    "CO": (3, 4),
    "NE": (4, 4),
    "MO": (5, 4),
    "TN": (6, 5),
    "KY": (7, 5),
    "NC": (8, 4),
    "MD": (9, 4),
    "NJ": (10, 4),
    "AZ": (2, 5),
    "NM": (3, 5),
    "KS": (4, 5),
    "AR": (5, 5),
    "MS": (6, 6),
    "AL": (7, 6),
    "SC": (8, 5),
    "VA": (9, 5),
    "DE": (10, 5),
    "DC": (11, 5),
    "HI": (1, 7),
    "OK": (4, 6),
    "LA": (5, 6),
    "TX": (4, 7),
    "FL": (8, 7),
    "GA": (8, 6),
}


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def display_start_for_cycle(cycle: int) -> str:
    return f"{cycle - 1}-01-01"


def normalize_rel_dir(value: str) -> str:
    value = value.replace("\\", "/").strip("/")
    if value in {"", "."}:
        return ""
    return value + "/"


def section_root_rel_dir(rel_dir: str) -> str:
    rel_dir = normalize_rel_dir(rel_dir)
    if not rel_dir:
        return ""

    parts = rel_dir.strip("/").split("/")
    if parts[0] == "candidates" and len(parts) >= 3 and parts[1] == "states":
        return normalize_rel_dir("/".join(parts[:3]))
    if parts[0] == "data":
        if len(parts) >= 3 and parts[1] == "charts" and parts[2] == "companies":
            return "data/charts/companies/"
        if len(parts) >= 2 and parts[1] == "companies":
            return "data/companies/"
        return "data/"
    return normalize_rel_dir(parts[0])


def resolve_cycle_target_rel_dir(target_cycle: int, current_rel_dir: str) -> str:
    target_dirs = CYCLE_PAGE_DIRS.get(target_cycle, set())
    candidate = normalize_rel_dir(current_rel_dir)
    while True:
        if candidate in target_dirs:
            return candidate
        fallback = section_root_rel_dir(candidate)
        if fallback == candidate:
            break
        candidate = fallback
    return "" if "" in target_dirs else candidate


def cycle_toggle_html(prefix: str) -> str:
    if CURRENT_RENDER_CYCLE is None or len(AVAILABLE_CYCLES) < 2:
        return ""

    site_prefix = prefix + "../"
    links: list[str] = []
    for cycle in AVAILABLE_CYCLES:
        if cycle == CURRENT_RENDER_CYCLE:
            links.append(f'<span class="cycle-pill current">{cycle}</span>')
            continue
        target_rel_dir = resolve_cycle_target_rel_dir(cycle, CURRENT_RENDER_REL_DIR)
        href = f"{site_prefix}{cycle}/"
        if target_rel_dir:
            href += target_rel_dir
        links.append(f'<a class="cycle-pill" href="{esc(href)}">{cycle}</a>')

    return (
        '<div class="cycle-toggle">'
        '<span class="cycle-label">Cycle</span>'
        + "".join(links)
        + "</div>"
    )


def float_value(value, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def int_value(value, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def esc(value) -> str:
    return html.escape("" if value is None else str(value))


def display_company_name(value: str) -> str:
    if value in COMPANY_LABELS:
        return COMPANY_LABELS[value]
    return value.replace("_", " ").title()


def money(value) -> str:
    amount = float(value or 0)
    return "${:,.0f}".format(amount)


def pct(value) -> str:
    if value in ("", None):
        return ""
    try:
        return "{:.1f}%".format(float(value))
    except (TypeError, ValueError):
        return ""


def pct_ratio(value) -> str:
    if value in ("", None):
        return ""
    try:
        return "{:.1f}%".format(float(value) * 100.0)
    except (TypeError, ValueError):
        return ""


def num(value) -> str:
    return "{:,}".format(int(float(value or 0)))


def truncate(text: str, limit: int = 72) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def state_name(code: str) -> str:
    code = (code or "").upper()
    return STATE_NAMES.get(code, code)


def state_slug(code: str) -> str:
    return (code or "").lower()


def candidate_sort_key(row: dict) -> tuple:
    return (
        -float_value(row.get("tech_itemized_receipts")),
        -float_value(row.get("ie_support_total")),
        -float_value(row.get("linked_committee_count")),
        row.get("cand_name", ""),
    )


def is_truthy_flag(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def display_candidate_rows(rows: list[dict]) -> list[dict]:
    filtered = [row for row in rows if is_truthy_flag(row.get("is_display_candidate"))]
    return filtered if filtered else rows


def district_slug(code: str) -> str:
    code = (code or "").upper()
    if code == "AL":
        return "at-large"
    return code.lower()


def district_display(code: str) -> str:
    code = (code or "").upper()
    if code == "AL":
        return "At-Large"
    return code


def normalize_candidate_district(value) -> str:
    text = str(value or "").strip().upper()
    if text.endswith(".0"):
        text = text[:-2]
    if text in {"", "NAN"}:
        return ""
    try:
        district_num = int(float(text))
    except ValueError:
        return text
    if district_num == 0:
        return "AL"
    return f"{district_num:02d}"


def state_detail_href(code: str, prefix: str = "") -> str:
    return f"{prefix}states/{state_slug(code)}/"


def district_detail_href(state_code: str, district_code: str, prefix: str = "") -> str:
    return f"{prefix}states/{state_slug(state_code)}/house/{district_slug(district_code)}/"


def senate_detail_href(state_code: str, prefix: str = "") -> str:
    return f"{prefix}states/{state_slug(state_code)}/senate/"


def presidential_detail_href(prefix: str = "") -> str:
    return f"{prefix}president/"


def party_badge_class(label: str) -> str:
    return f"party-{(label or 'Unknown').lower()}"


def is_candidate_committee(row: dict) -> bool:
    return row.get("cmte_tp") in {"H", "S", "P"}


def donor_top_committee(row: dict, limit: int = 46) -> str:
    committee = truncate(row.get("top_committee", ""), limit)
    if not committee:
        return ""
    amount = row.get("top_committee_amt")
    if amount in ("", None):
        return committee
    return f"{committee} ({money(amount)})"


def nav(prefix: str) -> str:
    links = [
        (f"{prefix}index.html", "Home"),
        (f"{prefix}companies/", "Companies"),
        (f"{prefix}candidates/", "Candidates"),
        (f"{prefix}political-bodies/", "Political Bodies"),
        (f"{prefix}donors/", "Donors"),
        (f"{prefix}methodology/", "Methodology"),
        (f"{prefix}data/", "Data"),
    ]
    return " | ".join(
        f'<a href="{esc(href)}">{esc(label)}</a>'
        for href, label in links
    )


def note(metadata: dict, *lines: str) -> str:
    items = [f"<strong>Data as of {esc(metadata['data_as_of'])}.</strong>"]
    items.extend(esc(line) for line in lines if line)
    return '<p class="meta">' + "<br>".join(items) + "</p>"


def site_header(prefix: str) -> str:
    return f"""
<div class="site-header">
  <div class="site-title">Tech Money</div>
  <p class="site-tagline">A plain web document about tech-linked money in federal politics.</p>
  {cycle_toggle_html(prefix)}
  <hr class="rule">
  <div class="navline">{nav(prefix)}</div>
  <hr class="rule">
</div>
"""


def shell(
    title: str,
    body: str,
    prefix: str = "",
    scripts: str = "",
    top_note: str = "",
    include_charts: bool = False,
) -> str:
    chart_script = ""
    if include_charts:
        chart_script = f'\n  <script src="{esc(prefix)}static/charts.js"></script>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <link rel="stylesheet" href="{esc(prefix)}static/site.css">
</head>
<body>
  <div class="page">
    {site_header(prefix)}
    {top_note}
    {body}
  </div>
  {chart_script}
  {scripts}
</body>
</html>
"""


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def reset_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{header}</th>" for header in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(row) + "</tr>")
    return (
        '<table border="1" cellpadding="4" cellspacing="0">'
        + "<thead><tr>" + head + "</tr></thead>"
        + "<tbody>"
        + "".join(body_rows)
        + "</tbody></table>"
    )


def candidate_money_note() -> str:
    return (
        '<p class="small">On candidate pages, <strong>Total Receipts</strong> means all '
        "itemized individual contributions flowing into linked candidate committees. "
        '<strong>Tech Receipts</strong> is the tracked tech-employer subset of that same '
        'pool. <strong>IE Support</strong> and <strong>IE Oppose</strong> are outside '
        "independent expenditures for or against the candidate, not direct committee receipts.</p>"
    )


def render_candidate_tile_map(state_rows: list[dict], prefix: str = "") -> str:
    row_lookup = {row["state_code"]: row for row in state_rows}
    tile_size = 64
    gap = 8
    margin = 12
    cols = max(col for col, _ in STATE_TILE_LAYOUT.values()) + 1
    rows = max(row for _, row in STATE_TILE_LAYOUT.values()) + 1
    width = margin * 2 + cols * tile_size + (cols - 1) * gap
    height = margin * 2 + rows * tile_size + (rows - 1) * gap
    svg_parts = [
        f'<svg class="tile-map" viewBox="0 0 {width} {height}" role="img" aria-label="Candidate navigation map by state">'
    ]
    for code, (col, row) in STATE_TILE_LAYOUT.items():
        x = margin + col * (tile_size + gap)
        y = margin + row * (tile_size + gap)
        state = row_lookup.get(code, {})
        href = state_detail_href(code, prefix)
        label = state.get("party_label", "Unknown")
        house_count = int_value(state.get("house_district_count"))
        senate_count = int_value(state.get("has_senate_race"))
        sublabel = ""
        if house_count > 0:
            sublabel = f"{house_count} House"
        elif senate_count > 0:
            sublabel = "Senate"
        title_bits = [state_name(code)]
        if house_count > 0:
            title_bits.append(f"{house_count} House districts")
        if senate_count > 0:
            title_bits.append("Senate race")
        tech_receipts = float_value(state.get("tech_itemized_receipts"))
        if tech_receipts > 0:
            title_bits.append(f"{money(tech_receipts)} tech-linked receipts")
        title = " | ".join(title_bits)
        svg_parts.append(
            f'<a href="{esc(href)}">'
            f'<title>{esc(title)}</title>'
            f'<rect class="tile {party_badge_class(label)}" x="{x}" y="{y}" width="{tile_size}" height="{tile_size}" rx="2" ry="2"></rect>'
            f'<text class="tile-state" x="{x + tile_size / 2}" y="{y + 26}">{esc(code)}</text>'
            f'<text class="tile-sub" x="{x + tile_size / 2}" y="{y + 46}">{esc(sublabel)}</text>'
            f"</a>"
        )
    svg_parts.append("</svg>")
    return "".join(svg_parts)


def candidate_rows_for_office(candidate_rows: list[dict], office: str) -> list[dict]:
    rows = [row for row in candidate_rows if row.get("cand_office") == office]
    rows = display_candidate_rows(rows)
    return sorted(rows, key=candidate_sort_key)


def page_home(metadata: dict, homepage: dict) -> str:
    display_start = display_start_for_cycle(int(metadata["cycle"]))
    top_company_rows = []
    for row in homepage["top_companies"][:5]:
        top_company_rows.append(
            [
                f'<td><a href="companies/{esc(row["slug"])}/">{esc(display_company_name(row["tech_canonical_name"]))}</a></td>',
                f'<td class="number">{money(row["net_total"])}</td>',
                f'<td class="number">{num(row["n_donors"])}</td>',
                f'<td class="number">{num(row["n_committees"])}</td>',
                f'<td class="number">{pct(row.get("pct_classified_recipients"))}</td>',
                f'<td class="number">{pct(row["pct_dem_by_donor"])}</td>',
            ]
        )

    top_candidate_rows = []
    for row in homepage["top_candidates"][:5]:
        top_candidate_rows.append(
            [
                f'<td>{esc(truncate(row["cmte_nm"], 56))}</td>',
                f'<td>{esc(row["cmte_tp"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row["tech_receipts"])}</td>',
                f'<td class="number">{pct(row["tech_pct"])}</td>',
                f'<td class="number">{num(row["tech_donors"])}</td>',
            ]
        )

    top_political_rows = []
    for row in homepage["top_political_bodies"][:5]:
        top_political_rows.append(
            [
                f'<td>{esc(truncate(row["cmte_nm"], 56))}</td>',
                f'<td>{esc(row["cmte_tp"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row["tech_receipts"])}</td>',
                f'<td class="number">{pct(row["tech_pct"])}</td>',
                f'<td class="number">{num(row["tech_donors"])}</td>',
            ]
        )

    top_donor_rows = []
    for row in homepage["top_donors"][:5]:
        top_donor_rows.append(
            [
                f'<td>{esc(row["name"])}</td>',
                f'<td class="number">{money(row.get("D"))}</td>',
                f'<td class="number">{pct_ratio(row.get("pct_d"))}</td>',
                f'<td class="number">{money(row.get("R"))}</td>',
                f'<td class="number">{money(row["net_total"])}</td>',
                f'<td>{esc(truncate(row.get("tech_companies", ""), 48))}</td>',
                f'<td>{esc(donor_top_committee(row, 34))}</td>',
            ]
        )

    section_rows = [
        [
            '<td><a href="companies/">Companies</a></td>',
            '<td>Who employees of tracked companies gave to, with one page per company.</td>',
        ],
        [
            '<td><a href="candidates/">Candidates</a></td>',
            '<td>Featured candidate committees that took meaningful tech-linked money.</td>',
        ],
        [
            '<td><a href="political-bodies/">Political Bodies</a></td>',
            '<td>PACs, Super PACs, party committees, and other non-candidate committees by tech-linked receipts.</td>',
        ],
        [
            '<td><a href="donors/">Donors</a></td>',
            '<td>Major individual donors currently visible through the employer-matching system.</td>',
        ],
        [
            '<td><a href="methodology/">Methodology</a></td>',
            '<td>Transaction-type rules, caveats, and what this site does and does not count.</td>',
        ],
        [
            '<td><a href="data/">Data</a></td>',
            '<td>Frontend-ready exports, downloadable CSV and JSON files, and chart payloads.</td>',
        ],
    ]

    body = f"""
<p><strong>Total tech-linked giving:</strong> {money(metadata["total_tech_linked_giving"])}<br>
<strong>Tech donors:</strong> {num(metadata["tech_donor_count"])}<br>
<strong>Tracked companies:</strong> {num(metadata["tracked_company_count"])}<br>
<strong>Committees receiving tech money:</strong> {num(metadata["committees_receiving_tech_money"])}</p>

<h2>Weekly Tech-Linked Giving</h2>
<div class="chart-wrap"><div id="weekly-chart"></div></div>
<p class="note">Chart display begins on {display_start}. The totals above keep the full validated export, including a small number of earlier rows present in the source files.</p>

<h2>Start Here</h2>
{table(["Section", "What It Shows"], section_rows)}

<h2>At A Glance: Companies</h2>
{table(["Company", "Total", "Donors", "Committees", "Pct Classified", "Pct Dem by Donor"], top_company_rows)}
<p><a href="companies/">See all companies.</a></p>

<h2>At A Glance: Candidates</h2>
{table(["Candidate Committee", "Type", "Lean", "Tech Receipts", "Tech Share", "Tech Donors"], top_candidate_rows)}
<p class="small">Committee tech share is a share of itemized individual contribution receipts, not all committee money.</p>
<p><a href="candidates/">See featured candidate committees.</a></p>

<h2>At A Glance: Political Bodies</h2>
{table(["Committee", "Type", "Lean", "Tech Receipts", "Tech Share", "Tech Donors"], top_political_rows)}
<p class="small">This page includes PACs, Super PACs, party committees, and other non-candidate committees.</p>
<p><a href="political-bodies/">See featured political bodies.</a></p>

<h2>At A Glance: Top Donors</h2>
{table(["Donor", "$ to Dem", "% to Dem", "$ to Rep", "Total", "Company Tags", "Top Committee"], top_donor_rows)}
<p><a href="donors/">See all major donors.</a></p>

<h2>Notes</h2>
<ul>
  <li>Use the cycle toggle above to switch between the included 2024 and 2026 exports.</li>
  <li>Employer matching is still an employer-string system, not full person/entity resolution.</li>
  <li>Candidate navigation now includes national, state, Senate, and House district views.</li>
</ul>
"""

    scripts = f"""
<script>
document.addEventListener("DOMContentLoaded", function () {{
  window.TechMoneyCharts.loadWeeklyChart(
    "weekly-chart",
    "data/charts/home_weekly_totals.json",
    {{ displayStart: "{display_start}" }}
  );
}});
</script>
"""

    top_note = note(
        metadata,
        f"This page uses the validated {metadata['cycle']} export included in this build.",
        "Party lean for PACs and Super PACs is inferred from candidate-facing spending when possible.",
        f"Weekly charts begin at {display_start}; aggregate totals keep the full validated export.",
    )
    return shell(
        "Tech Money",
        body,
        prefix="",
        scripts=scripts,
        top_note=top_note,
        include_charts=True,
    )


def page_companies_index(metadata: dict, companies: list[dict]) -> str:
    rows = []
    for row in companies:
        rows.append(
            [
                f'<td><a href="{esc(row["slug"])}/">{esc(display_company_name(row["tech_canonical_name"]))}</a></td>',
                f'<td class="number">{money(row["net_total"])}</td>',
                f'<td class="number">{num(row["n_donors"])}</td>',
                f'<td class="number">{num(row["n_committees"])}</td>',
                f'<td>{esc(row.get("sectors", ""))}</td>',
                f'<td class="number">{pct(row.get("pct_classified_recipients"))}</td>',
                f'<td class="number">{pct(row.get("pct_dem_by_donor"))}</td>',
            ]
        )

    body = f"""
<h1>Companies</h1>
<p>This page lists the tracked tech companies currently in the validated {metadata["cycle"]} export.</p>
{table(["Company", "Total", "Donors", "Committees", "Sector", "Pct Classified", "Pct Dem by Donor"], rows)}
"""
    top_note = note(
        metadata,
        f"This page lists tracked companies in the validated {metadata['cycle']} export.",
        "Company totals rely on the current employer-matching system.",
        "Recipient lean is inferred from committee behavior when direct party is absent.",
    )
    return shell("Companies - Tech Money", body, prefix="../", top_note=top_note)


def page_company(metadata: dict, company_payload: dict) -> str:
    display_start = display_start_for_cycle(int(metadata["cycle"]))
    summary = company_payload["summary"]
    top_donor_rows = []
    for row in company_payload["top_donors"]:
        top_donor_rows.append(
            [
                f'<td>{esc(row["name"])}</td>',
                f'<td class="number">{money(row["net_total"])}</td>',
                f'<td class="number">{num(row["n_contributions"])}</td>',
                f'<td>{esc(truncate(row.get("top_committee", ""), 42))}</td>',
            ]
        )

    top_committee_rows = []
    for row in company_payload["top_committees"]:
        top_committee_rows.append(
            [
                f'<td>{esc(truncate(row["cmte_nm"], 56))}</td>',
                f'<td>{esc(row["cmte_tp"])}</td>',
                f'<td>{esc(row["recipient_bucket"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row["net_total"])}</td>',
                f'<td class="number">{num(row["n_donors"])}</td>',
            ]
        )

    company_name = display_company_name(company_payload["company"])
    body = f"""
<h1>{esc(company_name)}</h1>

<p><strong>Total tech-linked giving:</strong> {money(summary["net_total"])}<br>
<strong>Donors:</strong> {num(summary["n_donors"])}<br>
<strong>Contribution rows:</strong> {num(summary["n_contributions"])}<br>
<strong>Recipient committees:</strong> {num(summary["n_committees"])}<br>
<strong>Sector:</strong> {esc(summary.get("sectors", ""))}</p>

<p><strong>Pct recipient dollars classified:</strong> {pct(summary.get("pct_classified_recipients"))}<br>
<strong>Pct Dem by inferred recipient lean:</strong> {pct(summary.get("pct_dem"))}<br>
<strong>Pct Dem by donor classification:</strong> {pct(summary.get("pct_dem_by_donor"))}</p>

<h2>Weekly Giving</h2>
<div class="chart-wrap"><div id="company-weekly-chart"></div></div>
<p class="note">Chart display begins on {display_start}. Totals above use the full validated export.</p>

<h2>Top Recipient Committees</h2>
{table(["Committee", "Type", "Bucket", "Lean", "Total", "Donors"], top_committee_rows)}

<h2>Top Donors</h2>
{table(["Donor", "Total", "Rows", "Top Committee"], top_donor_rows)}

<p class="small"><a href="../">Back to companies.</a></p>
"""

    scripts = f"""
<script>
document.addEventListener("DOMContentLoaded", function () {{
  window.TechMoneyCharts.loadWeeklyChart(
    "company-weekly-chart",
    "../../data/charts/companies/{esc(company_payload["slug"])}.json",
    {{ displayStart: "{display_start}" }}
  );
}});
</script>
"""

    top_note = note(
        metadata,
        f"This page uses the validated {metadata['cycle']} company export and employer-matching system.",
        "Recipient lean is inferred from committee behavior when direct party is absent.",
        f"Weekly chart display begins at {display_start}; totals above keep the full validated export.",
    )
    return shell(
        f"{company_name} - Tech Money",
        body,
        prefix="../../",
        scripts=scripts,
        top_note=top_note,
        include_charts=True,
    )


def page_committees(metadata: dict, committees: list[dict]) -> str:
    featured = [row for row in committees if row.get("is_featured")]
    featured_candidates = [row for row in featured if is_candidate_committee(row)]
    featured_political_bodies = [
        row for row in featured if not is_candidate_committee(row)
    ]

    body = f"""
<h1>Committees</h1>
<p>The old committees view is now split into two public pages.</p>
<ul>
  <li><a href="../candidates/">Candidates</a>: featured candidate committees that took meaningful tech-linked money.</li>
  <li><a href="../political-bodies/">Political Bodies</a>: PACs, Super PACs, party committees, and other non-candidate committees.</li>
</ul>
<p><strong>Featured candidate committees:</strong> {num(len(featured_candidates))}<br>
<strong>Featured political bodies:</strong> {num(len(featured_political_bodies))}</p>
"""
    top_note = note(
        metadata,
        f"This directory now points to the split committee views for the validated {metadata['cycle']} export.",
        "Committee tech share is based on itemized individual receipts, not all committee money.",
    )
    return shell("Committees - Tech Money", body, prefix="../", top_note=top_note)


def page_candidates(
    metadata: dict,
    candidate_race: list[dict],
    candidate_state: list[dict],
    candidate_house_district: list[dict],
    candidate_senate: list[dict],
) -> str:
    presidential_rows = []
    for row in candidate_rows_for_office(candidate_race, "P")[:6]:
        presidential_rows.append(
            [
                f'<td><a href="{esc(presidential_detail_href())}">{esc(row["cand_name"])}</a></td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
            ]
        )

    senate_rows = []
    for row in sorted(candidate_senate, key=lambda r: (-float_value(r.get("tech_itemized_receipts")), r.get("state_code", "")))[:12]:
        senate_rows.append(
            [
                f'<td><a href="{esc(senate_detail_href(row["state_code"]))}">{esc(state_name(row["state_code"]))}</a></td>',
                f'<td>{esc(truncate(row.get("dem_candidate_name", ""), 26))}</td>',
                f'<td>{esc(truncate(row.get("rep_candidate_name", ""), 26))}</td>',
                f'<td>{esc(row.get("party_label", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
            ]
        )

    house_rows = []
    for row in sorted(candidate_house_district, key=lambda r: (-float_value(r.get("tech_itemized_receipts")), r.get("district_label", "")))[:18]:
        house_rows.append(
            [
                f'<td><a href="{esc(district_detail_href(row["state_code"], row["district_code"]))}">{esc(row["district_label"])}</a></td>',
                f'<td>{esc(truncate(row.get("dem_candidate_name", ""), 24))}</td>',
                f'<td>{esc(truncate(row.get("rep_candidate_name", ""), 24))}</td>',
                f'<td>{esc(row.get("party_label", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
            ]
        )

    map_states = [
        row for row in candidate_state
        if row.get("state_code") in STATE_TILE_LAYOUT
    ]
    map_states = sorted(map_states, key=lambda r: r.get("state_code", ""))
    other_jurisdictions = [
        row for row in candidate_state
        if row.get("state_code") not in STATE_TILE_LAYOUT
    ]
    jurisdiction_rows = []
    for row in sorted(other_jurisdictions, key=lambda r: (-float_value(r.get("tech_itemized_receipts")), r.get("state_code", ""))):
        jurisdiction_rows.append(
            [
                f'<td><a href="{esc(state_detail_href(row["state_code"]))}">{esc(state_name(row["state_code"]))}</a></td>',
                f'<td>{esc(row.get("party_label", ""))}</td>',
                f'<td class="number">{num(row.get("house_district_count"))}</td>',
                f'<td class="number">{num(row.get("senate_candidate_count"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
            ]
        )

    total_house_districts = sum(int_value(row.get("house_district_count")) for row in candidate_state)
    states_with_senate = sum(int_value(row.get("has_senate_race")) for row in candidate_state)

    body = f"""
<h1>Candidates</h1>
<p>This page is the navigation hub for candidate coverage in the validated {metadata["cycle"]} build. Use the state tile map to jump to a state page, then drill into Senate and House districts from there.</p>

<p><strong>Candidate rows:</strong> {num(len(candidate_race))}<br>
<strong>States and jurisdictions with House or Senate candidates:</strong> {num(len(candidate_state))}<br>
<strong>House districts represented:</strong> {num(total_house_districts)}<br>
<strong>States with Senate candidates:</strong> {num(states_with_senate)}</p>

<h2>Navigate by State</h2>
<p>Click a tile to open that state page.</p>
{render_candidate_tile_map(map_states)}

<h2>Presidential</h2>
{table(["Candidate", "Party", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose", "Linked Committees"], presidential_rows)}

<h2>Senate Snapshot</h2>
{table(["State", "D Candidate", "R Candidate", "Lean", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose"], senate_rows)}

<h2>House Snapshot</h2>
{table(["District", "D Candidate", "R Candidate", "Lean", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose"], house_rows)}
"""
    if jurisdiction_rows:
        body += (
            "\n<h2>Other Jurisdictions</h2>\n"
            + table(["Jurisdiction", "Lean", "House Districts", "Senate Candidates", "Tech Receipts"], jurisdiction_rows)
        )

    body += "\n" + candidate_money_note() + "\n"

    top_note = note(
        metadata,
        f"This page uses candidate, linkage, and committee data from the validated {metadata['cycle']} export.",
        "Use the map for state navigation; state pages then link onward to Senate and House district views.",
        "Tech receipts here are based on itemized individual contributions into linked committees.",
    )
    return shell("Candidates - Tech Money", body, prefix="../", top_note=top_note)


def page_candidate_state(
    metadata: dict,
    state_row: dict,
    state_candidates: list[dict],
    state_districts: list[dict],
    state_senate_summary: dict | None,
) -> str:
    state_code = state_row["state_code"]
    state_title = state_name(state_code)
    state_candidates = display_candidate_rows(state_candidates)
    senate_candidates = candidate_rows_for_office(state_candidates, "S")
    house_candidates = candidate_rows_for_office(state_candidates, "H")

    senate_rows = []
    for row in senate_candidates:
        senate_rows.append(
            [
                f'<td><a href="senate/">{esc(row["cand_name"])}</a></td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
            ]
        )

    district_rows = []
    for row in sorted(state_districts, key=lambda r: int_value(r.get("district_sort"), 999)):
        district_rows.append(
            [
                f'<td><a href="house/{esc(district_slug(row["district_code"]))}/">{esc(row["district_label"])}</a></td>',
                f'<td>{esc(truncate(row.get("dem_candidate_name", ""), 24))}</td>',
                f'<td>{esc(truncate(row.get("rep_candidate_name", ""), 24))}</td>',
                f'<td>{esc(row.get("party_label", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
            ]
        )

    body = f"""
<h1>{esc(state_title)}</h1>
<p><strong>State lean by candidate tech receipts:</strong> {esc(state_row.get("party_label", ""))}<br>
<strong>Total candidate receipts:</strong> {money(state_row.get("total_itemized_receipts"))}<br>
<strong>Total candidate tech receipts:</strong> {money(state_row.get("tech_itemized_receipts"))}<br>
<strong>House districts:</strong> {num(state_row.get("house_district_count"))}<br>
<strong>Senate candidates:</strong> {num(state_row.get("senate_candidate_count"))}</p>
"""
    if state_senate_summary is not None:
        body += (
            f'<p><a href="senate/">Open the {esc(state_title)} Senate page.</a></p>'
        )
    if senate_rows:
        body += "\n<h2>Senate Candidates</h2>\n" + table(
            ["Candidate", "Party", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose", "Linked Committees"],
            senate_rows,
        )

    body += "\n<h2>House Districts</h2>\n" + table(
        ["District", "D Candidate", "R Candidate", "Lean", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose"],
        district_rows,
    )
    body += "\n" + candidate_money_note()
    body += '\n<p class="small"><a href="../../">Back to candidates.</a></p>'

    top_note = note(
        metadata,
        f"This page summarizes candidate money touchpoints for {state_title} in the validated {metadata['cycle']} export.",
        "District links below open full district-level candidate tables.",
    )
    return shell(f"{state_title} - Candidates - Tech Money", body, prefix="../../../", top_note=top_note)


def page_candidate_state_senate(
    metadata: dict,
    state_row: dict,
    senate_candidates: list[dict],
) -> str:
    state_code = state_row["state_code"]
    state_title = state_name(state_code)
    senate_candidates = display_candidate_rows(senate_candidates)
    rows = []
    senate_total_receipts = sum(
        float_value(row.get("total_itemized_receipts")) for row in senate_candidates
    )
    senate_tech_receipts = sum(
        float_value(row.get("tech_itemized_receipts")) for row in senate_candidates
    )
    for row in senate_candidates:
        rows.append(
            [
                f'<td>{esc(row["cand_name"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
                f'<td>{esc(truncate(row.get("linked_committee_names", ""), 48))}</td>',
            ]
        )

    body = f"""
<h1>{esc(state_title)} Senate</h1>
<p><strong>Total Senate candidate receipts:</strong> {money(senate_total_receipts)}<br>
<strong>Total Senate candidate tech receipts:</strong> {money(senate_tech_receipts)}<br>
<strong>Senate candidates:</strong> {num(len(senate_candidates))}</p>
{table(["Candidate", "Party", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose", "Linked Committees", "Committee Names"], rows)}
{candidate_money_note()}
<p class="small"><a href="../">Back to the state page.</a></p>
"""
    top_note = note(
        metadata,
        f"This page lists Senate candidates in {state_title} from the validated {metadata['cycle']} export.",
    )
    return shell(f"{state_title} Senate - Tech Money", body, prefix="../../../../", top_note=top_note)


def page_candidate_house_district(
    metadata: dict,
    district_row: dict,
    district_candidates: list[dict],
) -> str:
    state_code = district_row["state_code"]
    district_code = district_row["district_code"]
    district_title = district_row["district_label"]
    district_candidates = display_candidate_rows(district_candidates)
    rows = []
    for row in district_candidates:
        rows.append(
            [
                f'<td>{esc(row["cand_name"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
                f'<td>{esc(truncate(row.get("linked_committee_names", ""), 48))}</td>',
            ]
        )

    body = f"""
<h1>{esc(district_title)}</h1>
<p><strong>District lean by candidate tech receipts:</strong> {esc(district_row.get("party_label", ""))}<br>
<strong>Total district candidate receipts:</strong> {money(district_row.get("total_itemized_receipts"))}<br>
<strong>Total district tech receipts:</strong> {money(district_row.get("tech_itemized_receipts"))}<br>
<strong>Candidates:</strong> {num(len(district_candidates))}</p>
{table(["Candidate", "Party", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose", "Linked Committees", "Committee Names"], rows)}
{candidate_money_note()}
<p class="small"><a href="../../">Back to the state page.</a></p>
"""
    top_note = note(
        metadata,
        f"This page lists House candidates for {district_title} in the validated {metadata['cycle']} export.",
        f"State page: {state_name(state_code)}.",
    )
    return shell(f"{district_title} - Tech Money", body, prefix="../../../../../", top_note=top_note)


def page_president(metadata: dict, presidential_candidates: list[dict]) -> str:
    presidential_candidates = display_candidate_rows(presidential_candidates)
    rows = []
    for row in presidential_candidates:
        rows.append(
            [
                f'<td>{esc(row["cand_name"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row.get("total_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
                f'<td>{esc(truncate(row.get("linked_committee_names", ""), 48))}</td>',
            ]
        )

    body = f"""
<h1>Presidential</h1>
<p>This page lists presidential candidates present in the validated {metadata['cycle']} candidate export.</p>
{table(["Candidate", "Party", "Total Receipts", "Tech Receipts", "IE Support", "IE Oppose", "Linked Committees", "Committee Names"], rows)}
{candidate_money_note()}
<p class="small"><a href="../">Back to candidates.</a></p>
"""
    top_note = note(
        metadata,
        f"This page lists presidential candidates from the validated {metadata['cycle']} export.",
    )
    return shell("Presidential - Tech Money", body, prefix="../", top_note=top_note)


def page_political_bodies(metadata: dict, committees: list[dict]) -> str:
    featured = [row for row in committees if row.get("is_featured") and not is_candidate_committee(row)]
    rows = []
    for row in featured:
        rows.append(
            [
                f'<td>{esc(truncate(row["cmte_nm"], 56))}</td>',
                f'<td>{esc(row["cmte_tp"])}</td>',
                f'<td>{esc(row["recipient_bucket"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{money(row["tech_receipts"])}</td>',
                f'<td class="number">{pct(row["tech_pct"])}</td>',
                f'<td class="number">{num(row["tech_donors"])}</td>',
                f'<td>{esc(truncate(row["tech_companies"], 80))}</td>',
            ]
        )

    body = f"""
<h1>Political Bodies</h1>
<p>This page shows featured non-candidate committees that either took in at least {money(100000)} from tracked tech donors or had at least a 10% tech share of itemized individual receipts.</p>
{table(["Committee", "Type", "Bucket", "Lean", "Tech Receipts", "Tech Share", "Tech Donors", "Company Tags"], rows)}
<p class="small">This includes PACs, Super PACs, party committees, and other non-candidate political bodies.</p>
<p class="small">Committee tech share is based on itemized individual contribution receipts only. It is not a share of all committee money.</p>
"""
    top_note = note(
        metadata,
        f"This page shows featured non-candidate political bodies from the validated {metadata['cycle']} export.",
        "Committee lean is inferred from candidate-facing activity when direct party is absent.",
        "Committee tech share is based on itemized individual receipts, not all committee money.",
    )
    return shell("Political Bodies - Tech Money", body, prefix="../", top_note=top_note)


def page_donors(metadata: dict, donors: list[dict]) -> str:
    rows = []
    for row in donors:
        pct_rep = row.get("pct_r")
        if pct_rep in ("", None) and row.get("pct_d") not in ("", None):
            pct_rep = 1.0 - float(row["pct_d"])
        rows.append(
            [
                f'<td>{esc(row["name"])}</td>',
                f'<td class="number">{money(row.get("D"))}</td>',
                f'<td class="number">{pct_ratio(row.get("pct_d"))}</td>',
                f'<td class="number">{money(row.get("R"))}</td>',
                f'<td class="number">{pct_ratio(pct_rep)}</td>',
                f'<td class="number">{money(row["net_total"])}</td>',
                f'<td>{esc(truncate(row.get("tech_companies", ""), 72))}</td>',
                f'<td>{esc(donor_top_committee(row, 42))}</td>',
            ]
        )

    body = f"""
<h1>Major Donors</h1>
<p>This page currently lists tech-linked donors with at least {money(100000)} in the validated export.</p>
{table(["Name", "$ to Dem", "% to Dem", "$ to Rep", "% to Rep", "Total ($)", "Company Tags", "Top Committee Funded"], rows)}
"""
    top_note = note(
        metadata,
        f"This page shows major donors in the validated {metadata['cycle']} export.",
        f"The current public threshold is {money(100000)} and employer matching remains incomplete.",
    )
    return shell("Donors - Tech Money", body, prefix="../", top_note=top_note)


def page_methodology(metadata: dict) -> str:
    display_start = display_start_for_cycle(int(metadata["cycle"]))
    column_rows = [
        ['<td>Section</td>', '<td>The site area or page family listed in a navigation table.</td>'],
        ['<td>What It Shows</td>', '<td>A short plain-language description of that page or export.</td>'],
        ['<td>Company</td>', '<td>The tracked tech company or firm tied to a donor-employer tag.</td>'],
        ['<td>Candidate Committee</td>', '<td>A candidate-linked committee receiving contributions.</td>'],
        ['<td>Committee</td>', '<td>A political committee, which may be a candidate committee, PAC, party committee, Super PAC, or another filer.</td>'],
        ['<td>Donor</td>', '<td>A contributor name as grouped in the validated export.</td>'],
        ['<td>Name</td>', '<td>The donor name shown on the major-donor table.</td>'],
        ['<td>State</td>', '<td>The state-level Senate contest bucket or state navigation row.</td>'],
        ['<td>District</td>', '<td>A U.S. House district row.</td>'],
        ['<td>Jurisdiction</td>', '<td>A non-state jurisdiction such as DC or a territory.</td>'],
        ['<td>Candidate</td>', '<td>An individual candidate in the FEC candidate master and linkage data.</td>'],
        ['<td>D Candidate</td>', '<td>The Democratic candidate with the most tech receipts in that Senate or House summary row.</td>'],
        ['<td>R Candidate</td>', '<td>The Republican candidate with the most tech receipts in that Senate or House summary row.</td>'],
        ['<td>Party</td>', '<td>The candidate or committee party label shown in the export.</td>'],
        ['<td>Lean</td>', '<td>A simplified D, R, Mixed, or Unknown label based on where classified tech money flowed, not a race rating.</td>'],
        ['<td>Type</td>', '<td>The committee type from FEC filings, such as candidate committee, PAC, or party committee.</td>'],
        ['<td>Bucket</td>', '<td>A broad committee category used on this site, such as candidate, party, leadership PAC, or outside group.</td>'],
        ['<td>Sector</td>', '<td>The company-sector label from the tracked-company lookup.</td>'],
        ['<td>Total</td>', '<td>The total dollars for that row\'s main entity in that specific table. On company, donor, and committee tables this is the full amount shown for that entity within the export.</td>'],
        ['<td>Total ($)</td>', '<td>The donor\'s full validated total in dollars.</td>'],
        ['<td>Total Receipts</td>', '<td>All itemized individual contributions flowing into linked candidate committees, not just the tech-matched subset and not outside spending.</td>'],
        ['<td>Tech Receipts</td>', '<td>The portion of candidate or committee receipts matched to tracked tech employers.</td>'],
        ['<td>Tech-Linked Giving</td>', '<td>The site-wide sum of validated tech-matched contribution dollars in the current build.</td>'],
        ['<td>Tech Share</td>', '<td>The share of itemized individual receipts coming from tracked tech donors.</td>'],
        ['<td>Tech Donors</td>', '<td>The count of distinct donors in the tech-matched subset for that row.</td>'],
        ['<td>Donors</td>', '<td>The count of distinct donors represented by that row.</td>'],
        ['<td>Committees</td>', '<td>The number of committees represented or linked in that table row.</td>'],
        ['<td>Linked Committees</td>', '<td>The count of committees linked to a candidate through FEC candidate-committee linkage files.</td>'],
        ['<td>Committee Names</td>', '<td>The names of committees linked to that candidate.</td>'],
        ['<td>Top Committee</td>', '<td>The committee receiving the largest share of money from that donor or company row.</td>'],
        ['<td>Top Committee Funded</td>', '<td>The committee that received the most money from that donor in the public export.</td>'],
        ['<td>Company Tags</td>', '<td>The tracked tech-company labels attached to a donor or committee through matched contributions.</td>'],
        ['<td>Pct Classified</td>', '<td>The share of dollars that could be assigned a party direction under the site\'s classification rules.</td>'],
        ['<td>Pct Dem by Donor</td>', '<td>The share of classified donors, not dollars, whose giving leaned Democratic in that company summary.</td>'],
        ['<td>$ to Dem</td>', '<td>Dollars from that donor row that the pipeline classified as Democratic-leaning.</td>'],
        ['<td>% to Dem</td>', '<td>The Democratic share of that donor\'s classified giving.</td>'],
        ['<td>$ to Rep</td>', '<td>Dollars from that donor row that the pipeline classified as Republican-leaning.</td>'],
        ['<td>% to Rep</td>', '<td>The Republican share of that donor\'s classified giving.</td>'],
        ['<td>IE Support</td>', '<td>Independent expenditures by outside committees reported as supporting the candidate. These are not direct receipts to the candidate committee.</td>'],
        ['<td>IE Oppose</td>', '<td>Independent expenditures by outside committees reported as opposing the candidate. These are not direct receipts to the candidate committee.</td>'],
        ['<td>Rows</td>', '<td>The number of validated contribution rows rolled into that summary entry.</td>'],
        ['<td>House Districts</td>', '<td>The number of House districts represented in that state or jurisdiction row.</td>'],
        ['<td>Senate Candidates</td>', '<td>The number of Senate candidates represented in that state or jurisdiction row.</td>'],
        ['<td>File</td>', '<td>A downloadable frontend-ready export file.</td>'],
        ['<td>JSON</td>', '<td>A JSON payload produced for the site.</td>'],
        ['<td>Cycle</td>', '<td>The election cycle for that site build.</td>'],
        ['<td>Data As Of</td>', '<td>The latest transaction date present in the current validated build.</td>'],
        ['<td>Tracked Companies</td>', '<td>The number of companies or firms currently included in the employer-tag lookup.</td>'],
    ]
    body = f"""
<h1>Methodology</h1>

<p>This site is built from the validated tech-money pipeline in this repository.</p>

<h2>What is counted</h2>
<ul>
  <li>Validated individual-contribution transaction types from <code>itcont</code>.</li>
  <li>Refund types are subtracted from totals.</li>
  <li>Employer strings are matched against a manually tagged tech-employer lookup.</li>
  <li>Committee and donor lean is inferred from committee party filings plus candidate-facing spending patterns.</li>
</ul>

<h2>What is not yet fully captured</h2>
<ul>
  <li>People whose filings do not identify them through a tracked tech employer string.</li>
  <li>Per-candidate detail pages beyond the current national, state, Senate, and House district navigation.</li>
  <li>Outbound committee spending as a core public view; the current committee-spending export is still too broad.</li>
</ul>

<h2>Current limits</h2>
<ul>
  <li>This public build currently includes validated 2024 and 2026 cycle views.</li>
  <li>Committee tech share is a share of itemized individual contribution receipts, not all money received by a committee.</li>
  <li>Weekly charts shown on the site begin at {display_start} for display purposes, while aggregate totals keep the full validated export.</li>
</ul>

<h2>Column glossary</h2>
<p>The table below gives a plain-language meaning for every column label currently used on the public site.</p>
{table(["Column", "Plain English"], column_rows)}

<h2>Why the site looks like this</h2>
<p>The design goal is plain HTML, minimal CSS, and JavaScript only where charts require it.</p>
"""
    top_note = note(
        metadata,
        f"This page uses the validated {metadata['cycle']} export included in this build.",
    )
    return shell("Methodology - Tech Money", body, prefix="../", top_note=top_note)


def page_data(metadata: dict) -> str:
    display_start = display_start_for_cycle(int(metadata["cycle"]))
    files = [
        "site_metadata.json",
        "source_manifest.json",
        "homepage_summary.json",
        "companies.json",
        "committees.json",
        "major_donors.json",
        "charts/home_weekly_totals.json",
        "weekly_totals.csv",
        "weekly_by_company.csv",
        "weekly_by_recipient_bucket.csv",
        "weekly_by_recipient_party.csv",
        "entity_party_lean.csv",
        "entity_party_lean_companies.csv",
        "entity_party_lean_committees.csv",
        "entity_party_lean_donors.csv",
        "candidate_race_summary.csv",
        "candidate_state_summary.csv",
        "candidate_house_district_summary.csv",
        "candidate_senate_summary.csv",
    ]
    rows = []
    for name in files:
        rows.append([f'<td><a href="{html.escape(name)}">{html.escape(name)}</a></td>'])

    body = f"""
<h1>Data</h1>
<p>These are the current frontend-ready exports for the validated {metadata["cycle"]} build.</p>
{table(["File"], rows)}
<p><a href="companies/">Company detail JSON files</a></p>
<p><a href="charts/companies/">Company chart JSON files</a></p>
"""
    top_note = note(
        metadata,
        f"These files are frontend-ready exports from the validated {metadata['cycle']} build.",
        "source_manifest.json compares local bulk files against the current official FEC bulk-release timestamps.",
        f"Chart payloads use the same data but default to a visible timeline beginning at {display_start}.",
    )
    return shell("Data - Tech Money", body, prefix="../", top_note=top_note)


def page_company_data_index(metadata: dict, companies: list[dict]) -> str:
    rows = []
    for row in companies:
        rows.append(
            [
                f'<td>{esc(display_company_name(row["tech_canonical_name"]))}</td>',
                f'<td><a href="{esc(row["slug"])}.json">{esc(row["slug"])}.json</a></td>',
            ]
        )

    body = f"""
<h1>Company Detail JSON</h1>
<p>One JSON file per tracked company.</p>
{table(["Company", "JSON"], rows)}
"""
    top_note = note(
        metadata,
        f"One JSON detail file per tracked company in the validated {metadata['cycle']} export.",
    )
    return shell("Company Detail JSON - Tech Money", body, prefix="../../", top_note=top_note)


def page_company_chart_data_index(metadata: dict, companies: list[dict]) -> str:
    display_start = display_start_for_cycle(int(metadata["cycle"]))
    rows = []
    for row in companies:
        rows.append(
            [
                f'<td>{esc(display_company_name(row["tech_canonical_name"]))}</td>',
                f'<td><a href="{esc(row["slug"])}.json">{esc(row["slug"])}.json</a></td>',
            ]
        )

    body = f"""
<h1>Company Chart JSON</h1>
<p>One weekly-series JSON file per tracked company.</p>
{table(["Company", "JSON"], rows)}
"""
    top_note = note(
        metadata,
        f"One chart JSON file per tracked company in the validated {metadata['cycle']} export.",
        f"These weekly series keep full validated rows while the site display defaults to {display_start}.",
    )
    return shell("Company Chart JSON - Tech Money", body, prefix="../../../", top_note=top_note)


def load_cycle_bundle(cycle: int) -> dict:
    data_root = EXPORT_ROOT / str(cycle)
    return {
        "cycle": cycle,
        "data_root": data_root,
        "metadata": read_json(data_root / "site_metadata.json"),
        "homepage": read_json(data_root / "homepage_summary.json"),
        "companies": read_json(data_root / "companies.json"),
        "committees": read_json(data_root / "committees.json"),
        "donors": read_json(data_root / "major_donors.json"),
        "candidate_race": read_csv(data_root / "candidate_race_summary.csv"),
        "candidate_state": read_csv(data_root / "candidate_state_summary.csv"),
        "candidate_house_district": read_csv(data_root / "candidate_house_district_summary.csv"),
        "candidate_senate": read_csv(data_root / "candidate_senate_summary.csv"),
    }


def collect_cycle_page_dirs(bundle: dict) -> set[str]:
    page_dirs = {
        "",
        "companies/",
        "committees/",
        "candidates/",
        "candidates/president/",
        "political-bodies/",
        "donors/",
        "methodology/",
        "data/",
        "data/companies/",
        "data/charts/companies/",
    }
    for row in bundle["companies"]:
        page_dirs.add(normalize_rel_dir(f"companies/{row['slug']}"))
    for row in bundle["candidate_state"]:
        page_dirs.add(normalize_rel_dir(f"candidates/states/{state_slug(row['state_code'])}"))
    for row in bundle["candidate_senate"]:
        page_dirs.add(
            normalize_rel_dir(f"candidates/states/{state_slug(row['state_code'])}/senate")
        )
    for row in bundle["candidate_house_district"]:
        page_dirs.add(
            normalize_rel_dir(
                "candidates/states/"
                f"{state_slug(row['state_code'])}/house/{district_slug(row['district_code'])}"
            )
        )
    return page_dirs


def page_site_index(cycle_bundles: dict[int, dict]) -> str:
    if not cycle_bundles:
        raise ValueError("No cycle bundles available to build the site index.")

    default_cycle = max(cycle_bundles)
    rows = []
    for cycle in sorted(cycle_bundles, reverse=True):
        metadata = cycle_bundles[cycle]["metadata"]
        rows.append(
            [
                f'<td><a href="{cycle}/">{cycle}</a></td>',
                f'<td>{html.escape(metadata.get("data_as_of") or "")}</td>',
                f'<td class="number">{money(metadata.get("total_tech_linked_giving"))}</td>',
                f'<td class="number">{num(metadata.get("tech_donor_count"))}</td>',
                f'<td class="number">{num(metadata.get("tracked_company_count"))}</td>',
            ]
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Tech Money</title>
  <link rel="stylesheet" href="{default_cycle}/static/site.css">
</head>
<body>
  <div class="page">
    <div class="site-header">
      <div class="site-title">Tech Money</div>
      <p class="site-tagline">Choose a cycle-specific static build.</p>
      <hr class="rule">
    </div>
    <p>The site now builds separate static trees for each included cycle so the pages can switch cleanly between 2024 and 2026.</p>
    {table(["Cycle", "Data As Of", "Tech-Linked Giving", "Tech Donors", "Tracked Companies"], rows)}
  </div>
</body>
</html>
"""


def rel_dir_for_page(rel_path: str) -> str:
    parent = Path(rel_path).parent.as_posix()
    return normalize_rel_dir(parent)


def build_site() -> None:
    global AVAILABLE_CYCLES, CYCLE_PAGE_DIRS, CURRENT_RENDER_CYCLE, CURRENT_RENDER_REL_DIR

    available_cycles = sorted(
        [
            int(path.name)
            for path in EXPORT_ROOT.iterdir()
            if path.is_dir() and path.name.isdigit() and (path / "site_metadata.json").exists()
        ],
        reverse=True,
    )
    cycle_bundles = {cycle: load_cycle_bundle(cycle) for cycle in available_cycles}

    AVAILABLE_CYCLES = available_cycles
    CYCLE_PAGE_DIRS = {
        cycle: collect_cycle_page_dirs(bundle)
        for cycle, bundle in cycle_bundles.items()
    }

    reset_dir(SITE_ROOT)
    write(SITE_ROOT / "index.html", page_site_index(cycle_bundles))

    for cycle in available_cycles:
        bundle = cycle_bundles[cycle]
        metadata = bundle["metadata"]
        homepage = bundle["homepage"]
        companies = bundle["companies"]
        committees = bundle["committees"]
        donors = bundle["donors"]
        candidate_race = bundle["candidate_race"]
        candidate_state = bundle["candidate_state"]
        candidate_house_district = bundle["candidate_house_district"]
        candidate_senate = bundle["candidate_senate"]
        data_root = bundle["data_root"]
        cycle_root = SITE_ROOT / str(cycle)

        cycle_root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(ASSET_ROOT, cycle_root / "static")
        shutil.copytree(data_root, cycle_root / "data")

        def render(rel_path: str, content_fn) -> None:
            global CURRENT_RENDER_CYCLE, CURRENT_RENDER_REL_DIR
            CURRENT_RENDER_CYCLE = cycle
            CURRENT_RENDER_REL_DIR = rel_dir_for_page(rel_path)
            write(cycle_root / rel_path, content_fn())

        render("index.html", lambda: page_home(metadata, homepage))
        render("companies/index.html", lambda: page_companies_index(metadata, companies))
        render("committees/index.html", lambda: page_committees(metadata, committees))
        render(
            "candidates/index.html",
            lambda: page_candidates(
                metadata,
                candidate_race,
                candidate_state,
                candidate_house_district,
                candidate_senate,
            ),
        )
        render(
            "political-bodies/index.html",
            lambda: page_political_bodies(metadata, committees),
        )
        render("donors/index.html", lambda: page_donors(metadata, donors))
        render("methodology/index.html", lambda: page_methodology(metadata))
        render("data/index.html", lambda: page_data(metadata))
        render(
            "data/companies/index.html",
            lambda: page_company_data_index(metadata, companies),
        )
        render(
            "data/charts/companies/index.html",
            lambda: page_company_chart_data_index(metadata, companies),
        )

        for row in companies:
            slug = row["slug"]
            payload = read_json(data_root / "companies" / f"{slug}.json")
            render(
                f"companies/{slug}/index.html",
                lambda payload=payload: page_company(metadata, payload),
            )

        presidential_candidates = candidate_rows_for_office(candidate_race, "P")
        render(
            "candidates/president/index.html",
            lambda: page_president(metadata, presidential_candidates),
        )

        state_rows = {row["state_code"]: row for row in candidate_state}
        for state_code, state_row in state_rows.items():
            state_candidates = [
                row for row in candidate_race
                if (row.get("cand_office_st") or "").upper() == state_code
                and row.get("cand_office") in {"H", "S"}
            ]
            senate_candidates = [
                row for row in state_candidates if row.get("cand_office") == "S"
            ]
            house_district_rows = [
                row for row in candidate_house_district
                if (row.get("state_code") or "").upper() == state_code
            ]
            senate_summary_row = next(
                (
                    row
                    for row in candidate_senate
                    if (row.get("state_code") or "").upper() == state_code
                ),
                None,
            )

            state_rel = f"candidates/states/{state_slug(state_code)}/index.html"
            render(
                state_rel,
                lambda state_row=state_row, state_candidates=state_candidates,
                house_district_rows=house_district_rows,
                senate_summary_row=senate_summary_row: page_candidate_state(
                    metadata,
                    state_row,
                    state_candidates,
                    house_district_rows,
                    senate_summary_row,
                ),
            )
            if senate_candidates:
                render(
                    f"candidates/states/{state_slug(state_code)}/senate/index.html",
                    lambda state_row=state_row, senate_candidates=senate_candidates:
                    page_candidate_state_senate(metadata, state_row, senate_candidates),
                )

            for district_row in house_district_rows:
                district_candidates = [
                    row for row in state_candidates
                    if row.get("cand_office") == "H"
                    and normalize_candidate_district(row.get("cand_office_district"))
                    == district_row["district_code"]
                ]
                render(
                    "candidates/states/"
                    f"{state_slug(state_code)}/house/{district_slug(district_row['district_code'])}/index.html",
                    lambda district_row=district_row, district_candidates=district_candidates:
                    page_candidate_house_district(metadata, district_row, district_candidates),
                )

    CURRENT_RENDER_CYCLE = None
    CURRENT_RENDER_REL_DIR = ""
    print(f"Built multi-cycle site to {SITE_ROOT}")


if __name__ == "__main__":
    build_site()
