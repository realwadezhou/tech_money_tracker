from __future__ import annotations

import csv
import hashlib
import html
import json
import shutil
from pathlib import Path

from frontend import attribution, layout
from frontend.lobbying import lobbying_landing
from pipeline.tagging.registry import company_labels


FRONTEND_ROOT = Path(__file__).resolve().parent
ASSET_ROOT = FRONTEND_ROOT / "assets"
SITE_ROOT = FRONTEND_ROOT / "site"
EXPORT_ROOT = FRONTEND_ROOT.parent / "exports" / "site"

AVAILABLE_CYCLES: list[int] = []
CYCLE_PAGE_DIRS: dict[int, set[str]] = {}
CURRENT_RENDER_CYCLE: int | None = None
CURRENT_RENDER_REL_DIR = ""

# Display names come from the shared company list (data/reference/companies/companies.csv).
COMPANY_LABELS = company_labels()

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
            links.append(f'<span class="cycle-pill current" aria-current="true">{cycle}</span>')
            continue
        target_rel_dir = resolve_cycle_target_rel_dir(cycle, CURRENT_RENDER_REL_DIR)
        href = f"{site_prefix}{cycle}/"
        if target_rel_dir:
            href += target_rel_dir
        links.append(f'<a class="cycle-pill" href="{esc(href)}">{cycle}</a>')

    return (
        '<nav class="cycle-toggle" aria-label="Election cycle">'
        '<span class="cycle-label">Election cycle</span>'
        + "".join(links)
        + "</nav>"
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


def account_receipts_value(row: dict, scope: str = "") -> str:
    """Do not present overlapping parent/memo record sums as receipts."""
    if is_truthy_flag(row.get(f"{scope}has_unreconciled_memo_attributions")):
        return "Unreconciled"
    return money(row.get(f"{scope}total_itemized_receipts"))


def account_receipts_total(rows: list[dict]) -> str:
    if any(is_truthy_flag(row.get("has_unreconciled_memo_attributions")) for row in rows):
        return "Unreconciled"
    return money(sum(float_value(row.get("total_itemized_receipts")) for row in rows))


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
    if not any("is_display_candidate" in row for row in rows):
        return rows
    return [row for row in rows if is_truthy_flag(row.get("is_display_candidate"))]


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
    cycle_label = CURRENT_RENDER_CYCLE
    if cycle_label is None and AVAILABLE_CYCLES:
        cycle_label = max(AVAILABLE_CYCLES)
    election_label = f"Elections {cycle_label}:" if cycle_label else "Elections:"
    election_links = [
        (f"{prefix}donors/", "Donors"),
        (f"{prefix}candidates/", "Candidates"),
        (f"{prefix}races/", "Races"),
        (f"{prefix}political-bodies/", "Political bodies"),
        (f"{prefix}companies/", "Giving by employer"),
    ]
    after_links = [
        (f"{prefix}federal-lobbying/", "Federal lobbying"),
        (f"{prefix}campaign-finance-101/", "Campaign Finance 101"),
        (f"{prefix}methodology/", "Methodology"),
        (f"{prefix}data/", "Data"),
        (f"{prefix}about/", "About"),
    ]
    first_election_href, first_election_label = election_links[0]
    line_one = [
        f'<a href="{esc(prefix)}index.html">Home</a>',
        (
            f'<strong>{esc(election_label)}</strong> '
            f'<a href="{esc(first_election_href)}">{esc(first_election_label)}</a>'
        ),
    ]
    line_one.extend(
        f'<a href="{esc(href)}">{esc(label)}</a>'
        for href, label in election_links[1:]
    )
    line_two = [
        f'<a href="{esc(href)}">{esc(label)}</a>'
        for href, label in after_links
    ]
    return " | ".join(line_one) + "<br>" + " | ".join(line_two)


def note(metadata: dict, *lines: str) -> str:
    items = [f"<strong>Latest tech-matched transaction: {esc(metadata.get('data_as_of') or 'unavailable')}.</strong>"]
    release = metadata.get("latest_local_bulk_release_utc")
    if release:
        items.append(f"Latest installed FEC bulk release: {esc(release)}.")
    status = metadata.get("source_check_status")
    if status in {"stale", "unknown", "missing"}:
        items.append({
            "stale": "Newer FEC source files are available; this build is behind the latest release.",
            "unknown": "Freshness against the official FEC releases could not be verified.",
            "missing": "Some required local FEC source files are missing; source coverage is incomplete.",
        }[status])
    items.append("Transaction dates are not filing-completeness dates; late filings and amendments can change earlier totals.")
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
    current_section = CURRENT_RENDER_REL_DIR.strip("/").split("/")[0]
    return layout.render_shell(
        title, body,
        stylesheet_url=static_asset_url("site.css", prefix),
        tables_script_url=static_asset_url("tables.js", prefix),
        charts_script_url=static_asset_url("charts.js", prefix) if include_charts else None,
        navigation_prefix=prefix,
        home_href=f"{prefix}index.html",
        current_section=current_section,
        cycle_label=str(CURRENT_RENDER_CYCLE) if CURRENT_RENDER_CYCLE else "",
        cycle_controls=cycle_toggle_html(prefix),
        source_note=top_note,
        scripts=scripts,
        **lobbying_context_links(prefix, current_section),
    )


def lobbying_context_links(prefix: str, current_section: str) -> dict:
    """A cycle's lobbying entry page points at the spending page's method and downloads."""
    if current_section != "federal-lobbying" or not lobbying_landing().endswith("spending/"):
        return {}
    landing = f"{prefix}../{lobbying_landing()}"
    return {"definition_href": landing + "#methodology", "data_href": landing + "#downloads"}


def headline_stats(items: list[tuple[str, str, str]]) -> str:
    return '<dl class="headline-stats">' + ''.join(
        f'<div class="headline-stat"><dt>{esc(label)}</dt><dd>{value}</dd>'
        f'<dd class="stat-note">{esc(context)}</dd></div>'
        for label, value, context in items
    ) + '</dl>'


def compact_money(value) -> str:
    amount = float_value(value)
    for threshold, suffix in ((1_000_000_000, "b"), (1_000_000, "m")):
        if abs(amount) >= threshold:
            return f'<abbr title="{esc(money(value))}">${amount / threshold:,.1f}{suffix}</abbr>'
    return money(value)


def static_asset_url(name: str, prefix: str = "") -> str:
    """Keep relative asset routes while invalidating caches when bytes change."""
    version = hashlib.sha256((ASSET_ROOT / name).read_bytes()).hexdigest()[:12]
    return f"{prefix}static/{name}?v={version}"


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


def table(headers: list[str], rows: list[list[str]], filterable: bool = True) -> str:
    head = "".join(
        '<th scope="col">'
        f'<button type="button" class="sort-button">{esc(header)}</button>'
        "</th>"
        for header in headers
    )
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(row) + "</tr>")
    filter_control = ""
    if filterable:
        filter_control = (
            '<div class="table-control">'
            '<label class="table-search">'
            '<input class="table-filter" type="search" autocomplete="off" aria-label="Search table">'
            '<span class="search-icon" aria-hidden="true">'
            '<svg viewBox="0 0 16 16" focusable="false">'
            '<path d="M11.2 10.1 15 13.9 13.9 15l-3.8-3.8a6 6 0 1 1 1.1-1.1ZM6.5 11a4.5 4.5 0 1 0 0-9 4.5 4.5 0 0 0 0 9Z"/>'
            '</svg>'
            "</span>"
            "</label>"
            "</div>"
        )
    return (
        filter_control
        + '<div class="table-wrap">'
        '<table class="data-table" border="1" cellpadding="4" cellspacing="0">'
        + "<thead><tr>" + head + "</tr></thead>"
        + "<tbody>"
        + "".join(body_rows)
        + "</tbody></table></div>"
    )


def candidate_money_note() -> str:
    return (
        '<p class="small">On candidate pages, <strong>Account Receipts</strong> means net '
        "itemized contributions in the FEC individual-contributions file to principal and authorized campaign accounts plus "
        "FEC-confirmed former campaign accounts linked to that cycle. Former-account totals "
        "can include post-conversion PAC activity and must not be described as donations received by the candidate's campaign. "
        '<strong>Tech Account Receipts</strong> combines the employer-matched subset across all tracked companies. '
        'It does not capture all donor-attributed joint-fundraising allocations. '
        '<strong>IE Support</strong> and <strong>IE Oppose</strong> are outside '
        "independent expenditures for or against the candidate, not direct committee receipts. "
        "Linked committee counts and names also include affiliates whose receipts are excluded. "
        "Historical account attribution does not set a committee's party label.</p>"
        '<p class="small">Shared campaign accounts are assigned using the committee master\'s '
        'candidate association. Receipts are not split by the date an account changed candidates. '
        'A zero assigned to a former candidate does not establish zero historical giving to that campaign.</p>'
        '<p class="small"><strong>Unreconciled</strong> replaces the all-record receipt total when '
        'included nonzero memo attributions may overlap a parent receipt, such as partnership and '
        'partner records. Tech Share is also omitted for these groups. Downloads preserve the '
        'selected-record sum and memo diagnostics for review; adding parent and attribution rows '
        'does not establish unique dollars received. Tech-account amounts remain sums of '
        'employer-matched records, subject to the same attribution limitations.</p>'
        '<p class="small">The candidate roster includes master-file election years matching the selected '
        'even-year cycle. It omits odd-year special-election candidates and candidates fundraising for '
        'other election years. Receipts use the cycle source file, including any earlier-dated records, '
        'and are not separated by primary, general, special, or runoff election designation. '
        'State and district groups can combine multiple contests. Top D/R names identify the largest '
        'tech-account totals, not nominees or election winners.</p>'
    )


def candidate_account_breakdown(rows: list[dict]) -> str:
    affected = [row for row in rows if int_value(row.get("former_campaign_account_committee_count")) > 0]
    if not affected:
        return ""
    rendered = []
    for row in affected:
        rendered.append([
            f'<td>{esc(row.get("cand_name"))}</td>',
            f'<td class="number">{money(row.get("current_campaign_account_tech_itemized_receipts"))}</td>',
            f'<td class="number">{money(row.get("former_campaign_account_tech_itemized_receipts"))}</td>',
            f'<td class="number">{account_receipts_value(row, "former_campaign_account_")}</td>',
            f'<td>{esc(row.get("former_campaign_account_committee_ids"))}</td>',
        ])
    return (
        '<h2>Former Campaign Accounts</h2>'
        '<p>These candidates have accounts the FEC confirms converted to PACs. The figures below '
        'separate currently classified campaign accounts from former accounts; they do not split '
        'receipts before and after conversion. Former-account amounts are already included above.</p>'
        + table(["Candidate", "Current-Account Tech", "Former-Account Tech", "Former-Account Total", "Former Account IDs"], rendered)
    )


def donor_money_note() -> str:
    return (
        '<p class="small">Party dollar totals use all included contributions under each donor name, '
        'including records with unmatched employers. Party percentages divide by Democratic- plus '
        'Republican-leaning committee dollars only; Mixed and Unknown committees are excluded. '
        'Total includes only tech-matched records, so it can differ from the party dollar totals. '
        'Exact-name grouping can combine different people with the same name and split one person '
        'across name variants.</p>'
    )


def company_party_note() -> str:
    return (
        '<p class="small">Pct Classified measures the share of company-linked dollars going to '
        'Democratic- or Republican-leaning recipient committees. Pct Dem by Donor instead measures '
        'dollars from Democratic-leaning donors divided by dollars from Democratic- plus '
        'Republican-leaning donors; it is not a share of people or all giving. Donor lean uses all '
        'included contributions under the reported name, including unmatched employers. Mixed and '
        'Unknown groups are excluded from each Democratic percentage. Blank percentages indicate '
        'that there is no meaningful split, including some refund-heavy cases.</p>'
    )


def matched_record_scope_note(prefix: str = "") -> str:
    return (
        '<p class="small">These totals sum selected employer-matched FEC records, including adjustments; '
        'they are not fully reconciled totals of unique gifts. Refund matching and routed or attributed '
        f'gifts remain limitations; see <a href="{esc(prefix)}methodology/">Methodology</a>.</p>'
    )


def undated_contributions_note(metadata: dict, scope: str = "Across this cycle") -> str:
    count = int_value(metadata.get("undated_tech_contribution_count"))
    if not count:
        return ""
    amount = money(metadata.get("undated_tech_net_total"))
    return (
        f'<p class="note">{esc(scope)}, '
        f'{num(count)} tech-linked contribution rows ({amount} net) have no valid transaction date. '
        'They are included in contribution totals but excluded from weekly charts.</p>'
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


def page_home(metadata: dict, homepage: dict, attribution_reports: list[dict] | None = None) -> str:
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
            '<td><a href="donors/">Donors</a></td>',
            '<td>Contributors whose listed employer matches a tracked tech company.</td>',
        ],
        [
            '<td><a href="candidates/">Candidates</a></td>',
            '<td>Federal candidates who received tech-linked money, with state and district navigation.</td>',
        ],
        [
            '<td><a href="races/">Races</a></td>',
            '<td>Race-first views aren\'t split out yet; the candidate pages carry that context for now.</td>',
        ],
        [
            '<td><a href="political-bodies/">Political bodies</a></td>',
            '<td>PACs, Super PACs, party committees, and other non-candidate committees by tech-linked money received.</td>',
        ],
        [
            '<td><a href="companies/">Giving by employer</a></td>',
            '<td>One page per tracked employer, showing contributions carrying that company\'s employer tag.</td>',
        ],
        [
            '<td><a href="federal-lobbying/">Federal lobbying</a></td>',
            '<td>AI issue explorer with original filings, company watchlists, and historical snapshot coverage.</td>',
        ],
        [
            '<td><a href="campaign-finance-101/">Campaign Finance 101</a></td>',
            '<td>A plain-English intro to how U.S. federal campaign finance works.</td>',
        ],
        [
            '<td><a href="methodology/">Methodology</a></td>',
            '<td>What\'s counted, what isn\'t, and what each column label means.</td>',
        ],
        [
            '<td><a href="data/">Data</a></td>',
            '<td>Downloadable CSV and JSON exports of everything on this site.</td>',
        ],
        [
            '<td><a href="about/">About</a></td>',
            '<td>What this site is and why it exists.</td>',
        ],
    ]

    body = f"""
<div class="page-intro">
<div class="eyebrow">Federal campaign finance / {metadata['cycle']}</div>
<h1>Technology and political money</h1>
<p class="lede">Explore the people, employers, and political organizations connected by public campaign-finance records. Follow each figure back to its context.</p>
<div class="page-actions"><a class="button" href="companies/">Explore employers →</a><a class="button" href="federal-lobbying/">Explore federal lobbying</a></div>
</div>
{headline_stats([
    ("Employer-matched giving", compact_money(metadata["total_tech_linked_giving"]), "Net contributions"),
    ("Donor groups", num(metadata["tech_donor_count"]), "Reported-name groups"),
    ("Tracked employers", num(metadata["tracked_company_count"]), "Curated employer matches"),
    ("Recipient committees", num(metadata["committees_receiving_tech_money"]), "Across the selected cycle"),
])}
{matched_record_scope_note()}
{attribution.report_links(attribution_reports)}

<h2>Weekly Tech-Linked Giving</h2>
<div class="chart-wrap"><div id="weekly-chart"></div></div>
<p class="note">Chart display is limited to weeks ending on or after {display_start}. The cumulative line and totals above include any earlier rows present in the FEC source files.</p>
{undated_contributions_note(metadata)}

<h2>Start Here</h2>
{table(["Section", "What It Shows"], section_rows, filterable=False)}

<h2>At A Glance: Giving by Employer</h2>
{table(["Company", "Total", "Donors", "Committees", "Pct Classified", "Pct Dem by Donor"], top_company_rows, filterable=False)}
{company_party_note()}
<p><a href="companies/">See all tracked employers.</a></p>

<h2>At A Glance: Candidates</h2>
{table(["Candidate Committee", "Type", "Lean", "Tech Receipts", "Tech Share", "Tech Donors"], top_candidate_rows, filterable=False)}
<p class="small">Committee tech share uses net itemized contributions in the FEC individual-contributions file, not all committee money. It is omitted when refunds or overlapping memo attributions prevent a meaningful denominator.</p>
<p><a href="candidates/">See featured candidate committees.</a></p>

<h2>At A Glance: Political Bodies</h2>
{table(["Committee", "Type", "Lean", "Tech Receipts", "Tech Share", "Tech Donors"], top_political_rows, filterable=False)}
<p class="small">This page includes PACs, Super PACs, party committees, and other non-candidate committees.</p>
<p><a href="political-bodies/">See featured political bodies.</a></p>

<h2>At A Glance: Top Donors</h2>
{table(["Donor", "$ to Dem", "% to Dem", "$ to Rep", "Total", "Company Tags", "Top Committee"], top_donor_rows, filterable=False)}
{donor_money_note()}
<p><a href="donors/">See all major donors.</a></p>
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
        f"Figures cover the {metadata['cycle']} cycle. Weekly charts begin at {display_start}; totals above include earlier rows present in the FEC source files.",
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
<h1>Giving by Employer</h1>
<p>One page per tracked tech company, grouping contributions by the employer listed in each record. Employer matching does not verify employment or identity, and the FEC individual-contributions file also contains some nonindividual contributors. These totals are not a measure of company-directed giving or company PAC spending. See <a href="../about/">About</a> for how the matching works.</p>
{table(["Company", "Total", "Donors", "Committees", "Sector", "Pct Classified", "Pct Dem by Donor"], rows)}
{company_party_note()}
"""
    top_note = note(
        metadata,
        f"Figures cover the {metadata['cycle']} cycle. Matching of donors to companies is done by hand against a curated list of tech employers; it is not exhaustive.",
    )
    return shell("Giving by Employer - Tech Money", body, prefix="../", top_note=top_note)


def page_company(metadata: dict, company_payload: dict, attribution_reports: list[dict] | None = None) -> str:
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
<p class="lede">Contributions associated with reported employers mapped to {esc(company_name)}.</p>
{headline_stats([
    ("Employer-matched giving", compact_money(summary["net_total"]), "Net contributions"),
    ("Donor groups", num(summary["n_donors"]), "Reported-name groups"),
    ("Contribution records", num(summary["n_contributions"]), "Selected FEC records"),
    ("Recipient committees", num(summary["n_committees"]), "Political organizations"),
])}
<p class="small"><strong>Sector:</strong> {esc(summary.get("sectors", ""))}</p>
{matched_record_scope_note("../../")}
{attribution.report_links(attribution_reports, "../../", company_payload['company'])}

<p><strong>Pct recipient dollars classified:</strong> {pct(summary.get("pct_classified_recipients"))}<br>
<strong>Pct Dem by inferred recipient lean:</strong> {pct(summary.get("pct_dem"))}<br>
<strong>Pct Dem by donor classification:</strong> {pct(summary.get("pct_dem_by_donor"))}</p>
<p class="small">The recipient-based Democratic percentage divides dollars to Democratic-leaning committees by dollars to Democratic- plus Republican-leaning committees.</p>
{company_party_note()}

<h2>Weekly Giving</h2>
<div class="chart-wrap"><div id="company-weekly-chart"></div></div>
<p class="note">Chart display is limited to weeks ending on or after {display_start}. The cumulative line and totals above include any earlier rows present in the FEC source files.</p>
{undated_contributions_note(company_payload, "For this company")}

<h2>Top Recipient Committees</h2>
<p class="small">These tables and their JSON downloads show {num(len(company_payload["top_committees"]))} of {num(summary["n_committees"])} recipient committees and {num(len(company_payload["top_donors"]))} of {num(summary["n_donors"])} reported donor names. Missing rows do not imply zero giving.</p>
{table(["Committee", "Type", "Bucket", "Lean", "Total", "Donors"], top_committee_rows)}

<h2>Top Donors</h2>
{table(["Donor", "Total", "Rows", "Top Committee"], top_donor_rows)}

<p class="small"><a href="../">Back to giving by employer.</a></p>
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
        f"Figures cover the {metadata['cycle']} cycle. Contributions shown here carry this company's employer tag; they are not a measure of company-directed giving or company PAC spending.",
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
    top_note = note(metadata)
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
                f'<td class="number">{account_receipts_value(row)}</td>',
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
                f'<td class="number">{account_receipts_value(row)}</td>',
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
                f'<td class="number">{account_receipts_value(row)}</td>',
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
<p>Federal candidates &mdash; President, Senate, and House &mdash; and the tech-linked money flowing to their campaign and confirmed former campaign accounts. Use the state tile map to jump to a state page, then drill into Senate races and House districts from there.</p>

<p><strong>Candidate rows:</strong> {num(len(candidate_race))}<br>
<strong>States and jurisdictions with House or Senate candidates:</strong> {num(len(candidate_state))}<br>
<strong>House districts represented:</strong> {num(total_house_districts)}<br>
<strong>States with Senate candidates:</strong> {num(states_with_senate)}</p>

<h2>Navigate by State</h2>
<p>Click a tile to open that state page.</p>
{render_candidate_tile_map(map_states)}

<h2>Presidential</h2>
{table(["Candidate", "Party", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose", "Linked Committees"], presidential_rows)}

<h2>Senate Snapshot</h2>
{table(["State", "Top D by Tech", "Top R by Tech", "Lean", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose"], senate_rows)}

<h2>House Snapshot</h2>
{table(["District", "Top D by Tech", "Top R by Tech", "Lean", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose"], house_rows)}
"""
    if jurisdiction_rows:
        body += (
            "\n<h2>Other Jurisdictions</h2>\n"
            + table(["Jurisdiction", "Lean", "House Districts", "Senate Candidates", "Tech Account Receipts"], jurisdiction_rows)
        )

    body += "\n" + candidate_money_note() + "\n"

    top_note = note(
        metadata,
        f"Figures cover the {metadata['cycle']} cycle. \"Tech Account Receipts\" are itemized contributions carrying a tracked tech-employer tag; \"IE Support\" and \"IE Oppose\" are outside spending for or against the candidate, not direct contributions to their campaign (see Campaign Finance 101).",
    )
    return shell("Candidates - Tech Money", body, prefix="../", top_note=top_note)


def page_races(
    metadata: dict,
    candidate_state: list[dict],
    candidate_house_district: list[dict],
    candidate_senate: list[dict],
) -> str:
    rows = [
        [
            '<td><a href="../candidates/">Candidate navigation</a></td>',
            '<td>Current home for state, Senate, House district, and presidential candidate tables.</td>',
        ],
        [
            '<td><a href="../candidates/president/">Presidential</a></td>',
            '<td>Presidential candidates present in the current dataset.</td>',
        ],
        [
            '<td><a href="../candidates/">State and district pages</a></td>',
            '<td>Use the candidate map to open state pages, then Senate and House district pages.</td>',
        ],
    ]
    body = f"""
<h1>Races</h1>
<p>Race-first views aren't split out yet. The candidate pages carry the available race context for now &mdash; see <a href="../candidates/">Candidates</a> for state and district navigation.</p>
<p><strong>States and jurisdictions with candidate rows:</strong> {num(len(candidate_state))}<br>
<strong>Senate race summaries:</strong> {num(len(candidate_senate))}<br>
<strong>House district summaries:</strong> {num(len(candidate_house_district))}</p>
{table(["Current route", "Status"], rows)}
"""
    top_note = note(metadata)
    return shell("Races - Tech Money", body, prefix="../", top_note=top_note)


def page_federal_lobbying(metadata: dict) -> str:
    from frontend.lobbying import EXPORT as LOBBYING_EXPORT, explorer_published
    spending = ('<p>See what tracked tech companies reported spending on federal lobbying, quarter by quarter.</p>'
                '<p><a href="../../lobbying/spending/">Open lobbying spending by company &rarr;</a></p>'
                if (LOBBYING_EXPORT / "spending.json").exists()
                else '<p>The lobbying spending export has not been built in this checkout yet.</p>')
    if (LOBBYING_EXPORT / "phrase_topics.json").exists():
        spending += ('<p>See which subjects they name in their reports, such as artificial intelligence and data centers.</p>'
                     '<p><a href="../../lobbying/topics/">Open what tech lobbies about &rarr;</a></p>')
    explorer = ('<p>Explore AI references in federal lobbying reports by client, company watchlist, topic, and reporting quarter.</p>'
                '<p><a href="../../lobbying/">Open the AI lobbying explorer &rarr;</a></p>'
                '<p>Matching passages link to original filings. Company names and topic matches have separate review status.</p>'
                if explorer_published() else "")
    body = f"""
<h1>Federal Lobbying</h1>
{spending}
{explorer}
<p>Lobbying uses calendar reporting years, independent of this page's election cycle. Quarters whose reports are not yet due are left out, and later filings can change earlier periods.</p>
<p>Dollar amounts cover whole reports. They are not allocated to AI or any other issue.</p>
"""
    return shell("Federal Lobbying - Tech Money", body, prefix="../")


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
                f'<td class="number">{account_receipts_value(row)}</td>',
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
                f'<td class="number">{account_receipts_value(row)}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
            ]
        )

    body = f"""
<h1>{esc(state_title)}</h1>
<p><strong>State lean by candidate tech-account receipts:</strong> {esc(state_row.get("party_label", ""))}<br>
<strong>Total candidate account receipts:</strong> {account_receipts_value(state_row)}<br>
<strong>Total candidate tech-account receipts:</strong> {money(state_row.get("tech_itemized_receipts"))}<br>
<strong>House districts:</strong> {num(state_row.get("house_district_count"))}<br>
<strong>Senate candidates:</strong> {num(state_row.get("senate_candidate_count"))}</p>
"""
    if state_senate_summary is not None:
        body += (
            f'<p><a href="senate/">Open the {esc(state_title)} Senate page.</a></p>'
        )
    if senate_rows:
        body += "\n<h2>Senate Candidates</h2>\n" + table(
            ["Candidate", "Party", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose", "Linked Committees"],
            senate_rows,
        )

    body += "\n<h2>House Districts</h2>\n" + table(
        ["District", "Top D by Tech", "Top R by Tech", "Lean", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose"],
        district_rows,
    )
    body += "\n" + candidate_money_note()
    body += '\n<p class="small"><a href="../../">Back to candidates.</a></p>'

    top_note = note(
        metadata,
        f"Figures cover candidate money in {state_title} for the {metadata['cycle']} cycle. District links below open full district-level candidate tables.",
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
    senate_tech_receipts = sum(
        float_value(row.get("tech_itemized_receipts")) for row in senate_candidates
    )
    for row in senate_candidates:
        rows.append(
            [
                f'<td>{esc(row["cand_name"])}</td>',
                f'<td>{esc(row.get("party_dr", ""))}</td>',
                f'<td class="number">{account_receipts_value(row)}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
                f'<td>{esc(truncate(row.get("linked_committee_names", ""), 48))}</td>',
            ]
        )

    body = f"""
<h1>{esc(state_title)} Senate</h1>
<p><strong>Total Senate candidate account receipts:</strong> {account_receipts_total(senate_candidates)}<br>
<strong>Total Senate candidate tech-account receipts:</strong> {money(senate_tech_receipts)}<br>
<strong>Senate candidates:</strong> {num(len(senate_candidates))}</p>
{table(["Candidate", "Party", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose", "Linked Committees", "Committee Names"], rows)}
{candidate_account_breakdown(senate_candidates)}
{candidate_money_note()}
<p class="small"><a href="../">Back to the state page.</a></p>
"""
    top_note = note(
        metadata,
        f"Senate candidates in {state_title} for the {metadata['cycle']} cycle.",
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
                f'<td class="number">{account_receipts_value(row)}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
                f'<td>{esc(truncate(row.get("linked_committee_names", ""), 48))}</td>',
            ]
        )

    body = f"""
<h1>{esc(district_title)}</h1>
<p><strong>District lean by candidate tech-account receipts:</strong> {esc(district_row.get("party_label", ""))}<br>
<strong>Total district candidate account receipts:</strong> {account_receipts_value(district_row)}<br>
<strong>Total district tech-account receipts:</strong> {money(district_row.get("tech_itemized_receipts"))}<br>
<strong>Candidates:</strong> {num(len(district_candidates))}</p>
{table(["Candidate", "Party", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose", "Linked Committees", "Committee Names"], rows)}
{candidate_account_breakdown(district_candidates)}
{candidate_money_note()}
<p class="small"><a href="../../">Back to the state page.</a></p>
"""
    top_note = note(
        metadata,
        f"House candidates for {district_title} in the {metadata['cycle']} cycle. State page: {state_name(state_code)}.",
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
                f'<td class="number">{account_receipts_value(row)}</td>',
                f'<td class="number">{money(row.get("tech_itemized_receipts"))}</td>',
                f'<td class="number">{money(row.get("ie_support_total"))}</td>',
                f'<td class="number">{money(row.get("ie_oppose_total"))}</td>',
                f'<td class="number">{num(row.get("linked_committee_count"))}</td>',
                f'<td>{esc(truncate(row.get("linked_committee_names", ""), 48))}</td>',
            ]
        )

    body = f"""
<h1>Presidential</h1>
<p>Presidential candidates for the {metadata['cycle']} cycle.</p>
{table(["Candidate", "Party", "Account Receipts", "Tech Account Receipts", "IE Support", "IE Oppose", "Linked Committees", "Committee Names"], rows)}
{candidate_account_breakdown(presidential_candidates)}
{candidate_money_note()}
<p class="small"><a href="../">Back to candidates.</a></p>
"""
    top_note = note(metadata)
    return shell("Presidential - Tech Money", body, prefix="../../", top_note=top_note)


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
<p>PACs, Super PACs, party committees, and other non-candidate committees, ranked by tech-linked money received. These groups take contributions and spend money for or against candidates; they aren't candidates themselves. See <a href="../campaign-finance-101/">Campaign Finance 101</a> for the different committee types.</p>
<p>Included here: featured non-candidate committees that either took in at least {money(100000)} from tracked tech donors or had at least a 10% tech share of net itemized contributions.</p>
{table(["Committee", "Type", "Bucket", "Lean", "Tech Receipts", "Tech Share", "Tech Donors", "Company Tags"], rows)}
<p class="small">Tech Share uses net itemized contributions in the FEC individual-contributions file &mdash; not all committee money. It is omitted when refunds or overlapping memo attributions prevent a meaningful denominator.</p>
"""
    top_note = note(
        metadata,
        f"Figures cover the {metadata['cycle']} cycle. Party lean for PACs and Super PACs is inferred from their candidate-facing spending when the committee itself has no direct party affiliation.",
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
<p>Donors whose listed employer matches a tracked tech company, ranked by net tech-matched giving. Only itemized contribution records can be matched; these may include small gifts. Listed donors gave at least {money(100000)} in tech-matched contributions, net of employer-matched refunds.</p>
{donor_money_note()}
{table(["Name", "$ to Dem", "% to Dem", "$ to Rep", "% to Rep", "Total ($)", "Company Tags", "Top Committee Funded"], rows)}
"""
    top_note = note(
        metadata,
        f"Figures cover the {metadata['cycle']} cycle. Matching donors to tech companies relies on the employer field they wrote on the contribution form, hand-grouped into a clean company list; coverage is not exhaustive.",
    )
    return shell("Donors - Tech Money", body, prefix="../", top_note=top_note)


def page_about(metadata: dict) -> str:
    body = """
<h1>About</h1>

<p>Tech Money is a personal project to make tech-industry political giving easier to read.</p>

<h2>What's on this site</h2>
<p>Election-cycle figures are built from Federal Election Commission filings &mdash; the disclosures that campaigns, PACs, and party committees submit by law. The focus is contributions where the donor's listed employer matches a tracked tech company (Google, Meta, Nvidia, Anthropic, and so on). Company-linked totals reflect those matched contributions; company PAC giving is not separately attributed to the company.</p>
<p>The separate <a href="../federal-lobbying/">federal lobbying pages</a> use Lobbying Disclosure Act reports organized by calendar year. They show what tracked companies reported spending on lobbying in total; dollars are not assigned to AI or any other issue.</p>

<h2>Why it's not trivial</h2>
<p>The FEC publishes raw filings. Employers on those filings are free text &mdash; "Google," "Google LLC," "google inc," "goog," and "alphabet" all come in as different strings. Identifying tech-linked records means maintaining a lookup from employer strings to company names. Employer matching is one limit; receipt selection, refunds, routed and attributed gifts, and changing committee relationships also affect the totals.</p>

<h2>What's missing</h2>
<ul>
  <li>Employer matching is incomplete. Someone who wrote "Self-employed" or an unusual abbreviation of a tech employer won't be counted here.</li>
  <li>The tracked-company list is curated, not exhaustive.</li>
  <li>Contributions reported only as unitemized totals have no donor names or employers attached, so they cannot be matched here.</li>
</ul>

<h2>Related pages</h2>
<ul>
  <li><a href="../campaign-finance-101/">Campaign Finance 101</a> &mdash; a plain-English intro to how U.S. federal campaign finance works.</li>
  <li><a href="../methodology/">Methodology</a> &mdash; what's counted, what isn't, and what each column label means.</li>
</ul>

<h2>Who made this</h2>
<p>Wade Zhou.</p>
"""
    top_note = note(metadata)
    return shell("About - Tech Money", body, prefix="../", top_note=top_note)


def page_campaign_finance_101(metadata: dict) -> str:
    body = """
<h1>Campaign Finance 101</h1>

<p>A short plain-English guide to how U.S. federal political money works &mdash; enough to read the rest of this site.</p>

<h2>Who regulates it</h2>
<p>The <strong>Federal Election Commission (FEC)</strong> runs disclosure for federal elections: President, Senate, and House. Every campaign, PAC, and party committee files periodic reports listing money received and money spent. The FEC publishes these reports as bulk data, and this site is built from that bulk data.</p>

<h2>Itemized vs. unitemized contributions</h2>
<p><em>Itemized</em> contribution records identify the donor and may include employer and occupation. Even small gifts can appear in itemized records. <em>Unitemized</em> contributions are reported as an aggregate total without donor details. This site can only match itemized giving, because unitemized totals have no names or employers attached.</p>

<h2>Kinds of committees</h2>
<ul>
  <li><strong>Candidate committee</strong> &mdash; a candidate's own campaign. Takes capped contributions directly.</li>
  <li><strong>Party committee</strong> &mdash; national, state, or local parties (DNC, RNC, DCCC, NRCC, and so on).</li>
  <li><strong>PAC (political action committee)</strong> &mdash; a committee that pools contributions to give to candidates. Most companies, unions, and trade groups have one. Capped on both sides (what it can take in and what it can give out).</li>
  <li><strong>Super PAC</strong> &mdash; a PAC that can raise and spend <strong>unlimited</strong> amounts, on condition that it does not coordinate with candidates. It cannot give money to a candidate directly; it spends on ads and other outside activity.</li>
  <li><strong>Leadership PAC</strong> &mdash; a side PAC that an elected official uses to give money to other candidates.</li>
</ul>

<h2>Contributions vs. independent expenditures</h2>
<p>A <strong>contribution</strong> is money or something of value given to influence an election, including gifts to candidate, party, and PAC committees. Applicable limits depend on the contributor and recipient; contributions are not limited to candidate campaigns. See the <a href="https://www.fec.gov/help-candidates-and-committees/candidate-taking-receipts/types-contributions/">FEC's contribution definitions</a>. An <strong>independent expenditure</strong> (IE) pays for communications expressly advocating a candidate's election or defeat without coordinating with the campaign; it is reported separately from contributions.</p>
<p>So when this site shows <em>IE Support</em> or <em>IE Oppose</em> for a candidate, that money did not go <em>to</em> the candidate. It was spent <em>for</em> or <em>against</em> them by an outside group.</p>

<h2>Why employer data is messy</h2>
<p>Donors fill in their employer as free text on a contribution form. "Google," "Google LLC," "Google Inc.," "alphabet," "GOOG," and "google inc" are all separate strings in the raw FEC data. Grouping those together takes manual work. That cleaning step is the core data task this site is built on &mdash; see <a href="../methodology/">Methodology</a> for the rules.</p>
"""
    top_note = note(metadata)
    return shell("Campaign Finance 101 - Tech Money", body, prefix="../", top_note=top_note)


def page_methodology(metadata: dict) -> str:
    display_start = display_start_for_cycle(int(metadata["cycle"]))
    column_rows = [
        ['<td>Section</td>', '<td>The site area or page family listed in a navigation table.</td>'],
        ['<td>What It Shows</td>', '<td>A short plain-language description of that page or export.</td>'],
        ['<td>Company</td>', '<td>The tracked tech company or firm tied to a donor-employer tag.</td>'],
        ['<td>Candidate Committee</td>', '<td>A candidate-linked committee receiving contributions.</td>'],
        ['<td>Committee</td>', '<td>A political committee, which may be a candidate committee, PAC, party committee, Super PAC, or another filer.</td>'],
        ['<td>Donor</td>', '<td>A contributor name as grouped in the current dataset.</td>'],
        ['<td>Name</td>', '<td>The donor name shown on the major-donor table.</td>'],
        ['<td>State</td>', '<td>The state-level Senate contest bucket or state navigation row.</td>'],
        ['<td>District</td>', '<td>A U.S. House district row.</td>'],
        ['<td>Jurisdiction</td>', '<td>A non-state jurisdiction such as DC or a territory.</td>'],
        ['<td>Candidate</td>', '<td>An individual candidate from the FEC candidate master records and candidate-committee link files.</td>'],
        ['<td>Party</td>', '<td>The candidate or committee party label shown in the export.</td>'],
        ['<td>Lean</td>', '<td>A simplified D, R, Mixed, or Unknown label based on where classified tech money flowed, not a race rating.</td>'],
        ['<td>Type</td>', '<td>The committee type from FEC filings, such as candidate committee, PAC, or party committee.</td>'],
        ['<td>Bucket</td>', '<td>A broad committee category used on this site: candidate, party, PAC, outside spending, communication cost, or other.</td>'],
        ['<td>Sector</td>', '<td>The company-sector label from the tracked-company lookup.</td>'],
        ['<td>Total</td>', '<td>The total dollars for that row\'s main entity in that specific table. On company, donor, and committee tables, this is the full amount shown for that entity.</td>'],
        ['<td>Total ($)</td>', '<td>The donor name\'s net tech-matched giving in dollars.</td>'],
        ['<td>Account Receipts</td>', '<td>The net selected-record sum in the FEC individual-contributions file for principal and authorized campaign accounts plus FEC-confirmed former campaign accounts. Shown as Unreconciled when nonzero memo attributions could overlap parent receipts. Former accounts can include post-conversion PAC activity; outside spending is excluded.</td>'],
        ['<td>Tech Account Receipts</td>', '<td>The employer-matched subset of Account Receipts, including separately identified former campaign accounts.</td>'],
        ['<td>Current-Account Tech</td>', '<td>Employer-matched receipts to linked accounts currently classified as principal or authorized campaign committees. This is an account classification, not a verified election-designation total.</td>'],
        ['<td>Former-Account Tech / Total</td>', '<td>Employer-matched / all included itemized receipts to confirmed former campaign accounts. No conversion-date split is available; these amounts may include later PAC receipts.</td>'],
        ['<td>Top D by Tech / Top R by Tech</td>', '<td>The Democratic / Republican candidate with the highest tech-account receipts in the geographic group. Not a declaration of nominees or election winners.</td>'],
        ['<td>Tech Receipts</td>', '<td>The portion of committee receipts carrying a tracked tech-employer tag.</td>'],
        ['<td>Tech-Linked Giving</td>', '<td>The site-wide sum of tech-matched contribution dollars in the current dataset.</td>'],
        ['<td>Tech Share</td>', '<td>The share of net itemized contributions in the FEC individual-contributions file carrying tracked tech-employer tags. Omitted when refunds or retained nonzero memo attributions prevent a meaningful denominator. Selected-record dollar sums remain available in downloads for review.</td>'],
        ['<td>Tech Donors</td>', '<td>The count of distinct reported donor-name strings in the tech-matched subset. These are not verified unique people.</td>'],
        ['<td>Donors</td>', '<td>The count of distinct reported donor-name strings represented by that row. Name variants can split a person, and shared names can combine different people.</td>'],
        ['<td>Committees</td>', '<td>The number of committees represented or linked in that table row.</td>'],
        ['<td>Linked Committees</td>', '<td>The number of committees affiliated with a candidate, from the FEC candidate-committee link file. This includes committees whose receipts are excluded from candidate totals.</td>'],
        ['<td>Committee Names</td>', '<td>The names of committees linked to that candidate.</td>'],
        ['<td>Top Committee</td>', '<td>The committee receiving the largest share of money from that donor or company row.</td>'],
        ['<td>Top Committee Funded</td>', '<td>The committee that received the most money from that donor in the public export.</td>'],
        ['<td>Company Tags</td>', '<td>The tracked tech-company labels attached to a donor or committee through matched contributions.</td>'],
        ['<td>Pct Classified</td>', '<td>The share of dollars that could be assigned a party direction under the site\'s classification rules.</td>'],
        ['<td>Pct Dem by Donor</td>', '<td>The share of dollars from Democratic-leaning donors among dollars from Democratic- and Republican-leaning donors in that company summary. This uses donor classifications and is not a share of donor counts.</td>'],
        ['<td>$ to Dem</td>', '<td>Net giving under that donor name to Democratic-leaning committees, including records with unmatched employers.</td>'],
        ['<td>% to Dem</td>', '<td>The Democratic share of that donor\'s classified giving.</td>'],
        ['<td>$ to Rep</td>', '<td>Net giving under that donor name to Republican-leaning committees, including records with unmatched employers.</td>'],
        ['<td>% to Rep</td>', '<td>The Republican share of that donor\'s classified giving.</td>'],
        ['<td>IE Support</td>', '<td>Independent expenditures by outside committees reported as supporting the candidate. These are not direct receipts to the candidate committee.</td>'],
        ['<td>IE Oppose</td>', '<td>Independent expenditures by outside committees reported as opposing the candidate. These are not direct receipts to the candidate committee.</td>'],
        ['<td>Rows</td>', '<td>The number of contribution rows rolled into that summary entry.</td>'],
        ['<td>House Districts</td>', '<td>The number of House districts represented in that state or jurisdiction row.</td>'],
        ['<td>Senate Candidates</td>', '<td>The number of Senate candidates represented in that state or jurisdiction row.</td>'],
        ['<td>File</td>', '<td>A downloadable export file.</td>'],
        ['<td>JSON</td>', '<td>A JSON payload produced for the site.</td>'],
        ['<td>Cycle</td>', '<td>The election cycle this page covers.</td>'],
        ['<td>Latest Matched Transaction</td>', '<td>The latest valid transaction date among tech-matched records in the current dataset. This is not the last download date or a statement that all filings through that date are included.</td>'],
        ['<td>Tracked Companies</td>', '<td>The number of companies and firms with matched contributions in this cycle.</td>'],
    ]
    body = f"""
<h1>Methodology</h1>

<p>How the numbers on this site are built: what's counted, what isn't, and what each column label means. New to campaign finance terms? Start with <a href="../campaign-finance-101/">Campaign Finance 101</a>.</p>

<h2>Where the data comes from</h2>
<p>This site uses Federal Election Commission bulk files: individual contributions, candidate master records, committee master records, candidate-committee links, and committee transactions including independent expenditures. FEC committee history also confirms former campaign accounts that converted to PACs. This site groups and aggregates those records.</p>
<p>The FEC individual-contributions file includes some contributions from partnerships, organizations, and other nonindividual sources. Included transaction types, rather than an individual-only entity filter, define the contribution pool. Employer matching does not verify that a contributor is an employee or that a company directed the contribution.</p>

<h2>How donors are matched to tech companies</h2>
<p>Itemized contribution records contain donor details, including an employer when reported. The employer is free text, so "Google," "Google LLC," "google inc," "goog," and "alphabet" all appear as different strings in the raw data. Matching tech donors means keeping a lookup that maps these messy strings to a clean canonical company name, built and maintained by hand. Contributions get counted toward a company only when the employer string matches an entry in that lookup.</p>

<h2>What is counted</h2>
<ul>
  <li>Included itemized contribution records to federal committees, with refund transaction types subtracted.</li>
  <li>Contributions are attributed to a tech company when the donor's employer string maps to a tracked company in the hand-built lookup.</li>
  <li>Candidate-level account receipts include principal and authorized campaign accounts plus FEC-confirmed converted former campaign accounts linked to that election cycle. Current and former account amounts are exported separately and sum to the combined account total. Historical account totals can include post-conversion PAC activity; they must not be quoted as direct campaign contributions without further review.</li>
  <li>Other joint fundraising, leadership PAC, and unauthorized affiliates are excluded from candidate receipt totals. The committee master and FEC conversion history resolve candidate links; links that remain ambiguous are excluded. Historical account attribution does not determine committee party lean, which is classified separately.</li>
  <li>When an account changes candidates, the current committee-master association assigns its selected cycle receipts to one candidate. The pipeline does not reconstruct ownership on each receipt date. Zero assigned receipts for a former candidate are not evidence of zero historical campaign giving.</li>
  <li>Independent expenditures (IE Support / IE Oppose) come from support and opposition records in the FEC's committee-transaction file and are reported separately from account receipts. These totals cover all included reporting committees, not just committees receiving tech contributions.</li>
  <li>Exported tech-funded IE figures cover spending by committees with any positive net tech receipts. They do not identify which donor's dollars paid for a particular expenditure.</li>
  <li>Party lean for non-party committees (PACs, Super PACs) is inferred from their candidate-facing spending when the committee itself has no direct party affiliation.</li>
</ul>

<h2>What isn't counted</h2>
<ul>
  <li>Contributions reported only as unitemized totals &mdash; these have no donor names or employers to match.</li>
  <li>Donors whose employer string doesn't match a tracked tech company &mdash; including "Self-employed," odd abbreviations, blanks, and typos that haven't been mapped yet.</li>
  <li>Companies not on the curated tracked-company list.</li>
  <li>Company PAC giving is not separately attributed to its connected company in tech-linked contribution totals.</li>
  <li>Outbound spending by committees (ads, operations, transfers) beyond the IE views mentioned above.</li>
</ul>

<h2>Current limits</h2>
{undated_contributions_note(metadata)}
<ul>
  <li>Cycles currently covered: 2024 and 2026.</li>
  <li>A committee's "Tech Share" uses net itemized contributions in the FEC individual-contributions file &mdash; not all money received. It is omitted when refunds or included nonzero memo attributions prevent a meaningful denominator.</li>
  <li>Partnership contributions can include a parent receipt and memo records attributing that receipt to partners. These records can describe the same money. We retain attribution records for employer analysis, but do not display an all-record receipt total or calculate Tech Share for groups with unresolved memo attributions. The downloadable <code>selected_record_net_total</code> preserves the raw selected-record sum; <code>nonmemo_receipt_net_total</code> is a diagnostic, not a fully reconciled substitute. <code>has_unreconciled_memo_attributions</code> and <code>memo_receipt_record_count</code> identify affected groups.</li>
  <li>The exported tech-dominated committee count requires a valid Tech Share strictly above 50%; an exact 50% or an undefined share does not qualify.</li>
  <li>Refunds are matched to companies using their own reported employer field. A refund with a blank or unmatched employer reduces the overall contribution pool but cannot reduce a tech-company total through name matching alone. Tech-linked totals therefore mean net employer-matched records, not a fully reconciled donor ledger.</li>
  <li>Donors are grouped by exact reported name, which can combine different people with the same name and split one person across name variants. Donor party classifications use all included records under that name; company and tech-giving totals use only employer-matched records.</li>
  <li>District and Senate exports count donor/candidate pairs in <code>tech_donor_candidate_pairs</code>. The older <code>tech_itemized_donors</code> column in those aggregate files is a compatibility alias for the same sum, not distinct people across candidates. Candidate-level donor counts deduplicate reported names across that candidate's included accounts.</li>
  <li>Candidate navigation requires the candidate master election year to equal the selected even-year cycle. Odd-year special elections and fundraising by candidates for other election years are omitted. State and district groups are geographic summaries, not verified ballots or separate primary/general/special contests.</li>
  <li>Weekly charts begin their display at {display_start}; totals shown include earlier rows present in the source files.</li>
  <li>Per-candidate detail pages beyond national, state, Senate, and House-district views aren't built yet.</li>
</ul>

<h2>Column glossary</h2>
<p>Plain-language meanings for every column label currently used on the site.</p>
{table(["Column", "Plain English"], column_rows)}

<h2>Why the site looks like this</h2>
<p>The design goal is plain HTML, minimal CSS, a warm document background, and JavaScript only where charts or table sorting and filtering require it.</p>
"""
    top_note = note(metadata)
    return shell("Methodology - Tech Money", body, prefix="../", top_note=top_note)


def page_data(metadata: dict, attribution_reports: list[dict] | None = None) -> str:
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
<p>Downloadable CSV and JSON exports of everything rendered on this site, for the {metadata["cycle"]} cycle.</p>
<p class="small">Legacy receipt-total fields preserve selected-record sums, which can include overlapping parent and memo records. Check the memo-risk flags and account-scope fields before treating a sum as money received. Candidate tech totals combine all tracked companies; company tags alone do not isolate one company's giving. See <a href="../methodology/">Methodology</a>.</p>
{table(["File"], rows)}
<p><a href="companies/">Company detail JSON files</a></p>
<p><a href="charts/companies/">Company chart JSON files</a></p>
{'<p><a href="../attribution/">Employer-to-candidate attribution reviews and source evidence</a></p>' if attribution_reports else ''}
"""
    top_note = note(
        metadata,
        f"source_manifest.json compares local FEC bulk files against the current official FEC release timestamps. Chart payloads use the same underlying data; the visible timeline on charts defaults to {display_start}.",
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
    top_note = note(metadata)
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
        f"Weekly series keep all rows; on-page chart display defaults to {display_start}.",
    )
    return shell("Company Chart JSON - Tech Money", body, prefix="../../../", top_note=top_note)


def page_attribution_index(metadata: dict, reports: list[dict]) -> str:
    return shell(
        "Employer-to-candidate attribution reviews - Tech Money",
        attribution.index_body(reports, metadata["cycle"]),
        prefix="../",
    )


def page_attribution(metadata: dict, report: dict) -> str:
    return shell(
        f"{attribution.report_title(report)} - Attribution review - Tech Money",
        attribution.report_body(report, table),
        prefix="../../",
    )


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
        "attribution_reports": attribution.load_reports(data_root, cycle),
    }


def collect_cycle_page_dirs(bundle: dict) -> set[str]:
    page_dirs = {
        "",
        "companies/",
        "committees/",
        "candidates/",
        "candidates/president/",
        "races/",
        "political-bodies/",
        "donors/",
        "federal-lobbying/",
        "campaign-finance-101/",
        "methodology/",
        "data/",
        "data/companies/",
        "data/charts/companies/",
        "about/",
    }
    for row in bundle["companies"]:
        page_dirs.add(normalize_rel_dir(f"companies/{row['slug']}"))
    if bundle.get("attribution_reports"):
        page_dirs.add("attribution/")
        for report in bundle["attribution_reports"]:
            page_dirs.add(normalize_rel_dir(f"attribution/{report['_slug']}"))
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

    body = f"""
<div class="page-intro">
<div class="eyebrow">Technology · Politics · Public records</div>
<h1>Follow the money. Understand the records.</h1>
<p class="lede">Explore employer-matched federal campaign contributions and reported lobbying activity, with linked sources and clear definitions.</p>
<div class="page-actions"><a class="button" href="{default_cycle}/">Explore the {default_cycle} cycle →</a><a class="button" href="{lobbying_landing()}">Explore federal lobbying</a></div>
</div>
<h2>Election cycles</h2>
{table(["Cycle", "Latest Matched Transaction", "Employer-Matched Giving", "Donor Groups", "Tracked Employers"], rows, filterable=False)}
<p class="small">Employer-matched contributions describe reported donor records, not spending directed by an employer. Donor groups are based on reported names, not verified unique identities.</p>
"""
    return layout.render_shell(
        "Tech Money", body,
        stylesheet_url=static_asset_url("site.css", f"{default_cycle}/"),
        tables_script_url=static_asset_url("tables.js", f"{default_cycle}/"),
        navigation_prefix=f"{default_cycle}/",
        home_href="index.html",
        lobbying_href=lobbying_landing(),
        source_note='<p>Transaction dates do not indicate filing completeness. Late filings and amendments can change earlier totals; each cycle page identifies its installed source releases.</p>',
    )


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
        attribution_reports = bundle["attribution_reports"]
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

        render("index.html", lambda: page_home(metadata, homepage, attribution_reports))
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
            "races/index.html",
            lambda: page_races(
                metadata,
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
        render("federal-lobbying/index.html", lambda: page_federal_lobbying(metadata))
        render("campaign-finance-101/index.html", lambda: page_campaign_finance_101(metadata))
        render("methodology/index.html", lambda: page_methodology(metadata))
        render("data/index.html", lambda: page_data(metadata, attribution_reports))
        render("about/index.html", lambda: page_about(metadata))
        render(
            "data/companies/index.html",
            lambda: page_company_data_index(metadata, companies),
        )
        render(
            "data/charts/companies/index.html",
            lambda: page_company_chart_data_index(metadata, companies),
        )

        if attribution_reports:
            render("attribution/index.html", lambda: page_attribution_index(metadata, attribution_reports))
            for report in attribution_reports:
                render(
                    f"attribution/{report['_slug']}/index.html",
                    lambda report=report: page_attribution(metadata, report),
                )

        for row in companies:
            slug = row["slug"]
            payload = read_json(data_root / "companies" / f"{slug}.json")
            render(
                f"companies/{slug}/index.html",
                lambda payload=payload: page_company(metadata, payload, attribution_reports),
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

    from frontend.lobbying import build_lobbying_pages
    build_lobbying_pages(SITE_ROOT, available_cycles)
    CURRENT_RENDER_CYCLE = None
    CURRENT_RENDER_REL_DIR = ""
    print(f"Built multi-cycle site to {SITE_ROOT}")


if __name__ == "__main__":
    build_site()
