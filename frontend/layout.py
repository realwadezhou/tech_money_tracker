"""Shared, static Reference layout for campaign-finance and lobbying pages."""
from __future__ import annotations

from html import escape, unescape
import re


SECTIONS = (
    ("", "Overview", "index.html"),
    ("companies", "Giving by employer", "companies/"),
    ("donors", "Donors", "donors/"),
    ("candidates", "Candidates", "candidates/"),
    ("races", "Races", "races/"),
    ("committees", "Committees", "committees/"),
    ("political-bodies", "Political bodies", "political-bodies/"),
    ("federal-lobbying", "Federal lobbying", "federal-lobbying/"),
    ("campaign-finance-101", "Campaign Finance 101", "campaign-finance-101/"),
    ("methodology", "Methodology", "methodology/"),
    ("data", "Data & sources", "data/"),
    ("about", "About", "about/"),
)


def plain_text(markup: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", markup)).strip()


def heading_outline(body: str) -> tuple[str, list[tuple[str, str]]]:
    """Add stable section anchors without changing existing fragment URLs."""
    used = {unescape(value) for value in re.findall(r'\bid\s*=\s*[\"\']([^\"\']+)', body, flags=re.I)}
    outline: list[tuple[str, str]] = []

    def anchor(match: re.Match) -> str:
        attrs, contents = match.group(1), match.group(2)
        label = plain_text(contents)
        existing = re.search(r'\bid\s*=\s*[\"\']([^\"\']+)', attrs, flags=re.I)
        if existing:
            identifier = unescape(existing.group(1))
        else:
            base = "section-" + (re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-") or "details")
            identifier = base
            suffix = 2
            while identifier in used:
                identifier = f"{base}-{suffix}"
                suffix += 1
            used.add(identifier)
            attrs += f' id="{escape(identifier, quote=True)}"'
        outline.append((identifier, label))
        return f"<h2{attrs}>{contents}</h2>"

    return re.sub(r"<h2\b([^>]*)>(.*?)</h2>", anchor, body, flags=re.I | re.S), outline


def section_navigation(prefix: str, current_section: str = "", lobbying_href: str | None = None) -> str:
    parts: list[str] = []
    for start, end, caption in ((0, 7, "Explore"), (7, 8, "Federal lobbying"), (8, 12, "Understand the data")):
        links = []
        for key, label, route in SECTIONS[start:end]:
            href = lobbying_href if key == "federal-lobbying" and lobbying_href else prefix + route
            active = ' aria-current="page"' if key == current_section else ""
            links.append(f'<a href="{escape(href, quote=True)}"{active}>{escape(label)}</a>')
        parts.append(f'<div class="nav-group"><div class="nav-caption">{caption}</div>{"".join(links)}</div>')
    return "".join(parts)


def render_shell(
    title: str,
    body: str,
    *,
    stylesheet_url: str,
    tables_script_url: str | None = None,
    charts_script_url: str | None = None,
    extra_head: str = "",
    scripts: str = "",
    navigation_prefix: str = "",
    home_href: str = "index.html",
    lobbying_href: str | None = None,
    current_section: str = "",
    cycle_label: str = "",
    cycle_controls: str = "",
    source_note: str = "",
    main_id: str = "main-content",
    main_class: str = "",
    eyebrow: str = "",
    definition_href: str | None = None,
    data_href: str | None = None,
) -> str:
    body, outline = heading_outline(body)
    heading = re.search(r"<h1\b[^>]*>(.*?)</h1>", body, flags=re.I | re.S)
    page_name = plain_text(heading.group(1)) if heading else title.removesuffix(" - Tech Money")
    if not heading:
        body = f"<h1>{escape(page_name)}</h1>\n" + body
    section = "data" if current_section == "attribution" else current_section
    navigation = section_navigation(navigation_prefix, section, lobbying_href)
    primary = []
    for key, label in (("", "Overview"), ("companies", "Employers"), ("candidates", "Candidates"), ("federal-lobbying", "Lobbying"), ("data", "Sources")):
        route = next(route for candidate, _, route in SECTIONS if candidate == key)
        href = lobbying_href if key == "federal-lobbying" and lobbying_href else navigation_prefix + route
        active = ' aria-current="page"' if key == section else ""
        primary.append(f'<a href="{escape(href, quote=True)}"{active}>{label}</a>')
    context_links = "".join(f'<a href="#{escape(identifier, quote=True)}">{escape(label)}</a>' for identifier, label in outline)
    source_details = (
        '<details class="source-details" open><summary>Data coverage &amp; definitions</summary>'
        + source_note + "</details>"
        if source_note else ""
    )
    explicit_definition, explicit_data = definition_href, data_href
    definition = navigation_prefix + "methodology/"
    data_href = navigation_prefix + "data/"
    if section == "federal-lobbying":
        definition = "#methodology"
        data_href = "data/manifest.json" if main_id == "lobbying-explorer" else (lobbying_href or "../../lobbying/") + "data/manifest.json"
        if main_id != "lobbying-explorer":
            definition = (lobbying_href or "../../lobbying/") + "#methodology"
    # A page with its own methodology and downloads can point at them directly.
    definition = explicit_definition or definition
    data_href = explicit_data or data_href
    page_context = f'''<aside class="page-context" aria-label="Page context">
      {f'<div class="context-label">On this page</div><nav class="context-links" aria-label="On this page">{context_links}</nav>' if outline else ''}
      {source_details}
      <div class="context-note"><strong>Read with context</strong><p>Definitions, source coverage, and limitations matter when comparing these records.</p>
        <a href="{escape(definition, quote=True)}">How to use the data →</a></div>
    </aside>'''
    table_script = f'<script src="{escape(tables_script_url, quote=True)}" defer></script>' if tables_script_url else ""
    chart_script = f'<script src="{escape(charts_script_url, quote=True)}"></script>' if charts_script_url else ""
    crumb = f'<span>{escape(cycle_label)}</span><span aria-hidden="true">/</span>' if cycle_label else ""
    return f'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>{escape(title)}</title>
  <link rel="stylesheet" href="{escape(stylesheet_url, quote=True)}">
  {table_script}
  {extra_head}
</head>
<body>
  <a class="skip-link" href="#{escape(main_id, quote=True)}">Skip to content</a>
  <div class="page app-shell">
    <header class="site-header">
      <a class="site-title" href="{escape(home_href, quote=True)}">Tech Money</a>
      <nav class="primary-nav" aria-label="Main navigation">{"".join(primary)}</nav>
      {cycle_controls}
      <details class="mobile-navigation"><summary>Browse sections</summary><nav aria-label="Browse sections on mobile">{navigation}</nav></details>
    </header>
    <div class="site-layout">
      <aside class="site-sidebar"><nav aria-label="Browse sections">{navigation}</nav></aside>
      <main class="page-main {escape(main_class, quote=True)}" id="{escape(main_id, quote=True)}">
        <nav class="breadcrumbs" aria-label="Breadcrumb"><a href="{escape(home_href, quote=True)}">Tech Money</a><span aria-hidden="true">/</span>{crumb}<span>{escape(page_name)}</span></nav>
        {f'<div class="eyebrow">{escape(eyebrow)}</div>' if eyebrow else ''}
        {body}
      </main>
      {page_context}
    </div>
    <footer class="site-footer"><span>Tech Money · A public record of tech and politics</span><nav aria-label="Footer"><a href="{escape(definition, quote=True)}">Methodology</a><a href="{escape(data_href, quote=True)}">Data &amp; sources</a><a href="{escape(navigation_prefix + 'about/', quote=True)}">About</a></nav></footer>
  </div>
  {chart_script}
  {scripts}
</body>
</html>
'''
