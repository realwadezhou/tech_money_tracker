"""Static presentation of source-based employer-to-candidate attribution reviews."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import html
import json
from pathlib import Path
import re
from urllib.parse import urlsplit


COMPONENT_LABELS = {
    "direct_receipts": "Direct receipts",
    "jfc_allocations": "Joint-fundraising allocations",
    "partnership_attributions": "Partnership attributions",
    "signed_adjustments": "Signed adjustments",
    "refunds": "Refunds",
}


def esc(value) -> str:
    return html.escape(str(value if value is not None else ""))


def amount_text(value, *, signed: bool = False) -> str:
    """Preserve unknown amounts and cents; never turn a null into a zero."""
    if value is None or value == "":
        return "Unavailable"
    try:
        amount = Decimal(str(value))
    except InvalidOperation:
        return "Unavailable"
    if not amount.is_finite():
        return "Unavailable"
    sign = "-" if amount < 0 else "+" if signed and amount > 0 else ""
    digits = format(abs(amount), ",.2f")
    if digits.endswith(".00"):
        digits = digits[:-3]
    return f"{sign}${digits}"


def official_source_link(url, label: str) -> str:
    """Only official FEC HTTPS URLs may become source anchors."""
    try:
        parsed = urlsplit(str(url or ""))
        host = (parsed.hostname or "").lower()
        allowed = (
            parsed.scheme == "https"
            and (host == "fec.gov" or host.endswith(".fec.gov"))
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
        )
    except ValueError:
        allowed = False
    if not allowed:
        return f"{esc(label)} (source link unavailable)"
    return f'<a href="{esc(url)}">{esc(label)}</a>'


def load_reports(data_root: Path, cycle: int) -> list[dict]:
    reports = []
    for path in sorted((data_root / "attribution").glob("*.json")):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", path.stem):
            raise ValueError(f"Unsafe attribution report filename: {path.name}")
        report = json.loads(path.read_text(encoding="utf-8"))
        if report.get("schema_version") != 1 or report.get("cycle") != cycle:
            raise ValueError(f"Unsupported attribution report schema or cycle: {path}")
        reports.append({**report, "_slug": path.stem})
    return reports


def report_title(report: dict) -> str:
    return f'{report["company"]["label"]} → {report["candidate"]["name"]}'


def report_links(reports: list[dict] | None, prefix: str = "", company_key: str | None = None) -> str:
    selected = [r for r in reports or [] if company_key is None or r["company"]["key"] == company_key]
    if not selected:
        return ""
    links = "".join(
        f'<li><a href="{esc(prefix)}attribution/{esc(r["_slug"])}/">{esc(report_title(r))}</a></li>'
        for r in selected
    )
    return (
        '<h2>Employer-to-candidate reviews</h2>'
        '<p>These source-based reviews separate receipt routes, adjustments, and refunds '
        'for the stated reporting periods. They do not replace the broader selected-record totals.</p>'
        f"<ul>{links}</ul>"
    )


def index_body(reports: list[dict], cycle: int) -> str:
    return (
        '<h1>Employer-to-candidate attribution reviews</h1>'
        f'<p>Source-based reviews for the {esc(cycle)} cycle. Each case shows its reporting '
        'coverage, component amounts, unresolved records, and original FEC sources. '
        'Employer matches do not verify employment or company-directed giving.</p>'
        + report_links(reports, "../")
    )


def report_body(report: dict, table_renderer) -> str:
    scope = report.get("scope", {})
    components = report.get("components", {})
    component_rows = []
    for key, label in COMPONENT_LABELS.items():
        if key == "refunds":
            continue
        item = components.get(key, {})
        component_rows.append([
            f"<td>{esc(label)}</td>",
            f'<td class="number">{amount_text(item.get("amount"), signed=key == "signed_adjustments")}</td>',
            f'<td class="number">{esc(item.get("count", "Unavailable"))}</td>',
            f'<td>{esc(str(item.get("status", "unavailable")).replace("_", " "))}</td>',
            f'<td>{esc(item.get("description", ""))}</td>',
        ])

    combined = (report.get("combined_amount")
                if report.get("combined_status") == "resolved_for_scope" else None)
    net = report.get("net_amount") if report.get("net_status") == "resolved_for_scope" else None
    refund = components.get("refunds", {})
    committees = ", ".join(str(item) for item in scope.get("committee_ids", [])) or "Not specified"
    catalog_check = report.get("current_catalog_check")
    if catalog_check:
        catalog_note = (
            f'<strong>Latest catalog comparison:</strong> {esc(catalog_check.get("checked_at", "Not specified"))}; '
            f'{esc(catalog_check.get("reports", "Unavailable"))} report records; '
            f'{esc(str(catalog_check.get("status", "unknown")).replace("_", " "))}.'
        )
    else:
        catalog_note = 'No live catalog comparison was recorded for this build.'
    body = f"""
<h1>{esc(report_title(report))}</h1>
<p>Employer-to-candidate attribution review · {esc(report['cycle'])} cycle</p>
<p>{esc(scope.get('description', ''))}</p>
<p><strong>Reporting coverage:</strong> {esc(scope.get('coverage_start') or 'Not specified')} to {esc(scope.get('coverage_end') or 'Not specified')}. This is a review of the listed reports and accounts, not a complete-cycle total. A missing source period is not zero giving.</p>
<p><strong>Attributed receipts before refunds:</strong> {amount_text(combined)}<br>
{esc(report.get('combined_reason', ''))}</p>
<p>Amounts below describe employer-matched records supported within this scope; they do not verify employment or company-directed giving. Overlapping records are not automatically added together.</p>
<h2>Receipt components</h2>
{table_renderer(['Component', 'Amount', 'Records', 'Review status', 'Treatment'], component_rows, filterable=False)}
<h2>Refunds and net amount</h2>
<p><strong>Refunds:</strong> {amount_text(refund.get('amount'), signed=True)} ({esc(refund.get('count', 'Unavailable'))} linked records; {esc(str(refund.get('status', 'unavailable')).replace('_', ' '))})<br>
{esc(refund.get('description', ''))}</p>
<p><strong>Net amount after refunds:</strong> {amount_text(net)}<br>
{esc(report.get('net_reason', ''))}</p>
<h2>Accounts and source coverage</h2>
<p><strong>Candidate:</strong> {esc(report['candidate']['name'])} ({esc(report['candidate']['id'])})<br>
<strong>Committee IDs:</strong> {esc(committees)}<br>
<strong>Account ownership:</strong> {esc(scope.get('account_ownership', 'Not specified'))}<br>
<strong>Source completeness:</strong> {esc(scope.get('source_completeness', 'Not specified'))}<br>
<strong>Saved source catalog as of:</strong> {esc(report.get('source_catalog_as_of', 'Not specified'))}<br>
<strong>Report generated:</strong> {esc(report.get('generated_at', 'Not specified'))}</p>
<p>{catalog_note} The OpenFEC catalog lists API-processed reports. Matching that inventory does not establish complete current giving or that every filing has been processed.</p>
"""
    unresolved = report.get("unresolved", [])
    if unresolved:
        rows = [[f'<td>{esc(r.get("record_id") or "Coverage")}</td>',
                 f'<td class="number">{amount_text(r.get("amount"))}</td>',
                 f'<td>{esc(r.get("scope", ""))}</td>',
                 f'<td>{esc(r.get("reason", ""))}</td>'] for r in unresolved]
        body += '<h2>Unresolved records</h2>'
        if any(r.get("scope") == "refunds" for r in unresolved):
            body += (
                '<p>Any refund records listed below are campaign-wide records whose donor or employer '
                'relationships remain unresolved. They are not evidence that donors matched to '
                f'{esc(report["company"]["label"])} received refunds. Their reported amounts are not '
                "deductions from this employer's attributed receipts.</p>"
            )
        unresolved_table = table_renderer(["Record", "Amount", "Scope", "Reason"], rows)
        if len(unresolved) > 20:
            unresolved_table = (
                f'<details><summary>Inspect unresolved source records ({len(unresolved):,})</summary>'
                + unresolved_table + '</details>'
            )
        body += unresolved_table
    if report.get("limitations"):
        body += '<h2>Limits of this review</h2><ul>' + "".join(
            f"<li>{esc(item)}</li>" for item in report["limitations"]
        ) + "</ul>"

    sources = []
    for source in report.get("sources", []):
        label = f'FEC filing {source.get("file_number", "source")}'
        sources.append([
            f'<td>{official_source_link(source.get("url"), label)}</td>',
            f'<td>{esc(source.get("coverage_start") or "Not specified")} to {esc(source.get("coverage_end") or "Not specified")}</td>',
            f'<td><details><summary>SHA-256</summary><code>{esc(source.get("sha256", "Not specified"))}</code></details></td>',
        ])
    body += '<h2>Source filings</h2>' + table_renderer(["Source", "Reporting period", "File fingerprint"], sources, filterable=False)
    evidence = []
    for row in report.get("evidence", []):
        component_key = row.get("component")
        component = (COMPONENT_LABELS.get(component_key, str(component_key)) if component_key
                     else str(row.get("disposition") or "Unresolved").replace("_", " ").capitalize())
        evidence.append([
            f'<td>{official_source_link(row.get("source_url"), str(row.get("record_id", "Source record")))}</td>',
            f'<td>{esc(row.get("date", ""))}</td>',
            f'<td class="number">{amount_text(row.get("amount"), signed=True)}</td>',
            f'<td>{esc(component)}</td>',
            f'<td>{esc(row.get("disposition", ""))}</td>',
            f'<td>{esc(row.get("reason", ""))}</td>',
        ])
    body += '<h2>Record evidence</h2>' + table_renderer(["Record / source", "Date", "Reported amount", "Component", "Disposition", "Reason"], evidence)
    body += (
        f'<p><a href="../../data/attribution/{esc(report["_slug"])}.json">Download this report and its record evidence (JSON)</a></p>'
        '<p><a href="../">All attribution reviews</a></p>'
    )
    return body
