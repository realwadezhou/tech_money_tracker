"""Read original electronic reports without mixing amendment versions.

Supported layouts: FEC EFO 8.4/8.5, F3/F3P/F3X, Schedules A and B.
https://docquery.fec.gov/formatspecs/FEC_EFO_Format_Specifications.xlsx
Unsupported/ambiguous sources fail rather than silently supply partial totals.
Public records omit addresses; full original bytes and row hashes retain provenance.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
from pathlib import Path
import re


LAYOUT_URL = "https://docquery.fec.gov/formatspecs/FEC_EFO_Format_Specifications.xlsx"
LAYOUTS = {
    "F3": {"report": 11, "start": 15, "end": 16, "individual_total": 32,
           "refund_total": 51, "individual_lines": {"SA11AI"}, "refund_line": "SB20A"},
    "F3P": {"report": 11, "start": 15, "end": 16, "individual_total": 34,
            "refund_total": 58, "individual_lines": {"SA17AI", "SA17A"}, "refund_line": "SB28A"},
    "F3X": {"report": 9, "start": 13, "end": 14, "individual_total": 29,
            "refund_total": 56, "individual_lines": {"SA11AI"}, "refund_line": "SB28A"},
}


def decimal_amount(value: str) -> Decimal:
    try:
        result = Decimal(value)
        valid = result.is_finite() and result == result.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Invalid filed amount: {value!r}") from None
    if not valid:
        raise ValueError(f"Invalid filed dollar precision: {value!r}")
    return result


def iso_date(value: str) -> str:
    if not re.fullmatch(r"\d{8}", value):
        raise ValueError(f"Invalid filed date: {value!r}")
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def raw_row_hash(row: list[str]) -> str:
    return hashlib.sha256(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


@dataclass
class Filing:
    metadata: dict
    records: list[dict]


def parse_filing(path: Path, file_number: int, *, expected_sha256: str | None = None) -> Filing:
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 and digest != expected_sha256:
        raise ValueError(f"Source hash changed for filing {file_number}; review the new version")
    try:
        content = raw.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        content = raw.decode("cp1252")
        encoding = "cp1252"
    delimiter = "\x1c" if content.startswith("HDR\x1c") else ","
    rows = list(csv.reader(io.StringIO(content, newline=""), delimiter=delimiter))
    if len(rows) < 2 or rows[0][:2] != ["HDR", "FEC"]:
        raise ValueError(f"Not an electronic FEC report: {file_number}")
    header, form = rows[:2]
    if len(header) < 7 or not form:
        raise ValueError(f"Truncated electronic header: {file_number}")
    if header[2] not in {"8.4", "8.5"}:
        raise ValueError(f"Unsupported EFO version {header[2]} in {file_number}")
    match = re.fullmatch(r"(F3P|F3X|F3)([NAT])", form[0])
    if not match:
        raise ValueError(f"Unsupported reporting form {form[0]} in {file_number}")
    form_type, amendment = match.groups()
    layout = LAYOUTS[form_type]
    if len(form) <= max(layout["individual_total"], layout["refund_total"]):
        raise ValueError(f"Truncated {form_type} summary: {file_number}")
    committee = form[1]
    if not re.fullmatch(r"C\d{8}", committee):
        raise ValueError(f"Invalid filing committee: {committee}")
    original = int(header[5].removeprefix("FEC-")) if header[5] else file_number
    sequence = int(header[6] or "0")
    if sequence < 0 or (amendment == "A") != (sequence > 0) or (sequence == 0 and original != file_number):
        raise ValueError(f"Conflicting amendment header: {file_number}")
    start, end = iso_date(form[layout["start"]]), iso_date(form[layout["end"]])
    if end < start:
        raise ValueError(f"Reversed reporting period: {file_number}")
    records, identifiers = [], set()
    for position, row in enumerate(rows[2:], start=3):
        if not row or not row[0].startswith(("SA", "SB")):
            continue
        schedule_a = row[0].startswith("SA")
        expected_fields = 45 if schedule_a else 44
        # Some filing software writes an extra trailing delimiter. Accept only
        # empty tail padding; never shift columns or discard unfamiliar data.
        # Hash the complete original row below, including that padding.
        if (len(row) < expected_fields or any(row[expected_fields:])
                or row[1] != committee or not row[2]):
            raise ValueError(f"Invalid {row[0]} layout/identity in {file_number}, record {position}")
        record_id = f"{file_number}:{row[2]}"
        if record_id in identifiers:
            raise ValueError(f"Ambiguous repeated transaction ID: {record_id}")
        identifiers.add(record_id)
        memo_index = 42 if schedule_a else 41
        if row[memo_index] not in {"", "X"}:
            raise ValueError(f"Unknown memo flag in {record_id}")
        # A missing date remains explicit; report coverage is not a date filter.
        date = iso_date(row[19]) if row[19] else ""
        name = (row[7] + ", " + " ".join(x for x in row[8:10] if x)).strip(" ,")
        records.append({
            "record_id": record_id, "committee_id": committee, "file_number": str(file_number),
            "transaction_id": row[2], "schedule": row[0], "entity_type": row[5],
            "amount": format(decimal_amount(row[20]), ".2f"), "employer": row[23] if schedule_a else "",
            "memo": row[memo_index] == "X", "back_reference_transaction_id": row[3],
            "date": date, "election": row[17], "donor_name": name or row[6],
            "organization": row[6], "back_reference_schedule": row[4],
            "purpose": row[22],
            "memo_text": row[memo_index + 1], "other_committee_id": row[25] if schedule_a else row[24],
            "source_row_sha256": raw_row_hash(row), "source_record_number": position,
            "source_url": f"https://docquery.fec.gov/dcdev/posted/{file_number}.fec",
        })
    individual = sum((Decimal(r["amount"]) for r in records
                      if r["schedule"] in layout["individual_lines"] and not r["memo"]), Decimal(0))
    refunds = sum((Decimal(r["amount"]) for r in records
                   if r["schedule"] == layout["refund_line"] and not r["memo"]), Decimal(0))
    reported_individual = decimal_amount(form[layout["individual_total"]] or "0")
    reported_refunds = decimal_amount(form[layout["refund_total"]] or "0")
    return Filing({
        "file_number": file_number, "committee_id": committee, "form_type": form_type,
        "format_version": header[2], "encoding": encoding, "original_file_number": original,
        "amendment_sequence": sequence, "coverage_start": start, "coverage_end": end,
        "report_type": form[layout["report"]], "sha256": digest,
        "url": f"https://docquery.fec.gov/dcdev/posted/{file_number}.fec",
        "individual_itemized_reported": format(reported_individual, ".2f"),
        "individual_itemized_calculated": format(individual, ".2f"),
        "individual_itemized_reconciles": individual == reported_individual,
        "refunds_reported": format(reported_refunds, ".2f"),
        "refunds_itemized": format(refunds, ".2f"),
        "refunds_unitemized_or_unreconciled": format(reported_refunds - refunds, ".2f"),
    }, records)


def latest_filings(filings: list[Filing]) -> list[Filing]:
    """Select replacement reports by original report ID and amendment sequence.

    Different periods never dedupe by donor/name/amount. Incomplete chains,
    revised periods and overlapping independent reports need human review.
    """
    chains: dict[tuple, list[Filing]] = {}
    seen = set()
    for filing in filings:
        meta = filing.metadata
        if meta["file_number"] in seen:
            raise ValueError("Duplicate filing supplied")
        seen.add(meta["file_number"])
        chains.setdefault((meta["committee_id"], meta["original_file_number"]), []).append(filing)
    selected = []
    for chain in chains.values():
        chain.sort(key=lambda f: f.metadata["amendment_sequence"])
        if [f.metadata["amendment_sequence"] for f in chain] != list(range(len(chain))):
            raise ValueError("Missing or ambiguous amendment sequence; supply the entire known chain")
        identity = {(f.metadata["form_type"], f.metadata["report_type"],
                     f.metadata["coverage_start"], f.metadata["coverage_end"]) for f in chain}
        if len(identity) != 1:
            raise ValueError("Changed report period/type within amendment chain requires review")
        selected.append(chain[-1])
    prior_end = {}
    for filing in sorted(selected, key=lambda f: (f.metadata["committee_id"], f.metadata["coverage_start"])):
        meta = filing.metadata
        if meta["coverage_start"] <= prior_end.get(meta["committee_id"], ""):
            raise ValueError("Overlapping report periods require review; refusing to sum them")
        prior_end[meta["committee_id"]] = meta["coverage_end"]
    return sorted(selected, key=lambda f: (f.metadata["committee_id"], f.metadata["coverage_start"]))
