"""Independently check frozen cross-source case facts against original bytes.

No production parser or accounting imports. This verifies transcription and
source identity, not whether a filing is truthful or a candidate total complete.
"""
import argparse
import csv
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path


def verify(case, directories):
    original = {}
    for source in case["sources"]:
        number = str(source["file_number"])
        path = next((d / f"{number}.fec" for d in directories if (d / f"{number}.fec").is_file()), None)
        if path is None:
            raise ValueError(f"Original filing unavailable: {number}")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != source["sha256"]:
            raise ValueError(f"Changed source bytes: {number}")
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1252")
        rows = list(csv.reader(io.StringIO(text), delimiter="\x1c" if "\x1c" in text else ",", strict=True))
        if rows[0][:2] != ["HDR", "FEC"] or rows[0][2] not in {"8.4", "8.5"}:
            raise ValueError(f"Unreviewed file format: {number}")
        original[number] = rows
    for record in case["records"]:
        row = original[str(record["file_number"])][record["source_record_number"] - 1]
        digest = hashlib.sha256(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        if digest != record["source_row_sha256"] or row[2] != record["transaction_id"]:
            raise ValueError(f"Source row identity changed: {record['record_id']}")
        # One-based FEC EFO Schedule A positions: amount21, employer24,
        # memo43. Location columns deliberately never leave the raw source.
        for field, position in {"schedule": 0, "committee_id": 1, "transaction_id": 2,
                                "back_reference_transaction_id": 3, "entity_type": 5,
                                "election": 17, "amount": 20, "employer": 23,
                                "purpose": 22, "memo_text": 43}.items():
            if record[field] != row[position]:
                raise ValueError(f"Transcription differs: {record['record_id']} / {field}")
        date = datetime.strptime(row[19], "%Y%m%d").date().isoformat() if row[19] else ""
        if record["date"] != date or record["memo"] is not (row[42] == "X"):
            raise ValueError(f"Date/memo transcription differs: {record['record_id']}")
    return {"status": "passed", "case_id": case["case_id"],
            "source_files": len(original), "source_records": len(case["records"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", type=Path, required=True)
    parser.add_argument("--source-dir", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(json.loads(args.case.read_text(encoding="utf-8")), args.source_dir)
    result["case_sha256"] = hashlib.sha256(args.case.read_bytes()).hexdigest()
    result["checker_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
