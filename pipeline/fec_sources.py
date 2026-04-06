from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parent.parent
FEC_DATA_ROOT = PROJECT_ROOT.parent / "spending_tracker" / "data" / "interim" / "fec"


@dataclass(frozen=True)
class BulkFileSpec:
    key: str
    label: str
    remote_stem: str
    local_dir: str
    local_file: str
    local_zip_dir_template: str
    local_zip_filename: str


BULK_FILE_SPECS = [
    BulkFileSpec(
        key="committee_master",
        label="Committee master",
        remote_stem="cm{suffix}",
        local_dir="cm{suffix}",
        local_file="cm.txt",
        local_zip_dir_template="raw/fec/{cycle}",
        local_zip_filename="cm{suffix}.zip",
    ),
    BulkFileSpec(
        key="candidate_master",
        label="Candidate master",
        remote_stem="cn{suffix}",
        local_dir="cn{suffix}",
        local_file="cn.txt",
        local_zip_dir_template="interim/fec/{cycle}/cn{suffix}",
        local_zip_filename="cn{suffix}.zip",
    ),
    BulkFileSpec(
        key="candidate_committee_linkage",
        label="Candidate-committee linkage",
        remote_stem="ccl{suffix}",
        local_dir="ccl{suffix}",
        local_file="ccl.txt",
        local_zip_dir_template="interim/fec/{cycle}/ccl{suffix}",
        local_zip_filename="ccl{suffix}.zip",
    ),
    BulkFileSpec(
        key="itemized_individual_contributions",
        label="Itemized individual contributions",
        remote_stem="indiv{suffix}",
        local_dir="indiv{suffix}",
        local_file="itcont.txt",
        local_zip_dir_template="raw/fec/{cycle}",
        local_zip_filename="indiv{suffix}.zip",
    ),
    BulkFileSpec(
        key="committee_to_committee_transactions",
        label="Committee-to-committee transactions",
        remote_stem="oth{suffix}",
        local_dir="oth{suffix}",
        local_file="itoth.txt",
        local_zip_dir_template="raw/fec/{cycle}",
        local_zip_filename="oth{suffix}.zip",
    ),
    BulkFileSpec(
        key="candidate_contributions",
        label="Contributions to candidates",
        remote_stem="pas2{suffix}",
        local_dir="pas2{suffix}",
        local_file="itpas2.txt",
        local_zip_dir_template="raw/fec/{cycle}",
        local_zip_filename="pas2{suffix}.zip",
    ),
]


def cycle_suffix(cycle: int) -> str:
    return str(cycle)[2:]


def bulk_download_url(cycle: int, spec: BulkFileSpec) -> str:
    suffix = cycle_suffix(cycle)
    return (
        f"https://www.fec.gov/files/bulk-downloads/{cycle}/"
        f"{spec.remote_stem.format(suffix=suffix)}.zip"
    )


def local_bulk_path(cycle: int, spec: BulkFileSpec) -> Path:
    suffix = cycle_suffix(cycle)
    return (
        FEC_DATA_ROOT
        / str(cycle)
        / spec.local_dir.format(suffix=suffix)
        / spec.local_file
    )


def local_zip_path(cycle: int, spec: BulkFileSpec) -> Path:
    suffix = cycle_suffix(cycle)
    relative_dir = spec.local_zip_dir_template.format(cycle=cycle, suffix=suffix)
    filename = spec.local_zip_filename.format(cycle=cycle, suffix=suffix)
    return PROJECT_ROOT.parent / "spending_tracker" / "data" / relative_dir / filename


def _iso_utc_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _iso_utc_from_header(value: str | None) -> str | None:
    if not value:
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _head(url: str, timeout: int = 20) -> dict[str, str]:
    request = Request(url, method="HEAD", headers={"User-Agent": "tech-money/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return dict(response.headers.items())


def get_cycle_source_status(cycle: int) -> list[dict]:
    statuses: list[dict] = []
    for spec in BULK_FILE_SPECS:
        local_path = local_bulk_path(cycle, spec)
        local_exists = local_path.exists()
        local_mtime = _iso_utc_from_timestamp(local_path.stat().st_mtime) if local_exists else None
        remote_url = bulk_download_url(cycle, spec)

        remote_headers: dict[str, str] | None = None
        remote_error = ""
        try:
            remote_headers = _head(remote_url)
        except Exception as exc:  # pragma: no cover - network/path dependent
            remote_error = str(exc)

        remote_last_modified = _iso_utc_from_header(
            remote_headers.get("Last-Modified") if remote_headers else None
        )
        remote_content_length = (
            int(remote_headers["Content-Length"])
            if remote_headers and remote_headers.get("Content-Length")
            else None
        )

        remote_is_newer = False
        if local_mtime and remote_last_modified:
            remote_is_newer = remote_last_modified > local_mtime

        statuses.append(
            {
                "key": spec.key,
                "label": spec.label,
                "remote_url": remote_url,
                "local_zip_path": str(local_zip_path(cycle, spec)),
                "local_path": str(local_path),
                "local_exists": local_exists,
                "local_last_modified_utc": local_mtime,
                "remote_last_modified_utc": remote_last_modified,
                "remote_content_length": remote_content_length,
                "remote_etag": remote_headers.get("ETag") if remote_headers else None,
                "remote_is_newer": remote_is_newer,
                "remote_error": remote_error,
            }
        )
    return statuses


def build_source_manifest(cycle: int) -> dict:
    checked_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    bulk_sources = get_cycle_source_status(cycle)
    remote_timestamps = [
        row["remote_last_modified_utc"]
        for row in bulk_sources
        if row["remote_last_modified_utc"]
    ]
    stale_sources = [row["key"] for row in bulk_sources if row["remote_is_newer"]]
    return {
        "cycle": cycle,
        "checked_at_utc": checked_at,
        "latest_bulk_release_utc": max(remote_timestamps) if remote_timestamps else None,
        "bulk_sources": bulk_sources,
        "sources_with_remote_newer_than_local": stale_sources,
        "all_local_bulk_files_present": all(row["local_exists"] for row in bulk_sources),
        "openfec": {
            "developers_url": "https://api.open.fec.gov/developers/",
            "base_url": "https://api.open.fec.gov/v1/",
            "env_var": "OPENFEC_API_KEY",
            "notes": [
                "Use OpenFEC between bulk releases when fresher receipts or filings matter.",
                "FEC browse-data pages note that newly filed summary data may not appear for up to 48 hours.",
            ],
        },
    }


def write_source_manifest(cycle: int, out_dir: Path) -> dict:
    manifest = build_source_manifest(cycle)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "source_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def main(args: list[str] | None = None) -> None:
    cycle_args = args if args is not None else sys.argv[1:]
    cycles = [int(arg) for arg in cycle_args] if cycle_args else [2024]
    for cycle in cycles:
        manifest = build_source_manifest(cycle)
        print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
