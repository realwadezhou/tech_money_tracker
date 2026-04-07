from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = PROJECT_ROOT / "data"
REFERENCE_ROOT = DATA_ROOT / "reference"

FEC_ROOT = DATA_ROOT / "fec"
FEC_RAW_ROOT = FEC_ROOT / "raw"
FEC_INTERIM_ROOT = FEC_ROOT / "interim"
FEC_DERIVED_ROOT = FEC_ROOT / "derived"

EXPORTS_ROOT = PROJECT_ROOT / "exports"
SITE_EXPORT_ROOT = EXPORTS_ROOT / "site"

MANUAL_TAGGING_ROOT = PROJECT_ROOT / "manual_tagging"


def fec_cycle_raw_dir(cycle: int) -> Path:
    return FEC_RAW_ROOT / str(cycle)


def fec_cycle_interim_dir(cycle: int) -> Path:
    return FEC_INTERIM_ROOT / str(cycle)


def fec_cycle_derived_dir(cycle: int) -> Path:
    return FEC_DERIVED_ROOT / str(cycle)


def site_export_cycle_dir(cycle: int) -> Path:
    return SITE_EXPORT_ROOT / str(cycle)


def tech_employer_reference_dir() -> Path:
    return REFERENCE_ROOT / "tech_employers"


def tech_employer_review_path() -> Path:
    return tech_employer_reference_dir() / "employer_matches_for_review.csv"


def tech_employer_lookup_path() -> Path:
    return tech_employer_reference_dir() / "employer_matches_for_review_manual_mar31temp.csv"
