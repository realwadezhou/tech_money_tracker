from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = PROJECT_ROOT / "data"
REFERENCE_ROOT = DATA_ROOT / "reference"

FEC_ROOT = DATA_ROOT / "fec"
FEC_RAW_ROOT = FEC_ROOT / "raw"
FEC_INTERIM_ROOT = FEC_ROOT / "interim"
FEC_DERIVED_ROOT = FEC_ROOT / "derived"

LDA_ROOT = DATA_ROOT / "lda"
LDA_RAW_ROOT = LDA_ROOT / "raw"
LDA_INTERIM_ROOT = LDA_ROOT / "interim"
LDA_DERIVED_ROOT = LDA_ROOT / "derived"

EXPORTS_ROOT = PROJECT_ROOT / "exports"
SITE_EXPORT_ROOT = EXPORTS_ROOT / "site"

MANUAL_TAGGING_ROOT = PROJECT_ROOT / "manual_tagging"


def fec_cycle_raw_dir(cycle: int) -> Path:
    return FEC_RAW_ROOT / str(cycle)


def fec_cycle_interim_dir(cycle: int) -> Path:
    return FEC_INTERIM_ROOT / str(cycle)


def fec_cycle_derived_dir(cycle: int) -> Path:
    return FEC_DERIVED_ROOT / str(cycle)


def lda_year_raw_dir(year: int) -> Path:
    return LDA_RAW_ROOT / str(year)


def lda_year_interim_dir(year: int) -> Path:
    return LDA_INTERIM_ROOT / str(year)


def lda_year_derived_dir(year: int) -> Path:
    return LDA_DERIVED_ROOT / str(year)


def lda_lookup_raw_dir() -> Path:
    return LDA_RAW_ROOT / "lookups"


def site_export_cycle_dir(cycle: int) -> Path:
    return SITE_EXPORT_ROOT / str(cycle)


def company_reference_dir() -> Path:
    return REFERENCE_ROOT / "companies"


def company_curated_path() -> Path:
    """Hand-curated employer → canonical tech-company lookup. Source of truth.

    Read by the pipeline. Edited by hand. Never overwritten by automation.
    """
    return company_reference_dir() / "curated.csv"


def company_candidates_path() -> Path:
    """All candidate employer strings surfaced by the crude regex filters.

    Fully regenerated whenever `pipeline.tagging.companies` runs. Safe to delete.
    """
    return company_reference_dir() / "candidates.csv"


def company_review_queue_path() -> Path:
    """Candidates not yet present in curated.csv — the reviewer's worklist.

    Regenerated alongside candidates.csv. Safe to delete.
    """
    return company_reference_dir() / "review_queue.csv"


def individual_reference_dir() -> Path:
    return REFERENCE_ROOT / "individuals"


def individual_curated_path() -> Path:
    """Hand-curated donor name → consolidated identity lookup. Source of truth."""
    return individual_reference_dir() / "curated.csv"


def individual_candidates_path() -> Path:
    return individual_reference_dir() / "candidates.csv"


def individual_review_queue_path() -> Path:
    return individual_reference_dir() / "review_queue.csv"
