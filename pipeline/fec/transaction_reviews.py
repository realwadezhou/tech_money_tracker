"""Apply narrow, source-reviewed receipt exclusions without guessing duplicates.

Only an exact cycle/source-row ID plus its complete reviewed source signature
can be excluded. A reused ID with changed content stops the build for review.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from functools import lru_cache
import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline.common.paths import PROJECT_ROOT

REFERENCE = PROJECT_ROOT / "data/reference/transactions/reviewed_exclusions.json"
SOURCE_FIELDS = (
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state", "zip_code",
    "employer", "occupation", "transaction_dt", "transaction_amt", "other_id",
    "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
)
PUBLIC_SIGNATURE_FIELDS = tuple(k for k in SOURCE_FIELDS if k not in {"city", "state", "zip_code"})


def source_row_sha256(row) -> str:
    """Hash all source fields without persisting location details in references."""
    values = []
    for key in SOURCE_FIELDS:
        value = row[key]
        if key == "transaction_amt":
            try:
                amount = Decimal(str(value))
                if not amount.is_finite():
                    raise ValueError("Non-finite source amount")
                value = format(amount.normalize(), "f")
            except InvalidOperation as exc:
                raise ValueError("Invalid source amount") from exc
        values.append(str(value))
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


@lru_cache(maxsize=8)
def load_reviews(path: Path = REFERENCE) -> tuple[dict, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    seen = set()
    for review in payload["excluded_records"]:
        key = (review["cycle"], review["source_file"], review["source_signature"]["sub_id"])
        if (key in seen or set(review["source_signature"]) != set(PUBLIC_SIGNATURE_FIELDS)
                or len(review.get("source_row_sha256", "")) != 64):
            raise ValueError("Duplicate or incomplete reviewed transaction exclusion")
        original = review.get("retained_original_source_signature", {})
        if (set(original) != set(PUBLIC_SIGNATURE_FIELDS)
                or original.get("sub_id") != review.get("retained_original_sub_id")
                or original.get("cmte_id") != review["source_signature"]["cmte_id"]
                or len(review.get("retained_original_source_row_sha256", "")) != 64):
            raise ValueError("Reviewed exclusions require a complete retained-original signature")
        if not all(review.get(k) for k in ("reviewed_at", "reviewer", "reason", "evidence")):
            raise ValueError("Transaction exclusions require review provenance")
        seen.add(key)
    return tuple(payload["excluded_records"])


def _same_amount(actual, expected: str) -> bool:
    try:
        return Decimal(str(actual)) == Decimal(expected)
    except (InvalidOperation, ValueError):
        return False


def _matches_signature(row, expected: dict, expected_hash: str) -> bool:
    return all(
        _same_amount(row[key], expected[key]) if key == "transaction_amt"
        else str(row[key]) == expected[key]
        for key in PUBLIC_SIGNATURE_FIELDS
    ) and source_row_sha256(row) == expected_hash


class ReviewedExclusionDependencies:
    """Verify reviewed originals across a complete source, before output writes.

    Observe raw rows before candidate/employer/type selection. Originals may
    precede or follow their repeated memo in another chunk. Only the handful
    of reviewed IDs and validation states are retained in memory.
    """

    def __init__(self, cycle: int | None, *, source_file: str = "itcont.txt",
                 reference: Path = REFERENCE):
        self.cycle = cycle
        self.source_file = source_file
        self.reviews = {
            row["source_signature"]["sub_id"]: row
            for row in load_reviews(reference)
            if row["source_file"] == source_file and (cycle is None or row["cycle"] == cycle)
        }
        self.originals = {row["retained_original_sub_id"]: row for row in self.reviews.values()}
        self.watched_ids = set(self.reviews) | set(self.originals)
        self.seen_exclusions: set[str] = set()
        self.original_counts: dict[str, int] = {}
        self.original_matches: dict[str, bool] = {}

    def observe(self, frame: pd.DataFrame) -> None:
        if not self.watched_ids or frame.empty:
            return
        if "sub_id" not in frame:
            raise ValueError("Cannot validate review dependencies without source row IDs")
        selected = frame.loc[frame["sub_id"].isin(self.watched_ids)]
        if selected.empty:
            return
        if self.cycle is None:
            raise ValueError("Reviewed transaction IDs require an explicit source cycle")
        missing = set(SOURCE_FIELDS) - set(selected.columns)
        if missing:
            raise ValueError("Cannot validate review dependencies; source fields missing: " + ", ".join(sorted(missing)))
        for _, row in selected.iterrows():
            source_id = str(row["sub_id"])
            if source_id in self.reviews:
                review = self.reviews[source_id]
                if not _matches_signature(row, review["source_signature"], review["source_row_sha256"]):
                    raise ValueError(f"Stale reviewed transaction exclusion {source_id}: source signature changed")
                self.seen_exclusions.add(source_id)
            if source_id in self.originals:
                review = self.originals[source_id]
                self.original_counts[source_id] = self.original_counts.get(source_id, 0) + 1
                matches = _matches_signature(
                    row, review["retained_original_source_signature"],
                    review["retained_original_source_row_sha256"],
                )
                self.original_matches[source_id] = self.original_matches.get(source_id, True) and matches

    def finalize(self) -> dict:
        """Fail when an applied review no longer has its exact retained original."""
        for source_id in sorted(self.seen_exclusions):
            original_id = self.reviews[source_id]["retained_original_sub_id"]
            count = self.original_counts.get(original_id, 0)
            if count != 1:
                raise ValueError(
                    f"Reviewed exclusion {source_id} requires exactly one retained original "
                    f"{original_id}; found {count} in cycle {self.cycle} {self.source_file}"
                )
            if not self.original_matches[original_id]:
                raise ValueError(f"Stale retained original {original_id} for reviewed exclusion {source_id}")
        return {
            "cycle": self.cycle,
            "source_file": self.source_file,
            "reviewed_exclusions_observed": len(self.seen_exclusions),
            "retained_originals_verified": len(self.seen_exclusions),
        }


def reviewed_exclusion_mask(frame: pd.DataFrame, cycle: int | None = None, *,
                            source_file: str = "itcont.txt", reference: Path = REFERENCE) -> pd.Series:
    """Return exact reviewed rows; preserve equal-value gifts and memo adjustments."""
    mask = pd.Series(False, index=frame.index)
    if frame.empty or "sub_id" not in frame:
        return mask
    reviews = [r for r in load_reviews(reference) if r["source_file"] == source_file
               and (cycle is None or r["cycle"] == cycle)]
    by_id = {r["source_signature"]["sub_id"]: r for r in reviews}
    if not by_id:
        return mask
    candidates = frame["sub_id"].isin(by_id)
    if not candidates.any():
        return mask
    if cycle is None:
        raise ValueError("Reviewed transaction IDs require an explicit source cycle")
    missing = set(SOURCE_FIELDS) - set(frame.columns)
    if missing:
        raise ValueError("Cannot validate reviewed exclusion; source fields missing: " + ", ".join(sorted(missing)))
    for position in candidates.to_numpy().nonzero()[0]:
        row = frame.iloc[position]
        review = by_id[row["sub_id"]]
        if not _matches_signature(row, review["source_signature"], review["source_row_sha256"]):
            raise ValueError(f"Stale reviewed transaction exclusion {row['sub_id']}: source signature changed")
        mask.iloc[position] = True
    return mask
