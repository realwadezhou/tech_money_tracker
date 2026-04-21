from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone

import pandas as pd
from pandas.errors import EmptyDataError

from pipeline.common.paths import (
    company_curated_path,
    lda_year_derived_dir,
    lda_year_interim_dir,
)


COMPANY_SUFFIX_TOKENS = {
    "INC",
    "INCORPORATED",
    "LLC",
    "L L C",
    "LTD",
    "LIMITED",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
    "LP",
    "L P",
    "LLP",
    "L L P",
    "PLC",
    "P L C",
    "HOLDINGS",
    "HOLDING",
    "GROUP",
    "GROUPS",
}


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.upper().replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _strip_company_suffixes(value: str) -> str:
    tokens = value.split()
    while tokens and tokens[-1] in COMPANY_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens).strip()


def _compact_text(value: str) -> str:
    return value.replace(" ", "")


def _contains_digit(value: str) -> bool:
    return any(char.isdigit() for char in value)


def _eligible_for_contains(alias: str) -> bool:
    compact = _compact_text(alias)
    return len(compact) >= 6 or _contains_digit(compact)


def _confidence_label(score: int) -> str:
    if score >= 95:
        return "high"
    if score >= 80:
        return "medium"
    return "low"


def _read_csv(year: int, filename: str, **kwargs) -> pd.DataFrame:
    path = lda_year_interim_dir(year) / filename
    try:
        return pd.read_csv(path, **kwargs)
    except EmptyDataError:
        return pd.DataFrame()


def _load_tech_aliases() -> pd.DataFrame:
    aliases = pd.read_csv(company_curated_path(), dtype="string", na_filter=False)
    aliases = aliases[aliases["include"].str.upper() == "TRUE"].copy()

    for column in ["employer", "matched_searches", "canonical_name", "sector", "notes"]:
        if column not in aliases.columns:
            aliases[column] = ""

    alias_rows: list[dict[str, str]] = []
    alias_sources = {
        "employer": "employer",
        "matched_searches": "matched_searches",
        "canonical_name": "canonical_name",
    }
    for row in aliases.to_dict(orient="records"):
        for field, alias_source in alias_sources.items():
            alias_value = row.get(field, "")
            if not alias_value:
                continue
            alias_rows.append(
                {
                    "matched_alias": alias_value,
                    "alias_source": alias_source,
                    "canonical_name": row.get("canonical_name", ""),
                    "sector": row.get("sector", ""),
                    "source_employer": row.get("employer", ""),
                    "source_notes": row.get("notes", ""),
                }
            )

    alias_df = pd.DataFrame(alias_rows).drop_duplicates().copy()
    alias_df["alias_norm"] = alias_df["matched_alias"].map(_normalize_text)
    alias_df = alias_df[alias_df["alias_norm"] != ""].copy()
    alias_df["alias_core"] = alias_df["alias_norm"].map(_strip_company_suffixes)
    alias_df["alias_compact"] = alias_df["alias_norm"].map(_compact_text)
    alias_df["alias_core_compact"] = alias_df["alias_core"].map(_compact_text)
    alias_df["contains_ok"] = alias_df["alias_norm"].map(_eligible_for_contains)
    alias_df["alias_length"] = alias_df["alias_compact"].str.len()
    alias_df = alias_df.sort_values(
        ["alias_length", "alias_source", "matched_alias"],
        ascending=[False, True, True],
    ).reset_index(drop=True)
    return alias_df


def _summarize_clients(year: int) -> pd.DataFrame:
    clients = _read_csv(year, "clients.csv", dtype={"client_api_id": "Int64"})
    filings = _read_csv(year, "filings.csv")
    if clients.empty:
        return pd.DataFrame()

    summary = (
        filings.groupby(["client_api_id", "client_name"], dropna=False)
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_registrants=("registrant_api_id", "nunique"),
            income_sum=("income", "sum"),
            expenses_sum=("expenses", "sum"),
            first_posted=("dt_posted", "min"),
            last_posted=("dt_posted", "max"),
        )
        .reset_index()
    )
    summary["income_sum"] = summary["income_sum"].fillna(0.0)
    summary["expenses_sum"] = summary["expenses_sum"].fillna(0.0)
    summary["total_reported_spend"] = summary["income_sum"] + summary["expenses_sum"]

    base = clients[["client_api_id", "client_name", "general_description", "state", "country"]].drop_duplicates()
    result = base.merge(summary, on=["client_api_id", "client_name"], how="left")
    result["entity_type"] = "client"
    result["entity_id"] = result["client_api_id"]
    result["entity_name"] = result["client_name"]
    result["entity_description"] = result["general_description"]
    result["entity_state"] = result["state"]
    result["entity_country"] = result["country"]
    result["n_filings"] = result["n_filings"].fillna(0).astype(int)
    result["n_counterparties"] = result["n_registrants"].fillna(0).astype(int)
    result["counterparty_label"] = "registrants"
    result["total_reported_spend"] = result["total_reported_spend"].fillna(0.0)
    return result[
        [
            "entity_type",
            "entity_id",
            "entity_name",
            "entity_description",
            "entity_state",
            "entity_country",
            "n_filings",
            "n_counterparties",
            "counterparty_label",
            "total_reported_spend",
            "first_posted",
            "last_posted",
        ]
    ].copy()


def _summarize_registrants(year: int) -> pd.DataFrame:
    registrants = _read_csv(year, "registrants.csv", dtype={"registrant_api_id": "Int64"})
    filings = _read_csv(year, "filings.csv")
    if registrants.empty:
        return pd.DataFrame()

    summary = (
        filings.groupby(["registrant_api_id", "registrant_name"], dropna=False)
        .agg(
            n_filings=("filing_uuid", "nunique"),
            n_clients=("client_api_id", "nunique"),
            income_sum=("income", "sum"),
            expenses_sum=("expenses", "sum"),
            first_posted=("dt_posted", "min"),
            last_posted=("dt_posted", "max"),
        )
        .reset_index()
    )
    summary["income_sum"] = summary["income_sum"].fillna(0.0)
    summary["expenses_sum"] = summary["expenses_sum"].fillna(0.0)
    summary["total_reported_spend"] = summary["income_sum"] + summary["expenses_sum"]

    base = registrants[
        ["registrant_api_id", "registrant_name", "description", "state", "country"]
    ].drop_duplicates()
    result = base.merge(summary, on=["registrant_api_id", "registrant_name"], how="left")
    result["entity_type"] = "registrant"
    result["entity_id"] = result["registrant_api_id"]
    result["entity_name"] = result["registrant_name"]
    result["entity_description"] = result["description"]
    result["entity_state"] = result["state"]
    result["entity_country"] = result["country"]
    result["n_filings"] = result["n_filings"].fillna(0).astype(int)
    result["n_counterparties"] = result["n_clients"].fillna(0).astype(int)
    result["counterparty_label"] = "clients"
    result["total_reported_spend"] = result["total_reported_spend"].fillna(0.0)
    return result[
        [
            "entity_type",
            "entity_id",
            "entity_name",
            "entity_description",
            "entity_state",
            "entity_country",
            "n_filings",
            "n_counterparties",
            "counterparty_label",
            "total_reported_spend",
            "first_posted",
            "last_posted",
        ]
    ].copy()


def _build_entities(year: int) -> pd.DataFrame:
    clients = _summarize_clients(year)
    registrants = _summarize_registrants(year)
    frames = [frame for frame in [clients, registrants] if not frame.empty]
    if not frames:
        return pd.DataFrame()

    entities = pd.concat(frames, ignore_index=True)
    entities["entity_name_norm"] = entities["entity_name"].map(_normalize_text)
    entities["entity_name_core"] = entities["entity_name_norm"].map(_strip_company_suffixes)
    entities["entity_name_compact"] = entities["entity_name_norm"].map(_compact_text)
    entities["entity_name_core_compact"] = entities["entity_name_core"].map(_compact_text)
    return entities


def _match_row(entity: pd.Series, alias: pd.Series) -> dict[str, object] | None:
    alias_norm = alias["alias_norm"]
    alias_core = alias["alias_core"]
    entity_norm = entity["entity_name_norm"]
    entity_core = entity["entity_name_core"]

    match_type = None
    match_score = 0

    if entity_norm == alias_norm:
        match_type = "exact_name"
        match_score = 100
    elif alias_core and entity_core == alias_core:
        match_type = "exact_core_name"
        match_score = 96
    elif alias_norm and alias["contains_ok"] and alias_norm in entity_norm:
        match_type = "phrase_in_name"
        match_score = 86
    elif alias_core and alias["contains_ok"] and alias_core in entity_core:
        match_type = "phrase_in_core_name"
        match_score = 82

    if not match_type:
        return None

    return {
        "entity_type": entity["entity_type"],
        "entity_id": entity["entity_id"],
        "entity_name": entity["entity_name"],
        "entity_description": entity["entity_description"],
        "entity_state": entity["entity_state"],
        "entity_country": entity["entity_country"],
        "n_filings": entity["n_filings"],
        "n_counterparties": entity["n_counterparties"],
        "counterparty_label": entity["counterparty_label"],
        "total_reported_spend": entity["total_reported_spend"],
        "first_posted": entity["first_posted"],
        "last_posted": entity["last_posted"],
        "entity_name_norm": entity_norm,
        "entity_name_core": entity_core,
        "match_score": match_score,
        "match_confidence": _confidence_label(match_score),
        "match_type": match_type,
        "matched_alias": alias["matched_alias"],
        "alias_source": alias["alias_source"],
        "canonical_name": alias["canonical_name"],
        "sector": alias["sector"],
        "source_employer": alias["source_employer"],
        "source_notes": alias["source_notes"],
        "review_status": "needs_review",
        "analyst_notes": "",
    }


def build_tech_entity_matches(year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    entities = _build_entities(year)
    aliases = _load_tech_aliases()
    if entities.empty or aliases.empty:
        empty = pd.DataFrame()
        return empty, empty

    exact_norm_lookup: dict[str, list[pd.Series]] = {}
    exact_core_lookup: dict[str, list[pd.Series]] = {}
    contains_aliases: list[pd.Series] = []

    for alias in aliases.to_dict(orient="records"):
        alias_series = pd.Series(alias)
        exact_norm_lookup.setdefault(alias["alias_norm"], []).append(alias_series)
        if alias["alias_core"]:
            exact_core_lookup.setdefault(alias["alias_core"], []).append(alias_series)
        if alias["contains_ok"]:
            contains_aliases.append(alias_series)

    match_rows: list[dict[str, object]] = []

    for entity in entities.to_dict(orient="records"):
        entity_series = pd.Series(entity)
        seen_keys: set[tuple[object, ...]] = set()

        candidate_aliases: list[pd.Series] = []
        candidate_aliases.extend(exact_norm_lookup.get(entity["entity_name_norm"], []))
        candidate_aliases.extend(exact_core_lookup.get(entity["entity_name_core"], []))

        for alias_series in contains_aliases:
            alias_norm = alias_series["alias_norm"]
            alias_core = alias_series["alias_core"]
            if alias_norm and alias_norm in entity["entity_name_norm"]:
                candidate_aliases.append(alias_series)
            elif alias_core and alias_core in entity["entity_name_core"]:
                candidate_aliases.append(alias_series)

        for alias_series in candidate_aliases:
            match = _match_row(entity_series, alias_series)
            if not match:
                continue
            dedupe_key = (
                match["entity_type"],
                match["entity_id"],
                match["matched_alias"],
                match["alias_source"],
                match["match_type"],
            )
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            match_rows.append(match)

    detailed = pd.DataFrame(match_rows)
    if detailed.empty:
        return detailed, detailed

    detailed = detailed.sort_values(
        ["entity_type", "match_score", "total_reported_spend", "entity_name", "matched_alias"],
        ascending=[True, False, False, True, True],
    ).reset_index(drop=True)

    review = (
        detailed.sort_values(
            ["entity_type", "entity_id", "match_score", "total_reported_spend", "matched_alias"],
            ascending=[True, True, False, False, True],
        )
        .groupby(
            [
                "entity_type",
                "entity_id",
                "entity_name",
                "entity_description",
                "entity_state",
                "entity_country",
                "n_filings",
                "n_counterparties",
                "counterparty_label",
                "total_reported_spend",
                "first_posted",
                "last_posted",
            ],
            dropna=False,
        )
        .agg(
            best_match_score=("match_score", "max"),
            best_match_confidence=("match_confidence", "first"),
            best_match_type=("match_type", "first"),
            best_alias=("matched_alias", "first"),
            best_alias_source=("alias_source", "first"),
            canonical_name=("canonical_name", "first"),
            sector=("sector", "first"),
            n_candidate_matches=("matched_alias", "count"),
            candidate_aliases=("matched_alias", lambda values: " | ".join(dict.fromkeys(values))),
        )
        .reset_index()
    )
    review["review_status"] = "needs_review"
    review["analyst_notes"] = ""
    review = review.sort_values(
        ["entity_type", "best_match_score", "total_reported_spend", "entity_name"],
        ascending=[True, False, False, True],
    ).reset_index(drop=True)

    return detailed, review


def build_year(year: int) -> dict[str, object]:
    detailed, review = build_tech_entity_matches(year)
    out_dir = lda_year_derived_dir(year)
    out_dir.mkdir(parents=True, exist_ok=True)

    outputs: dict[str, pd.DataFrame] = {
        "tech_entity_match_candidates.csv": detailed,
        "tech_entity_review.csv": review,
        "tech_client_review.csv": review[review["entity_type"] == "client"].copy()
        if not review.empty
        else pd.DataFrame(),
        "tech_registrant_review.csv": review[review["entity_type"] == "registrant"].copy()
        if not review.empty
        else pd.DataFrame(),
    }

    manifest = {
        "year": year,
        "built_at_utc": _iso_utc_now(),
        "source_lookup_path": str(company_curated_path()),
        "note": (
            "These outputs are a project-curated, review-oriented classification layer built "
            "from the existing tech employer alias list. They are not complete or authoritative."
        ),
        "outputs": {},
    }

    for filename, frame in outputs.items():
        frame.to_csv(out_dir / filename, index=False)
        manifest["outputs"][filename] = {"rows": int(len(frame))}

    (out_dir / "tech_overlay_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a review-oriented LDA tech-entity overlay from the existing tech employer lookup.",
    )
    parser.add_argument("years", nargs="+", type=int, help="LDA filing years to process")
    args = parser.parse_args()

    manifests = [build_year(year) for year in args.years]
    print(json.dumps(manifests, indent=2))


if __name__ == "__main__":
    main()
