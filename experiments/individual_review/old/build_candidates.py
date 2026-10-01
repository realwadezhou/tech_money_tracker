"""
Generate experimental individual-donor review candidates.

All outputs stay in this directory:
    candidates.csv
    review_queue.csv

This script is intentionally isolated from the production pipeline. It reads
production data as input, but does not import production modules or write to
production data directories.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
import unicodedata
from collections.abc import Iterable
from pathlib import Path

import pandas as pd


EXPERIMENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EXPERIMENT_DIR.parents[1]

CANDIDATES_PATH = EXPERIMENT_DIR / "candidates.csv"
REVIEW_QUEUE_PATH = EXPERIMENT_DIR / "review_queue.csv"
DECISIONS_PATH = EXPERIMENT_DIR / "curated_individuals.csv"
STATE_DIR = EXPERIMENT_DIR / "state"
RUN_METRICS_PATH = STATE_DIR / "last_run_metrics.json"

COMPANY_CURATED_PATH = PROJECT_ROOT / "data" / "reference" / "companies" / "curated.csv"
MANUAL_SUMMARY_PATH = PROJECT_ROOT / "manual_tagging" / "top_donors_summary.csv"
MANUAL_VARIANTS_PATH = PROJECT_ROOT / "manual_tagging" / "top_donors_variants.csv"
FEC_INTERIM_ROOT = PROJECT_ROOT / "data" / "fec" / "interim"


ITCONT_COLS = [
    "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi", "image_num",
    "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_dt", "transaction_amt",
    "other_id", "tran_id", "file_num", "memo_cd", "memo_text", "sub_id",
]

RAW_USECOLS = [
    "cmte_id", "transaction_tp", "entity_tp", "name", "city", "state",
    "zip_code", "employer", "occupation", "transaction_amt", "memo_cd",
]

INCLUDE_TYPES = {
    "10", "15", "15E", "15C", "11", "30", "31", "32", "30E", "31E", "32E",
    "30T", "31T", "32T", "42Y", "41Y",
}
REFUND_TYPES = {"22Y", "21Y"}
MEMO_X_EXCLUDE_TYPES = {"15E"}

FINAL_STATUSES = {
    "reviewed_not_tech",
    "tech_figure",
    "same_identity",
    "not_individual",
}

THRESHOLD_RECALL_WARNING = (
    "IMPORTANT RECALL LIMITATION: this review queue is seeded from exact raw "
    "aliases that clear the major-donor threshold, plus tech-employer and "
    "possible-tech context hits. A salient person can be missed if no exact "
    "raw alias reaches the threshold and the raw rows do not carry tech context."
)

SUFFIXES = {
    "JR", "SR", "II", "III", "IV", "V",
    "MR", "MRS", "MS", "MISS", "DR",
    "MD", "M.D", "PHD", "PH.D", "ESQ",
}
NICKNAMES = {
    "BEN": "BENJAMIN",
    "BOB": "ROBERT",
    "ROB": "ROBERT",
    "BILL": "WILLIAM",
    "WILL": "WILLIAM",
    "MIKE": "MICHAEL",
    "JIM": "JAMES",
    "JIMMY": "JAMES",
    "DAVE": "DAVID",
    "DAN": "DANIEL",
    "STEVE": "STEVEN",
    "MATT": "MATTHEW",
    "CHRIS": "CHRISTOPHER",
    "ALEX": "ALEXANDER",
    "KATE": "KATHERINE",
    "KATIE": "KATHERINE",
    "KATHY": "KATHERINE",
    "LIZ": "ELIZABETH",
    "BETH": "ELIZABETH",
    "JON": "JONATHAN",
    "MARC": "MARC",
    "MARK": "MARC",
}

POSSIBLE_TECH_RE = re.compile(
    r"\b(?:"
    r"AI|ARTIFICIAL INTELLIGENCE|SOFTWARE|ENGINEER|ENGINEERING|FOUNDER|"
    r"CO[- ]?FOUNDER|VENTURE|VC|INVESTOR|STARTUP|CRYPTO|BLOCKCHAIN|"
    r"TECH|TECHNOLOGY|PRODUCT|CEO|CTO|CIO|CISO|DATA SCIENTIST|PROGRAMMER|"
    r"DEVELOPER|MACHINE LEARNING|COMPUTER|SEMICONDUCTOR"
    r")\b",
    re.IGNORECASE,
)


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Z0-9\s,.'&-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def simple_tokens(value: object) -> list[str]:
    text = normalize_text(value)
    return [token for token in re.split(r"[^A-Z0-9]+", text) if token]


def fingerprint(value: object) -> str:
    tokens = sorted(set(simple_tokens(value)))
    return " ".join(tokens)


def parse_fec_name(name: object) -> dict[str, str]:
    text = normalize_text(name)
    if not text:
        return {
            "last_name": "",
            "first_name": "",
            "middle_initial": "",
            "first_initial": "",
            "canonical_first": "",
            "name_fingerprint": "",
        }

    if "," in text:
        last_part, rest = text.split(",", 1)
        last_tokens = simple_tokens(last_part)
        given_tokens = simple_tokens(rest)
    else:
        tokens = simple_tokens(text)
        last_tokens = tokens[-1:]
        given_tokens = tokens[:-1]

    given_tokens = [token for token in given_tokens if token not in SUFFIXES]
    last_name = " ".join(token for token in last_tokens if token not in SUFFIXES)
    first_name = given_tokens[0] if given_tokens else ""
    middle_initial = ""
    if len(given_tokens) > 1 and given_tokens[1]:
        middle_initial = given_tokens[1][0]

    canonical_first = NICKNAMES.get(first_name, first_name)
    return {
        "last_name": last_name,
        "first_name": first_name,
        "middle_initial": middle_initial,
        "first_initial": first_name[:1],
        "canonical_first": canonical_first,
        "name_fingerprint": fingerprint(text),
    }


def display_name_from_fec(name: object) -> str:
    parsed = parse_fec_name(name)
    first = parsed["first_name"].title()
    last = parsed["last_name"].title()
    if first and last:
        return f"{first} {last}"
    return str(name).strip()


def unique_join(values: Iterable[object], limit: int = 18) -> str:
    cleaned = []
    seen = set()
    for value in values:
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        if text not in seen:
            cleaned.append(text)
            seen.add(text)
    cleaned = sorted(cleaned)
    if len(cleaned) > limit:
        return "; ".join(cleaned[:limit]) + f"; ... (+{len(cleaned) - limit})"
    return "; ".join(cleaned)


def unique_join_split(values: Iterable[object], limit: int = 18) -> str:
    """Merge values that may already be semicolon-delimited summaries."""
    expanded: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        expanded.extend(part.strip() for part in text.split(";") if part.strip())
    return unique_join(expanded, limit=limit)


def source_manifest(path: Path) -> dict[str, object]:
    stat = path.stat()
    return {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def manifest_matches(path: Path, manifest_path: Path, extra: dict[str, object] | None = None) -> bool:
    if not path.exists() or not manifest_path.exists():
        return False
    try:
        current = source_manifest(path)
        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if saved.get("source") != current:
        return False
    if extra is not None and saved.get("extra") != extra:
        return False
    return True


def write_manifest(path: Path, manifest_path: Path, extra: dict[str, object] | None = None) -> None:
    manifest_path.write_text(
        json.dumps({"source": source_manifest(path), "extra": extra or {}}, indent=2),
        encoding="utf-8",
    )


def name_universe_path(cycle: int) -> Path:
    return STATE_DIR / f"name_universe_{cycle}.csv"


def name_universe_manifest_path(cycle: int) -> Path:
    return STATE_DIR / f"name_universe_{cycle}.manifest.json"


def tuple_evidence_path(cycle: int) -> Path:
    return STATE_DIR / f"tuple_evidence_{cycle}.csv"


def tuple_evidence_manifest_path(cycle: int) -> Path:
    return STATE_DIR / f"tuple_evidence_{cycle}.manifest.json"


def candidate_signature(candidate_names: Iterable[str]) -> str:
    payload = "\n".join(sorted({str(name) for name in candidate_names if str(name).strip()}))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def now_seconds() -> float:
    return time.perf_counter()


def elapsed_seconds(start: float) -> float:
    return round(time.perf_counter() - start, 2)


def format_variant_evidence(group: pd.DataFrame, limit: int = 24) -> str:
    """Human-readable tuple evidence for one raw contributor_name."""
    amount_col = "net_usd" if "net_usd" in group.columns else "net_total"
    sort_group = group.copy()
    sort_group[amount_col] = pd.to_numeric(
        sort_group[amount_col], errors="coerce"
    ).fillna(0.0)
    sort_group = sort_group.sort_values(amount_col, ascending=False)

    lines = []
    for _, row in sort_group.head(limit).iterrows():
        parts = [
            f"${float(row.get(amount_col, 0.0)):,.0f}",
            str(row.get("states", row.get("state", ""))).strip(),
            str(row.get("employer", "")).strip(),
            str(row.get("occupation", "")).strip(),
        ]
        lines.append(" | ".join(part for part in parts if part))
    if len(sort_group) > limit:
        lines.append(f"... (+{len(sort_group) - limit} more tuples)")
    return "\n".join(lines)


def load_company_lookup() -> dict[str, str]:
    if not COMPANY_CURATED_PATH.exists():
        return {}
    companies = pd.read_csv(COMPANY_CURATED_PATH, dtype="string", na_filter=False)
    companies = companies[companies["include"].str.upper() == "TRUE"].copy()
    companies["employer_upper"] = companies["employer"].map(normalize_text)
    companies = companies.drop_duplicates(subset=["employer_upper"])
    return dict(zip(companies["employer_upper"], companies["canonical_name"]))


def load_final_reviewed_keys() -> set[str]:
    if not DECISIONS_PATH.exists():
        return set()
    decisions = pd.read_csv(DECISIONS_PATH, dtype="string", na_filter=False)
    if "decision_status" not in decisions.columns or "review_key" not in decisions.columns:
        return set()
    final = decisions["decision_status"].str.strip().str.lower().isin(FINAL_STATUSES)
    return set(decisions.loc[final, "review_key"].str.strip())


def add_name_keys(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = [
        "last_name", "first_name", "middle_initial", "first_initial",
        "canonical_first", "name_fingerprint", "review_key",
        "display_name_suggestion", "strong_name_key", "loose_name_key",
    ]
    base = df.drop(columns=[col for col in key_cols if col in df.columns]).copy()
    parsed = base["contributor_name"].map(parse_fec_name).apply(pd.Series)
    out = pd.concat([base.reset_index(drop=True), parsed.reset_index(drop=True)], axis=1)
    if "normalized_name" not in out.columns:
        out["normalized_name"] = out["contributor_name"].map(normalize_text)
    out["review_key"] = out["normalized_name"].fillna("").where(
        out["normalized_name"].fillna("").ne(""),
        out["contributor_name"].map(normalize_text),
    )
    out["display_name_suggestion"] = out["contributor_name"].map(display_name_from_fec)
    out["strong_name_key"] = (
        out["last_name"].fillna("") + "|" + out["canonical_first"].fillna("")
    )
    out["loose_name_key"] = (
        out["last_name"].fillna("") + "|" + out["first_initial"].fillna("")
    )
    out.loc[out["last_name"].eq("") | out["first_initial"].eq(""), "loose_name_key"] = ""
    out.loc[out["last_name"].eq("") | out["canonical_first"].eq(""), "strong_name_key"] = ""
    return out


def add_cluster_suggestions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["suggested_cluster_size"] = 1
    out["suggested_cluster_members"] = ""

    usable = out[out["strong_name_key"].ne("")].copy()
    clusters = usable.groupby("strong_name_key")["contributor_name"].apply(
        lambda values: sorted(set(values))
    )
    cluster_map = {
        key: names for key, names in clusters.items()
        if 1 < len(names) <= 20
    }
    out["suggested_cluster_members"] = out["strong_name_key"].map(
        lambda key: "; ".join(cluster_map.get(key, []))
    )
    out["suggested_cluster_size"] = out["strong_name_key"].map(
        lambda key: len(cluster_map.get(key, [])) if key in cluster_map else 1
    )
    return out


def review_reasons(row: pd.Series) -> str:
    reasons = []
    if bool(row.get("is_major_donor", False)):
        reasons.append("major_donor")
    if bool(row.get("is_tech_linked", False)):
        reasons.append("tech_linked_employer")
    if bool(row.get("is_possible_tech", False)):
        reasons.append("possible_tech_context")
    if bool(row.get("is_name_key_expansion", False)):
        reasons.append("name_key_expansion")
    return "; ".join(reasons)


def build_from_manual(min_major_net: float) -> pd.DataFrame:
    if not MANUAL_SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Missing {MANUAL_SUMMARY_PATH}")

    summary = pd.read_csv(MANUAL_SUMMARY_PATH, dtype="string", na_filter=False)
    for col in ["net_total_usd", "n_contributions", "n_variants", "n_committees"]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0)

    variants = pd.DataFrame()
    if MANUAL_VARIANTS_PATH.exists():
        variants = pd.read_csv(MANUAL_VARIANTS_PATH, dtype="string", na_filter=False)
        variants["net_usd"] = pd.to_numeric(variants["net_usd"], errors="coerce").fillna(0)

    tech_lookup = load_company_lookup()
    if len(variants):
        variants["employer_upper"] = variants["employer"].map(normalize_text)
        variants["tech_company_hint"] = variants["employer_upper"].map(tech_lookup).fillna("")
        variant_evidence = (
            variants.groupby("contributor_name")
            .apply(format_variant_evidence, include_groups=False)
            .rename("variant_evidence")
            .reset_index()
        )
        tech_by_name = (
            variants[variants["tech_company_hint"].ne("")]
            .groupby("contributor_name")
            .agg(
                tech_companies=("tech_company_hint", unique_join),
                tech_linked_net=("net_usd", "sum"),
            )
            .reset_index()
        )
        summary = summary.merge(tech_by_name, on="contributor_name", how="left")
        summary = summary.merge(variant_evidence, on="contributor_name", how="left")
    else:
        summary["tech_companies"] = ""
        summary["tech_linked_net"] = 0.0
        summary["variant_evidence"] = ""

    summary["tech_companies"] = summary.get("tech_companies", "").fillna("")
    summary["tech_linked_net"] = pd.to_numeric(
        summary.get("tech_linked_net", 0), errors="coerce"
    ).fillna(0)
    summary["possible_tech_text"] = (
        summary["employers"].fillna("") + " " + summary["occupations"].fillna("")
    )
    summary["is_major_donor"] = summary["net_total_usd"].abs() >= min_major_net
    summary["is_tech_linked"] = summary["tech_companies"].ne("")
    summary["is_possible_tech"] = summary["possible_tech_text"].str.contains(
        POSSIBLE_TECH_RE, na=False
    )
    summary["review_reasons"] = summary.apply(review_reasons, axis=1)
    summary["normalized_name"] = summary["contributor_name"].map(normalize_text)
    summary = summary.rename(
        columns={
            "net_total_usd": "net_total",
            "n_variants": "variant_count",
        }
    )
    summary["cycles"] = "2024-manual"
    summary["gross_positive"] = ""

    columns = [
        "contributor_name", "normalized_name", "net_total", "gross_positive",
        "n_contributions", "variant_count", "n_committees", "states",
        "employers", "occupations", "variant_evidence", "cycles",
        "tech_companies", "tech_linked_net", "is_major_donor",
        "is_tech_linked", "is_possible_tech", "review_reasons",
    ]
    return summary[columns].copy()


def raw_itcont_path(cycle: int) -> Path:
    suffix = str(cycle)[2:]
    return FEC_INTERIM_ROOT / str(cycle) / f"indiv{suffix}" / "itcont.txt"


def read_raw_cycle(cycle: int, chunksize: int) -> Iterable[pd.DataFrame]:
    path = raw_itcont_path(cycle)
    try:
        exists = path.exists()
    except PermissionError as exc:
        raise PermissionError(f"Cannot read {path}: {exc}") from exc
    if not exists:
        print(f"Skipping {cycle}: missing {path}", flush=True)
        return []
    return pd.read_csv(
        path,
        sep="|",
        header=None,
        names=ITCONT_COLS,
        dtype="string",
        na_filter=False,
        usecols=RAW_USECOLS,
        chunksize=chunksize,
        low_memory=False,
    )


def prepare_raw_contribution_chunk(chunk: pd.DataFrame, cycle: int, tech_lookup: dict[str, str]) -> pd.DataFrame:
    all_types = INCLUDE_TYPES | REFUND_TYPES
    df = chunk[chunk["transaction_tp"].isin(all_types)].copy()
    df = df[(df["memo_cd"] != "X") | (~df["transaction_tp"].isin(MEMO_X_EXCLUDE_TYPES))]
    df = df[df["entity_tp"].isin(["", "IND", "CAN"])].copy()
    df["contributor_name"] = df["name"].astype("string").str.strip()
    df["normalized_name"] = df["contributor_name"].map(normalize_text)
    df = df[df["normalized_name"].ne("")]

    df["transaction_amt"] = pd.to_numeric(df["transaction_amt"], errors="coerce").fillna(0.0)
    df["net_amt"] = df["transaction_amt"].where(
        ~df["transaction_tp"].isin(REFUND_TYPES),
        -df["transaction_amt"],
    )
    df["gross_positive"] = df["transaction_amt"].where(df["transaction_amt"] > 0, 0.0)
    df["zip5"] = df["zip_code"].astype(str).str.extract(r"(\d{5})", expand=False).fillna("")
    df["employer_upper"] = df["employer"].map(normalize_text)
    df["tech_company_hint"] = df["employer_upper"].map(tech_lookup).fillna("")
    df["tech_linked_amt"] = df["net_amt"].where(df["tech_company_hint"].ne(""), 0.0)
    df["cycle"] = str(cycle)
    df["is_possible_tech_row"] = (
        df["employer"].fillna("").astype(str) + " " + df["occupation"].fillna("").astype(str)
    ).str.contains(POSSIBLE_TECH_RE, na=False)
    return df


def summarize_raw_chunk(chunk: pd.DataFrame, cycle: int, tech_lookup: dict[str, str]) -> pd.DataFrame:
    df = prepare_raw_contribution_chunk(chunk, cycle, tech_lookup)
    return (
        df.groupby(
            [
                "contributor_name", "normalized_name", "state", "zip5",
                "employer", "occupation", "tech_company_hint", "cycle",
            ],
            dropna=False,
        )
        .agg(
            net_total=("net_amt", "sum"),
            gross_positive=("gross_positive", "sum"),
            n_contributions=("net_amt", "size"),
            n_committees=("cmte_id", "nunique"),
        )
        .reset_index()
    )


def summarize_name_universe_chunk(chunk: pd.DataFrame, cycle: int, tech_lookup: dict[str, str]) -> pd.DataFrame:
    df = prepare_raw_contribution_chunk(chunk, cycle, tech_lookup)
    return (
        df.groupby(["contributor_name", "normalized_name"], dropna=False)
        .agg(
            net_total=("net_amt", "sum"),
            gross_positive=("gross_positive", "sum"),
            n_contributions=("net_amt", "size"),
            n_committees=("cmte_id", "nunique"),
            states=("state", unique_join),
            employers=("employer", unique_join),
            occupations=("occupation", unique_join),
            tech_companies=("tech_company_hint", unique_join),
            tech_linked_net=("tech_linked_amt", "sum"),
            is_possible_tech=("is_possible_tech_row", "max"),
        )
        .reset_index()
        .assign(cycles=str(cycle))
    )


def combine_name_universe(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    if not chunks:
        return pd.DataFrame()
    combined = pd.concat(chunks, ignore_index=True)
    out = (
        combined.groupby(["contributor_name", "normalized_name"], dropna=False)
        .agg(
            net_total=("net_total", "sum"),
            gross_positive=("gross_positive", "sum"),
            n_contributions=("n_contributions", "sum"),
            n_committees=("n_committees", "sum"),
            states=("states", unique_join_split),
            employers=("employers", unique_join_split),
            occupations=("occupations", unique_join_split),
            cycles=("cycles", unique_join_split),
            tech_companies=("tech_companies", unique_join_split),
            tech_linked_net=("tech_linked_net", "sum"),
            is_possible_tech=("is_possible_tech", "max"),
        )
        .reset_index()
    )
    out["is_tech_linked"] = out["tech_companies"].ne("")
    out["variant_count"] = 0
    out["variant_evidence"] = ""
    return out


def build_name_universe_for_cycle(
    cycle: int,
    chunksize: int,
    tech_lookup: dict[str, str],
    force_state: bool,
) -> tuple[pd.DataFrame, dict[str, object]]:
    path = raw_itcont_path(cycle)
    cache_path = name_universe_path(cycle)
    manifest_path = name_universe_manifest_path(cycle)
    if not force_state and manifest_matches(path, manifest_path):
        print(f"Using cached name universe for {cycle}: {cache_path.relative_to(PROJECT_ROOT)}", flush=True)
        cached = pd.read_csv(cache_path, dtype="string", na_filter=False)
        return cached, {
            "cycle": cycle,
            "stage": "name_universe",
            "used_cache": True,
            "rows": int(len(cached)),
            "seconds": 0.0,
        }

    print(f"Building name universe for {cycle}...", flush=True)
    started = now_seconds()
    chunks: list[pd.DataFrame] = []
    reader = read_raw_cycle(cycle, chunksize)
    for i, chunk in enumerate(reader, start=1):
        chunks.append(summarize_name_universe_chunk(chunk, cycle, tech_lookup))
        if i % 10 == 0:
            print(f"  {cycle}: scanned {i * chunksize:,} input rows", flush=True)

    universe = combine_name_universe(chunks)
    if universe.empty:
        print(f"  {cycle}: no usable individual contribution rows found", flush=True)
        return universe, {
            "cycle": cycle,
            "stage": "name_universe",
            "used_cache": False,
            "rows": 0,
            "seconds": elapsed_seconds(started),
        }

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    universe.to_csv(cache_path, index=False, quoting=csv.QUOTE_MINIMAL)
    write_manifest(path, manifest_path)
    print(f"  cached {len(universe):,} raw names for {cycle}", flush=True)
    return universe, {
        "cycle": cycle,
        "stage": "name_universe",
        "used_cache": False,
        "rows": int(len(universe)),
        "seconds": elapsed_seconds(started),
    }


def combine_cycle_universes(universes: list[pd.DataFrame]) -> pd.DataFrame:
    if not universes:
        raise RuntimeError("No raw FEC name universes were loaded.")
    combined = pd.concat(universes, ignore_index=True)
    for col in ["net_total", "gross_positive", "n_contributions", "n_committees", "tech_linked_net"]:
        combined[col] = pd.to_numeric(combined.get(col, 0), errors="coerce").fillna(0)
    combined["is_possible_tech"] = combined.get("is_possible_tech", False).astype(str).str.upper().isin(["TRUE", "1"])
    out = (
        combined.groupby(["contributor_name", "normalized_name"], dropna=False)
        .agg(
            net_total=("net_total", "sum"),
            gross_positive=("gross_positive", "sum"),
            n_contributions=("n_contributions", "sum"),
            n_committees=("n_committees", "sum"),
            states=("states", unique_join_split),
            employers=("employers", unique_join_split),
            occupations=("occupations", unique_join_split),
            cycles=("cycles", unique_join_split),
            tech_companies=("tech_companies", unique_join_split),
            tech_linked_net=("tech_linked_net", "sum"),
            is_possible_tech=("is_possible_tech", "max"),
        )
        .reset_index()
    )
    out["is_tech_linked"] = out["tech_companies"].ne("")
    out["variant_count"] = 0
    out["variant_evidence"] = ""
    return out


def select_candidate_names(donors: pd.DataFrame, min_major_net: float) -> pd.DataFrame:
    donors = add_name_keys(donors)
    donors["is_major_donor"] = donors["net_total"].abs() >= min_major_net
    donors["is_tech_linked"] = donors["tech_companies"].ne("")
    donors["is_possible_tech"] = donors["is_possible_tech"].astype(bool)

    print("Selecting salient seeds and name-key expansions...", flush=True)
    seed_mask = (
        donors["is_major_donor"] |
        donors["is_tech_linked"] |
        donors["is_possible_tech"]
    )
    seed_strong_keys = set(donors.loc[seed_mask, "strong_name_key"].dropna())
    seed_strong_keys.discard("")
    expanded_mask = donors["strong_name_key"].isin(seed_strong_keys)

    donors["is_salient_seed"] = seed_mask
    donors["is_name_key_expansion"] = expanded_mask & ~seed_mask
    candidates = donors[seed_mask | expanded_mask].copy()
    candidates["review_reasons"] = candidates.apply(review_reasons, axis=1)
    print(f"  Salient exact-name seeds: {int(seed_mask.sum()):,}", flush=True)
    print(f"  Review candidates after strong-key expansion: {len(candidates):,}", flush=True)
    return candidates


def build_tuple_evidence_for_cycle(
    cycle: int,
    candidate_names: set[str],
    signature: str,
    chunksize: int,
    tech_lookup: dict[str, str],
    force_state: bool,
) -> tuple[pd.DataFrame, dict[str, object]]:
    path = raw_itcont_path(cycle)
    cache_path = tuple_evidence_path(cycle)
    manifest_path = tuple_evidence_manifest_path(cycle)
    extra = {"candidate_signature": signature}
    if not force_state and manifest_matches(path, manifest_path, extra=extra):
        print(f"Using cached tuple evidence for {cycle}: {cache_path.relative_to(PROJECT_ROOT)}", flush=True)
        cached = pd.read_csv(cache_path, dtype="string", na_filter=False)
        return cached, {
            "cycle": cycle,
            "stage": "tuple_evidence",
            "used_cache": True,
            "rows": int(len(cached)),
            "seconds": 0.0,
        }

    print(f"Collecting tuple evidence for selected names in {cycle}...", flush=True)
    started = now_seconds()
    chunks: list[pd.DataFrame] = []
    reader = read_raw_cycle(cycle, chunksize)
    for i, chunk in enumerate(reader, start=1):
        df = prepare_raw_contribution_chunk(chunk, cycle, tech_lookup)
        df = df[df["contributor_name"].isin(candidate_names)].copy()
        if not df.empty:
            chunks.append(
                df.groupby(
                    [
                        "contributor_name", "normalized_name", "state", "zip5",
                        "employer", "occupation", "tech_company_hint", "cycle",
                    ],
                    dropna=False,
                )
                .agg(
                    net_total=("net_amt", "sum"),
                    gross_positive=("gross_positive", "sum"),
                    n_contributions=("net_amt", "size"),
                    n_committees=("cmte_id", "nunique"),
                )
                .reset_index()
            )
        if i % 10 == 0:
            print(f"  {cycle}: scanned {i * chunksize:,} input rows for tuple evidence", flush=True)

    if chunks:
        evidence = pd.concat(chunks, ignore_index=True)
        evidence = (
            evidence.groupby(
                [
                    "contributor_name", "normalized_name", "state", "zip5",
                    "employer", "occupation", "tech_company_hint", "cycle",
                ],
                dropna=False,
            )
            .agg(
                net_total=("net_total", "sum"),
                gross_positive=("gross_positive", "sum"),
                n_contributions=("n_contributions", "sum"),
                n_committees=("n_committees", "sum"),
            )
            .reset_index()
        )
    else:
        evidence = pd.DataFrame(
            columns=[
                "contributor_name", "normalized_name", "state", "zip5",
                "employer", "occupation", "tech_company_hint", "cycle",
                "net_total", "gross_positive", "n_contributions", "n_committees",
            ]
        )

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    evidence.to_csv(cache_path, index=False, quoting=csv.QUOTE_MINIMAL)
    write_manifest(path, manifest_path, extra=extra)
    print(f"  cached {len(evidence):,} selected tuple rows for {cycle}", flush=True)
    return evidence, {
        "cycle": cycle,
        "stage": "tuple_evidence",
        "used_cache": False,
        "rows": int(len(evidence)),
        "seconds": elapsed_seconds(started),
    }


def attach_tuple_evidence(candidates: pd.DataFrame, variants: pd.DataFrame) -> pd.DataFrame:
    if variants.empty:
        candidates = candidates.copy()
        candidates["variant_count"] = 0
        candidates["variant_evidence"] = ""
        return candidates

    for col in ["net_total", "gross_positive", "n_contributions", "n_committees"]:
        variants[col] = pd.to_numeric(variants[col], errors="coerce").fillna(0)

    evidence = (
        variants.groupby("contributor_name")
        .agg(
            variant_count=("employer", "size"),
            zip5s=("zip5", unique_join),
            states_evidence=("state", unique_join),
            employers_evidence=("employer", unique_join),
            occupations_evidence=("occupation", unique_join),
            cycles_evidence=("cycle", unique_join),
            tech_companies_evidence=("tech_company_hint", unique_join),
            tech_linked_net_evidence=("net_total", lambda s: s[variants.loc[s.index, "tech_company_hint"].ne("")].sum()),
        )
        .reset_index()
    )
    variant_evidence = (
        variants.groupby("contributor_name")
        .apply(format_variant_evidence, include_groups=False)
        .rename("variant_evidence")
        .reset_index()
    )
    evidence = evidence.merge(variant_evidence, on="contributor_name", how="left")

    base = candidates.drop(
        columns=[col for col in ["variant_count", "variant_evidence", "zip5s"] if col in candidates.columns]
    ).copy()
    out = base.merge(evidence, on="contributor_name", how="left")
    out["variant_count"] = pd.to_numeric(out["variant_count"], errors="coerce").fillna(0).astype(int)
    out["zip5s"] = out["zip5s"].fillna("")
    out["variant_evidence"] = out["variant_evidence"].fillna("")
    for base, evidence_col in [
        ("states", "states_evidence"),
        ("employers", "employers_evidence"),
        ("occupations", "occupations_evidence"),
        ("cycles", "cycles_evidence"),
        ("tech_companies", "tech_companies_evidence"),
    ]:
        out[base] = out[evidence_col].fillna("").where(out[evidence_col].fillna("").ne(""), out[base])
        out = out.drop(columns=[evidence_col])
    out["tech_linked_net"] = pd.to_numeric(
        out["tech_linked_net_evidence"], errors="coerce"
    ).fillna(out["tech_linked_net"])
    out = out.drop(columns=["tech_linked_net_evidence"])
    return out


def build_from_raw(
    cycles: list[int],
    min_major_net: float,
    chunksize: int,
    force_state: bool,
) -> tuple[pd.DataFrame, dict[str, object]]:
    tech_lookup = load_company_lookup()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    metrics: dict[str, object] = {
        "cycles": cycles,
        "min_major_net": min_major_net,
        "chunksize": chunksize,
        "stages": [],
    }
    total_started = now_seconds()
    universes = []
    for cycle in cycles:
        universe, stage_metrics = build_name_universe_for_cycle(
            cycle, chunksize, tech_lookup, force_state
        )
        universes.append(universe)
        metrics["stages"].append(stage_metrics)

    print("Combining cycle name universes...", flush=True)
    combine_started = now_seconds()
    donors = combine_cycle_universes(universes)
    metrics["stages"].append(
        {
            "stage": "combine_name_universes",
            "rows": int(len(donors)),
            "seconds": elapsed_seconds(combine_started),
        }
    )

    select_started = now_seconds()
    candidates = select_candidate_names(donors, min_major_net)
    metrics["stages"].append(
        {
            "stage": "select_candidates",
            "rows": int(len(candidates)),
            "seconds": elapsed_seconds(select_started),
        }
    )

    signature = candidate_signature(candidates["contributor_name"])
    candidate_names = set(candidates["contributor_name"].astype(str))
    tuple_frames = []
    for cycle in cycles:
        frame, stage_metrics = build_tuple_evidence_for_cycle(
            cycle, candidate_names, signature, chunksize, tech_lookup, force_state
        )
        tuple_frames.append(frame)
        metrics["stages"].append(stage_metrics)
    variants = pd.concat(tuple_frames, ignore_index=True) if tuple_frames else pd.DataFrame()
    attach_started = now_seconds()
    candidates = attach_tuple_evidence(candidates, variants)
    metrics["stages"].append(
        {
            "stage": "attach_tuple_evidence",
            "rows": int(len(candidates)),
            "tuple_rows": int(len(variants)),
            "seconds": elapsed_seconds(attach_started),
        }
    )
    metrics["total_seconds"] = elapsed_seconds(total_started)
    metrics["candidate_rows"] = int(len(candidates))
    metrics["candidate_signature"] = signature
    return candidates, metrics


def build_queue(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "strong_name_key" not in candidates.columns:
        candidates = add_name_keys(candidates)
    candidates = add_cluster_suggestions(candidates)
    candidates = candidates.sort_values(
        ["is_major_donor", "net_total", "is_tech_linked", "suggested_cluster_size"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    reviewed = load_final_reviewed_keys()
    review = candidates[~candidates["review_key"].isin(reviewed)].copy()
    return candidates, review


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote {len(df):,} rows to {path.relative_to(PROJECT_ROOT)}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manual", action="store_true", help="Use manual_tagging top-donor CSVs.")
    parser.add_argument("--cycle", type=int, action="append", help="FEC cycle to scan; repeatable.")
    parser.add_argument("--min-major-net", type=float, default=100_000)
    parser.add_argument("--chunksize", type=int, default=350_000)
    parser.add_argument(
        "--force-state",
        action="store_true",
        help="Rebuild cached name-universe and tuple-evidence state even if source files look unchanged.",
    )
    args = parser.parse_args()

    run_started = now_seconds()
    metrics: dict[str, object] = {"mode": "manual" if args.manual else "raw"}

    if args.manual:
        candidates = build_from_manual(args.min_major_net)
    else:
        cycles = args.cycle or [2024, 2026]
        candidates, raw_metrics = build_from_raw(
            cycles, args.min_major_net, args.chunksize, args.force_state
        )
        metrics.update(raw_metrics)

    candidates, review = build_queue(candidates)
    write_csv(candidates, CANDIDATES_PATH)
    write_csv(review, REVIEW_QUEUE_PATH)
    metrics["candidate_rows_after_queue_build"] = int(len(candidates))
    metrics["review_queue_rows"] = int(len(review))
    metrics["overall_seconds"] = elapsed_seconds(run_started)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    RUN_METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print("", flush=True)
    print("Review reasons:", flush=True)
    exploded = review.assign(
        review_reason=review["review_reasons"].str.split("; ")
    ).explode("review_reason")
    counts = exploded["review_reason"].value_counts()
    for reason, count in counts.items():
        if reason:
            print(f"  {reason}: {count:,}", flush=True)
    print("", flush=True)
    print("Run timing:", flush=True)
    if "stages" in metrics:
        for stage in metrics["stages"]:
            cycle_text = f" cycle={stage['cycle']}" if "cycle" in stage else ""
            cache_text = " cache" if stage.get("used_cache") else ""
            rows_text = f" rows={stage['rows']:,}" if "rows" in stage else ""
            tuple_rows_text = (
                f" tuple_rows={stage['tuple_rows']:,}" if "tuple_rows" in stage else ""
            )
            print(
                f"  {stage['stage']}{cycle_text}{cache_text}:{rows_text}{tuple_rows_text} "
                f"seconds={stage['seconds']}",
                flush=True,
            )
    print(f"  overall_seconds={metrics['overall_seconds']}", flush=True)
    print(f"  metrics_file={RUN_METRICS_PATH.relative_to(PROJECT_ROOT)}", flush=True)
    print("", flush=True)
    print(THRESHOLD_RECALL_WARNING, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
