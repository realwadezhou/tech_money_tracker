"""
Generate company-tagging candidates from raw FEC itcont data.

This is the crude-regex first pass. It surfaces every employer string that
matches a broad pattern for a tracked tech company, along with stats, and
writes two files:

    data/reference/companies/
        candidates.csv       — every surfaced employer + stats (overwritten)
        review_queue.csv     — candidates NOT yet in curated.csv (overwritten)

It NEVER touches curated.csv. See data/reference/companies/README.md.

Usage:
    python -m pipeline.tagging.companies                 # all available cycles
    python -m pipeline.tagging.companies --cycle 2024    # single cycle
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from pipeline.common.paths import (
    FEC_INTERIM_ROOT,
    company_candidates_path,
    company_curated_path,
    company_review_queue_path,
)
from pipeline.fec.load import ITCONT_COLS


# ── Broad regex searches ─────────────────────────────────────────────
# Intentionally wide. False positives get rejected during manual review.
# Each key is the provenance label that will be written to matched_searches.

TECH_SEARCHES: dict[str, list[str]] = {
    "google": [r"\bGOOG\b", r"\bGOOGL\b", "GOOGLE", "GOOGLE LLC", "ALPHABET", "GOOGLE CLOUD", "DEEPMIND", "GOOGLE DEEPMIND", "GOOGLE BRAIN", "YOUTUBE", "WAYMO", "VERILY"],
    "microsoft": [r"\bMSFT\b", "MICROSOFT", r"MICROSOFT CORP(?:ORATION)?", "AZURE", "LINKEDIN", "GITHUB", "MICROSOFT RESEARCH"],
    "meta": ["META PLATFORMS", "FACEBOOK", "INSTAGRAM", "WHATSAPP", "META AI", "REALITY LABS"],
    "apple": [r"\bAAPL\b", r"\bAPPLE\b", "APPLE INC", "APPLE COMPUTER"],
    "amazon": [r"\bAMZN\b", "AMAZON", r"AMAZON\.COM", "AMAZON WEB SERVICES", r"\bAWS\b", "TWITCH", "ZOOX", "KUIPER"],

    "openai": [r"OPEN\s?AI"],
    "anthropic": ["ANTHROPIC", "ANTHROPIC PBC"],
    "xai": [r"\bXAI\b", r"\bX\.AI\b", "X AI", "GROK"],
    "mistral": ["MISTRAL AI"],
    "cohere": ["COHERE AI", r"\bCOHERE\b(?!\s+HEALTH)"],
    "perplexity": ["PERPLEXITY", "PERPLEXITY AI"],
    "scale": ["SCALE AI", "SCALEAI"],
    "huggingface": [r"HUGGING[\s-]?FACE"],
    "stability": ["STABILITY AI", "STABLE DIFFUSION"],
    "runway": ["RUNWAYML", "RUNWAY AI"],
    "ssi": ["SAFE SUPERINTELLIGENCE", "SSI INC"],
    "together": ["TOGETHER AI", "TOGETHER COMPUTER"],
    "character": [r"CHARACTER\.AI", "CHARACTER AI"],
    "midjourney": ["MIDJOURNEY"],
    "elevenlabs": [r"ELEVEN\s?LABS"],
    "langchain": ["LANGCHAIN", "LANGSMITH"],
    "glean": ["GLEAN TECHNOLOGIES", r"\bGLEAN\b"],
    "anduril": ["ANDURIL", "ANDURIL INDUSTRIES"],
    "coreweave": [r"\bCRWV\b", "COREWEAVE"],
    "wandb": [r"WEIGHTS\s*&\s*BIASES", "WEIGHTS AND BIASES", r"\bWANDB\b"],
    "c3ai": [r"C3\.AI", "C3 AI", "C3AI"],
    "soundhound": ["SOUNDHOUND AI", "SOUNDHOUND"],
    "tempus": ["TEMPUS AI"],
    "uipath": ["UIPATH"],
    "groq": [r"\bGROQ\b"],
    "sambanova": ["SAMBANOVA", "SAMBA NOVA"],
    "cursor": ["ANYSPHERE", r"\bCURSOR\b"],

    "tesla": [r"\bTSLA\b", "TESLA"],
    "spacex": ["SPACEX", "SPACE X", "SPACE EXPLORATION TECH"],
    "boring": ["BORING COMPANY"],
    "neuralink": ["NEURALINK"],

    "nvidia": [r"\bNVDA\b", "NVIDIA", r"NVIDIA CORP(?:ORATION)?"],
    "intel": [r"\bINTC\b", r"\bINTEL\b", r"INTEL CORP(?:ORATION)?"],
    "amd": [r"\bAMD\b", "ADVANCED MICRO DEVICES"],
    "qualcomm": [r"\bQCOM\b", "QUALCOMM"],
    "broadcom": [r"\bAVGO\b", "BROADCOM"],
    "tsmc": [r"\bTSM\b", r"\bTSMC\b", "TAIWAN SEMICONDUCTOR"],
    "arm": ["ARM HOLDINGS"],
    "micron": ["MICRON", "MICRON TECHNOLOGY"],
    "marvell": [r"\bMRVL\b", "MARVELL"],

    "salesforce": ["SALESFORCE", "SLACK", "TABLEAU", "MULESOFT"],
    "oracle": [r"\bORCL\b", r"\bORACLE\b", "NETSUITE", "CERNER", "ORACLE HEALTH"],
    "sap": ["SAP SE", "SAP AMERICA"],
    "adobe": [r"\bADBE\b", r"\bADOBE\b"],
    "vmware": ["VMWARE"],
    "snowflake": ["SNOWFLAKE", "SNOWFLAKE COMPUTING"],
    "palantir": [r"\bPLTR\b", "PALANTIR"],
    "databricks": ["DATABRICKS", "MOSAICML", "MOSAIC AI"],
    "servicenow": ["SERVICENOW", r"SERVICE\s?NOW", "MOVEWORKS"],
    "workday": [r"\bWDAY\b", "WORKDAY"],
    "atlassian": ["ATLASSIAN", "JIRA", "CONFLUENCE", "TRELLO", "LOOM"],
    "hubspot": ["HUBSPOT"],
    "datadog": [r"\bDDOG\b", "DATADOG"],
    "cloudflare": ["CLOUDFLARE"],
    "mongodb": [r"\bMDB\b", "MONGODB"],
    "okta": ["OKTA"],
    "gitlab": [r"\bGTLB\b", "GITLAB"],
    "box": [r"BOX,?\s*INC\.?", r"BOX\.COM"],

    "twitter": ["TWITTER", "TWITTER INC", "X CORP", r"X\.COM"],
    "snap": ["SNAP INC", r"SNAP INC\.", r"\bSNAP\b(?!PLE|[\s-]?ON|[\s-]?CHAT)"],
    "snapchat": ["SNAPCHAT"],
    "pinterest": ["PINTEREST"],
    "reddit": [r"\bRDDT\b", r"\bREDDIT\b"],
    "tiktok": ["TIKTOK", "TIK TOK", "BYTEDANCE"],
    "spotify": ["SPOTIFY"],
    "discord": [r"\bDISCORD\b"],

    "uber": [r"\bUBER\b"],
    "lyft": [r"\bLYFT\b", "LYFT INC"],
    "doordash": ["DOORDASH", r"DOOR\s?DASH"],
    "instacart": ["INSTACART", "MAPLEBEAR"],
    "airbnb": [r"\bABNB\b", "AIRBNB"],

    "stripe": [r"\bSTRIPE\b"],
    "block_inc": [r"BLOCK,?\s*INC\.?", "BLOCK INC", "SQUARE", "CASH APP", "AFTERPAY", "TIDAL", "BITKEY", "PROTO"],
    "coinbase": ["COINBASE"],
    "ripple": ["RIPPLE", "RIPPLE LABS"],
    "robinhood": ["ROBINHOOD"],
    "plaid": [r"\bPLAID\b"],
    "a16z": ["ANDREESSEN HOROWITZ", "A16Z"],
    "sequoia": ["SEQUOIA CAPITAL", "SEQUOIA CAP"],

    "kleiner": ["KLEINER PERKINS", "KLEINER"],
    "khosla": ["KHOSLA VENTURES", "KHOSLA"],
    "greylock": ["GREYLOCK"],
    "benchmark": ["BENCHMARK CAPITAL"],
    "accel": [r"\bACCEL\b(?! ENTER)"],

    "dell": [r"\bDELL\b(?! MONTE| ICIOUS)", "DELL TECHNOLOGIES"],
    "hp": [r"\bHPQ\b", "HP INC", r"HEWLETT[\s-]?PACKARD"],
    "hpe": [r"\bHPE\b", r"HEWLETT[\s-]?PACKARD ENTERPRISE"],
    "cisco": [r"\bCSCO\b", r"\bCISCO\b"],
    "ibm": [r"\bIBM\b"],

    "att": [r"AT\s*&\s*T", r"\bATT\b", "AT AND T"],
    "verizon": ["VERIZON"],
    "tmobile": [r"\bTMUS\b", r"T[\s-]?MOBILE"],
    "comcast": [r"\bCMCSA\b", "COMCAST"],

    "crowdstrike": [r"\bCRWD\b", "CROWDSTRIKE", "CROWD STRIKE"],
    "palo_alto": [r"\bPANW\b", "PALO ALTO NETWORKS"],
    "fortinet": [r"\bFTNT\b", "FORTINET"],
    "sentinelone": ["SENTINELONE", "SENTINEL ONE"],
    "zscaler": [r"\bZS\b", "ZSCALER"],

    "netflix": [r"\bNFLX\b", "NETFLIX"],
    "zoom": ["ZOOM COMMUNICATIONS", "ZOOM VIDEO", r"\bZOOM\b(?! INFO)"],
    "dropbox": [r"\bDBX\b", "DROPBOX"],
    "figma": [r"\bFIGMA\b", "FIGMA INC"],
    "canva": [r"\bCANVA\b"],
    "shopify": ["SHOPIFY"],
    "intuit": [r"\bINTU\b", r"\bINTUIT\b", "MAILCHIMP", "CREDIT KARMA", "TURBOTAX", "QUICKBOOKS"],
    "paypal": [r"\bPYPL\b", "PAYPAL", "VENMO"],
    "ebay": [r"\bEBAY\b"],
}

# Short tickers and ambiguous strings that only count when the employer field
# is EXACTLY that value (after trim + upper). Too noisy to include in regex.
EXACT_TICKER_ONLY: dict[str, list[str]] = {
    "meta": ["META", "FB"],
    "arm": ["ARM"],
    "sap": ["SAP"],
    "salesforce": ["CRM"],
    "servicenow": ["NOW"],
    "snowflake": ["SNOW"],
    "atlassian": ["TEAM"],
    "hubspot": ["HUBS"],
    "cloudflare": ["NET"],
    "box": ["BOX"],
    "c3ai": ["AI"],
    "soundhound": ["SOUN"],
    "tempus": ["TEM"],
    "uipath": ["PATH"],
    "coinbase": ["COIN"],
    "robinhood": ["HOOD"],
    "doordash": ["DASH"],
    "instacart": ["CART"],
    "pinterest": ["PINS"],
    "spotify": ["SPOT"],
    "shopify": ["SHOP"],
    "att": ["T"],
    "verizon": ["VZ"],
    "figma": ["FIG"],
    "sentinelone": ["S"],
    "vmware": ["VMW"],
    "twitter": ["TWTR"],
    "block_inc": ["SQ", "XYZ"],
}

# Patterns known to generate false positives — flagged in output for extra review.
NOISY_PATTERNS = [
    "SQUARE",      # construction, town squares
    "PROTO",       # prototyping labs, biotech
    "TIDAL",       # tidal energy
    "SLACK",       # uncommon but possible
    r"\bZS\b",     # ZS Associates consulting vs Zscaler
    r"\bCURSOR\b", # cursor as non-tech
]


def _load_itcont_employers(cycle: int) -> pd.DataFrame:
    """Load only the columns we need from a cycle's itcont file."""
    base = FEC_INTERIM_ROOT / str(cycle)
    suffix = str(cycle)[2:]
    itcont = pd.read_csv(
        base / f"indiv{suffix}" / "itcont.txt",
        sep="|", header=None, names=ITCONT_COLS,
        dtype="string", na_filter=False,
        usecols=["employer", "name", "transaction_amt"],
    )
    itcont["transaction_amt"] = pd.to_numeric(
        itcont["transaction_amt"], errors="coerce"
    ).fillna(0.0)
    return itcont


def _summarise_matches(itcont: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct employer string: contribution count, total, donor count."""
    return (
        itcont.groupby("employer", sort=False)
        .agg(
            n_contributions=("transaction_amt", "size"),
            total_usd=("transaction_amt", "sum"),
            n_donors=("name", "nunique"),
        )
        .reset_index()
    )


def build_candidates_from_itcont(itcont: pd.DataFrame) -> pd.DataFrame:
    """Run broad regex + exact-match searches against employer strings.

    Returns one row per unique employer string with provenance columns:
    matched_searches, match_types, flagged_noisy, and the three stats.
    """
    stats = _summarise_matches(itcont)
    employer_upper = stats["employer"].str.strip().str.upper()

    # Regex matches.
    regex_hits: dict[str, set[str]] = {}  # employer -> set of company slugs
    for slug, patterns in TECH_SEARCHES.items():
        combined = re.compile("|".join(patterns), re.IGNORECASE)
        mask = stats["employer"].str.contains(combined, regex=True, na=False)
        for emp in stats.loc[mask, "employer"]:
            regex_hits.setdefault(emp, set()).add(slug)

    # Exact-match matches.
    exact_hits: dict[str, set[str]] = {}
    for slug, terms in EXACT_TICKER_ONLY.items():
        upper_terms = {t.upper() for t in terms}
        mask = employer_upper.isin(upper_terms)
        for emp in stats.loc[mask, "employer"]:
            exact_hits.setdefault(emp, set()).add(slug)

    all_employers = set(regex_hits) | set(exact_hits)
    if not all_employers:
        return pd.DataFrame(columns=[
            "employer", "n_contributions", "total_usd", "n_donors",
            "matched_searches", "match_types", "flagged_noisy",
        ])

    match_rows = []
    for emp in all_employers:
        regex_set = regex_hits.get(emp, set())
        exact_set = exact_hits.get(emp, set())
        all_slugs = sorted(regex_set | exact_set)
        types = []
        if regex_set:
            types.append("regex")
        if exact_set:
            types.append("exact")
        match_rows.append({
            "employer": emp,
            "matched_searches": "; ".join(all_slugs),
            "match_types": "; ".join(types),
        })
    matches = pd.DataFrame(match_rows)

    noisy_re = re.compile("|".join(NOISY_PATTERNS), re.IGNORECASE)

    candidates = matches.merge(stats, on="employer", how="left")
    candidates["flagged_noisy"] = candidates["employer"].str.contains(
        noisy_re, regex=True, na=False
    )
    candidates = candidates.sort_values("total_usd", ascending=False).reset_index(drop=True)

    return candidates[[
        "employer", "n_contributions", "total_usd", "n_donors",
        "matched_searches", "match_types", "flagged_noisy",
    ]]


def build_candidates(cycles: Iterable[int]) -> pd.DataFrame:
    """Build candidates by unioning employer stats across cycles."""
    frames = []
    for cycle in cycles:
        print(f"Loading {cycle} itcont employer data...")
        frames.append(_load_itcont_employers(cycle))
    pooled = pd.concat(frames, ignore_index=True)
    print(f"Pooled {len(pooled):,} rows across {len(frames)} cycle(s)")
    return build_candidates_from_itcont(pooled)


def build_review_queue(candidates: pd.DataFrame) -> pd.DataFrame:
    """Candidates whose employer string is NOT present in curated.csv."""
    curated_path = company_curated_path()
    if curated_path.exists():
        curated = pd.read_csv(curated_path, dtype="string", na_filter=False)
        known = set(curated["employer"].str.strip().str.upper())
    else:
        known = set()

    emp_upper = candidates["employer"].str.strip().str.upper()
    unreviewed = candidates.loc[~emp_upper.isin(known)].copy()

    # Append empty decision columns for the reviewer to fill in.
    for col in ["include", "canonical_name", "sector", "notes"]:
        unreviewed[col] = ""

    return unreviewed[[
        "employer", "n_contributions", "total_usd", "n_donors",
        "matched_searches", "match_types", "flagged_noisy",
        "include", "canonical_name", "sector", "notes",
    ]]


def _available_cycles() -> list[int]:
    if not FEC_INTERIM_ROOT.exists():
        return []
    return sorted(
        int(p.name)
        for p in FEC_INTERIM_ROOT.iterdir()
        if p.is_dir() and p.name.isdigit()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cycle", type=int, action="append",
        help="Cycle year to include (repeatable). Defaults to all available.",
    )
    args = parser.parse_args()

    cycles = args.cycle or _available_cycles()
    if not cycles:
        print("No FEC cycles available under data/fec/interim/")
        return 1
    print(f"Building candidates from cycles: {cycles}")

    candidates = build_candidates(cycles)
    candidates_path = company_candidates_path()
    candidates.to_csv(candidates_path, index=False)
    print(f"Wrote {len(candidates)} candidates to "
          f"{candidates_path.relative_to(Path.cwd()) if candidates_path.is_relative_to(Path.cwd()) else candidates_path}")

    review = build_review_queue(candidates)
    review_path = company_review_queue_path()
    review.to_csv(review_path, index=False)
    print(f"Wrote {len(review)} unreviewed rows to "
          f"{review_path.relative_to(Path.cwd()) if review_path.is_relative_to(Path.cwd()) else review_path}")

    if len(review) == 0:
        print("Review queue is empty — every candidate is already in curated.csv.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
