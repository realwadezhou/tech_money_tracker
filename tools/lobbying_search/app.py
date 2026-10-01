"""Private, local search over every installed LDA quarterly report.

Two views:
- Phrase trends: how often a word or phrase appears in issue descriptions,
  quarter by quarter (an "Ngram viewer" for lobbying disclosures).
- Company lookup: every reported client name matching a fragment, with
  report counts and reported amounts by quarter.

Reads data/lda/interim/<year>/ only. Binds to 127.0.0.1; nothing is published.

    python -m tools.lobbying_search            # then open http://127.0.0.1:8765/
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
from datetime import date, timedelta
import json
from pathlib import Path
import pickle
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import webbrowser

import numpy as np
import pandas as pd

from pipeline.common.paths import LDA_DERIVED_ROOT, LDA_INTERIM_ROOT
from pipeline.lda.build_explorer import PERIODS, normalized_name, rows, select_current_reports

HERE = Path(__file__).resolve().parent
CACHE_PATH = LDA_DERIVED_ROOT / "lobbying_search_cache.pkl"
CACHE_VERSION = 1
# LD-2 reports are due 20 days after the quarter ends.
FILING_DEADLINE_DAYS = 20
MAX_ENTRY_ROWS = 300


def installed_years(root: Path = LDA_INTERIM_ROOT) -> list[int]:
    return sorted(int(p.name) for p in root.iterdir() if p.name.isdigit()
                  and (p / "filings.csv").exists() and (p / "filing_activities.csv").exists())


def source_fingerprint(years: list[int], root: Path) -> list:
    files = [root / str(y) / name for y in years for name in ("filings.csv", "filing_activities.csv")]
    return [CACHE_VERSION] + [(str(f), f.stat().st_mtime_ns, f.stat().st_size) for f in files]


def amount(value: str) -> float:
    try:
        return float(value) if value else np.nan
    except ValueError:
        return np.nan


def load_year(year: int, root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Current quarterly reports and their issue entries for one reporting year."""
    current, _, _ = select_current_reports(list(rows(root / str(year) / "filings.csv")))
    reports = pd.DataFrame([{
        "filing_uuid": r["filing_uuid"],
        "quarter": int(r["filing_year"]) * 10 + PERIODS[r["filing_period"]],
        "filing_type": r["filing_type"],
        "client_name": r["client_name"],
        "client_key": normalized_name(r["client_name"]),
        "registrant_name": r["registrant_name"],
        "income": amount(r["income"]),
        "expenses": amount(r["expenses"]),
        "filing_url": r["filing_document_url"],
        "dt_posted": r["dt_posted"],
    } for r in current.values()])
    activities = pd.read_csv(root / str(year) / "filing_activities.csv", dtype=str, keep_default_na=False,
                             usecols=["activity_id", "filing_uuid", "general_issue_code",
                                      "general_issue_code_display", "description"])
    activities = activities[activities.filing_uuid.isin(reports.filing_uuid)]
    return reports, activities


class SearchIndex:
    def __init__(self, reports: pd.DataFrame, activities: pd.DataFrame):
        self.reports = reports.reset_index(drop=True)
        entries = activities.merge(
            reports[["filing_uuid", "quarter", "client_name", "client_key", "registrant_name",
                     "income", "expenses", "filing_url"]], on="filing_uuid", how="inner")
        entries["text"] = entries.description.str.lower()
        self.entries = entries.reset_index(drop=True)
        cutoff = pd.to_datetime(self.reports.dt_posted, utc=True, errors="coerce", format="ISO8601").max()
        self.cutoff = cutoff.date() if pd.notna(cutoff) else date.today()
        self.quarters = sorted(self.reports.quarter.unique().tolist())
        self._lock = threading.Lock()
        self._masks: OrderedDict = OrderedDict()

    @classmethod
    def load(cls, root: Path = LDA_INTERIM_ROOT, cache: Path | None = CACHE_PATH, rebuild: bool = False):
        years = installed_years(root)
        if not years:
            raise SystemExit(f"No normalized LDA years found under {root}")
        fingerprint = source_fingerprint(years, root)
        if cache and cache.exists() and not rebuild:
            with cache.open("rb") as handle:
                saved = pickle.load(handle)
            if saved["fingerprint"] == fingerprint:
                print(f"Loaded cached index for {years[0]}-{years[-1]}.")
                return cls(saved["reports"], saved["activities"])
        frames = []
        for year in years:
            print(f"Reading {year}...", flush=True)
            frames.append(load_year(year, root))
        reports = pd.concat([f[0] for f in frames], ignore_index=True)
        activities = pd.concat([f[1] for f in frames], ignore_index=True)
        if cache:
            cache.parent.mkdir(parents=True, exist_ok=True)
            with cache.open("wb") as handle:
                pickle.dump({"fingerprint": fingerprint, "reports": reports, "activities": activities},
                            handle, protocol=pickle.HIGHEST_PROTOCOL)
        return cls(reports, activities)

    # Matching

    @staticmethod
    def pattern(term: str, mode: str) -> re.Pattern:
        term = term.strip()
        if not term:
            raise ValueError("Empty search term")
        if mode == "regex":
            return re.compile(term, re.IGNORECASE)
        escaped = re.escape(term.lower())
        # Whole-word mode keeps "AI" from matching "said" or "chain", but allows a
        # plural so "tariff" also finds "tariffs".
        return re.compile(rf"(?<!\w){escaped}(?:s|es)?(?!\w)" if mode == "word" else escaped)

    def _cached(self, key, compute) -> np.ndarray:
        with self._lock:
            if key in self._masks:
                self._masks.move_to_end(key)
                return self._masks[key]
        mask = compute()
        with self._lock:
            self._masks[key] = mask
            while len(self._masks) > 64:
                self._masks.popitem(last=False)
        return mask

    def term_mask(self, term: str, mode: str) -> np.ndarray:
        pattern = self.pattern(term, mode)
        needle = term.strip().lower()

        def compute() -> np.ndarray:
            if mode == "regex":
                return self.entries.text.str.contains(pattern).to_numpy()
            mask = self.entries.text.str.contains(needle, regex=False).to_numpy()
            if mode == "word":
                # Plain substring search is fast; run the whole-word regex only on those hits.
                candidates = np.flatnonzero(mask)
                mask[candidates] = self.entries.text.iloc[candidates].str.contains(pattern).to_numpy()
            return mask

        return self._cached(("term", needle if mode != "regex" else term.strip(), mode), compute)

    def client_mask(self, frame: str, client: str) -> np.ndarray:
        table = self.entries if frame == "entries" else self.reports
        if not client.strip():
            return np.ones(len(table), dtype=bool)
        needle = normalized_name(client)
        return self._cached((frame, needle),
                            lambda: table.client_key.str.contains(needle, regex=False).to_numpy())

    def quarter_info(self) -> list[dict]:
        info = []
        for q in self.quarters:
            year, quarter = divmod(q, 10)
            end = (date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)) - timedelta(days=1)
            info.append({"id": q, "label": f"{year} Q{quarter}",
                         "partial": self.cutoff < end + timedelta(days=FILING_DEADLINE_DAYS)})
        return info

    # Queries

    def trend(self, terms: list[str], mode: str, client: str = "") -> dict:
        base = self.client_mask("entries", client)
        scoped = self.entries[base]
        totals_entries = scoped.groupby("quarter").size()
        totals_clients = scoped.groupby("quarter").client_key.nunique()
        series = []
        for term in terms:
            hits = self.entries[base & self.term_mask(term, mode)]
            entries = hits.groupby("quarter").size()
            clients = hits.groupby("quarter").client_key.nunique()
            series.append({"term": term,
                           "entries": [int(entries.get(q, 0)) for q in self.quarters],
                           "clients": [int(clients.get(q, 0)) for q in self.quarters]})
        return {"quarters": self.quarter_info(),
                "totals": {"entries": [int(totals_entries.get(q, 0)) for q in self.quarters],
                           "clients": [int(totals_clients.get(q, 0)) for q in self.quarters]},
                "series": series}

    def matching_entries(self, term: str, mode: str, client: str = "", quarter: int | None = None) -> dict:
        mask = self.client_mask("entries", client) & self.term_mask(term, mode)
        hits = self.entries[mask]
        if quarter:
            hits = hits[hits.quarter == quarter]
        top = (hits.groupby("client_key")
                   .agg(client_name=("client_name", "first"), entries=("activity_id", "size"),
                        reports=("filing_uuid", "nunique"), registrants=("registrant_name", "nunique"))
                   .sort_values("entries", ascending=False).head(50))
        pattern = self.pattern(term, mode)
        sample = hits.sort_values(["quarter", "client_key"], ascending=[False, True]).head(MAX_ENTRY_ROWS)
        return {"total_entries": int(len(hits)),
                "total_clients": int(hits.client_key.nunique()),
                "top_clients": top.reset_index(drop=True).to_dict("records"),
                "entries": [entry_row(r, pattern) for r in sample.itertuples()]}

    def clients(self, fragment: str) -> dict:
        if len(fragment.strip()) < 2:
            raise ValueError("Type at least two characters")
        hits = self.reports[self.client_mask("reports", fragment)]
        names = []
        for name, group in hits.groupby("client_name"):
            by_q = group.groupby("quarter")[["income", "expenses"]].sum(min_count=1)
            names.append({
                "client_name": name,
                "reports": int(len(group)),
                "registrants": int(group.registrant_name.nunique()),
                "self_filed": bool((group.registrant_name.map(normalized_name) == normalized_name(name)).any()),
                "first": int(group.quarter.min()), "last": int(group.quarter.max()),
                "income": [none_if_nan(by_q.income.get(q)) for q in self.quarters],
                "expenses": [none_if_nan(by_q.expenses.get(q)) for q in self.quarters],
            })
        names.sort(key=lambda n: -n["reports"])
        return {"quarters": self.quarter_info(), "names": names[:300], "total_names": len(names)}

    def client_reports(self, name: str, quarter: int | None = None) -> dict:
        reports = self.reports[self.reports.client_name == name]
        if quarter:
            reports = reports[reports.quarter == quarter]
        reports = reports.sort_values(["quarter", "registrant_name"], ascending=[False, True]).head(200)
        entries = self.entries[self.entries.filing_uuid.isin(reports.filing_uuid)]
        issues = {uid: group[["general_issue_code_display", "description"]].to_dict("records")
                  for uid, group in entries.groupby("filing_uuid")}
        return {"reports": [{
            "quarter": int(r.quarter), "registrant_name": r.registrant_name, "filing_type": r.filing_type,
            "income": none_if_nan(r.income), "expenses": none_if_nan(r.expenses), "filing_url": r.filing_url,
            "issues": issues.get(r.filing_uuid, []),
        } for r in reports.itertuples()]}


def none_if_nan(value):
    return None if value is None or pd.isna(value) else float(value)


def entry_row(row, pattern: re.Pattern) -> dict:
    # Match offsets come from the lowered text. Lowercasing can change length for
    # a few Unicode characters; then show the lowered text so offsets still line up.
    haystack = row.description if len(row.text) == len(row.description) else row.text
    match = pattern.search(row.text)
    start, end = (match.start(), match.end()) if match else (0, 0)
    lo, hi = max(0, start - 200), min(len(haystack), end + 300)
    return {"quarter": int(row.quarter), "client_name": row.client_name,
            "registrant_name": row.registrant_name, "issue": row.general_issue_code_display,
            "snippet": haystack[lo:hi], "match": [start - lo, end - lo], "clipped": [lo > 0, hi < len(haystack)],
            "income": none_if_nan(row.income), "expenses": none_if_nan(row.expenses),
            "filing_url": row.filing_url}


def make_handler(index: SearchIndex):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, status: int, body: bytes, content_type: str):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            params = {k: v[0] for k, v in parse_qs(url.query).items()}
            mode = params.get("mode", "word")
            quarter = int(params["quarter"]) if params.get("quarter") else None
            routes = {
                "/api/trend": lambda: index.trend(
                    [t for t in params.get("terms", "").split("|") if t.strip()][:8], mode, params.get("client", "")),
                "/api/entries": lambda: index.matching_entries(
                    params.get("term", ""), mode, params.get("client", ""), quarter),
                "/api/clients": lambda: index.clients(params.get("q", "")),
                "/api/client_reports": lambda: index.client_reports(params.get("name", ""), quarter),
                "/api/meta": lambda: {"cutoff": index.cutoff.isoformat(), "quarters": index.quarter_info(),
                                      "reports": int(len(index.reports)), "entries": int(len(index.entries))},
            }
            if url.path in ("/", "/index.html"):
                return self.send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
            if url.path not in routes:
                return self.send(404, b"Not found", "text/plain")
            try:
                body = json.dumps(routes[url.path](), allow_nan=False).encode("utf-8")
                self.send(200, body, "application/json")
            except (ValueError, re.error) as error:
                self.send(400, json.dumps({"error": str(error)}).encode("utf-8"), "application/json")

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--rebuild-cache", action="store_true", help="Ignore the saved index and reread the CSVs")
    args = parser.parse_args()
    index = SearchIndex.load(rebuild=args.rebuild_cache)
    print(f"{len(index.reports):,} reports, {len(index.entries):,} issue entries; "
          f"newest posting {index.cutoff.isoformat()}.")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(index))
    url = f"http://127.0.0.1:{args.port}/"
    print(f"Open {url}  (Ctrl+C to stop)")
    if not args.no_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
