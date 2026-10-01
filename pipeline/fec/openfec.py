from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.common.env import load_project_env


DEFAULT_BASE_URL = "https://api.open.fec.gov/v1/"


class OpenFECError(RuntimeError):
    """Raised when an OpenFEC request fails."""


@dataclass
class OpenFECClient:
    api_key: str | None = None
    base_url: str = DEFAULT_BASE_URL
    user_agent: str = "tech-money/1.0"

    def __post_init__(self) -> None:
        load_project_env()
        if not self.api_key:
            self.api_key = (
                os.getenv("OPENFEC_API_KEY")
                or os.getenv("FEC_API_KEY")
                or os.getenv("FEC_KEY")
            )
        if not self.api_key:
            raise OpenFECError(
                "OpenFEC API key missing. Set OPENFEC_API_KEY, FEC_API_KEY, FEC_KEY, "
                "or pass api_key explicitly."
            )
        self.base_url = self.base_url.rstrip("/") + "/"

    def build_url(self, path: str, **params) -> str:
        query = {"api_key": self.api_key}
        query.update({key: value for key, value in params.items() if value is not None})
        return self.base_url + path.lstrip("/") + "?" + urlencode(query, doseq=True)

    def get(self, path: str, **params) -> dict:
        url = self.build_url(path, **params)
        request = Request(url, headers={"User-Agent": self.user_agent})
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - network/auth dependent
            safe_url = url.replace(self.api_key or "", "***")
            raise OpenFECError(f"OpenFEC request failed for {safe_url}: {exc}") from exc

    def iter_results(
        self,
        path: str,
        *,
        per_page: int = 100,
        max_pages: int | None = None,
        **params,
    ) -> Iterator[dict]:
        """Page ordinary endpoints and follow itemized schedules' seek cursors.

        Schedules A/B ignore page numbers and their counts may be approximate.
        Continue until an empty response; never add API results to bulk totals
        without reconciling the overlapping records.
        """
        if per_page <= 0 or (max_pages is not None and max_pages <= 0):
            raise ValueError("per_page and max_pages must be positive")
        keyset = path.strip("/") in {"schedules/schedule_a", "schedules/schedule_b"}
        if keyset and "page" in params:
            raise ValueError("Itemized schedules use last_indexes cursors, not page numbers")
        cursor = {
            key: params.pop(key) for key in list(params)
            if keyset and (key.startswith("last_") or key == "sort_null_only")
        }
        seen_cursors = {json.dumps(cursor, sort_keys=True)} if cursor else set()
        page = 1
        while True:
            request_params = params | cursor | {"per_page": per_page}
            if not keyset:
                request_params["page"] = page
            payload = self.get(path, **request_params)
            results = payload.get("results", [])
            if not results:
                break
            pagination = payload.get("pagination") or {}
            # Other itemized endpoints can also return keyset pagination.
            if "last_indexes" in pagination:
                keyset = True
            next_cursor = pagination.get("last_indexes")
            if keyset:
                if not isinstance(next_cursor, dict) or not next_cursor:
                    raise OpenFECError("Itemized endpoint returned records without a pagination cursor")
                signature = json.dumps(next_cursor, sort_keys=True)
                if signature in seen_cursors:
                    raise OpenFECError("OpenFEC pagination cursor repeated; refusing duplicate-page totals")
                seen_cursors.add(signature)
            for row in results:
                yield row

            total_pages = int(pagination.get("pages") or 0)
            if max_pages is not None and page >= max_pages:
                break
            if not keyset and total_pages and page >= total_pages:
                break
            if keyset:
                # Replace the whole cursor: when entering null-date results,
                # sort_null_only replaces last_contribution_receipt_date.
                cursor = next_cursor
            page += 1
