from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "https://api.open.fec.gov/v1/"


class OpenFECError(RuntimeError):
    """Raised when an OpenFEC request fails."""


@dataclass
class OpenFECClient:
    api_key: str | None = None
    base_url: str = DEFAULT_BASE_URL
    user_agent: str = "tech-money/1.0"

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = os.getenv("OPENFEC_API_KEY") or os.getenv("FEC_API_KEY")
        if not self.api_key:
            raise OpenFECError(
                "OpenFEC API key missing. Set OPENFEC_API_KEY or pass api_key explicitly."
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
            raise OpenFECError(f"OpenFEC request failed for {url}: {exc}") from exc

    def iter_results(
        self,
        path: str,
        *,
        per_page: int = 100,
        max_pages: int | None = None,
        **params,
    ) -> Iterator[dict]:
        page = 1
        while True:
            payload = self.get(path, page=page, per_page=per_page, **params)
            results = payload.get("results", [])
            for row in results:
                yield row

            pagination = payload.get("pagination") or {}
            total_pages = int(pagination.get("pages") or 0)
            if not results:
                break
            if max_pages is not None and page >= max_pages:
                break
            if total_pages and page >= total_pages:
                break
            page += 1
