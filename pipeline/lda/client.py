from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.common.env import load_project_env


DEFAULT_BASE_URL = "https://lda.gov/api/v1/"


class LDAError(RuntimeError):
    """Raised when an LDA request fails."""


@dataclass
class LDAClient:
    api_key: str | None = None
    base_url: str = DEFAULT_BASE_URL
    user_agent: str = "tech-money/1.0"

    def __post_init__(self) -> None:
        load_project_env()
        if not self.api_key:
            self.api_key = os.getenv("LDA_API_KEY") or os.getenv("LDA_KEY")
        self.base_url = self.base_url.rstrip("/") + "/"

    def build_url(self, path: str, **params) -> str:
        query = {key: value for key, value in params.items() if value is not None}
        return self.base_url + path.lstrip("/") + "?" + urlencode(query, doseq=True)

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": self.user_agent}
        if self.api_key:
            headers["Authorization"] = f"Token {self.api_key}"
        return headers

    def get(self, path: str, **params) -> dict:
        url = self.build_url(path, **params)
        request = Request(url, headers=self._headers())
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - network/auth dependent
            raise LDAError(f"LDA request failed for {url}: {exc}") from exc

    def iter_results(
        self,
        path: str,
        *,
        page_size: int = 25,
        max_pages: int | None = None,
        **params,
    ) -> Iterator[dict]:
        page = 1
        while True:
            payload = self.get(path, page=page, page_size=page_size, **params)
            results = payload.get("results", [])
            for row in results:
                yield row

            if not results:
                break
            if payload.get("next") is None:
                break
            if max_pages is not None and page >= max_pages:
                break
            page += 1
