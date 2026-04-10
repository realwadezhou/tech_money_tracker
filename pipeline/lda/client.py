from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Iterator
from urllib.error import HTTPError
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
    max_retries: int = 8

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
        for attempt in range(self.max_retries + 1):
            try:
                with urlopen(request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8"))
            except HTTPError as exc:  # pragma: no cover - network/auth dependent
                should_retry = exc.code in {429, 500, 502, 503, 504}
                if not should_retry or attempt >= self.max_retries:
                    raise LDAError(f"LDA request failed for {url}: {exc}") from exc

                retry_after = exc.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_seconds = int(retry_after)
                else:
                    sleep_seconds = min(60, 2 ** attempt)
                time.sleep(max(1, sleep_seconds))
            except Exception as exc:  # pragma: no cover - network/auth dependent
                if attempt >= self.max_retries:
                    raise LDAError(f"LDA request failed for {url}: {exc}") from exc
                time.sleep(min(30, 2 ** attempt))

        raise LDAError(f"LDA request failed for {url}: exhausted retries")

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
