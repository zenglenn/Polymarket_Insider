from __future__ import annotations

from typing import Any

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

BASE_URL = "https://gamma-api.polymarket.com"
TIMEOUT = (5, 20)


def _should_retry(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        if exc.response is None:
            return True
        status = exc.response.status_code
        return status >= 500 or status == 429
    return True


class GammaClient:
    def __init__(self) -> None:
        self.session = requests.Session()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        retry=retry_if_exception(_should_retry),
        reraise=True,
    )
    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{BASE_URL}{path}"
        response = self.session.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()

    def list_markets(self, max_markets: int) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        offset = 0
        limit = min(100, max_markets)
        while len(markets) < max_markets:
            params = {
                "limit": limit,
                "offset": offset,
                "active": "true",
                "closed": "false",
            }
            payload = self._get_json("/markets", params=params)
            batch = self._extract_markets(payload)
            if not batch:
                break
            markets.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return markets[:max_markets]

    @staticmethod
    def _extract_markets(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("markets", "data", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []
