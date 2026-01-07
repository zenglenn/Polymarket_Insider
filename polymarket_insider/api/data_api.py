from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

import requests

BASE_URL = "https://data-api.polymarket.com"


class DataApiClient:
    def __init__(
        self,
        error_dir: Path | None = None,
        response_dir: Path | None = None,
        timeout_s: int = 15,
        retry_max: int = 3,
        backoff_seconds: Iterable[int] | None = None,
        max_backoff_budget_s: int = 60,
    ) -> None:
        self.session = requests.Session()
        self.error_dir = error_dir
        self.response_dir = response_dir
        self.timeout = (timeout_s, timeout_s)
        self.retry_max = retry_max
        self.backoff_seconds = list(backoff_seconds) if backoff_seconds is not None else [1, 2, 4, 8]
        self.max_backoff_budget_s = max_backoff_budget_s
        self.remaining_backoff_budget_s = max_backoff_budget_s
        self.retry_count = 0
        self.rate_limited_count = 0

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{BASE_URL}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.retry_max + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                last_exc = exc
                status = exc.response.status_code if exc.response is not None else None
                if status == 429:
                    self.rate_limited_count += 1
                if status is None or status >= 500 or status == 429:
                    if attempt >= self.retry_max:
                        raise
                    self._sleep_backoff(attempt)
                    continue
                raise
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                if attempt >= self.retry_max:
                    raise
                self._sleep_backoff(attempt)
        if last_exc:
            raise last_exc
        raise RuntimeError("Data API request failed without exception")

    def get_holders(self, market_id: str, limit: int) -> list[dict[str, Any]]:
        params = {"limit": limit}
        try:
            payload = self._get_json(f"/markets/{market_id}/holders", params=params)
            self._save_response("markets_holders", market_id, payload)
            return self._extract_list(payload)
        except requests.HTTPError as exc:
            self._save_error("markets_holders", market_id, exc.response, params)
            if exc.response is None or exc.response.status_code not in (400, 404):
                raise
        try:
            payload = self._get_json("/holders", params={"market_id": market_id, **params})
            self._save_response("holders_market_id", market_id, payload)
            return self._extract_list(payload)
        except requests.HTTPError as exc:
            self._save_error("holders_market_id", market_id, exc.response, {"market_id": market_id, **params})
            if exc.response is None or exc.response.status_code not in (400, 404):
                raise
        try:
            payload = self._get_json("/holders", params={"market": market_id, **params})
            self._save_response("holders_market", market_id, payload)
            return self._extract_list(payload)
        except requests.HTTPError as exc:
            self._save_error("holders_market", market_id, exc.response, {"market": market_id, **params})
            if exc.response is None or exc.response.status_code not in (400, 404):
                raise
        return []

    def get_trades(self, market_id: str, limit: int = 200) -> list[dict[str, Any]]:
        params = {"limit": limit}
        try:
            payload = self._get_json(f"/markets/{market_id}/trades", params=params)
            return self._extract_list(payload)
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 404:
                raise
        payload = self._get_json("/trades", params={"market_id": market_id, **params})
        return self._extract_list(payload)

    def _sleep_backoff(self, attempt: int) -> None:
        if not self.backoff_seconds:
            return
        delay = self.backoff_seconds[min(attempt, len(self.backoff_seconds) - 1)]
        if delay <= 0:
            return
        if self.remaining_backoff_budget_s < delay:
            return
        self.retry_count += 1
        self.remaining_backoff_budget_s -= delay
        time.sleep(delay)

    def _save_error(
        self,
        label: str,
        identifier: str,
        response: requests.Response | None,
        params: dict[str, Any],
    ) -> None:
        if self.error_dir is None:
            return
        if response is None:
            status = "no_response"
            body = ""
            url = ""
        else:
            status = str(response.status_code)
            url = response.url
            body = response.text or ""
        safe_id = "".join(ch if ch.isalnum() else "_" for ch in str(identifier))
        safe_id = safe_id[:80] if safe_id else "unknown"
        filename = f"{label}_{safe_id}_{status}.txt"
        path = self.error_dir / filename
        payload = {
            "status": status,
            "url": url,
            "params": params,
            "body": body[:5000],
        }
        try:
            path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        except OSError:
            return

    def _save_response(self, label: str, identifier: str, payload: Any) -> None:
        if self.response_dir is None:
            return
        safe_id = "".join(ch if ch.isalnum() else "_" for ch in str(identifier))
        safe_id = safe_id[:80] if safe_id else "unknown"
        path = self.response_dir / f"{label}_{safe_id}.json"
        try:
            path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
        except OSError:
            return

    @staticmethod
    def _extract_list(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            if payload and all(isinstance(item, dict) and "holders" in item for item in payload):
                flattened: list[dict[str, Any]] = []
                for entry in payload:
                    holders = entry.get("holders")
                    token = entry.get("token")
                    if not isinstance(holders, list):
                        continue
                    for holder in holders:
                        if not isinstance(holder, dict):
                            continue
                        if token and "token" not in holder:
                            holder = dict(holder)
                            holder["token"] = token
                        flattened.append(holder)
                return flattened
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in ("holders", "trades", "data", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []


def compute_backoff_schedule(
    backoff_seconds: Iterable[int],
    retry_max: int,
    max_budget_s: int,
) -> list[int]:
    schedule: list[int] = []
    remaining = max_budget_s
    backoffs = list(backoff_seconds)
    for attempt in range(retry_max):
        if not backoffs:
            break
        delay = backoffs[min(attempt, len(backoffs) - 1)]
        if delay <= 0 or remaining < delay:
            break
        schedule.append(delay)
        remaining -= delay
    return schedule
