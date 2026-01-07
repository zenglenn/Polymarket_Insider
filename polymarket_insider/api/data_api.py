from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

BASE_URL = "https://data-api.polymarket.com"
TIMEOUT = (5, 20)


def _should_retry(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        if exc.response is None:
            return True
        status = exc.response.status_code
        return status >= 500 or status == 429
    return True


class DataApiClient:
    def __init__(self, error_dir: Path | None = None, response_dir: Path | None = None) -> None:
        self.session = requests.Session()
        self.error_dir = error_dir
        self.response_dir = response_dir

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
