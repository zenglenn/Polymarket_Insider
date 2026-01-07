from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_gzip_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)


def load_gzip_json(path: Path) -> Any:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def gamma_payload_diagnostics(payload: Any) -> dict[str, Any]:
    top_level_keys: list[str] = []
    first_market_keys: list[str] = []
    market_count = 0
    market_list: list[Any] | None = None

    if isinstance(payload, dict):
        top_level_keys = sorted(str(key) for key in payload.keys())
        for key in ("markets", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                market_list = value
                break
    elif isinstance(payload, list):
        top_level_keys = ["<list>"]
        market_list = payload

    if market_list is not None:
        market_count = len(market_list)
        for item in market_list:
            if isinstance(item, dict):
                first_market_keys = sorted(str(key) for key in item.keys())
                break

    return {
        "top_level_keys": top_level_keys,
        "first_market_keys": first_market_keys,
        "market_count": market_count,
    }
