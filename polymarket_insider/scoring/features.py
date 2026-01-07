from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Iterable

from polymarket_insider.utils.time import parse_datetime


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def hhi_concentration(exposures: Iterable[float]) -> float:
    cleaned = [value for value in exposures if value and value > 0]
    total = sum(cleaned)
    if total <= 0:
        return 0.0
    return sum((value / total) ** 2 for value in cleaned)


def log_norm(value: float) -> float:
    return math.log1p(max(value, 0.0))


def days_until_close(close_time: str | None, run_date: date) -> int | None:
    close_dt = parse_datetime(close_time)
    if close_dt is None:
        return None
    return max((close_dt.date() - run_date).days, 0)


def extract_wallet(holder: dict[str, Any]) -> str:
    for key in ("wallet", "address", "trader", "user", "account", "proxyWallet"):
        value = holder.get(key)
        if value:
            return str(value)
    return "unknown"


def extract_wallet_created(holder: dict[str, Any]) -> datetime | None:
    for key in ("firstSeen", "createdAt", "created_at", "first_seen"):
        value = holder.get(key)
        parsed = parse_datetime(value)
        if parsed:
            return parsed
    return None


def extract_exposure_usd(holder: dict[str, Any], market: dict[str, Any]) -> float:
    for key in ("usdValue", "valueUsd", "value_usd", "usd_value"):
        if key in holder:
            return safe_float(holder.get(key))
    for key in ("notionalValue", "notional_value"):
        if key in holder:
            return safe_float(holder.get(key))
    amount = None
    for key in ("amount", "shares", "balance", "size"):
        if key in holder:
            amount = safe_float(holder.get(key))
            break
    price = None
    for key in ("price", "lastPrice", "last_price"):
        if key in holder:
            price = safe_float(holder.get(key))
            break
    if price is None:
        price = safe_float(market.get("last_price"))
    if amount is None:
        return 0.0
    if price is None:
        return amount
    return amount * price
