from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from polymarket_insider.api.data_api import DataApiClient
from polymarket_insider.api.gamma import GammaClient
from polymarket_insider.config import AppConfig
from polymarket_insider.db import store
from polymarket_insider.scoring.features import (
    days_until_close,
    extract_wallet,
    extract_wallet_created,
    safe_float,
)
from polymarket_insider.scoring.weights import stable_sorted
from polymarket_insider.utils.io import ensure_dir, gamma_payload_diagnostics, load_gzip_json, save_gzip_json

logger = logging.getLogger(__name__)


def collect_data(
    config: AppConfig,
    run_date: date,
    raw_dir: Path,
    conn,
    commit: bool = True,
) -> dict[str, Any]:
    gamma = GammaClient()
    error_dir = raw_dir / "errors"
    response_dir = raw_dir / "responses"
    ensure_dir(error_dir)
    ensure_dir(response_dir)
    data_api = DataApiClient(
        error_dir=error_dir,
        response_dir=response_dir,
        timeout_s=config.holders.request_timeout_s,
        retry_max=config.holders.retry_max,
        backoff_seconds=config.holders.backoff_seconds,
        max_backoff_budget_s=config.holders.max_backoff_budget_s,
    )

    raw_markets = gamma.list_markets(config.run.max_markets)
    markets_path = raw_dir / "markets.json.gz"
    save_gzip_json(markets_path, raw_markets)
    diagnostics = gamma_payload_diagnostics(load_gzip_json(markets_path))
    logger.info(
        "Gamma diagnostics top_keys=%s first_market_keys=%s",
        diagnostics.get("top_level_keys"),
        diagnostics.get("first_market_keys"),
    )

    normalized = [normalize_market(market) for market in raw_markets]

    filtered = []
    reason_counts: dict[str, int] = {}
    missing_close_time = 0
    for market in normalized:
        keep, reasons, missing_close = evaluate_market(market, run_date, config)
        if missing_close:
            missing_close_time += 1
        if keep:
            filtered.append(market)
        else:
            for reason in reasons:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

    if not filtered:
        logger.warning("No markets matched filters")

    store.insert_market_snapshots(conn, run_date.isoformat(), filtered, commit=commit)
    store.upsert_markets(conn, filtered, commit=commit)
    logger.info(
        "Collection summary markets_fetched=%d markets_kept=%d missing_close_time=%d",
        len(normalized),
        len(filtered),
        missing_close_time,
    )
    if reason_counts:
        sorted_reasons = sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)
        logger.info("Top filter reasons: %s", sorted_reasons[:5])

    holders_payloads: dict[str, list[dict[str, Any]]] = {}
    trades_payloads: dict[str, list[dict[str, Any]]] = {}
    markets_for_holders = stable_sorted(
        filtered,
        key=lambda item: (item.get("volume_usd", 0.0), item.get("liquidity_usd", 0.0)),
        reverse=True,
        tie_breaker=lambda item: item.get("market_id"),
    )[: min(len(filtered), config.holders.max_markets_to_fetch)]
    markets_for_holders_ids = {market["market_id"] for market in markets_for_holders}

    holder_markets_succeeded = 0
    holder_markets_failed = 0
    unknown_outcome_rows = 0
    for market in filtered:
        market_id = market["market_id"]
        if market_id not in markets_for_holders_ids:
            holders_payloads[market_id] = []
            trades_payloads[market_id] = []
            continue
        candidate_ids = _holder_identifiers(market)
        try:
            raw_holders, used_id = fetch_holders_with_fallback(
                data_api, candidate_ids, config.holders.top_n
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch holders for %s: %s", market_id, exc)
            raw_holders = []
            used_id = None
        if used_id and used_id != market_id:
            logger.info("Used holders identifier %s for market %s", used_id, market_id)
        if not raw_holders:
            raw_trades, trades_id = fetch_trades_with_fallback(
                data_api, candidate_ids, config.holders.top_n * 5
            )
            trades_payloads[market_id] = raw_trades
            if raw_trades:
                if trades_id and trades_id != market_id:
                    logger.info("Used trades identifier %s for market %s", trades_id, market_id)
                derived_holders = holders_from_trades(raw_trades, config.holders.top_n)
                holders_payloads[market_id] = derived_holders
            else:
                holders_payloads[market_id] = []
        else:
            holders_payloads[market_id] = select_top_holders(raw_holders, config.holders.top_n)

        source = "holders_api" if raw_holders else "trades_derived"
        normalized_holders = [
            normalize_holder(holder, market, run_date, config.thresholds.new_wallet_days, source=source)
            for holder in holders_payloads[market_id]
        ]
        if normalized_holders:
            holder_markets_succeeded += 1
        else:
            holder_markets_failed += 1
        unknown_outcome_rows += sum(
            1 for holder in normalized_holders if holder.get("outcome") == "unknown"
        )
        store.insert_holders(conn, run_date.isoformat(), market_id, normalized_holders, commit=commit)

    save_gzip_json(raw_dir / "holders.json.gz", holders_payloads)
    save_gzip_json(raw_dir / "trades.json.gz", trades_payloads)

    diagnostics.update(
        {
            "markets_fetched": len(normalized),
            "markets_kept": len(filtered),
            "kept_markets": len(filtered),
            "missing_close_time": missing_close_time,
            "filter_reasons": reason_counts,
            "holder_markets_targeted": len(markets_for_holders),
            "holder_markets_succeeded": holder_markets_succeeded,
            "holder_markets_failed": holder_markets_failed,
            "markets_with_holders": holder_markets_succeeded,
            "unknown_outcome_rows": unknown_outcome_rows,
            "holders_rate_limited_count": data_api.rate_limited_count,
            "holders_retry_count": data_api.retry_count,
        }
    )
    return diagnostics


def normalize_market(market: dict[str, Any]) -> dict[str, Any]:
    market_id = (
        market.get("id")
        or market.get("marketId")
        or market.get("market_id")
        or market.get("conditionId")
        or market.get("condition_id")
    )
    close_time = (
        market.get("closeTime")
        or market.get("closesAt")
        or market.get("close_time")
        or market.get("endDate")
        or market.get("endDateIso")
        or market.get("endDateTime")
        or market.get("end_time")
    )
    volume_usd = safe_float(
        market.get("volume")
        or market.get("volumeUSD")
        or market.get("volume_usd")
        or market.get("volumeNum")
        or market.get("volumeUsd")
    )
    liquidity_usd = safe_float(
        market.get("liquidity")
        or market.get("liquidityUSD")
        or market.get("liquidity_usd")
        or market.get("liquidityNum")
        or market.get("liquidityUsd")
    )
    last_price = safe_float(market.get("lastPrice") or market.get("last_price"))
    is_active = market.get("active")
    if is_active is None:
        is_active = market.get("isActive")
    is_closed = bool(market.get("closed") or market.get("isClosed"))
    is_resolved = bool(market.get("resolved") or market.get("isResolved"))
    is_archived = bool(market.get("archived") or market.get("isArchived"))
    status = market.get("status")
    if not status:
        if is_resolved or is_closed or is_archived:
            status = "closed"
        elif is_active is False:
            status = "inactive"
        else:
            status = "active"
    cluster_key = build_cluster_key(market)
    return {
        "market_id": str(market_id) if market_id is not None else "unknown",
        "question": market.get("question") or market.get("title"),
        "slug": market.get("slug"),
        "status": status,
        "cluster_key": cluster_key,
        "close_time": close_time,
        "volume_usd": volume_usd,
        "liquidity_usd": liquidity_usd,
        "last_price": last_price,
        "condition_id": market.get("conditionId") or market.get("condition_id"),
        "token_id": market.get("tokenId") or market.get("token_id"),
        "yes_token_id": market.get("yesTokenId") or market.get("yes_token_id"),
        "no_token_id": market.get("noTokenId") or market.get("no_token_id"),
        "clob_token_ids": market.get("clobTokenIds") or market.get("clob_token_ids"),
        "question_id": market.get("questionID") or market.get("questionId") or market.get("question_id"),
        "is_active": bool(is_active) if is_active is not None else None,
        "is_closed": is_closed,
        "is_resolved": is_resolved,
        "is_archived": is_archived,
        "raw": market,
    }


def evaluate_market(market: dict[str, Any], run_date: date, config: AppConfig) -> tuple[bool, list[str], bool]:
    reasons: list[str] = []
    filters = config.market_filters
    missing_close = False
    try:
        if market.get("is_closed") or market.get("is_resolved") or market.get("is_archived"):
            reasons.append("closed_or_resolved")

        volume = safe_float(market.get("volume_usd"))
        liquidity = safe_float(market.get("liquidity_usd"))
        if volume < filters.min_volume:
            reasons.append("below_min_volume")
        if liquidity < filters.min_liquidity:
            reasons.append("below_min_liquidity")

        days = days_until_close(market.get("close_time"), run_date)
        if days is None:
            missing_close = True
        elif days > filters.days_to_close:
            reasons.append("outside_days_to_close")
    except Exception:  # noqa: BLE001
        reasons.append("parse_error")

    exclude_reasons = {
        "closed_or_resolved",
        "outside_days_to_close",
        "below_min_volume",
        "below_min_liquidity",
        "parse_error",
    }
    keep = not any(reason in exclude_reasons for reason in reasons)
    return keep, [reason for reason in reasons if reason in exclude_reasons], missing_close


def _holder_identifiers(market: dict[str, Any]) -> list[str]:
    identifiers = [
        market.get("condition_id"),
        market.get("market_id"),
        market.get("token_id"),
        market.get("yes_token_id"),
        market.get("no_token_id"),
        market.get("question_id"),
    ]
    clob_tokens = market.get("clob_token_ids")
    if isinstance(clob_tokens, list):
        identifiers.extend(clob_tokens)
    seen = set()
    result = []
    for value in identifiers:
        if value is None:
            continue
        text = str(value)
        if text not in seen:
            seen.add(text)
            result.append(text)
    return result


def fetch_holders_with_fallback(
    client: DataApiClient,
    candidate_ids: list[str],
    limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    for identifier in candidate_ids:
        try:
            holders = client.get_holders(identifier, limit)
        except Exception:  # noqa: BLE001
            continue
        if holders:
            return holders, identifier
    if candidate_ids:
        try:
            return client.get_holders(candidate_ids[0], limit), candidate_ids[0]
        except Exception:  # noqa: BLE001
            return [], candidate_ids[0]
    return [], None


def fetch_trades_with_fallback(
    client: DataApiClient,
    candidate_ids: list[str],
    limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    for identifier in candidate_ids:
        try:
            trades = client.get_trades(identifier, limit)
        except Exception:  # noqa: BLE001
            continue
        if trades:
            return trades, identifier
    if candidate_ids:
        try:
            return client.get_trades(candidate_ids[0], limit), candidate_ids[0]
        except Exception:  # noqa: BLE001
            return [], candidate_ids[0]
    return [], None


def holders_from_trades(trades: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    totals_value_usd: dict[str, float] = {}
    totals_amount: dict[str, float] = {}
    counts: dict[str, int] = {}
    fetched_at = datetime.utcnow().isoformat()
    for trade in trades:
        wallet = extract_wallet(trade)
        if wallet == "unknown":
            continue
        amount = safe_float(trade.get("size") or trade.get("amount"))
        price = safe_float(trade.get("price") or trade.get("avgPrice") or trade.get("avg_price"))
        value_usd = amount * price if price else None
        if value_usd is not None:
            totals_value_usd[wallet] = totals_value_usd.get(wallet, 0.0) + value_usd
        totals_amount[wallet] = totals_amount.get(wallet, 0.0) + amount
        counts[wallet] = counts.get(wallet, 0) + 1

    rows = [
        {
            "wallet": wallet,
            "address": wallet,
            "outcome": "unknown",
            "shares": totals_amount.get(wallet, 0.0),
            "value_usd": totals_value_usd.get(wallet),
            "fetched_at": fetched_at,
            "exposure_usd": (
                totals_value_usd[wallet]
                if wallet in totals_value_usd
                else totals_amount.get(wallet, 0.0)
            ),
            "is_new_wallet": False,
            "source": "trades_derived",
            "raw": {"source": "trades", "trade_count": counts.get(wallet, 0)},
        }
        for wallet in totals_amount.keys()
    ]

    sorted_rows = stable_sorted(
        rows,
        key=lambda item: item.get("exposure_usd", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("wallet"),
    )
    return sorted_rows[:top_n]


def select_top_holders(holders: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    if not holders:
        return []

    def sort_value(holder: dict[str, Any]) -> float:
        for key in ("valueUsd", "valueUSD", "value_usd", "usdValue", "usd_value", "amount", "shares", "size"):
            if key in holder:
                return safe_float(holder.get(key))
        return 0.0

    return stable_sorted(
        holders,
        key=sort_value,
        reverse=True,
        tie_breaker=lambda item: extract_wallet(item),
    )[:top_n]


def extract_holder_address(holder: dict[str, Any]) -> str | None:
    for key in ("address", "wallet", "user", "account", "proxyWallet"):
        value = holder.get(key)
        if value:
            return str(value)
    return None


def extract_holder_outcome(holder: dict[str, Any], market: dict[str, Any]) -> str:
    for key in ("outcome", "position", "side", "outcomeLabel", "outcome_label"):
        value = holder.get(key)
        if value:
            return str(value)
    outcome_index = holder.get("outcomeIndex")
    outcomes = []
    raw = market.get("raw") or {}
    if isinstance(raw, dict):
        outcomes = _coerce_list(raw.get("outcomes"))
    if isinstance(outcome_index, int) and isinstance(outcomes, list) and outcome_index < len(outcomes):
        outcome = outcomes[outcome_index]
        if outcome:
            return str(outcome)
    return "unknown"


def extract_holder_shares(holder: dict[str, Any]) -> float | None:
    for key in ("shares", "share", "size", "amount", "balance", "position", "quantity"):
        if key in holder:
            value = safe_float(holder.get(key))
            return value
    return None


def extract_holder_value_usd(holder: dict[str, Any], market: dict[str, Any]) -> float | None:
    for key in (
        "valueUsd",
        "valueUSD",
        "value_usd",
        "usdValue",
        "usd_value",
        "notionalValue",
        "notional_value",
    ):
        if key in holder:
            value = safe_float(holder.get(key))
            return value
    shares = extract_holder_shares(holder)
    if shares is None:
        return None
    price = safe_float(holder.get("price"))
    raw = market.get("raw") or {}
    if not price and isinstance(raw, dict):
        outcome_prices = _coerce_list(raw.get("outcomePrices") or raw.get("outcome_prices"))
        outcome_index = holder.get("outcomeIndex")
        if isinstance(outcome_prices, list) and isinstance(outcome_index, int):
            if outcome_index < len(outcome_prices):
                price = safe_float(outcome_prices[outcome_index])
    if not price and isinstance(raw, dict):
        price = safe_float(raw.get("lastTradePrice") or raw.get("last_price"))
    if not price:
        price = safe_float(market.get("last_price"))
    if price:
        return shares * price
    return None


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def build_cluster_key(market: dict[str, Any]) -> str:
    raw = market
    events = raw.get("events")
    if isinstance(events, list):
        for event in events:
            if isinstance(event, dict):
                event_id = event.get("id") or event.get("eventId") or event.get("event_id")
                if event_id:
                    return f"event:{event_id}"
            elif isinstance(event, str):
                return f"event:{event}"

    group_title = raw.get("groupItemTitle") or raw.get("group_item_title")
    if group_title:
        return f"group:{normalize_cluster_text(group_title)}"

    slug = raw.get("slug")
    if isinstance(slug, str) and slug:
        date_match = re.search(r"-\\d{4}-\\d{2}-\\d{2}", slug)
        if date_match:
            prefix = slug[: date_match.start()]
        elif "--" in slug:
            prefix = slug.split("--")[0]
        else:
            prefix = slug
        return f"slug:{normalize_cluster_text(prefix)}"

    question = raw.get("question") or raw.get("title") or ""
    return f"q:{question_cluster_key(question)}"


def normalize_cluster_text(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", " ", text.lower()).strip()
    return re.sub(r"\\s+", " ", cleaned)


def question_cluster_key(question: str) -> str:
    q = question.strip().lower()
    if q.startswith("will ") and " win super bowl 2026" in q:
        return "super_bowl_2026_winner"
    if "nfc championship" in q:
        return "nfc_championship"
    if "afc championship" in q:
        return "afc_championship"
    if "gta vi" in q or "gta 6" in q:
        return "gta_6"
    return normalize_cluster_text(q)[:60] or "unknown"


def normalize_holder(
    holder: dict[str, Any],
    market: dict[str, Any],
    run_date: date,
    new_wallet_days: int,
    source: str | None = None,
) -> dict[str, Any]:
    fetched_at = datetime.utcnow().isoformat()
    address = extract_holder_address(holder)
    wallet = address or extract_wallet(holder)
    outcome = extract_holder_outcome(holder, market)
    shares = extract_holder_shares(holder)
    value_usd = extract_holder_value_usd(holder, market)
    exposure_usd = value_usd if value_usd is not None else (shares or 0.0)
    created = extract_wallet_created(holder)
    is_new_wallet = False
    if created is not None:
        delta = (run_date - created.date()).days
        is_new_wallet = delta <= new_wallet_days
    return {
        "wallet": wallet,
        "address": address or wallet,
        "outcome": outcome,
        "shares": shares,
        "value_usd": value_usd,
        "fetched_at": fetched_at,
        "exposure_usd": exposure_usd,
        "is_new_wallet": is_new_wallet,
        "source": source or holder.get("source"),
        "raw": holder,
    }
