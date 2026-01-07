from __future__ import annotations

import math
from typing import Any, Iterable

from polymarket_insider.analytics import flow as flow_analytics
from polymarket_insider.scoring.weights import stable_sorted


def compute_consensus(conn, run_date: str, config) -> dict[str, Any]:
    run_pairs = _run_pairs(conn, run_date, config.consensus.lookback_days)
    if not run_pairs:
        return {
            "run_date": run_date,
            "prior_run_date": None,
            "lookback_days": config.consensus.lookback_days,
            "consensus_entries": [],
            "consensus_wallets": [],
        }

    position_rows: list[dict[str, Any]] = []
    wallet_tiers: dict[str, str] = {}
    for current_run, prev_run in run_pairs:
        flow_results = flow_analytics.compute_flow(conn, current_run, config)
        tiers_for_run = {
            row.get("address"): row.get("tier")
            for row in flow_results.get("wallets_flow", [])
            if row.get("tier") in {"TIER_A", "TIER_B"}
        }
        wallet_tiers.update(tiers_for_run)
        position_rows.extend(_position_deltas(conn, current_run, prev_run))

    consensus_entries, consensus_wallets = compute_consensus_from_inputs(
        position_rows, wallet_tiers, config.consensus
    )
    prior_run_date = run_pairs[0][1]
    return {
        "run_date": run_date,
        "prior_run_date": prior_run_date,
        "lookback_days": config.consensus.lookback_days,
        "consensus_entries": consensus_entries,
        "consensus_wallets": consensus_wallets,
    }


def compute_consensus_from_inputs(
    position_rows: Iterable[dict[str, Any]],
    wallet_tiers: dict[str, str],
    consensus,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    aggregated: dict[tuple[str, str], dict[str, Any]] = {}
    wallet_support: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}

    for row in position_rows:
        classification = row.get("classification")
        if classification not in {"NEW_POSITION", "INCREASE"}:
            continue
        delta = row.get("delta_usd") or 0.0
        if delta < consensus.min_position_delta_usd:
            continue
        address = row.get("address")
        if not address:
            continue
        tier = wallet_tiers.get(address)
        if consensus.require_tierA_or_B and tier not in {"TIER_A", "TIER_B"}:
            continue

        market_id = row.get("market_id")
        outcome = row.get("outcome") or "Unknown"
        key = (market_id, outcome)
        entry = aggregated.setdefault(
            key,
            {
                "market_id": market_id,
                "outcome": outcome,
                "question": row.get("question"),
                "cluster_key": row.get("cluster_key") or "unknown",
                "wallets_supporting": set(),
                "wallets_new": set(),
                "wallets_increasing": set(),
                "tiers_A": 0,
                "tiers_B": 0,
                "total_delta_usd": 0.0,
                "total_new_usd": 0.0,
                "total_increase_usd": 0.0,
                "top_wallet": None,
                "top_wallet_delta": 0.0,
                "top_wallet_share": 0.0,
            },
        )

        entry["wallets_supporting"].add(address)
        if classification == "NEW_POSITION":
            entry["wallets_new"].add(address)
            entry["total_new_usd"] += delta
        else:
            entry["wallets_increasing"].add(address)
            entry["total_increase_usd"] += delta
        entry["total_delta_usd"] += delta
        if tier == "TIER_A":
            entry["tiers_A"] += 1
        elif tier == "TIER_B":
            entry["tiers_B"] += 1

        wallet_bucket = wallet_support.setdefault(key, {})
        wallet_row = wallet_bucket.setdefault(
            address,
            {
                "market_id": market_id,
                "outcome": outcome,
                "address": address,
                "tier": tier or "TIER_C",
                "delta_usd": 0.0,
                "classification": classification,
            },
        )
        wallet_row["delta_usd"] += delta
        if wallet_row["classification"] != "NEW_POSITION" and classification == "NEW_POSITION":
            wallet_row["classification"] = "NEW_POSITION"

    entries: list[dict[str, Any]] = []
    for entry in aggregated.values():
        wallets_supporting = entry["wallets_supporting"]
        total_delta = entry["total_delta_usd"]
        if len(wallets_supporting) < consensus.min_wallets:
            continue
        if total_delta < consensus.min_total_delta_usd:
            continue

        top_wallet, top_delta = _top_wallet(wallet_support.get((entry["market_id"], entry["outcome"]), {}))
        top_share = (top_delta / total_delta) if total_delta > 0 else 0.0
        entry["wallets_supporting"] = len(wallets_supporting)
        entry["wallets_new"] = len(entry["wallets_new"])
        entry["wallets_increasing"] = len(entry["wallets_increasing"])
        entry["top_wallet"] = top_wallet
        entry["top_wallet_delta"] = top_delta
        entry["top_wallet_share"] = top_share
        entry["score_consensus"] = _score_consensus(entry, consensus)
        entries.append(entry)

    entries = stable_sorted(
        entries,
        key=lambda item: (
            item.get("score_consensus", 0.0),
            item.get("total_delta_usd", 0.0),
            item.get("wallets_supporting", 0),
        ),
        reverse=True,
        tie_breaker=lambda item: (item.get("market_id"), item.get("outcome")),
    )

    consensus_wallet_rows: list[dict[str, Any]] = []
    for entry in entries[: consensus.top_n]:
        key = (entry["market_id"], entry["outcome"])
        wallets = list(wallet_support.get(key, {}).values())
        wallets = stable_sorted(
            wallets,
            key=lambda item: item.get("delta_usd", 0.0),
            reverse=True,
            tie_breaker=lambda item: item.get("address"),
        )
        consensus_wallet_rows.extend(wallets)

    return entries, consensus_wallet_rows


def _run_pairs(conn, run_date: str, lookback_days: int) -> list[tuple[str, str]]:
    run_dates = conn.execute(
        "SELECT run_date FROM runs WHERE run_date <= ? ORDER BY run_date DESC LIMIT ?",
        (run_date, max(lookback_days + 1, 2)),
    ).fetchall()
    dates = [row["run_date"] for row in run_dates]
    pairs: list[tuple[str, str]] = []
    for idx in range(min(lookback_days, len(dates) - 1)):
        pairs.append((dates[idx], dates[idx + 1]))
    return pairs


def _position_deltas(conn, run_date: str, prior_run_date: str) -> list[dict[str, Any]]:
    today_rows = conn.execute(
        """
        SELECT address, market_id, cluster_key, outcome, value_usd
        FROM wallet_market_daily
        WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()
    prev_rows = conn.execute(
        """
        SELECT address, market_id, cluster_key, outcome, value_usd
        FROM wallet_market_daily
        WHERE run_date = ?
        """,
        (prior_run_date,),
    ).fetchall()
    today_map = {(row["address"], row["market_id"], row["outcome"]): dict(row) for row in today_rows}
    prev_map = {(row["address"], row["market_id"], row["outcome"]): dict(row) for row in prev_rows}

    context = _market_context(conn, run_date)
    rows: list[dict[str, Any]] = []
    keys = set(today_map) | set(prev_map)
    for key in keys:
        address, market_id, outcome = key
        today_value = today_map.get(key, {}).get("value_usd") or 0.0
        prev_value = prev_map.get(key, {}).get("value_usd") or 0.0
        delta = today_value - prev_value
        classification = _classify_position(today_value, prev_value, delta)
        ctx = context.get(market_id, {})
        rows.append(
            {
                "run_date": run_date,
                "address": address,
                "market_id": market_id,
                "outcome": outcome,
                "cluster_key": ctx.get("cluster_key") or "unknown",
                "question": ctx.get("question"),
                "delta_usd": delta,
                "classification": classification,
            }
        )
    return rows


def _market_context(conn, run_date: str) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ms.market_id, ms.question, ms.cluster_key, m.question AS market_question, m.cluster_key AS market_cluster
        FROM market_snapshots ms
        LEFT JOIN markets m ON m.market_id = ms.market_id
        WHERE ms.run_date = ?
        """,
        (run_date,),
    ).fetchall()
    context: dict[str, dict[str, Any]] = {}
    for row in rows:
        question = row["question"] or row["market_question"]
        cluster_key = row["cluster_key"] or row["market_cluster"]
        context[row["market_id"]] = {
            "question": question,
            "cluster_key": cluster_key,
        }
    return context


def _classify_position(today_value: float, prev_value: float, delta: float) -> str:
    if prev_value <= 0 and today_value > 0:
        return "NEW_POSITION"
    if prev_value > 0 and today_value <= 0:
        return "CLOSED"
    if prev_value > 0 and delta > 0:
        return "INCREASE"
    if prev_value > 0 and delta < 0 and today_value > 0:
        return "DECREASE"
    return "UNCHANGED"


def _top_wallet(wallets: dict[str, dict[str, Any]]) -> tuple[str | None, float]:
    top_wallet = None
    top_delta = 0.0
    for address, row in wallets.items():
        delta = row.get("delta_usd", 0.0)
        if delta > top_delta or (delta == top_delta and address < (top_wallet or address)):
            top_wallet = address
            top_delta = delta
    return top_wallet, top_delta


def _score_consensus(entry: dict[str, Any], consensus) -> float:
    total_delta = entry.get("total_delta_usd", 0.0)
    wallets_supporting = entry.get("wallets_supporting", 0)
    wallets_new = entry.get("wallets_new", 0)
    tiers_A = entry.get("tiers_A", 0)
    top_share = entry.get("top_wallet_share", 0.0)
    concentration_penalty = 0.0
    if top_share > consensus.max_top_wallet_share:
        concentration_penalty = (top_share - consensus.max_top_wallet_share)
    return (
        math.log1p(total_delta) * consensus.weights.w_total_delta
        + wallets_supporting * consensus.weights.w_wallets
        + wallets_new * consensus.weights.w_new
        + tiers_A * consensus.weights.w_tierA
        - concentration_penalty * consensus.weights.w_concentration_penalty
    )
