from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable

from polymarket_insider.scoring.weights import stable_sorted


def build_wallet_market_daily(conn, run_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT h.address, h.wallet, h.market_id, h.outcome, h.value_usd, ms.cluster_key
        FROM holders h
        LEFT JOIN market_snapshots ms
          ON ms.run_date = h.run_date AND ms.market_id = h.market_id
        WHERE h.run_date = ? AND h.value_usd IS NOT NULL
        """,
        (run_date,),
    ).fetchall()

    created_at = datetime.utcnow().isoformat()
    output: list[dict[str, Any]] = []
    for row in rows:
        address = row["address"] or row["wallet"]
        if not address:
            continue
        outcome = normalize_outcome(row["outcome"])
        output.append(
            {
                "address": address,
                "market_id": row["market_id"],
                "cluster_key": row["cluster_key"] or "unknown",
                "outcome": outcome,
                "value_usd": row["value_usd"],
                "created_at": created_at,
            }
        )
    return output


def compute_flow(conn, run_date: str, config) -> dict[str, Any]:
    prior_run_date = _prior_run_date(conn, run_date)
    if not prior_run_date:
        return {
            "prior_run_date": None,
            "wallets_flow": [],
            "positions_flow": [],
            "markets_flow": [],
        }

    wallet_today = _wallet_metrics_by_address(conn, run_date)
    wallet_prev = _wallet_metrics_by_address(conn, prior_run_date)
    cluster_today = _cluster_totals(conn, run_date)
    cluster_prev = _cluster_totals(conn, prior_run_date)

    wallets_flow: list[dict[str, Any]] = []
    for address, today in wallet_today.items():
        prev = wallet_prev.get(address, {})
        total_today = today.get("total_usd") or 0.0
        total_prev = prev.get("total_usd") or 0.0
        total_delta = total_today - total_prev
        markets_delta = (today.get("markets_count") or 0) - (prev.get("markets_count") or 0)
        clusters_delta = (today.get("clusters_count") or 0) - (prev.get("clusters_count") or 0)
        top_cluster_today, top_cluster_share = _top_cluster(cluster_today.get(address, {}), total_today)
        top_cluster_delta, top_cluster_delta_usd = _top_cluster_delta(
            cluster_today.get(address, {}),
            cluster_prev.get(address, {}),
        )
        new_clusters_entered = _new_clusters_entered(
            cluster_today.get(address, {}),
            cluster_prev.get(address, {}),
        )

        flow_row = {
            "address": address,
            "total_usd_today": total_today,
            "total_usd_prev": total_prev,
            "total_usd_delta": total_delta,
            "markets_today": today.get("markets_count") or 0,
            "markets_prev": prev.get("markets_count") or 0,
            "markets_delta": markets_delta,
            "clusters_today": today.get("clusters_count") or 0,
            "clusters_prev": prev.get("clusters_count") or 0,
            "clusters_delta": clusters_delta,
            "top_cluster_today": top_cluster_today,
            "top_cluster_share_today": top_cluster_share,
            "top_cluster_delta": top_cluster_delta,
            "top_cluster_delta_usd": top_cluster_delta_usd,
            "new_clusters_entered_count": new_clusters_entered,
        }

        if not _flow_wallet_passes(flow_row, config.flow):
            continue

        flow_row["score_flow"] = _flow_score(flow_row, config.flow)
        wallets_flow.append(flow_row)

    wallets_flow = stable_sorted(
        wallets_flow,
        key=lambda item: item.get("score_flow", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("address"),
    )

    top_wallets = wallets_flow[: config.flow.top_wallets]
    positions_flow = _positions_flow(
        conn,
        run_date,
        prior_run_date,
        [wallet.get("address") for wallet in top_wallets],
        config.flow,
    )
    markets_flow = _markets_flow(positions_flow)

    return {
        "prior_run_date": prior_run_date,
        "wallets_flow": wallets_flow,
        "positions_flow": positions_flow,
        "markets_flow": markets_flow,
    }


def normalize_outcome(outcome: Any) -> str:
    text = (outcome or "").strip().lower()
    if text == "yes":
        return "Yes"
    if text == "no":
        return "No"
    return "Unknown"


def _prior_run_date(conn, run_date: str) -> str | None:
    row = conn.execute(
        "SELECT run_date FROM runs WHERE run_date < ? ORDER BY run_date DESC LIMIT 1",
        (run_date,),
    ).fetchone()
    return row["run_date"] if row else None


def _wallet_metrics_by_address(conn, run_date: str) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT address, total_usd, markets_count, clusters_count
        FROM wallet_metrics
        WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()
    return {row["address"]: dict(row) for row in rows}


def _cluster_totals(conn, run_date: str) -> dict[str, dict[str, float]]:
    rows = conn.execute(
        """
        SELECT address, cluster_key, value_usd
        FROM wallet_market_daily
        WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()
    totals: dict[str, dict[str, float]] = {}
    for row in rows:
        address = row["address"]
        cluster_key = row["cluster_key"] or "unknown"
        value = row["value_usd"] or 0.0
        totals.setdefault(address, {})
        totals[address][cluster_key] = totals[address].get(cluster_key, 0.0) + value
    return totals


def _top_cluster(cluster_totals: dict[str, float], total_usd: float) -> tuple[str | None, float | None]:
    if total_usd <= 0 or not cluster_totals:
        return None, None
    top_cluster = max(cluster_totals.items(), key=lambda item: (item[1], item[0]))[0]
    return top_cluster, cluster_totals[top_cluster] / total_usd


def _top_cluster_delta(
    today: dict[str, float],
    prev: dict[str, float],
) -> tuple[str | None, float]:
    all_clusters = set(today) | set(prev)
    if not all_clusters:
        return None, 0.0
    deltas = {}
    for cluster in all_clusters:
        deltas[cluster] = today.get(cluster, 0.0) - prev.get(cluster, 0.0)
    top_cluster = max(deltas.items(), key=lambda item: (item[1], item[0]))[0]
    return top_cluster, deltas[top_cluster]


def _new_clusters_entered(today: dict[str, float], prev: dict[str, float]) -> int:
    count = 0
    for cluster, value in today.items():
        if value > 0 and prev.get(cluster, 0.0) <= 0:
            count += 1
    return count


def _flow_wallet_passes(metric: dict[str, Any], flow) -> bool:
    total_today = metric.get("total_usd_today") or 0.0
    total_delta = metric.get("total_usd_delta") or 0.0
    top_cluster_share = metric.get("top_cluster_share_today")
    if total_today < flow.min_total_usd_today:
        return False
    if total_delta < flow.min_total_delta_usd:
        return False
    if top_cluster_share is None or top_cluster_share > flow.max_top_cluster_share_today:
        return False
    return True


def _flow_score(metric: dict[str, Any], flow) -> float:
    total_delta = metric.get("total_usd_delta") or 0.0
    new_clusters = metric.get("new_clusters_entered_count") or 0
    clusters_delta = metric.get("clusters_delta") or 0
    top_cluster_delta_usd = metric.get("top_cluster_delta_usd") or 0.0
    top_cluster_share = metric.get("top_cluster_share_today") or 0.0

    concentration_penalty = max(0.0, top_cluster_share - flow.max_top_cluster_share_today)
    cluster_bonus = 1.0 if clusters_delta > 0 else 0.0

    return (
        flow.weights.w_flow_delta * math.log1p(max(total_delta, 0.0))
        + flow.weights.w_new_clusters * new_clusters
        + flow.weights.w_cluster_delta * math.log1p(max(top_cluster_delta_usd, 0.0))
        + cluster_bonus
        - flow.weights.w_concentration_penalty * concentration_penalty
    )


def _positions_flow(
    conn,
    run_date: str,
    prior_run_date: str,
    addresses: Iterable[str],
    flow,
) -> list[dict[str, Any]]:
    address_list = [address for address in addresses if address]
    if not address_list:
        return []
    placeholders = ",".join("?" for _ in address_list)
    params_today = [run_date, *address_list]
    params_prev = [prior_run_date, *address_list]
    today_rows = conn.execute(
        f"""
        SELECT address, market_id, cluster_key, outcome, value_usd
        FROM wallet_market_daily
        WHERE run_date = ? AND address IN ({placeholders})
        """,
        params_today,
    ).fetchall()
    prev_rows = conn.execute(
        f"""
        SELECT address, market_id, cluster_key, outcome, value_usd
        FROM wallet_market_daily
        WHERE run_date = ? AND address IN ({placeholders})
        """,
        params_prev,
    ).fetchall()

    today_map = {(row["address"], row["market_id"], row["outcome"]): dict(row) for row in today_rows}
    prev_map = {(row["address"], row["market_id"], row["outcome"]): dict(row) for row in prev_rows}

    market_context = _market_context(conn, run_date)

    positions: list[dict[str, Any]] = []
    keys = set(today_map) | set(prev_map)
    for key in keys:
        address, market_id, outcome = key
        today_value = today_map.get(key, {}).get("value_usd") or 0.0
        prev_value = prev_map.get(key, {}).get("value_usd") or 0.0
        delta = today_value - prev_value
        classification = _classify_position(today_value, prev_value, delta)
        if classification == "NEW_POSITION" and today_value < flow.min_new_position_usd:
            continue
        if classification == "INCREASE" and delta < flow.min_position_delta_usd:
            continue
        if classification not in ("NEW_POSITION", "INCREASE"):
            continue
        context = market_context.get(market_id, {})
        positions.append(
            {
                "address": address,
                "market_id": market_id,
                "question": context.get("question"),
                "cluster_key": context.get("cluster_key") or "unknown",
                "outcome": outcome,
                "usd_today": today_value,
                "usd_prev": prev_value,
                "delta_usd": delta,
                "classification": classification,
            }
        )

    positions = stable_sorted(
        positions,
        key=lambda item: item.get("delta_usd", 0.0),
        reverse=True,
        tie_breaker=lambda item: (item.get("address"), item.get("market_id"), item.get("outcome")),
    )

    per_wallet: dict[str, list[dict[str, Any]]] = {}
    for row in positions:
        per_wallet.setdefault(row["address"], []).append(row)

    trimmed: list[dict[str, Any]] = []
    for address in sorted(per_wallet):
        trimmed.extend(per_wallet[address][: flow.top_positions_per_wallet])
    return trimmed


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


def _markets_flow(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    markets: dict[str, dict[str, Any]] = {}
    for row in positions:
        market_id = row["market_id"]
        delta = row.get("delta_usd") or 0.0
        market = markets.setdefault(
            market_id,
            {
                "market_id": market_id,
                "question": row.get("question"),
                "cluster_key": row.get("cluster_key") or "unknown",
                "wallets_increasing": set(),
                "wallets_new": set(),
                "total_delta_usd": 0.0,
                "top_wallet": None,
                "top_wallet_delta": None,
            },
        )
        market["total_delta_usd"] += delta
        market["wallets_increasing"].add(row["address"])
        if row.get("classification") == "NEW_POSITION":
            market["wallets_new"].add(row["address"])
        top_delta = market["top_wallet_delta"] if market["top_wallet_delta"] is not None else -1.0
        if delta > top_delta:
            market["top_wallet"] = row["address"]
            market["top_wallet_delta"] = delta

    rows: list[dict[str, Any]] = []
    for market in markets.values():
        rows.append(
            {
                "market_id": market["market_id"],
                "question": market["question"],
                "cluster_key": market["cluster_key"],
                "wallets_increasing": len(market["wallets_increasing"]),
                "wallets_new": len(market["wallets_new"]),
                "total_delta_usd": market["total_delta_usd"],
                "top_wallet": market["top_wallet"],
                "top_wallet_delta": market["top_wallet_delta"],
            }
        )

    return stable_sorted(
        rows,
        key=lambda item: item.get("total_delta_usd", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("market_id"),
    )


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
