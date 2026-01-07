from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable

from polymarket_insider.scoring.weights import stable_sorted


def compute_wallet_metrics(conn, run_date: str) -> list[dict[str, Any]]:
    market_context = _load_market_context(conn, run_date)
    rows = conn.execute(
        """
        SELECT market_id, wallet, address, outcome, value_usd
        FROM holders
        WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()

    aggregates: dict[str, dict[str, Any]] = {}
    for row in rows:
        address = row["address"] or row["wallet"]
        if not address:
            continue
        market_id = row["market_id"]
        outcome = (row["outcome"] or "").strip().lower()
        value_usd = row["value_usd"]
        value = float(value_usd) if value_usd is not None else 0.0

        context = market_context.get(market_id, {})
        cluster_key = context.get("cluster_key") or "unknown"

        wallet_state = aggregates.setdefault(
            address,
            {
                "total_usd": 0.0,
                "markets": set(),
                "clusters": set(),
                "market_usd": {},
                "cluster_usd": {},
                "yes_usd": 0.0,
                "no_usd": 0.0,
            },
        )

        wallet_state["total_usd"] += value
        wallet_state["markets"].add(market_id)
        wallet_state["clusters"].add(cluster_key)

        if value:
            wallet_state["market_usd"][market_id] = wallet_state["market_usd"].get(market_id, 0.0) + value
            wallet_state["cluster_usd"][cluster_key] = wallet_state["cluster_usd"].get(cluster_key, 0.0) + value

        if outcome == "yes":
            wallet_state["yes_usd"] += value
        elif outcome == "no":
            wallet_state["no_usd"] += value

    created_at = datetime.utcnow().isoformat()
    metrics: list[dict[str, Any]] = []
    for address, wallet_state in aggregates.items():
        total_usd = wallet_state["total_usd"]
        markets_count = len(wallet_state["markets"])
        clusters_count = len(wallet_state["clusters"])
        market_usd = wallet_state["market_usd"]
        cluster_usd = wallet_state["cluster_usd"]

        top_cluster_share = _share_of_max(cluster_usd.values(), total_usd)
        top_market_share = _share_of_max(market_usd.values(), total_usd)
        hhi_markets = _hhi(market_usd.values(), total_usd)
        hhi_clusters = _hhi(cluster_usd.values(), total_usd)

        yes_usd = wallet_state["yes_usd"]
        no_usd = wallet_state["no_usd"]
        yes_share = _ratio(yes_usd, yes_usd + no_usd)
        sidedness = None
        if yes_share is not None:
            sidedness = abs(yes_share - 0.5) * 2

        metrics.append(
            {
                "run_date": run_date,
                "address": address,
                "total_usd": total_usd,
                "markets_count": markets_count,
                "clusters_count": clusters_count,
                "top_cluster_share": top_cluster_share,
                "yes_usd": yes_usd,
                "no_usd": no_usd,
                "yes_share": yes_share,
                "sidedness": sidedness,
                "top_market_share": top_market_share,
                "hhi_markets": hhi_markets,
                "hhi_clusters": hhi_clusters,
                "created_at": created_at,
            }
        )

    return metrics


def score_wallet_metrics(
    metrics: Iterable[dict[str, Any]],
    config,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    passed: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for metric in metrics:
        reasons = _wallet_filter_reasons(metric, config.wallet_filters)
        if reasons:
            excluded.append({**metric, "reasons": ",".join(reasons)})
            continue
        score_wallet = _wallet_score(metric, config.wallet_ranking)
        passed.append({**metric, "score_wallet": score_wallet})

    passed = stable_sorted(
        passed,
        key=lambda item: item.get("score_wallet", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("address"),
    )
    excluded = stable_sorted(
        excluded,
        key=lambda item: item.get("total_usd", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("address"),
    )
    return passed, excluded


def wallet_positions(
    conn,
    run_date: str,
    addresses: Iterable[str],
    top_n_positions: int,
) -> list[dict[str, Any]]:
    address_list = [address for address in addresses if address]
    if not address_list:
        return []

    placeholders = ",".join("?" for _ in address_list)
    params = [run_date, run_date, *address_list]
    rows = conn.execute(
        f"""
        SELECT h.address, h.wallet, h.market_id, h.outcome, h.value_usd,
               COALESCE(ms.question, m.question) AS question,
               COALESCE(ms.cluster_key, m.cluster_key) AS cluster_key
        FROM holders h
        LEFT JOIN market_snapshots ms
          ON ms.run_date = ? AND ms.market_id = h.market_id
        LEFT JOIN markets m
          ON m.market_id = h.market_id
        WHERE h.run_date = ? AND h.address IN ({placeholders})
        """,
        params,
    ).fetchall()

    per_wallet: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        address = row["address"] or row["wallet"]
        if not address:
            continue
        per_wallet.setdefault(address, []).append(
            {
                "address": address,
                "market_id": row["market_id"],
                "question": row["question"],
                "cluster_key": row["cluster_key"] or "unknown",
                "outcome": row["outcome"],
                "value_usd": row["value_usd"],
            }
        )

    output: list[dict[str, Any]] = []
    for address in sorted(per_wallet):
        rows_for_wallet = per_wallet[address]
        ranked = stable_sorted(
            rows_for_wallet,
            key=lambda item: item.get("value_usd") or 0.0,
            reverse=True,
            tie_breaker=lambda item: item.get("market_id"),
        )
        output.extend(ranked[:top_n_positions])
    return output


def clusters_summary(conn, run_date: str) -> list[dict[str, Any]]:
    market_context = _load_market_context(conn, run_date)
    rows = conn.execute(
        """
        SELECT market_id, wallet, address, value_usd
        FROM holders
        WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()

    clusters: dict[str, dict[str, Any]] = {}
    for row in rows:
        address = row["address"] or row["wallet"]
        if not address:
            continue
        market_id = row["market_id"]
        value_usd = row["value_usd"]
        value = float(value_usd) if value_usd is not None else 0.0
        cluster_key = market_context.get(market_id, {}).get("cluster_key") or "unknown"

        cluster_state = clusters.setdefault(
            cluster_key,
            {
                "total_usd": 0.0,
                "markets": set(),
                "wallets": set(),
                "wallet_usd": {},
            },
        )
        cluster_state["total_usd"] += value
        cluster_state["markets"].add(market_id)
        cluster_state["wallets"].add(address)
        cluster_state["wallet_usd"][address] = cluster_state["wallet_usd"].get(address, 0.0) + value

    summary: list[dict[str, Any]] = []
    for cluster_key, cluster_state in clusters.items():
        wallet_usd = cluster_state["wallet_usd"]
        top_wallet = None
        top_wallet_usd = None
        if wallet_usd:
            top_wallet = max(wallet_usd.items(), key=lambda item: (item[1], item[0]))[0]
            top_wallet_usd = wallet_usd[top_wallet]
        summary.append(
            {
                "cluster_key": cluster_key,
                "markets_in_cluster": len(cluster_state["markets"]),
                "total_holder_usd": cluster_state["total_usd"],
                "wallets": len(cluster_state["wallets"]),
                "top_wallet": top_wallet,
                "top_wallet_usd": top_wallet_usd,
            }
        )

    return stable_sorted(
        summary,
        key=lambda item: item.get("total_holder_usd", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("cluster_key"),
    )


def wallet_top_clusters(conn, run_date: str, addresses: Iterable[str]) -> dict[str, str]:
    address_list = [address for address in addresses if address]
    if not address_list:
        return {}
    market_context = _load_market_context(conn, run_date)
    placeholders = ",".join("?" for _ in address_list)
    params = [run_date, *address_list]
    rows = conn.execute(
        f"""
        SELECT market_id, wallet, address, value_usd
        FROM holders
        WHERE run_date = ? AND address IN ({placeholders})
        """,
        params,
    ).fetchall()

    per_wallet: dict[str, dict[str, float]] = {}
    for row in rows:
        address = row["address"] or row["wallet"]
        if not address:
            continue
        market_id = row["market_id"]
        cluster_key = market_context.get(market_id, {}).get("cluster_key") or "unknown"
        value_usd = row["value_usd"]
        value = float(value_usd) if value_usd is not None else 0.0
        per_wallet.setdefault(address, {})
        per_wallet[address][cluster_key] = per_wallet[address].get(cluster_key, 0.0) + value

    top_clusters: dict[str, str] = {}
    for address, clusters in per_wallet.items():
        if not clusters:
            continue
        top_cluster = max(clusters.items(), key=lambda item: (item[1], item[0]))[0]
        top_clusters[address] = top_cluster
    return top_clusters


def _load_market_context(conn, run_date: str) -> dict[str, dict[str, Any]]:
    snapshots = conn.execute(
        """
        SELECT market_id, question, cluster_key
        FROM market_snapshots
        WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()
    markets = conn.execute(
        """
        SELECT market_id, question, cluster_key
        FROM markets
        """,
    ).fetchall()

    context: dict[str, dict[str, Any]] = {}
    for row in markets:
        context[row["market_id"]] = {
            "question": row["question"],
            "cluster_key": row["cluster_key"] or "unknown",
        }
    for row in snapshots:
        market_id = row["market_id"]
        entry = context.setdefault(market_id, {})
        if row["question"]:
            entry["question"] = row["question"]
        if row["cluster_key"]:
            entry["cluster_key"] = row["cluster_key"]
    return context


def _wallet_filter_reasons(metric: dict[str, Any], filters) -> list[str]:
    reasons: list[str] = []
    total_usd = metric.get("total_usd")
    markets_count = metric.get("markets_count")
    clusters_count = metric.get("clusters_count")
    top_cluster_share = metric.get("top_cluster_share")
    top_market_share = metric.get("top_market_share")
    hhi_clusters = metric.get("hhi_clusters")
    sidedness = metric.get("sidedness")

    if total_usd is None or total_usd < filters.min_total_usd:
        reasons.append("min_total_usd")
    if markets_count is None or markets_count < filters.min_markets:
        reasons.append("min_markets")
    if clusters_count is None or clusters_count < filters.min_clusters:
        reasons.append("min_clusters")
    if top_cluster_share is None or top_cluster_share > filters.max_top_cluster_share:
        reasons.append("max_top_cluster_share")
    if top_market_share is None or top_market_share > filters.max_top_market_share:
        reasons.append("max_top_market_share")
    if hhi_clusters is None or hhi_clusters > filters.max_hhi_clusters:
        reasons.append("max_hhi_clusters")
    if sidedness is not None and sidedness > filters.max_sidedness:
        reasons.append("max_sidedness")
    return reasons


def _wallet_score(metric: dict[str, Any], weights) -> float:
    total_usd = metric.get("total_usd") or 0.0
    markets_count = metric.get("markets_count") or 0
    clusters_count = metric.get("clusters_count") or 0
    top_cluster_share = _safe_metric(metric.get("top_cluster_share"), 1.0)
    hhi_clusters = _safe_metric(metric.get("hhi_clusters"), 1.0)
    top_market_share = _safe_metric(metric.get("top_market_share"), 1.0)
    hhi_markets = _safe_metric(metric.get("hhi_markets"), 1.0)
    sidedness = _safe_metric(metric.get("sidedness"), 0.0)

    diversity_bonus = (1 - top_cluster_share) + (1 - hhi_clusters)
    concentration_penalty = hhi_markets + top_market_share

    return (
        weights.w_total_usd_log * math.log1p(total_usd)
        + weights.w_markets * markets_count
        + weights.w_clusters * clusters_count
        + weights.w_diversity_bonus * diversity_bonus
        + weights.w_concentration_penalty * concentration_penalty
        + weights.w_sidedness_penalty * sidedness
    )


def _share_of_max(values: Iterable[float], total: float) -> float | None:
    if total <= 0:
        return None
    max_value = max(values, default=0.0)
    return max_value / total


def _hhi(values: Iterable[float], total: float) -> float | None:
    if total <= 0:
        return None
    return sum((value / total) ** 2 for value in values if value > 0)


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _safe_metric(value: float | None, default: float) -> float:
    return value if value is not None else default
