from __future__ import annotations

import csv
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from polymarket_insider.config import load_config
from polymarket_insider.db import store
from polymarket_insider.scoring.weights import stable_sorted
from polymarket_insider.utils.io import ensure_dir


def write_report(run_date: date, db_path: Path, out_dir: Path) -> None:
    logger = logging.getLogger(__name__)
    root = Path(__file__).resolve().parents[2]
    config = load_config(root / "config.yaml")
    conn = store.get_connection(db_path)
    diagnostics = store.fetch_run_diagnostics(conn, run_date.isoformat())
    trades_derived = conn.execute(
        "SELECT COUNT(*) AS count FROM holders WHERE run_date = ? AND source = 'trades_derived'",
        (run_date.isoformat(),),
    ).fetchone()
    trades_derived_count = trades_derived["count"] if trades_derived else 0

    rows = conn.execute(
        """
        SELECT ms.market_id, ms.score, ms.signals_json,
               m.question, m.slug, m.close_time, m.volume_usd, m.liquidity_usd, m.cluster_key
        FROM market_scores ms
        JOIN markets m ON m.market_id = ms.market_id
        WHERE ms.run_date = ?
        """,
        (run_date.isoformat(),),
    ).fetchall()
    conn.close()

    records = []
    for row in rows:
        signals = json.loads(row["signals_json"]) if row["signals_json"] else {}
        records.append(
            {
                "market_id": row["market_id"],
                "question": row["question"],
                "slug": row["slug"],
                "cluster_key": row["cluster_key"],
                "close_time": row["close_time"],
                "volume_usd": row["volume_usd"],
                "liquidity_usd": row["liquidity_usd"],
                "score": row["score"],
                **signals,
            }
        )

    records = stable_sorted(
        records,
        key=lambda item: item.get("score", 0.0),
        reverse=True,
        tie_breaker=lambda item: item.get("market_id"),
    )

    ensure_dir(out_dir)
    report_date = run_date.isoformat()
    md_path = out_dir / f"report_{report_date}.md"
    csv_path = out_dir / f"report_{report_date}.csv"
    watchlist_path = out_dir / "watchlist.json"

    headers = [
        "market_id",
        "question",
        "slug",
        "score",
        "volume_usd",
        "liquidity_usd",
        "close_time",
        "conviction_wallets",
        "conviction_wallets_usd",
        "conviction_wallets_shares",
        "whale_wallets",
        "whale_wallets_usd",
        "whale_wallets_shares",
        "new_wallets",
        "convergence",
        "wallet_signal",
    ]
    csv_rows = [{key: record.get(key) for key in headers} for record in records]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(csv_rows)

    diversified_top50 = apply_cluster_cap(
        records,
        limit=50,
        max_per_cluster=config.diversity.max_per_cluster_top50,
        enabled=config.diversity.enabled,
    )
    diversified_watchlist = apply_cluster_cap(
        records,
        limit=20,
        max_per_cluster=config.diversity.max_per_cluster_watchlist,
        enabled=config.diversity.enabled,
    )
    top50_cluster_counts = cluster_counts(diversified_top50)
    max_cluster_share = (
        max(top50_cluster_counts.values()) / len(diversified_top50)
        if diversified_top50
        else 0.0
    )

    logger.info(
        "Top50 cluster share max=%.3f top_clusters=%s",
        max_cluster_share,
        sorted(top50_cluster_counts.items(), key=lambda item: item[1], reverse=True)[:5],
    )

    with md_path.open("w", encoding="utf-8") as handle:
        handle.write(f"# Polymarket Insider Report ({report_date})\n\n")
        handle.write("## Collection diagnostics\n\n")
        kept = diagnostics.get("markets_kept", 0)
        holders_succeeded = diagnostics.get("holder_markets_succeeded", 0)
        coverage_pct = (holders_succeeded / kept * 100.0) if kept else 0.0
        handle.write(
            f"- markets_fetched: {diagnostics.get('markets_fetched', 0)}\n"
            f"- markets_kept: {diagnostics.get('markets_kept', 0)}\n"
            f"- markets_with_holders: {diagnostics.get('markets_with_holders', 0)}\n"
            f"- scored_markets: {diagnostics.get('scored_markets', 0)}\n"
            f"- scored_wallets: {diagnostics.get('scored_wallets', 0)}\n"
            f"- missing_close_time: {diagnostics.get('missing_close_time', 0)}\n"
            f"- holder_markets_targeted: {diagnostics.get('holder_markets_targeted', 0)}\n"
            f"- holder_markets_succeeded: {diagnostics.get('holder_markets_succeeded', 0)}\n"
            f"- holder_markets_failed: {diagnostics.get('holder_markets_failed', 0)}\n"
            f"- holders_coverage_pct: {coverage_pct:.1f}\n"
            f"- holders_rate_limited_count: {diagnostics.get('holders_rate_limited_count', 0)}\n"
            f"- holders_retry_count: {diagnostics.get('holders_retry_count', 0)}\n"
            f"- unknown_outcome_rows: {diagnostics.get('unknown_outcome_rows', 0)}\n"
            f"- top50_max_cluster_share: {max_cluster_share:.3f}\n"
        )
        reasons = diagnostics.get("filter_reasons", {})
        if isinstance(reasons, dict) and reasons:
            sorted_reasons = sorted(reasons.items(), key=lambda item: item[1], reverse=True)
            handle.write("- top_filter_reasons:\n")
            for reason, count in sorted_reasons[:5]:
                handle.write(f"  - {reason}: {count}\n")
        else:
            handle.write("- top_filter_reasons: none\n")
        handle.write("\n")
        if trades_derived_count > 0:
            handle.write(
                f"WARNING: {trades_derived_count} holder rows derived from trades (no holders API).\n\n"
            )
        if top50_cluster_counts:
            handle.write("Top clusters in Top 50:\n")
            for key, count in sorted(top50_cluster_counts.items(), key=lambda item: item[1], reverse=True)[:10]:
                handle.write(f"- {key}: {count}\n")
            handle.write("\n")
        if not records:
            handle.write("No markets scored for this run.\n")
        else:
            handle.write("| Rank | Market | Score | Conviction (usd/shares) | Whales (usd/shares) | New | Close |\n")
            handle.write("| --- | --- | --- | --- | --- | --- | --- |\n")
            for idx, record in enumerate(diversified_top50, start=1):
                handle.write(
                    f"| {idx} | {record.get('question')} | {record.get('score'):.4f} "
                    f"| {record.get('conviction_wallets_usd', 0)}/{record.get('conviction_wallets_shares', 0)} "
                    f"| {record.get('whale_wallets_usd', 0)}/{record.get('whale_wallets_shares', 0)} "
                    f"| {record.get('new_wallets', 0)} | {record.get('close_time')} |\n"
                )

    watchlist = [
        {
            "market_id": record.get("market_id"),
            "question": record.get("question"),
            "score": record.get("score"),
            "signals": {
                "conviction_wallets": record.get("conviction_wallets", 0),
                "conviction_wallets_usd": record.get("conviction_wallets_usd", 0),
                "conviction_wallets_shares": record.get("conviction_wallets_shares", 0),
                "whale_wallets": record.get("whale_wallets", 0),
                "whale_wallets_usd": record.get("whale_wallets_usd", 0),
                "whale_wallets_shares": record.get("whale_wallets_shares", 0),
                "new_wallets": record.get("new_wallets", 0),
                "convergence": record.get("convergence", False),
                "wallet_signal": record.get("wallet_signal", 0.0),
            },
        }
        for record in diversified_watchlist
    ]

    with watchlist_path.open("w", encoding="utf-8") as handle:
        json.dump(watchlist, handle, ensure_ascii=True, indent=2)


def apply_cluster_cap(
    records: list[dict[str, Any]],
    limit: int,
    max_per_cluster: int,
    enabled: bool = True,
) -> list[dict[str, Any]]:
    if not enabled:
        return records[:limit]
    selected = []
    counts: dict[str, int] = {}
    for record in records:
        if len(selected) >= limit:
            break
        cluster_key = record.get("cluster_key") or "unknown"
        count = counts.get(cluster_key, 0)
        if count >= max_per_cluster:
            continue
        counts[cluster_key] = count + 1
        selected.append(record)
    return selected


def cluster_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        key = record.get("cluster_key") or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts
