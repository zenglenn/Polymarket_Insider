from __future__ import annotations

import csv
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from polymarket_insider.analytics import consensus as consensus_analytics
from polymarket_insider.analytics import flow as flow_analytics
from polymarket_insider.analytics import wallet_metrics as wallet_analytics
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
    wallet_metrics_rows = store.fetch_wallet_metrics(conn, run_date.isoformat())
    ranked_wallets, excluded_wallets = wallet_analytics.score_wallet_metrics(
        wallet_metrics_rows,
        config,
    )
    top_wallets = ranked_wallets[: config.report.top_wallets]
    top_excluded = excluded_wallets[: config.report.top_wallets]
    top_wallet_addresses = [wallet.get("address") for wallet in top_wallets if wallet.get("address")]
    wallet_positions_rows = wallet_analytics.wallet_positions(
        conn,
        run_date.isoformat(),
        top_wallet_addresses,
        top_n_positions=10,
    )
    cluster_summary_rows = wallet_analytics.clusters_summary(conn, run_date.isoformat())
    top_wallet_cluster_map = wallet_analytics.wallet_top_clusters(
        conn,
        run_date.isoformat(),
        [wallet.get("address") for wallet in ranked_wallets[:50]],
    )
    flow_results = flow_analytics.compute_flow(conn, run_date.isoformat(), config)
    consensus_results = consensus_analytics.compute_consensus(conn, run_date.isoformat(), config)
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
    wallets_ranked_path = out_dir / f"wallets_ranked_{report_date}.csv"
    wallets_concentrated_path = out_dir / f"wallets_concentrated_{report_date}.csv"
    wallets_positions_path = out_dir / f"wallet_positions_top_{report_date}.csv"
    clusters_summary_path = out_dir / f"clusters_summary_{report_date}.csv"
    wallets_flow_path = out_dir / f"wallets_flow_{report_date}.csv"
    wallets_positions_flow_path = out_dir / f"wallet_positions_flow_{report_date}.csv"
    markets_flow_path = out_dir / f"markets_flow_{report_date}.csv"
    consensus_flow_path = out_dir / f"consensus_flow_{report_date}.csv"
    consensus_flow_wallets_path = out_dir / f"consensus_flow_wallets_{report_date}.csv"

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

    wallets_ranked_headers = [
        "rank",
        "address",
        "score_wallet",
        "total_usd",
        "markets_count",
        "clusters_count",
        "top_cluster_share",
        "hhi_clusters",
        "hhi_markets",
        "top_market_share",
        "yes_share",
        "sidedness",
    ]
    with wallets_ranked_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=wallets_ranked_headers)
        writer.writeheader()
        for idx, wallet in enumerate(ranked_wallets, start=1):
            row = {"rank": idx}
            row.update({key: wallet.get(key) for key in wallets_ranked_headers if key != "rank"})
            writer.writerow(row)

    wallets_concentrated_headers = [
        "address",
        "total_usd",
        "markets_count",
        "clusters_count",
        "top_cluster_share",
        "reason",
    ]
    with wallets_concentrated_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=wallets_concentrated_headers)
        writer.writeheader()
        for wallet in excluded_wallets:
            writer.writerow(
                {
                    "address": wallet.get("address"),
                    "total_usd": wallet.get("total_usd"),
                    "markets_count": wallet.get("markets_count"),
                    "clusters_count": wallet.get("clusters_count"),
                    "top_cluster_share": wallet.get("top_cluster_share"),
                    "reason": wallet.get("reasons"),
                }
            )

    wallet_positions_headers = [
        "address",
        "market_id",
        "question",
        "cluster_key",
        "outcome",
        "value_usd",
    ]
    with wallets_positions_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=wallet_positions_headers)
        writer.writeheader()
        writer.writerows(wallet_positions_rows)

    cluster_summary_headers = [
        "cluster_key",
        "markets_in_cluster",
        "total_holder_usd",
        "wallets",
        "top_wallet",
        "top_wallet_usd",
    ]
    with clusters_summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=cluster_summary_headers)
        writer.writeheader()
        writer.writerows(cluster_summary_rows)

    wallets_flow_headers = [
        "address",
        "tier",
        "score_flow",
        "total_usd_today",
        "total_usd_prev",
        "total_usd_delta",
        "markets_today",
        "markets_prev",
        "markets_delta",
        "clusters_today",
        "clusters_prev",
        "clusters_delta",
        "top_cluster_today",
        "top_cluster_share_today",
        "top_cluster_delta",
        "top_cluster_delta_usd",
        "new_clusters_entered_count",
    ]
    with wallets_flow_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=wallets_flow_headers)
        writer.writeheader()
        for row in flow_results.get("wallets_flow", []):
            writer.writerow({key: row.get(key) for key in wallets_flow_headers})

    wallets_positions_flow_headers = [
        "address",
        "market_id",
        "question",
        "cluster_key",
        "outcome",
        "usd_today",
        "usd_prev",
        "delta_usd",
        "classification",
    ]
    with wallets_positions_flow_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=wallets_positions_flow_headers)
        writer.writeheader()
        writer.writerows(flow_results.get("positions_flow", []))

    markets_flow_headers = [
        "market_id",
        "question",
        "cluster_key",
        "wallets_increasing",
        "wallets_new",
        "total_delta_usd",
        "top_wallet",
        "top_wallet_delta",
    ]
    with markets_flow_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=markets_flow_headers)
        writer.writeheader()
        writer.writerows(flow_results.get("markets_flow", []))

    consensus_flow_headers = [
        "rank",
        "score_consensus",
        "market_id",
        "question",
        "cluster_key",
        "outcome",
        "wallets_supporting",
        "wallets_new",
        "wallets_increasing",
        "tiers_A",
        "tiers_B",
        "total_delta_usd",
        "total_new_usd",
        "total_increase_usd",
        "top_wallet",
        "top_wallet_delta",
        "top_wallet_share",
        "fallback",
    ]
    with consensus_flow_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=consensus_flow_headers)
        writer.writeheader()
        for idx, row in enumerate(consensus_results.get("consensus_entries", []), start=1):
            payload = {key: row.get(key) for key in consensus_flow_headers if key not in {"rank"}}
            payload["rank"] = idx
            writer.writerow(payload)

    consensus_wallet_headers = [
        "market_id",
        "outcome",
        "address",
        "tier",
        "delta_usd",
        "classification",
    ]
    with consensus_flow_wallets_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=consensus_wallet_headers)
        writer.writeheader()
        writer.writerows(consensus_results.get("consensus_wallets", []))

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
    wallet_cluster_counts = cluster_counts(
        [{"cluster_key": cluster} for cluster in top_wallet_cluster_map.values()]
    )
    wallet_cluster_max_share = (
        max(wallet_cluster_counts.values()) / len(top_wallet_cluster_map)
        if top_wallet_cluster_map
        else 0.0
    )
    logger.info(
        "Top50 wallet cluster share max=%.3f top_clusters=%s",
        wallet_cluster_max_share,
        sorted(wallet_cluster_counts.items(), key=lambda item: item[1], reverse=True)[:5],
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
        handle.write("## Wallet Summary\n\n")
        handle.write(
            "Signal candidates pass the wallet filters: minimum USD exposure, participation across multiple "
            "markets and clusters, and concentration/sidedness caps. Rankings reward diversified exposure and "
            "penalize single-cluster or single-market concentration.\n\n"
        )
        if top_wallets:
            handle.write(
                "| Rank | Address | Score | Total USD | Markets | Clusters | Top cluster share | "
                "HHI clusters | HHI markets | Top market share | Yes share | Sidedness |\n"
            )
            handle.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for idx, wallet in enumerate(top_wallets, start=1):
                handle.write(
                    f"| {idx} | {wallet.get('address')} | {_fmt(wallet.get('score_wallet'), 4)} "
                    f"| {_fmt(wallet.get('total_usd'), 2)} | {wallet.get('markets_count')} "
                    f"| {wallet.get('clusters_count')} | {_fmt(wallet.get('top_cluster_share'), 2)} "
                    f"| {_fmt(wallet.get('hhi_clusters'), 2)} | {_fmt(wallet.get('hhi_markets'), 2)} "
                    f"| {_fmt(wallet.get('top_market_share'), 2)} | {_fmt(wallet.get('yes_share'), 2)} "
                    f"| {_fmt(wallet.get('sidedness'), 2)} |\n"
                )
        else:
            handle.write("No wallets passed filters for this run.\n")
        handle.write("\n")
        if top_excluded:
            handle.write("### Concentrated Event Traders (Excluded)\n\n")
            handle.write("| Rank | Address | Total USD | Markets | Clusters | Top cluster share | Reason |\n")
            handle.write("| --- | --- | --- | --- | --- | --- | --- |\n")
            for idx, wallet in enumerate(top_excluded, start=1):
                handle.write(
                    f"| {idx} | {wallet.get('address')} | {_fmt(wallet.get('total_usd'), 2)} "
                    f"| {wallet.get('markets_count')} | {wallet.get('clusters_count')} "
                    f"| {_fmt(wallet.get('top_cluster_share'), 2)} | {wallet.get('reasons')} |\n"
                )
            handle.write("\n")
        handle.write("## Flow (Since Prior Run)\n\n")
        prior_run = flow_results.get("prior_run_date")
        if not prior_run:
            handle.write("No prior run available to compute flow deltas.\n\n")
        else:
            handle.write(f"- prior_run_date: {prior_run}\n\n")
            top_flow_wallets = flow_results.get("wallets_flow", [])[:15]
            top_flow_markets = flow_results.get("markets_flow", [])[:15]
            top_flow_shortlist = [
                wallet
                for wallet in flow_results.get("wallets_flow", [])
                if wallet.get("tier") in {"TIER_A", "TIER_B"}
            ][:5]
            new_positions = [
                row for row in flow_results.get("positions_flow", []) if row.get("classification") == "NEW_POSITION"
            ]
            new_positions = stable_sorted(
                new_positions,
                key=lambda item: item.get("delta_usd", 0.0),
                reverse=True,
                tie_breaker=lambda item: (item.get("address"), item.get("market_id"), item.get("outcome")),
            )[:10]
            handle.write(
                "Flow wallets are signal candidates whose total exposure increased and stayed diversified across "
                "clusters. Position flow highlights new or increasing positions above thresholds.\n\n"
            )
            handle.write("Today's Flow Shortlist:\n")
            if top_flow_shortlist:
                for wallet in top_flow_shortlist:
                    handle.write(
                        f"- {wallet.get('tier')}: {wallet.get('address')} "
                        f"(delta {_fmt(wallet.get('total_usd_delta'), 2)}, "
                        f"top_cluster_delta {_fmt(wallet.get('top_cluster_delta_usd'), 2)})\n"
                    )
            else:
                handle.write("- none\n")
            handle.write("\n")
            if top_flow_wallets:
                handle.write(
                    "| Rank | Address | Score | Total USD Delta | New Clusters | Top Cluster Delta USD |\n"
                )
                handle.write("| --- | --- | --- | --- | --- | --- |\n")
                for idx, wallet in enumerate(top_flow_wallets, start=1):
                    handle.write(
                        f"| {idx} | {wallet.get('address')} | {_fmt(wallet.get('score_flow'), 4)} "
                        f"| {_fmt(wallet.get('total_usd_delta'), 2)} | {wallet.get('new_clusters_entered_count')} "
                        f"| {_fmt(wallet.get('top_cluster_delta_usd'), 2)} |\n"
                    )
            else:
                handle.write("No flow wallets met the thresholds.\n")
            handle.write("\n")
            handle.write("Top NEW positions (by delta):\n")
            if new_positions:
                handle.write("| Rank | Address | Market | Delta USD | Outcome |\n")
                handle.write("| --- | --- | --- | --- | --- |\n")
                for idx, position in enumerate(new_positions, start=1):
                    handle.write(
                        f"| {idx} | {position.get('address')} | {position.get('question')} "
                        f"| {_fmt(position.get('delta_usd'), 2)} | {position.get('outcome')} |\n"
                    )
            else:
                handle.write("- none\n")
            handle.write("\n")
            if top_flow_markets:
                handle.write("| Rank | Market | Total Delta USD | Wallets Increasing | Wallets New |\n")
                handle.write("| --- | --- | --- | --- | --- |\n")
                for idx, market in enumerate(top_flow_markets, start=1):
                    handle.write(
                        f"| {idx} | {market.get('question')} | {_fmt(market.get('total_delta_usd'), 2)} "
                        f"| {market.get('wallets_increasing')} | {market.get('wallets_new')} |\n"
                    )
            else:
                handle.write("No market flow met the thresholds.\n")
            handle.write("\n")
        if wallet_cluster_counts:
            handle.write("Top clusters in Top 50 wallets:\n")
            for key, count in sorted(wallet_cluster_counts.items(), key=lambda item: item[1], reverse=True)[:10]:
                handle.write(f"- {key}: {count}\n")
            handle.write(f"- top50_wallets_max_cluster_share: {wallet_cluster_max_share:.3f}\n\n")

        handle.write("## Consensus Flow\n\n")
        prior_run = consensus_results.get("prior_run_date") or "none"
        diagnostics = consensus_results.get("diagnostics", {})
        handle.write(
            f"- prior_run_date: {prior_run}\n"
            f"- lookback_days: {consensus_results.get('lookback_days')}\n"
            f"- candidate_flow_wallets_A_B: {diagnostics.get('candidate_flow_wallets_A_B', 0)}\n"
            f"- candidate_position_deltas: {diagnostics.get('candidate_position_deltas', 0)}\n"
            f"- unique_candidate_keys: {diagnostics.get('unique_candidate_keys', 0)}\n"
            f"- keys_meeting_min_wallets: {diagnostics.get('keys_meeting_min_wallets', 0)}\n"
            f"- keys_meeting_min_total_delta: {diagnostics.get('keys_meeting_min_total_delta', 0)}\n"
            f"- keys_meeting_max_top_wallet_share: {diagnostics.get('keys_meeting_max_top_wallet_share', 0)}\n"
            f"- final_consensus_rows: {diagnostics.get('final_consensus_rows', 0)}\n"
            f"- fallback_used: {diagnostics.get('fallback_used', 0)}\n\n"
        )
        consensus_top = consensus_results.get("consensus_entries", [])[:10]
        handle.write(
            "Consensus Flow highlights markets/outcomes seeing independent NEW/INCREASE from multiple "
            "Tier A/B wallets.\n\n"
        )
        if consensus_top:
            handle.write(
                "| Rank | Market | Outcome | Wallets | Total Delta USD | Top Wallet Share |\n"
            )
            handle.write("| --- | --- | --- | --- | --- | --- |\n")
            for idx, row in enumerate(consensus_top, start=1):
                handle.write(
                    f"| {idx} | {row.get('question')} | {row.get('outcome')} "
                    f"| {row.get('wallets_supporting')} | {_fmt(row.get('total_delta_usd'), 2)} "
                    f"| {_fmt(row.get('top_wallet_share'), 2)} |\n"
                )
        else:
            handle.write("No consensus flow entries met thresholds.\n")
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


def _fmt(value: Any, digits: int) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"
