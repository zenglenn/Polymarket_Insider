from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

from polymarket_insider.db import store
from polymarket_insider.scoring.weights import stable_sorted
from polymarket_insider.utils.io import ensure_dir


def write_report(run_date: date, db_path: Path, out_dir: Path) -> None:
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
               m.question, m.slug, m.close_time, m.volume_usd, m.liquidity_usd
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

    with md_path.open("w", encoding="utf-8") as handle:
        handle.write(f"# Polymarket Insider Report ({report_date})\n\n")
        handle.write("## Collection diagnostics\n\n")
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
            f"- unknown_outcome_rows: {diagnostics.get('unknown_outcome_rows', 0)}\n"
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
        if not records:
            handle.write("No markets scored for this run.\n")
        else:
            handle.write("| Rank | Market | Score | Conviction (usd/shares) | Whales (usd/shares) | New | Close |\n")
            handle.write("| --- | --- | --- | --- | --- | --- | --- |\n")
            for idx, record in enumerate(records[:50], start=1):
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
        for record in records[:20]
    ]

    with watchlist_path.open("w", encoding="utf-8") as handle:
        json.dump(watchlist, handle, ensure_ascii=True, indent=2)
