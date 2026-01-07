from __future__ import annotations

from pathlib import Path

from polymarket_insider.analytics import flow as flow_analytics
from polymarket_insider.analytics import wallet_metrics as wallet_analytics
from polymarket_insider.config import load_config
from polymarket_insider.db import store
from polymarket_insider.scoring.weights import stable_sorted


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    db_path = root / "data" / "polymarket_insider.sqlite"
    config = load_config(root / "config.yaml")

    conn = store.get_connection(db_path)
    row = conn.execute("SELECT run_date FROM runs ORDER BY run_date DESC LIMIT 1").fetchone()
    if not row:
        print("No runs found.")
        return
    run_date = row["run_date"]
    diagnostics = store.fetch_run_diagnostics(conn, run_date)

    wallet_metrics_rows = store.fetch_wallet_metrics(conn, run_date)
    ranked_wallets, _ = wallet_analytics.score_wallet_metrics(wallet_metrics_rows, config)
    flow_results = flow_analytics.compute_flow(conn, run_date, config)
    conn.close()

    print(f"latest_run_date: {run_date}")
    print(f"prior_run_date: {flow_results.get('prior_run_date') or 'none'}")
    print(
        "counts: kept=%s markets_with_holders=%s ranked_wallets=%s flow_wallets=%s"
        % (
            diagnostics.get("markets_kept", 0),
            diagnostics.get("markets_with_holders", 0),
            len(ranked_wallets),
            len(flow_results.get("wallets_flow", [])),
        )
    )

    print("top_ranked_wallets:")
    for wallet in ranked_wallets[:5]:
        print(
            f"- {wallet.get('address')} score={_fmt(wallet.get('score_wallet'), 4)} "
            f"top_cluster_share={_fmt(wallet.get('top_cluster_share'), 2)}"
        )

    print("top_flow_wallets:")
    for wallet in flow_results.get("wallets_flow", [])[:10]:
        print(
            f"- {wallet.get('tier')} {wallet.get('address')} delta={_fmt(wallet.get('total_usd_delta'), 2)} "
            f"new_clusters={wallet.get('new_clusters_entered_count')}"
        )

    new_positions = [
        row
        for row in flow_results.get("positions_flow", [])
        if row.get("classification") == "NEW_POSITION"
    ]
    new_positions = stable_sorted(
        new_positions,
        key=lambda item: item.get("delta_usd", 0.0),
        reverse=True,
        tie_breaker=lambda item: (item.get("address"), item.get("market_id"), item.get("outcome")),
    )[:10]
    print("top_new_positions:")
    for row in new_positions:
        print(
            f"- {row.get('address')} {row.get('market_id')} {row.get('outcome')} "
            f"delta={_fmt(row.get('delta_usd'), 2)}"
        )


def _fmt(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


if __name__ == "__main__":
    main()
