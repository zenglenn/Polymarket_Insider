from __future__ import annotations

from pathlib import Path

from polymarket_insider.analytics import flow as flow_analytics
from polymarket_insider.config import load_config
from polymarket_insider.db import store


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
    flow_results = flow_analytics.compute_flow(conn, run_date, config)
    prior = flow_results.get("prior_run_date")
    wallets = flow_results.get("wallets_flow", [])[:10]
    markets = flow_results.get("markets_flow", [])[:10]

    print(f"latest_run_date: {run_date}")
    print(f"prior_run_date: {prior or 'none'}")
    print("top_flow_wallets:")
    for wallet in wallets:
        print(
            f"- {wallet.get('address')} delta={_fmt(wallet.get('total_usd_delta'), 2)} "
            f"new_clusters={wallet.get('new_clusters_entered_count')}"
        )
    print("top_flow_markets:")
    for market in markets:
        print(
            f"- {market.get('market_id')} delta={_fmt(market.get('total_delta_usd'), 2)} "
            f"wallets_new={market.get('wallets_new')}"
        )


def _fmt(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


if __name__ == "__main__":
    main()
