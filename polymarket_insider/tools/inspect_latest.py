from __future__ import annotations

from pathlib import Path

from polymarket_insider.analytics import wallet_metrics as wallet_analytics
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
    diagnostics = store.fetch_run_diagnostics(conn, run_date)
    metrics = store.fetch_wallet_metrics(conn, run_date)
    ranked_wallets, _ = wallet_analytics.score_wallet_metrics(metrics, config)

    print(f"latest_run_date: {run_date}")
    print(f"markets_fetched: {diagnostics.get('markets_fetched', 0)}")
    print(f"markets_kept: {diagnostics.get('markets_kept', 0)}")
    print(f"markets_with_holders: {diagnostics.get('markets_with_holders', 0)}")
    print("top_wallets:")
    for wallet in ranked_wallets[:10]:
        print(
            f"- {wallet.get('address')} score={_fmt(wallet.get('score_wallet'), 4)} "
            f"total_usd={_fmt(wallet.get('total_usd'), 2)} "
            f"markets={wallet.get('markets_count')} clusters={wallet.get('clusters_count')} "
            f"top_cluster_share={_fmt(wallet.get('top_cluster_share'), 2)} "
            f"hhi_clusters={_fmt(wallet.get('hhi_clusters'), 2)}"
        )


def _fmt(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


if __name__ == "__main__":
    main()
