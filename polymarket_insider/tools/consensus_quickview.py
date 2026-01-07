from __future__ import annotations

from pathlib import Path

from polymarket_insider.analytics import consensus as consensus_analytics
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
    results = consensus_analytics.compute_consensus(conn, run_date, config)
    conn.close()

    print(f"latest_run_date: {run_date}")
    print(f"prior_run_date: {results.get('prior_run_date') or 'none'}")
    consensus_entries = results.get("consensus_entries", [])[:10]
    print("top_consensus_entries:")
    for row in consensus_entries:
        print(
            f"- {row.get('market_id')} {row.get('outcome')} wallets={row.get('wallets_supporting')} "
            f"delta={_fmt(row.get('total_delta_usd'), 2)} "
            f"top_wallet_share={_fmt(row.get('top_wallet_share'), 2)}"
        )

    if consensus_entries:
        top_market = consensus_entries[0]
        supporting = [
            row
            for row in results.get("consensus_wallets", [])
            if row.get("market_id") == top_market.get("market_id")
            and row.get("outcome") == top_market.get("outcome")
        ][:10]
        print("top_supporting_wallets:")
        for row in supporting:
            print(
                f"- {row.get('address')} tier={row.get('tier')} "
                f"delta={_fmt(row.get('delta_usd'), 2)} {row.get('classification')}"
            )


def _fmt(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


if __name__ == "__main__":
    main()
