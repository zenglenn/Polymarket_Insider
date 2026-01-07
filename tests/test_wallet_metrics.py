from __future__ import annotations

import sqlite3
from datetime import date
from types import SimpleNamespace

import pytest

from polymarket_insider.analytics import wallet_metrics
from polymarket_insider.config import WalletFilters, WalletRanking
from polymarket_insider.db import store
from polymarket_insider.pipeline.report import write_report


def _setup_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    store.init_db(conn)
    return conn


def _seed_markets(conn: sqlite3.Connection, run_date: str) -> None:
    markets = [
        {"market_id": "m1", "question": "Q1", "slug": "m1", "cluster_key": "event:a"},
        {"market_id": "m2", "question": "Q2", "slug": "m2", "cluster_key": "event:a"},
        {"market_id": "m3", "question": "Q3", "slug": "m3", "cluster_key": "event:b"},
        {"market_id": "m4", "question": "Q4", "slug": "m4", "cluster_key": "event:c"},
        {"market_id": "m5", "question": "Q5", "slug": "m5", "cluster_key": "event:d"},
    ]
    store.upsert_markets(conn, markets, commit=False)
    store.insert_market_snapshots(conn, run_date, markets, commit=False)
    conn.commit()


def _insert_holders(conn: sqlite3.Connection, run_date: str, market_id: str, address: str, outcome: str, value: float) -> None:
    store.insert_holders(
        conn,
        run_date,
        market_id,
        [
            {
                "wallet": address,
                "address": address,
                "outcome": outcome,
                "shares": value,
                "value_usd": value,
                "fetched_at": run_date,
                "exposure_usd": value,
                "is_new_wallet": False,
                "source": "holders_api",
                "raw": {},
            }
        ],
        commit=False,
    )


def _seed_holders(conn: sqlite3.Connection, run_date: str) -> None:
    # Wallet A: concentrated in one cluster across two markets.
    _insert_holders(conn, run_date, "m1", "A", "Yes", 1000.0)
    _insert_holders(conn, run_date, "m2", "A", "No", 1000.0)

    # Wallet B: diversified across three clusters.
    _insert_holders(conn, run_date, "m1", "B", "Yes", 1000.0)
    _insert_holders(conn, run_date, "m3", "B", "No", 1000.0)
    _insert_holders(conn, run_date, "m4", "B", "Yes", 1000.0)

    # Wallet C: heavy single market.
    _insert_holders(conn, run_date, "m5", "C", "Yes", 5000.0)

    # Wallet D: always YES.
    _insert_holders(conn, run_date, "m3", "D", "Yes", 1500.0)
    _insert_holders(conn, run_date, "m4", "D", "Yes", 500.0)

    # Wallet E: diversified but smaller.
    _insert_holders(conn, run_date, "m1", "E", "Yes", 800.0)
    _insert_holders(conn, run_date, "m3", "E", "No", 700.0)
    _insert_holders(conn, run_date, "m4", "E", "No", 500.0)
    conn.commit()


def test_wallet_metrics_computation() -> None:
    conn = _setup_conn()
    run_date = "2026-01-07"
    _seed_markets(conn, run_date)
    _seed_holders(conn, run_date)

    metrics = wallet_metrics.compute_wallet_metrics(conn, run_date)
    metrics_by_wallet = {row["address"]: row for row in metrics}

    wallet_b = metrics_by_wallet["B"]
    assert wallet_b["markets_count"] == 3
    assert wallet_b["clusters_count"] == 3
    assert wallet_b["top_cluster_share"] == pytest.approx(1 / 3, rel=1e-3)
    assert wallet_b["hhi_clusters"] == pytest.approx(1 / 3, rel=1e-3)
    assert wallet_b["yes_share"] == pytest.approx(2 / 3, rel=1e-3)
    assert wallet_b["sidedness"] == pytest.approx(1 / 3, rel=1e-3)

    wallet_a = metrics_by_wallet["A"]
    assert wallet_a["clusters_count"] == 1
    assert wallet_a["top_cluster_share"] == pytest.approx(1.0, rel=1e-3)

    wallet_d = metrics_by_wallet["D"]
    assert wallet_d["sidedness"] == pytest.approx(1.0, rel=1e-3)


def test_wallet_filtering_and_ranking() -> None:
    conn = _setup_conn()
    run_date = "2026-01-07"
    _seed_markets(conn, run_date)
    _seed_holders(conn, run_date)

    metrics = wallet_metrics.compute_wallet_metrics(conn, run_date)
    config = SimpleNamespace(
        wallet_filters=WalletFilters(
            min_total_usd=500,
            min_markets=2,
            min_clusters=2,
            max_top_cluster_share=0.7,
            max_top_market_share=0.5,
            max_hhi_clusters=0.65,
            max_sidedness=0.9,
        ),
        wallet_ranking=WalletRanking(),
    )
    ranked, excluded = wallet_metrics.score_wallet_metrics(metrics, config)

    ranked_addresses = [row["address"] for row in ranked]
    assert ranked_addresses[0] == "B"
    assert "E" in ranked_addresses

    excluded_reasons = {row["address"]: row["reasons"] for row in excluded}
    assert "max_sidedness" in excluded_reasons["D"]
    assert "min_clusters" in excluded_reasons["A"]
    assert "max_top_market_share" in excluded_reasons["C"]


def test_report_wallet_outputs(tmp_path) -> None:
    db_path = tmp_path / "test.sqlite"
    out_dir = tmp_path / "out"
    conn = store.get_connection(db_path)
    store.init_db(conn)
    run_date = "2026-01-07"
    _seed_markets(conn, run_date)
    _insert_holders(conn, run_date, "m1", "Z", "Yes", 12000.0)
    _insert_holders(conn, run_date, "m3", "Z", "No", 6000.0)
    _insert_holders(conn, run_date, "m4", "Z", "Yes", 4000.0)
    metrics = wallet_metrics.compute_wallet_metrics(conn, run_date)
    store.insert_wallet_metrics(conn, run_date, metrics, commit=False)
    store.insert_run_diagnostics(conn, run_date, {"markets_fetched": 1, "markets_kept": 1}, commit=False)
    conn.commit()
    conn.close()

    write_report(date.fromisoformat(run_date), db_path, out_dir)

    ranked_path = out_dir / f"wallets_ranked_{run_date}.csv"
    concentrated_path = out_dir / f"wallets_concentrated_{run_date}.csv"
    positions_path = out_dir / f"wallet_positions_top_{run_date}.csv"
    clusters_path = out_dir / f"clusters_summary_{run_date}.csv"
    report_path = out_dir / f"report_{run_date}.md"

    assert ranked_path.exists()
    assert concentrated_path.exists()
    assert positions_path.exists()
    assert clusters_path.exists()
    assert report_path.exists()

    first_line = ranked_path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line.startswith("rank,address,score_wallet,total_usd")
    report_text = report_path.read_text(encoding="utf-8")
    assert "## Wallet Summary" in report_text
