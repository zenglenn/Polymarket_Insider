from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from polymarket_insider.analytics import flow
from polymarket_insider.analytics import wallet_metrics
from polymarket_insider.config import FlowConfig, FlowWeights
from polymarket_insider.db import store


def _setup_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    store.init_db(conn)
    return conn


def _seed_markets(conn: sqlite3.Connection, run_date: str) -> None:
    markets = [
        {"market_id": "m1", "question": "Q1", "slug": "m1", "cluster_key": "cluster:a"},
        {"market_id": "m2", "question": "Q2", "slug": "m2", "cluster_key": "cluster:a"},
        {"market_id": "m3", "question": "Q3", "slug": "m3", "cluster_key": "cluster:b"},
        {"market_id": "m4", "question": "Q4", "slug": "m4", "cluster_key": "cluster:c"},
    ]
    store.upsert_markets(conn, markets, commit=False)
    store.insert_market_snapshots(conn, run_date, markets, commit=False)


def _insert_holder(conn, run_date: str, market_id: str, address: str, outcome: str, value: float) -> None:
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


def _build_metrics(conn, run_date: str) -> None:
    metrics = wallet_metrics.compute_wallet_metrics(conn, run_date)
    store.insert_wallet_metrics(conn, run_date, metrics, commit=False)
    daily = flow.build_wallet_market_daily(conn, run_date)
    store.insert_wallet_market_daily(conn, run_date, daily, commit=False)


def test_flow_deltas_and_classification() -> None:
    conn = _setup_conn()
    prev_date = "2026-01-06"
    today_date = "2026-01-07"
    _seed_markets(conn, prev_date)
    _seed_markets(conn, today_date)
    store.ensure_run(conn, prev_date, "UTC", status="success", commit=False)
    store.ensure_run(conn, today_date, "UTC", status="success", commit=False)

    # Wallet A: concentrated in one cluster only.
    _insert_holder(conn, prev_date, "m1", "A", "Yes", 4000.0)
    _insert_holder(conn, today_date, "m1", "A", "Yes", 7000.0)

    # Wallet B: enters new cluster and increases overall exposure.
    _insert_holder(conn, prev_date, "m1", "B", "Yes", 3000.0)
    _insert_holder(conn, today_date, "m1", "B", "Yes", 3500.0)
    _insert_holder(conn, today_date, "m3", "B", "No", 3000.0)

    _build_metrics(conn, prev_date)
    _build_metrics(conn, today_date)
    conn.commit()

    config = SimpleNamespace(
        flow=FlowConfig(
            min_runs_seen=2,
            lookback_runs=5,
            override_total_usd_today_for_new_wallet=25000,
            min_total_usd_today=5000,
            min_total_delta_usd=2000,
            min_position_delta_usd=1000,
            min_new_position_usd=1500,
            max_top_cluster_share_today=0.6,
            top_wallets=10,
            top_positions_per_wallet=10,
            weights=FlowWeights(),
        )
    )

    results = flow.compute_flow(conn, today_date, config)
    assert results["prior_run_date"] == prev_date

    wallets_flow = results["wallets_flow"]
    assert [row["address"] for row in wallets_flow] == ["B"]
    assert wallets_flow[0]["total_usd_delta"] == 3500.0
    assert wallets_flow[0]["new_clusters_entered_count"] == 1

    positions_flow = results["positions_flow"]
    assert len(positions_flow) == 1
    position = positions_flow[0]
    assert position["address"] == "B"
    assert position["market_id"] == "m3"
    assert position["classification"] == "NEW_POSITION"
    assert position["delta_usd"] == 3000.0

    markets_flow = results["markets_flow"]
    assert len(markets_flow) == 1
    assert markets_flow[0]["market_id"] == "m3"
    assert markets_flow[0]["wallets_new"] == 1
    assert markets_flow[0]["total_delta_usd"] == 3000.0


def test_flow_min_runs_seen_and_tiers() -> None:
    conn = _setup_conn()
    prev_date = "2026-01-06"
    today_date = "2026-01-07"
    _seed_markets(conn, prev_date)
    _seed_markets(conn, today_date)
    store.ensure_run(conn, prev_date, "UTC", status="success", commit=False)
    store.ensure_run(conn, today_date, "UTC", status="success", commit=False)

    # Wallet X: only today, below override.
    _insert_holder(conn, today_date, "m1", "X", "Yes", 3000.0)
    _insert_holder(conn, today_date, "m3", "X", "No", 3000.0)

    # Wallet Y: only today, above override.
    _insert_holder(conn, today_date, "m1", "Y", "Yes", 13000.0)
    _insert_holder(conn, today_date, "m3", "Y", "No", 13000.0)

    # Wallet Z: present in both runs, tier C.
    _insert_holder(conn, prev_date, "m1", "Z", "Yes", 8000.0)
    _insert_holder(conn, today_date, "m1", "Z", "Yes", 8000.0)
    _insert_holder(conn, today_date, "m3", "Z", "No", 4000.0)
    _insert_holder(conn, today_date, "m4", "Z", "Yes", 4000.0)

    _build_metrics(conn, prev_date)
    _build_metrics(conn, today_date)
    conn.commit()

    config = SimpleNamespace(
        flow=FlowConfig(
            min_runs_seen=2,
            lookback_runs=5,
            override_total_usd_today_for_new_wallet=25000,
            min_total_usd_today=5000,
            min_total_delta_usd=5000,
            min_position_delta_usd=2000,
            min_new_position_usd=2500,
            max_top_cluster_share_today=0.5,
            top_wallets=10,
            top_positions_per_wallet=10,
            weights=FlowWeights(),
        )
    )

    results = flow.compute_flow(conn, today_date, config)
    wallets = {row["address"]: row for row in results["wallets_flow"]}

    assert "X" not in wallets
    assert "Y" in wallets
    assert wallets["Y"]["tier"] == "TIER_B"
    assert wallets["Z"]["tier"] == "TIER_C"


def test_flow_ordering_ties() -> None:
    conn = _setup_conn()
    prev_date = "2026-01-06"
    today_date = "2026-01-07"
    _seed_markets(conn, prev_date)
    _seed_markets(conn, today_date)
    store.ensure_run(conn, prev_date, "UTC", status="success", commit=False)
    store.ensure_run(conn, today_date, "UTC", status="success", commit=False)

    for wallet in ("AA", "AB"):
        _insert_holder(conn, prev_date, "m1", wallet, "Yes", 5000.0)
        _insert_holder(conn, today_date, "m1", wallet, "Yes", 6000.0)
        _insert_holder(conn, today_date, "m3", wallet, "No", 6000.0)

    _build_metrics(conn, prev_date)
    _build_metrics(conn, today_date)
    conn.commit()

    config = SimpleNamespace(
        flow=FlowConfig(
            min_runs_seen=2,
            lookback_runs=5,
            override_total_usd_today_for_new_wallet=25000,
            min_total_usd_today=5000,
            min_total_delta_usd=5000,
            min_position_delta_usd=2000,
            min_new_position_usd=2500,
            max_top_cluster_share_today=0.5,
            top_wallets=10,
            top_positions_per_wallet=10,
            weights=FlowWeights(),
        )
    )

    results = flow.compute_flow(conn, today_date, config)
    addresses = [row["address"] for row in results["wallets_flow"]]
    assert addresses == ["AA", "AB"]
