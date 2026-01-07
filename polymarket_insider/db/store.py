from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from polymarket_insider.db.schema import SCHEMA_SQL


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    _ensure_column(conn, "markets", "status", "TEXT")
    _ensure_column(conn, "holders", "source", "TEXT")
    _ensure_column(conn, "holders", "address", "TEXT")
    _ensure_column(conn, "holders", "outcome", "TEXT")
    _ensure_column(conn, "holders", "shares", "REAL")
    _ensure_column(conn, "holders", "value_usd", "REAL")
    _ensure_column(conn, "holders", "fetched_at", "TEXT")
    _ensure_column(conn, "runs", "status", "TEXT")
    _ensure_column(conn, "runs", "finished_at", "TEXT")
    _ensure_column(conn, "runs", "error_message", "TEXT")
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError:
        return


def ensure_run(
    conn: sqlite3.Connection,
    run_date: str,
    timezone: str,
    status: str | None = None,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO runs (run_date, timezone, created_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_date, timezone, datetime.utcnow().isoformat(), status),
    )
    if commit:
        conn.commit()


def update_run_status(
    conn: sqlite3.Connection,
    run_date: str,
    status: str,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE runs
        SET status = ?, finished_at = ?, error_message = ?
        WHERE run_date = ?
        """,
        (status, datetime.utcnow().isoformat(), error_message, run_date),
    )
    conn.commit()


def upsert_markets(
    conn: sqlite3.Connection,
    markets: Iterable[dict[str, Any]],
    commit: bool = True,
) -> None:
    rows = []
    now = datetime.utcnow().isoformat()
    for market in markets:
        rows.append(
            (
                market.get("market_id"),
                market.get("question"),
                market.get("slug"),
                market.get("status"),
                market.get("close_time"),
                market.get("volume_usd"),
                market.get("liquidity_usd"),
                json.dumps(market.get("raw", {}), ensure_ascii=True),
                now,
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO markets
        (market_id, question, slug, status, close_time, volume_usd, liquidity_usd, raw_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    if commit:
        conn.commit()


def insert_market_snapshots(
    conn: sqlite3.Connection,
    run_date: str,
    markets: Iterable[dict[str, Any]],
    commit: bool = True,
) -> None:
    rows = []
    for market in markets:
        rows.append(
            (
                run_date,
                market.get("market_id"),
                market.get("question"),
                market.get("slug"),
                market.get("status"),
                market.get("close_time"),
                market.get("volume_usd"),
                market.get("liquidity_usd"),
                json.dumps(market.get("raw", {}), ensure_ascii=True),
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO market_snapshots
        (run_date, market_id, question, slug, status, close_time, volume_usd, liquidity_usd, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    if commit:
        conn.commit()


def insert_holders(
    conn: sqlite3.Connection,
    run_date: str,
    market_id: str,
    holders: Iterable[dict[str, Any]],
    commit: bool = True,
) -> None:
    rows = []
    for holder in holders:
        rows.append(
            (
                run_date,
                market_id,
                holder.get("wallet"),
                holder.get("address"),
                holder.get("outcome"),
                holder.get("shares"),
                holder.get("value_usd"),
                holder.get("fetched_at"),
                holder.get("exposure_usd"),
                1 if holder.get("is_new_wallet") else 0,
                holder.get("source"),
                json.dumps(holder.get("raw", {}), ensure_ascii=True),
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO holders
        (run_date, market_id, wallet, address, outcome, shares, value_usd, fetched_at,
         exposure_usd, is_new_wallet, source, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    if commit:
        conn.commit()


def insert_market_scores(
    conn: sqlite3.Connection,
    run_date: str,
    scores: Iterable[dict[str, Any]],
    commit: bool = True,
) -> None:
    rows = []
    for score in scores:
        rows.append(
            (
                run_date,
                score.get("market_id"),
                score.get("score"),
                json.dumps(score.get("signals", {}), ensure_ascii=True),
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO market_scores
        (run_date, market_id, score, signals_json)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    if commit:
        conn.commit()


def insert_wallet_scores(
    conn: sqlite3.Connection,
    run_date: str,
    scores: Iterable[dict[str, Any]],
    commit: bool = True,
) -> None:
    rows = []
    for score in scores:
        rows.append(
            (
                run_date,
                score.get("wallet"),
                score.get("score"),
                json.dumps(score.get("signals", {}), ensure_ascii=True),
            )
        )
    conn.executemany(
        """
        INSERT OR REPLACE INTO wallet_scores
        (run_date, wallet, score, signals_json)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    if commit:
        conn.commit()


def fetch_markets(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT market_id, question, slug, status, close_time, volume_usd, liquidity_usd, raw_json FROM markets"
    ).fetchall()
    results = []
    for row in rows:
        results.append(
            {
                "market_id": row["market_id"],
                "question": row["question"],
                "slug": row["slug"],
                "status": row["status"],
                "close_time": row["close_time"],
                "volume_usd": row["volume_usd"],
                "liquidity_usd": row["liquidity_usd"],
                "raw": json.loads(row["raw_json"]) if row["raw_json"] else {},
            }
        )
    return results


def fetch_market_snapshots(conn: sqlite3.Connection, run_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT market_id, question, slug, status, close_time, volume_usd, liquidity_usd, raw_json
        FROM market_snapshots WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()
    results = []
    for row in rows:
        results.append(
            {
                "market_id": row["market_id"],
                "question": row["question"],
                "slug": row["slug"],
                "status": row["status"],
                "close_time": row["close_time"],
                "volume_usd": row["volume_usd"],
                "liquidity_usd": row["liquidity_usd"],
                "raw": json.loads(row["raw_json"]) if row["raw_json"] else {},
            }
        )
    return results


def fetch_holders_for_run(conn: sqlite3.Connection, run_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT run_date, market_id, wallet, address, outcome, shares, value_usd,
               fetched_at, exposure_usd, is_new_wallet, source, raw_json
        FROM holders WHERE run_date = ?
        """,
        (run_date,),
    ).fetchall()
    results = []
    for row in rows:
        results.append(
            {
                "run_date": row["run_date"],
                "market_id": row["market_id"],
                "wallet": row["wallet"],
                "address": row["address"],
                "outcome": row["outcome"],
                "shares": row["shares"],
                "value_usd": row["value_usd"],
                "fetched_at": row["fetched_at"],
                "exposure_usd": row["exposure_usd"],
                "is_new_wallet": bool(row["is_new_wallet"]),
                "source": row["source"],
                "raw": json.loads(row["raw_json"]) if row["raw_json"] else {},
            }
        )
    return results


def insert_run_diagnostics(
    conn: sqlite3.Connection,
    run_date: str,
    diagnostics: dict[str, Any],
    commit: bool = True,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO run_diagnostics (run_date, diagnostics_json) VALUES (?, ?)",
        (run_date, json.dumps(diagnostics, ensure_ascii=True)),
    )
    if commit:
        conn.commit()


def fetch_run_diagnostics(conn: sqlite3.Connection, run_date: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT diagnostics_json FROM run_diagnostics WHERE run_date = ?",
        (run_date,),
    ).fetchone()
    if row and row["diagnostics_json"]:
        return json.loads(row["diagnostics_json"])
    return {}


def clear_run_data(conn: sqlite3.Connection, run_date: str) -> None:
    conn.execute("DELETE FROM holders WHERE run_date = ?", (run_date,))
    conn.execute("DELETE FROM market_scores WHERE run_date = ?", (run_date,))
    conn.execute("DELETE FROM wallet_scores WHERE run_date = ?", (run_date,))
    conn.execute("DELETE FROM market_snapshots WHERE run_date = ?", (run_date,))
    conn.execute("DELETE FROM run_diagnostics WHERE run_date = ?", (run_date,))
