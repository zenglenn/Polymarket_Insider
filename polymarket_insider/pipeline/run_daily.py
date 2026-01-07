from __future__ import annotations

import logging
from pathlib import Path

from dateutil import parser as date_parser

from polymarket_insider.config import load_config
from polymarket_insider.db import store
from polymarket_insider.pipeline.collect import collect_data
from polymarket_insider.pipeline.report import write_report
from polymarket_insider.pipeline.score import score_run
from polymarket_insider.utils.time import local_today

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    root = Path(__file__).resolve().parents[2]
    config_path = root / "config.yaml"
    config = load_config(config_path)

    if config.run.date_override:
        run_date = date_parser.isoparse(config.run.date_override).date()
    else:
        run_date = local_today(config.run.timezone)

    db_path = root / "data" / "polymarket_insider.sqlite"
    raw_dir = root / "data" / "raw" / run_date.isoformat()
    out_dir = root / "out"

    conn = store.get_connection(db_path)
    store.init_db(conn)
    store.ensure_run(conn, run_date.isoformat(), config.run.timezone, status="running")

    try:
        conn.execute("BEGIN")
        store.clear_run_data(conn, run_date.isoformat())

        logger.info("Collecting markets and holders")
        diagnostics = collect_data(config, run_date, raw_dir, conn, commit=False)

        logger.info("Scoring markets and wallets")
        scored_markets, scored_wallets = score_run(config, run_date, conn, commit=False)

        diagnostics.update(
            {
                "scored_markets": scored_markets,
                "scored_wallets": scored_wallets,
            }
        )
        store.insert_run_diagnostics(conn, run_date.isoformat(), diagnostics, commit=False)

        kept = diagnostics.get("markets_kept", 0)
        if scored_markets > kept:
            raise AssertionError("scored_markets exceeds markets_kept")

        conn.commit()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        store.update_run_status(conn, run_date.isoformat(), "failed", str(exc))
        raise

    try:
        logger.info("Writing reports")
        write_report(run_date, db_path, out_dir)
        store.update_run_status(conn, run_date.isoformat(), "success")
    except Exception as exc:  # noqa: BLE001
        store.update_run_status(conn, run_date.isoformat(), "failed", str(exc))
        raise
    finally:
        logger.info(
            "Run summary fetched=%s kept=%s holders_targeted=%s holders_succeeded=%s holders_failed=%s "
            "scored_markets=%s scored_wallets=%s outputs=%s",
            diagnostics.get("markets_fetched", 0),
            diagnostics.get("markets_kept", 0),
            diagnostics.get("holder_markets_targeted", 0),
            diagnostics.get("holder_markets_succeeded", 0),
            diagnostics.get("holder_markets_failed", 0),
            diagnostics.get("scored_markets", 0),
            diagnostics.get("scored_wallets", 0),
            out_dir,
        )

    conn.close()


if __name__ == "__main__":
    main()
