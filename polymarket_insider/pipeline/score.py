from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date
from polymarket_insider.config import AppConfig
from polymarket_insider.db import store
from polymarket_insider.scoring.features import days_until_close, hhi_concentration, log_norm
from polymarket_insider.scoring.weights import weighted_sum

logger = logging.getLogger(__name__)


def score_run(
    config: AppConfig,
    run_date: date,
    conn,
    commit: bool = True,
) -> tuple[int, int]:
    markets = store.fetch_market_snapshots(conn, run_date.isoformat())
    holders = store.fetch_holders_for_run(conn, run_date.isoformat())

    holders_by_market = defaultdict(list)
    for holder in holders:
        holders_by_market[holder["market_id"]].append(holder)

    market_scores = []
    wallet_aggregate = {}

    for market in markets:
        market_id = market["market_id"]
        market_holders = holders_by_market.get(market_id, [])

        exposures = [holder.get("exposure_usd", 0.0) for holder in market_holders]
        hhi = hhi_concentration(exposures)
        num_holders = len(market_holders)
        days_close = days_until_close(market.get("close_time"), run_date)

        closing_soon_score = 0.0
        if days_close is not None and config.market_filters.days_to_close > 0:
            closing_soon_score = 1.0 - min(days_close / config.market_filters.days_to_close, 1.0)

        market_features = {
            "volume": log_norm(market.get("volume_usd") or 0.0),
            "liquidity": log_norm(market.get("liquidity_usd") or 0.0),
            "holders": log_norm(num_holders),
            "closing_soon": closing_soon_score,
            "concentration": 1.0 - hhi,
        }
        market_score = weighted_sum(market_features, config.weights.market)

        conviction_wallets_usd = 0
        conviction_wallets_shares = 0
        whale_wallets_usd = 0
        whale_wallets_shares = 0
        new_wallets = 0
        wallet_scores = []

        for holder in market_holders:
            value_usd = holder.get("value_usd")
            shares = holder.get("shares")
            exposure = value_usd if value_usd is not None else (shares or 0.0)
            is_new_wallet = bool(holder.get("is_new_wallet"))
            if value_usd is not None:
                if value_usd >= config.thresholds.conviction_exposure_usd:
                    conviction_wallets_usd += 1
                if value_usd >= config.thresholds.whale_exposure_usd:
                    whale_wallets_usd += 1
            elif shares is not None:
                if shares >= config.thresholds.conviction_exposure_shares:
                    conviction_wallets_shares += 1
                if shares >= config.thresholds.whale_exposure_shares:
                    whale_wallets_shares += 1
            if is_new_wallet:
                new_wallets += 1
            conviction_flag = False
            whale_flag = False
            if value_usd is not None:
                conviction_flag = value_usd >= config.thresholds.conviction_exposure_usd
                whale_flag = value_usd >= config.thresholds.whale_exposure_usd
            elif shares is not None:
                conviction_flag = shares >= config.thresholds.conviction_exposure_shares
                whale_flag = shares >= config.thresholds.whale_exposure_shares
            wallet_features = {
                "exposure": log_norm(exposure),
                "conviction": 1.0 if conviction_flag else 0.0,
                "whale": 1.0 if whale_flag else 0.0,
                "new_wallet": 1.0 if is_new_wallet else 0.0,
            }
            wallet_score = weighted_sum(wallet_features, config.weights.wallet)
            wallet_scores.append(wallet_score)

            wallet_id = holder.get("wallet")
            if wallet_id:
                stats = wallet_aggregate.setdefault(
                    wallet_id,
                    {
                        "value_usd_total": 0.0,
                        "shares_total": 0.0,
                        "markets": set(),
                        "new_wallet": False,
                    },
                )
                if value_usd is not None:
                    stats["value_usd_total"] += value_usd
                elif shares is not None:
                    stats["shares_total"] += shares
                stats["markets"].add(market_id)
                stats["new_wallet"] = stats["new_wallet"] or is_new_wallet

        wallet_signal = sum(wallet_scores) / len(wallet_scores) if wallet_scores else 0.0
        total_score = market_score + wallet_signal

        conviction_total = conviction_wallets_usd + conviction_wallets_shares
        whale_total = whale_wallets_usd + whale_wallets_shares
        signals = {
            **market_features,
            "wallet_signal": wallet_signal,
            "conviction_wallets": conviction_total,
            "whale_wallets": whale_total,
            "conviction_wallets_usd": conviction_wallets_usd,
            "conviction_wallets_shares": conviction_wallets_shares,
            "whale_wallets_usd": whale_wallets_usd,
            "whale_wallets_shares": whale_wallets_shares,
            "new_wallets": new_wallets,
            "convergence": conviction_total >= config.thresholds.convergence_min_wallets,
        }

        market_scores.append(
            {
                "market_id": market_id,
                "score": total_score,
                "signals": signals,
            }
        )

    wallet_scores_rows = []
    for wallet, stats in wallet_aggregate.items():
        value_usd_total = stats["value_usd_total"]
        shares_total = stats["shares_total"]
        exposure_total = value_usd_total if value_usd_total > 0 else shares_total
        use_usd = value_usd_total > 0
        wallet_features = {
            "exposure": log_norm(exposure_total),
            "conviction": 1.0
            if (
                exposure_total
                >= (
                    config.thresholds.conviction_exposure_usd
                    if use_usd
                    else config.thresholds.conviction_exposure_shares
                )
            )
            else 0.0,
            "whale": 1.0
            if (
                exposure_total
                >= (
                    config.thresholds.whale_exposure_usd
                    if use_usd
                    else config.thresholds.whale_exposure_shares
                )
            )
            else 0.0,
            "new_wallet": 1.0 if stats["new_wallet"] else 0.0,
        }
        wallet_score = weighted_sum(wallet_features, config.weights.wallet)
        wallet_scores_rows.append(
            {
                "wallet": wallet,
                "score": wallet_score,
                "signals": {
                    **wallet_features,
                    "markets": len(stats["markets"]),
                    "basis": "usd" if use_usd else "shares",
                },
            }
        )

    store.insert_market_scores(conn, run_date.isoformat(), market_scores, commit=commit)
    store.insert_wallet_scores(conn, run_date.isoformat(), wallet_scores_rows, commit=commit)

    logger.info("Scored %d markets and %d wallets", len(market_scores), len(wallet_scores_rows))
    return len(market_scores), len(wallet_scores_rows)
