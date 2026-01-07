from __future__ import annotations

from types import SimpleNamespace

from polymarket_insider.analytics import consensus
from polymarket_insider.config import ConsensusConfig, ConsensusWeights


def _config(
    min_wallets: int = 2,
    require_tier: bool = True,
    max_top_wallet_share: float = 0.8,
) -> SimpleNamespace:
    return SimpleNamespace(
        consensus=ConsensusConfig(
            lookback_days=1,
            min_wallets=min_wallets,
            require_tierA_or_B=require_tier,
            min_position_delta_usd=1000,
            min_total_delta_usd=3000,
            max_top_wallet_share=max_top_wallet_share,
            top_n=10,
            weights=ConsensusWeights(),
        )
    )


def test_consensus_inclusion_and_filters() -> None:
    config = _config()
    tiers = {"A1": "TIER_A", "B1": "TIER_B", "C1": "TIER_C"}
    position_rows = [
        {
            "address": "A1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 4000.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "B1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 3000.0,
            "classification": "INCREASE",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "C1",
            "market_id": "m2",
            "outcome": "No",
            "delta_usd": 6000.0,
            "classification": "NEW_POSITION",
            "question": "Q2",
            "cluster_key": "c2",
        },
    ]
    entries, wallets, diagnostics, _ = consensus.compute_consensus_from_inputs(
        position_rows, tiers, config.consensus
    )
    assert len(entries) == 1
    assert entries[0]["market_id"] == "m1"
    assert entries[0]["wallets_supporting"] == 2
    assert entries[0]["tiers_A"] == 1
    assert entries[0]["tiers_B"] == 1
    assert len(wallets) == 2
    assert diagnostics["candidate_flow_wallets_A_B"] == 2


def test_consensus_min_wallets() -> None:
    config = _config(min_wallets=2)
    tiers = {"A1": "TIER_A"}
    position_rows = [
        {
            "address": "A1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 6000.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        }
    ]
    entries, _, _, _ = consensus.compute_consensus_from_inputs(position_rows, tiers, config.consensus)
    assert entries == []


def test_consensus_require_tier() -> None:
    config = _config(require_tier=True)
    tiers = {"C1": "TIER_C", "C2": "TIER_C"}
    position_rows = [
        {
            "address": "C1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 4000.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "C2",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 4000.0,
            "classification": "INCREASE",
            "question": "Q1",
            "cluster_key": "c1",
        },
    ]
    entries, _, _, _ = consensus.compute_consensus_from_inputs(position_rows, tiers, config.consensus)
    assert entries == []


def test_consensus_concentration_penalty() -> None:
    config = _config(max_top_wallet_share=0.5)
    tiers = {"A1": "TIER_A", "B1": "TIER_B"}
    position_rows = [
        {
            "address": "A1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 9000.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "B1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 2000.0,
            "classification": "INCREASE",
            "question": "Q1",
            "cluster_key": "c1",
        },
    ]
    entries, _, _, _ = consensus.compute_consensus_from_inputs(position_rows, tiers, config.consensus)
    assert entries == []


def test_consensus_sorting_ties() -> None:
    config = _config()
    tiers = {"A1": "TIER_A", "B1": "TIER_B", "A2": "TIER_A", "B2": "TIER_B"}
    position_rows = [
        {
            "address": "A1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 3000.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "B1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 3000.0,
            "classification": "INCREASE",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "A2",
            "market_id": "m2",
            "outcome": "No",
            "delta_usd": 3000.0,
            "classification": "NEW_POSITION",
            "question": "Q2",
            "cluster_key": "c2",
        },
        {
            "address": "B2",
            "market_id": "m2",
            "outcome": "No",
            "delta_usd": 3000.0,
            "classification": "INCREASE",
            "question": "Q2",
            "cluster_key": "c2",
        },
    ]
    entries, _, _, _ = consensus.compute_consensus_from_inputs(position_rows, tiers, config.consensus)
    assert [row["market_id"] for row in entries] == ["m1", "m2"]


def test_consensus_fallback_mode() -> None:
    config = _config()
    tiers = {"A1": "TIER_A", "B1": "TIER_B"}
    position_rows = [
        {
            "address": "A1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 1400.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "B1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 1200.0,
            "classification": "INCREASE",
            "question": "Q1",
            "cluster_key": "c1",
        },
    ]
    entries, _, diagnostics, _, fallback_used = consensus.compute_consensus_with_fallback(
        position_rows, tiers, config.consensus
    )
    assert len(entries) == 1
    assert entries[0]["fallback"] == 1
    assert fallback_used == 1
    assert diagnostics["keys_meeting_min_wallets"] == 1
    assert diagnostics["keys_meeting_min_total_delta"] == 0


def test_consensus_diagnostics_and_sorting_with_fallback() -> None:
    config = _config()
    tiers = {"A1": "TIER_A", "B1": "TIER_B", "A2": "TIER_A", "B2": "TIER_B"}
    position_rows = [
        {
            "address": "A1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 1500.0,
            "classification": "NEW_POSITION",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "B1",
            "market_id": "m1",
            "outcome": "Yes",
            "delta_usd": 1200.0,
            "classification": "INCREASE",
            "question": "Q1",
            "cluster_key": "c1",
        },
        {
            "address": "A2",
            "market_id": "m2",
            "outcome": "No",
            "delta_usd": 1500.0,
            "classification": "NEW_POSITION",
            "question": "Q2",
            "cluster_key": "c2",
        },
        {
            "address": "B2",
            "market_id": "m2",
            "outcome": "No",
            "delta_usd": 1200.0,
            "classification": "INCREASE",
            "question": "Q2",
            "cluster_key": "c2",
        },
    ]
    entries, _, diagnostics, _, fallback_used = consensus.compute_consensus_with_fallback(
        position_rows, tiers, config.consensus
    )
    assert entries
    assert diagnostics["unique_candidate_keys"] == 2
    assert fallback_used == 1
    assert [row["market_id"] for row in entries] == ["m1", "m2"]
