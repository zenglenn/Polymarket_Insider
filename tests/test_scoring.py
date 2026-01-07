import pytest

from polymarket_insider.api.data_api import compute_backoff_schedule
from polymarket_insider.pipeline.collect import build_cluster_key, extract_holder_outcome, extract_holder_value_usd
from polymarket_insider.pipeline.report import apply_cluster_cap
from polymarket_insider.scoring.features import hhi_concentration
from polymarket_insider.scoring.weights import stable_sorted


def test_hhi_concentration():
    assert hhi_concentration([50, 50]) == pytest.approx(0.5)
    assert hhi_concentration([100]) == pytest.approx(1.0)
    assert hhi_concentration([0, 0, 0]) == 0.0


def test_stable_sort_determinism():
    items_a = [
        {"market_id": "b", "score": 1.0},
        {"market_id": "a", "score": 1.0},
        {"market_id": "c", "score": 2.0},
    ]
    items_b = [
        {"market_id": "a", "score": 1.0},
        {"market_id": "c", "score": 2.0},
        {"market_id": "b", "score": 1.0},
    ]

    sorted_a = stable_sorted(
        items_a,
        key=lambda item: item["score"],
        reverse=True,
        tie_breaker=lambda item: item["market_id"],
    )
    sorted_b = stable_sorted(
        items_b,
        key=lambda item: item["score"],
        reverse=True,
        tie_breaker=lambda item: item["market_id"],
    )

    assert [item["market_id"] for item in sorted_a] == ["c", "a", "b"]
    assert [item["market_id"] for item in sorted_b] == ["c", "a", "b"]


def test_value_usd_none_without_price():
    holder = {"amount": 10}
    market = {"raw": {}}
    assert extract_holder_value_usd(holder, market) is None


def test_outcome_mapping_from_index():
    holder = {"outcomeIndex": 1}
    market = {"raw": {"outcomes": ["Yes", "No"]}}
    assert extract_holder_outcome(holder, market) == "No"


def test_cluster_key_deterministic():
    market = {
        "question": "Will the Tigers win Super Bowl 2026?",
        "slug": "nfl-super-bowl-2026",
        "events": [{"id": "evt_123"}],
    }
    assert build_cluster_key(market) == "event:evt_123"


def test_diversification_cap():
    records = [
        {"cluster_key": "a", "score": 3},
        {"cluster_key": "a", "score": 2},
        {"cluster_key": "a", "score": 1},
        {"cluster_key": "b", "score": 3},
        {"cluster_key": "b", "score": 2},
        {"cluster_key": "c", "score": 1},
    ]
    selected = apply_cluster_cap(records, limit=5, max_per_cluster=2, enabled=True)
    counts = {}
    for record in selected:
        counts[record["cluster_key"]] = counts.get(record["cluster_key"], 0) + 1
    assert max(counts.values()) <= 2


def test_backoff_schedule_budget():
    schedule = compute_backoff_schedule([1, 2, 4, 8], retry_max=4, max_budget_s=5)
    assert sum(schedule) <= 5
