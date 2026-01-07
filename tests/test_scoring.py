import pytest

from polymarket_insider.pipeline.collect import extract_holder_outcome, extract_holder_value_usd
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
