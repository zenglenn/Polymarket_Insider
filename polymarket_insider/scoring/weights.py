from __future__ import annotations

from typing import Any, Callable, Iterable, Sequence, TypeVar

T = TypeVar("T")


def weighted_sum(features: dict[str, float], weights: dict[str, float]) -> float:
    total = 0.0
    for key, weight in weights.items():
        value = features.get(key, 0.0)
        total += value * weight
    return total


def stable_sorted(
    items: Iterable[T],
    key: Callable[[T], Any],
    reverse: bool = False,
    tie_breaker: Callable[[T], Any] | None = None,
) -> list[T]:
    if tie_breaker is None:
        tie_breaker = _default_tie_breaker
    items_list = list(items)
    if reverse:
        items_list = sorted(items_list, key=tie_breaker)
        return sorted(items_list, key=key, reverse=True)

    def sort_key(item: T) -> tuple[Any, Any]:
        return (key(item), tie_breaker(item))

    return sorted(items_list, key=sort_key)


def _default_tie_breaker(item: Any) -> Any:
    if isinstance(item, dict):
        for key in ("market_id", "id", "slug", "wallet"):
            if key in item and item[key] is not None:
                return str(item[key])
    return str(item)
