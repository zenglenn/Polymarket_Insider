from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel


class RunConfig(BaseModel):
    max_markets: int = 200
    date_override: Optional[str] = None
    timezone: str = "America/New_York"


class MarketFilters(BaseModel):
    days_to_close: int = 60
    min_volume: float = 0
    min_liquidity: float = 0


class HoldersConfig(BaseModel):
    top_n: int = 50
    max_markets_to_fetch: int = 25


class Thresholds(BaseModel):
    new_wallet_days: int = 14
    conviction_exposure_usd: float = 2000
    whale_exposure_usd: float = 20000
    conviction_exposure_shares: float = 1000
    whale_exposure_shares: float = 5000
    convergence_min_wallets: int = 3


class Weights(BaseModel):
    market: Dict[str, float]
    wallet: Dict[str, float]


class AppConfig(BaseModel):
    run: RunConfig = RunConfig()
    market_filters: MarketFilters = MarketFilters()
    holders: HoldersConfig = HoldersConfig()
    thresholds: Thresholds = Thresholds()
    weights: Weights


def load_config(path: str | Path) -> AppConfig:
    data: Dict[str, Any] = {}
    config_path = Path(path)
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
            if isinstance(loaded, dict):
                data = loaded
    if "weights" not in data:
        raise ValueError("config.yaml must include weights")
    return AppConfig(**data)
