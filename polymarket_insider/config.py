from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

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
    max_markets_to_fetch: int = 60
    request_timeout_s: int = 15
    retry_max: int = 3
    backoff_seconds: List[int] = [1, 2, 4, 8]
    max_backoff_budget_s: int = 60


class Thresholds(BaseModel):
    new_wallet_days: int = 14
    conviction_exposure_usd: float = 2000
    whale_exposure_usd: float = 20000
    conviction_exposure_shares: float = 1000
    whale_exposure_shares: float = 5000
    convergence_min_wallets: int = 3


class DiversityConfig(BaseModel):
    enabled: bool = True
    max_per_cluster_top50: int = 10
    max_per_cluster_watchlist: int = 8
    mode: str = "cap"


class WalletFilters(BaseModel):
    min_total_usd: float = 5000
    min_markets: int = 3
    min_clusters: int = 2
    max_top_cluster_share: float = 0.7
    max_top_market_share: float = 0.5
    max_hhi_clusters: float = 0.65
    max_sidedness: float = 0.9


class WalletRanking(BaseModel):
    w_total_usd_log: float = 1.0
    w_markets: float = 0.4
    w_clusters: float = 0.8
    w_diversity_bonus: float = 1.2
    w_concentration_penalty: float = -1.0
    w_sidedness_penalty: float = -0.4


class ReportConfig(BaseModel):
    top_wallets: int = 15


class FlowWeights(BaseModel):
    w_flow_delta: float = 1.0
    w_new_clusters: float = 0.8
    w_cluster_delta: float = 0.3
    w_concentration_penalty: float = 1.2


class FlowConfig(BaseModel):
    min_total_usd_today: float = 5000
    min_total_delta_usd: float = 2000
    min_position_delta_usd: float = 1000
    min_new_position_usd: float = 1500
    max_top_cluster_share_today: float = 0.6
    top_wallets: int = 25
    top_positions_per_wallet: int = 10
    weights: FlowWeights = FlowWeights()


class Weights(BaseModel):
    market: Dict[str, float]
    wallet: Dict[str, float]


class AppConfig(BaseModel):
    run: RunConfig = RunConfig()
    market_filters: MarketFilters = MarketFilters()
    holders: HoldersConfig = HoldersConfig()
    thresholds: Thresholds = Thresholds()
    diversity: DiversityConfig = DiversityConfig()
    wallet_filters: WalletFilters = WalletFilters()
    wallet_ranking: WalletRanking = WalletRanking()
    report: ReportConfig = ReportConfig()
    flow: FlowConfig = FlowConfig()
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
