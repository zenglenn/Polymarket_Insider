# Polymarket Insider

Polymarket Insider is a local-first pipeline that pulls public Polymarket Gamma and Data API data, scores markets deterministically, and writes daily reports and watchlists.

## Run

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e .
python3 -m polymarket_insider.pipeline.run_daily
```

## What To Look At After A Run

- Report (human-readable): `out/report_YYYY-MM-DD.md`
- Market ranking (CSV): `out/report_YYYY-MM-DD.csv`
- Signal candidates: `out/wallets_ranked_YYYY-MM-DD.csv`
- Concentrated event traders (excluded): `out/wallets_concentrated_YYYY-MM-DD.csv`
- Top wallet positions: `out/wallet_positions_top_YYYY-MM-DD.csv`
- Cluster summary: `out/clusters_summary_YYYY-MM-DD.csv`
- Watchlist (JSON): `out/watchlist.json`
- SQLite: `data/polymarket_insider.sqlite`
- Raw API captures: `data/raw/YYYY-MM-DD/`

Quick local inspection:

```bash
python3 -m polymarket_insider.tools.inspect_latest
```

## Clusters and Diversification

Markets are grouped into deterministic clusters (event/group/slug/question) to avoid a single event family dominating the Top 50 and watchlist. Configure caps in `config.yaml`:

- `diversity.max_per_cluster_top50`
- `diversity.max_per_cluster_watchlist`

## Holders Coverage

Holders are fetched for the top N kept markets by volume/liquidity. Coverage is reported as a percent of kept markets. Configure:

- `holders.max_markets_to_fetch`

## Limitations

- Uses public APIs only; data availability can be incomplete or delayed.
- Scores are deterministic and heuristic; they do not imply insider certainty.
- USD values come from API fields or price-implied estimates; missing prices leave value_usd null.
- These outputs surface signal candidates and concentrated event traders, not attribution.
