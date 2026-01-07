# Polymarket Insider

Polymarket Insider is a local-first pipeline that pulls public Polymarket Gamma and Data API data, scores markets deterministically, and writes daily reports and watchlists.

## Run

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m polymarket_insider.pipeline.run_daily
```

## Outputs

- SQLite: `data/polymarket_insider.sqlite`
- Raw API captures: `data/raw/YYYY-MM-DD/`
- Reports: `out/report_YYYY-MM-DD.md`, `out/report_YYYY-MM-DD.csv`, `out/watchlist.json`

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
