SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_date TEXT PRIMARY KEY,
    timezone TEXT NOT NULL,
    created_at TEXT NOT NULL,
    status TEXT,
    finished_at TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS markets (
    market_id TEXT PRIMARY KEY,
    question TEXT,
    slug TEXT,
    status TEXT,
    cluster_key TEXT,
    close_time TEXT,
    volume_usd REAL,
    liquidity_usd REAL,
    raw_json TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    run_date TEXT NOT NULL,
    market_id TEXT NOT NULL,
    question TEXT,
    slug TEXT,
    status TEXT,
    cluster_key TEXT,
    close_time TEXT,
    volume_usd REAL,
    liquidity_usd REAL,
    raw_json TEXT,
    PRIMARY KEY (run_date, market_id)
);

CREATE TABLE IF NOT EXISTS holders (
    run_date TEXT NOT NULL,
    market_id TEXT NOT NULL,
    wallet TEXT NOT NULL,
    address TEXT,
    outcome TEXT,
    shares REAL,
    value_usd REAL,
    fetched_at TEXT,
    exposure_usd REAL,
    is_new_wallet INTEGER,
    source TEXT,
    raw_json TEXT,
    PRIMARY KEY (run_date, market_id, wallet)
);

CREATE TABLE IF NOT EXISTS market_scores (
    run_date TEXT NOT NULL,
    market_id TEXT NOT NULL,
    score REAL NOT NULL,
    signals_json TEXT,
    PRIMARY KEY (run_date, market_id)
);

CREATE TABLE IF NOT EXISTS wallet_scores (
    run_date TEXT NOT NULL,
    wallet TEXT NOT NULL,
    score REAL NOT NULL,
    signals_json TEXT,
    PRIMARY KEY (run_date, wallet)
);

CREATE TABLE IF NOT EXISTS wallet_metrics (
    run_date TEXT NOT NULL,
    address TEXT NOT NULL,
    total_usd REAL,
    markets_count INTEGER,
    clusters_count INTEGER,
    top_cluster_share REAL,
    yes_usd REAL,
    no_usd REAL,
    yes_share REAL,
    sidedness REAL,
    top_market_share REAL,
    hhi_markets REAL,
    hhi_clusters REAL,
    created_at TEXT,
    PRIMARY KEY (run_date, address)
);

CREATE TABLE IF NOT EXISTS run_diagnostics (
    run_date TEXT PRIMARY KEY,
    diagnostics_json TEXT
);
"""
