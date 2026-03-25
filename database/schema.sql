-- 拡張機能：TimescaleDBの有効化
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Token Universe (監視対象トークン)
CREATE TABLE IF NOT EXISTS tokens (
    chain VARCHAR(50),
    address VARCHAR(100),
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INT DEFAULT 18,
    market_cap NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chain, address)
);

-- 2. Market Data (1分足やティック集計用)
CREATE TABLE IF NOT EXISTS ohlcv (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    quote_volume NUMERIC,
    trades_count INT
);
-- ハイパーテーブルへの変換
SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);
-- クエリ用インデックス
CREATE INDEX IF NOT EXISTS ix_ohlcv_exchange_symbol ON ohlcv (exchange, symbol, time DESC);

-- 3. Raw Ticks (1-tick レベルの全歩み値)
CREATE TABLE IF NOT EXISTS ticks (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price NUMERIC NOT NULL,
    amount NUMERIC NOT NULL,
    side VARCHAR(10), -- 'buy', 'sell'
    is_buyer_maker BOOLEAN
);
SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_ticks_symbol_time ON ticks (symbol, time DESC);

-- 4. Open Interest (未決済建玉)
CREATE TABLE IF NOT EXISTS open_interest (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    oi_asset NUMERIC,
    oi_usd NUMERIC
);
SELECT create_hypertable('open_interest', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_oi_exchange_symbol ON open_interest (exchange, symbol, time DESC);

-- 4. DEX Swaps (Smart Money監視用スワップデータ)
CREATE TABLE IF NOT EXISTS dex_swaps (
    time TIMESTAMPTZ NOT NULL,
    chain VARCHAR(50) NOT NULL,
    tx_hash VARCHAR(100) NOT NULL,
    sender VARCHAR(100),
    recipient VARCHAR(100),
    token_in_address VARCHAR(100),
    token_out_address VARCHAR(100),
    amount_in NUMERIC,
    amount_out NUMERIC,
    amount_usd NUMERIC,
    is_smart_money BOOLEAN DEFAULT FALSE,
    wallet_label VARCHAR(100)
);
SELECT create_hypertable('dex_swaps', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_dex_swaps_token_in ON dex_swaps (chain, token_in_address, time DESC);
CREATE INDEX IF NOT EXISTS ix_dex_swaps_token_out ON dex_swaps (chain, token_out_address, time DESC);

-- 5. Pump Events (グラウンドトゥルース/検出履歴テーブル・学習用)
CREATE TABLE IF NOT EXISTS pump_events (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50),
    symbol VARCHAR(100),
    chain VARCHAR(50),
    token_address VARCHAR(100),
    pump_type VARCHAR(50), -- 'Pre-Accumulation', 'Instantaneous', 'False Positive'
    price_start NUMERIC,
    price_peak NUMERIC,
    volume_surge_ratio NUMERIC,
    is_confirmed BOOLEAN DEFAULT FALSE,
    label_source VARCHAR(50) -- 'Telegram', 'Manual', 'Auto'
);

-- 6. Wallet Labels (疑似Nansen用)
CREATE TABLE IF NOT EXISTS wallet_labels (
    address VARCHAR(100) PRIMARY KEY,
    entity_name VARCHAR(100),
    label_type VARCHAR(50), -- e.g. 'exchange', 'smart_money', 'fund'
    chain VARCHAR(50),
    confidence_score INT,
    source VARCHAR(50),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 7. Signal Audits (シグナル後パフォーマンス追跡)
CREATE TABLE IF NOT EXISTS signal_audits (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    alert_time TIMESTAMPTZ NOT NULL,
    alert_price NUMERIC,
    checkpoint_1h NUMERIC,
    checkpoint_4h NUMERIC,
    checkpoint_24h NUMERIC,
    checkpoint_7d NUMERIC,
    status VARCHAR(20) DEFAULT 'ACTIVE' -- 'ACTIVE', 'DONE'
);
CREATE INDEX IF NOT EXISTS ix_signal_audits_symbol ON signal_audits (symbol, alert_time DESC);

-- 8. Whale Trades (大口取引記録)
CREATE TABLE IF NOT EXISTS whale_trades (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    price NUMERIC,
    amount NUMERIC,
    usd_amount NUMERIC,
    side VARCHAR(10)
);
SELECT create_hypertable('whale_trades', 'time', if_not_exists => TRUE);

-- 9. Traditional Candles (1分足、5分足など)
CREATE TABLE IF NOT EXISTS candles (
    time TIMESTAMPTZ NOT NULL,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    interval VARCHAR(10) NOT NULL, -- '1m', '5m', '1h', '1d'
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC
);
SELECT create_hypertable('candles', 'time', if_not_exists => TRUE);
