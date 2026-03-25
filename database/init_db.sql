-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop existing for clean init (if re-running)
DROP TABLE IF EXISTS dex_swaps;
DROP TABLE IF EXISTS whale_trades;
DROP TABLE IF EXISTS candles;
DROP TABLE IF EXISTS wallet_labels;
DROP TABLE IF EXISTS tokens;

-- 1. Tokens Table (Base Metadata)
CREATE TABLE IF NOT EXISTS tokens (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    address VARCHAR(128), -- For DEX coins
    network VARCHAR(50) DEFAULT 'CEX', -- 'ETH', 'SOL', 'CEX'
    market_cap DECIMAL,
    is_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, network, address)
);

-- 2. Candles Table (Time-series)
CREATE TABLE IF NOT EXISTS candles (
    time TIMESTAMPTZ NOT NULL,
    token_id INTEGER REFERENCES tokens(id),
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume DECIMAL
);

SELECT create_hypertable('candles', 'time', if_not_exists => TRUE);

-- 3. Whale Trades (On-chain/CEX Whale tracking)
CREATE TABLE IF NOT EXISTS whale_trades (
    time TIMESTAMPTZ NOT NULL,
    token_id INTEGER REFERENCES tokens(id),
    wallet_address VARCHAR(128),
    amount DECIMAL,
    side VARCHAR(10), -- 'buy', 'sell'
    tx_hash VARCHAR(128)
);

SELECT create_hypertable('whale_trades', 'time', if_not_exists => TRUE);

-- 4. Wallet Labels (Pseudo-Nansen Intelligence)
CREATE TABLE IF NOT EXISTS wallet_labels (
    address VARCHAR(128) PRIMARY KEY,
    label VARCHAR(100) NOT NULL, -- 'Smart Money', 'Exchange', 'Whale'
    source VARCHAR(50), -- 'Arkham', 'Manual', 'Algorithm'
    confidence FLOAT DEFAULT 1.0,
    last_seen TIMESTAMPTZ DEFAULT NOW()
);

-- 5. DEX Swaps (Real-time tracking)
CREATE TABLE IF NOT EXISTS dex_swaps (
    time TIMESTAMPTZ NOT NULL,
    chain VARCHAR(50),
    tx_hash VARCHAR(128),
    sender VARCHAR(128),
    recipient VARCHAR(128),
    amount_in DECIMAL,
    amount_out DECIMAL,
    is_smart_money BOOLEAN DEFAULT FALSE,
    wallet_label TEXT
);

SELECT create_hypertable('dex_swaps', 'time', if_not_exists => TRUE);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens(symbol);
CREATE INDEX IF NOT EXISTS idx_labels_address ON wallet_labels(address);
CREATE INDEX IF NOT EXISTS idx_swaps_smart ON dex_swaps(is_smart_money) WHERE is_smart_money = TRUE;

-- 6. Signal Audits (Post-alert Performance Tracking)
CREATE TABLE IF NOT EXISTS signal_audits (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    alert_time TIMESTAMPTZ NOT NULL,
    alert_price DECIMAL NOT NULL,
    checkpoint_1h DECIMAL,
    checkpoint_4h DECIMAL,
    checkpoint_24h DECIMAL,
    checkpoint_7d DECIMAL,
    max_pump_reached DECIMAL,
    status VARCHAR(20) DEFAULT 'ACTIVE'
);
