import asyncio
import logging
from datetime import datetime, timezone
from database.db_manager import DatabaseManager

class DataLogger:
    """
    Persists real-time market data to PostgreSQL/TimescaleDB.
    """
    def __init__(self, db_manager):
        self.db = db_manager

    async def log_tick(self, exchange: str, symbol: str, price: float, amount: float, side: str, is_maker: bool):
        query = """
            INSERT INTO ticks (time, exchange, symbol, price, amount, side, is_buyer_maker)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        await self.db.execute(query, datetime.now(timezone.utc), exchange, symbol, 
                             float(price), float(amount), side, is_maker)

    async def log_candle(self, exchange: str, symbol: str, open_p, high, low, close, vol):
        query = """
            INSERT INTO ohlcv (time, exchange, symbol, open, high, low, close, volume)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        await self.db.execute(query, datetime.now(timezone.utc), exchange, symbol,
                             float(open_p), float(high), float(low), float(close), float(vol))

    async def log_whale_trade(self, symbol, wallet, amount, side, tx_hash=None):
        token_id = await self.get_token_id(symbol)
        query = """
            INSERT INTO whale_trades (time, token_id, wallet_address, amount, side, tx_hash)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        await self.db.execute(query, datetime.now(timezone.utc), token_id, 
                             wallet, float(amount), side, tx_hash)

    async def get_token_id(self, symbol: str):
        # Check if already in DB
        res = await self.db.fetch("SELECT id FROM tokens WHERE symbol = $1", symbol)
        if res:
            return res[0][0]
        else:
            # Insert and get ID
            await self.db.execute("INSERT INTO tokens (symbol) VALUES ($1) ON CONFLICT DO NOTHING", symbol)
            res = await self.db.fetch("SELECT id FROM tokens WHERE symbol = $1", symbol)
            return res[0][0]
