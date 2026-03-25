import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import ccxt.async_support as ccxt

class SignalAuditor:
    """
    Tracks performance of signals over time.
    Intervals: 1h, 2h, 4h, 8h, 12h, 24h, 2d, 4d, 7d.
    """
    def __init__(self, db_manager):
        self.db = db_manager
        self.exchange = ccxt.binance({'enableRateLimit': True})
        self.checkpoints = {
            '1h': timedelta(hours=1),
            '2h': timedelta(hours=2),
            '4h': timedelta(hours=4),
            '8h': timedelta(hours=8),
            '12h': timedelta(hours=12),
            '24h': timedelta(hours=24),
            '2d': timedelta(days=2),
            '4d': timedelta(days=4),
            '7d': timedelta(days=7)
        }

    async def add_signal(self, symbol: str, price: float):
        query = """
            INSERT INTO signal_audits (symbol, alert_time, alert_price, status)
            VALUES ($1, $2, $3, 'ACTIVE')
        """
        await self.db.execute(query, symbol, datetime.now(timezone.utc), float(price))
        logging.info(f"Signal audit added for {symbol} at ${price}")

    async def run_audit_cycle(self):
        """Checks one pending checkpoint if time has passed"""
        # Fetch active audits
        audits = await self.db.fetch("SELECT * FROM signal_audits WHERE status = 'ACTIVE'")
        if not audits: return

        now = datetime.now(timezone.utc)
        for audit in audits:
            # Check maximum reached price in the meantime? 
            # (In a real system, we'd pole High prices continuously)
            
            # Simple check for checkpoints
            alert_time = audit[2] # alert_time
            symbol = audit[1]     # symbol
            
            # For each interval, check if time passed and column is empty
            # Map columns index: 4: 1h, 5: 4h, 6: 24h, 7: 7d (Simplified set for DB schema)
            # Schema columns: id(0), sym(1), time(2), price(3), 1h(4), 4h(5), 24h(6), 7d(7)
            
            intervals_to_check = [
                ('1h', 4), ('4h', 5), ('24h', 6), ('7d', 7)
            ]
            
            for label, col_idx in intervals_to_check:
                if audit[col_idx] is None:
                    wait_time = self.checkpoints[label]
                    if now >= alert_time + wait_time:
                        await self.update_checkpoint(audit[0], symbol, label, col_idx)

    async def update_checkpoint(self, audit_id, symbol, label, col_idx):
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            price = ticker['last']
            
            col_names = {4: 'checkpoint_1h', 5: 'checkpoint_4h', 6: 'checkpoint_24h', 7: 'checkpoint_7d'}
            col_name = col_names[col_idx]
            
            await self.db.execute(f"UPDATE signal_audits SET {col_name} = $1 WHERE id = $2", float(price), audit_id)
            
            # Final 7d check marks as DONE
            if label == '7d':
                await self.db.execute("UPDATE signal_audits SET status = 'DONE' WHERE id = $1", audit_id)
            
            logging.info(f"Audit {audit_id} ({symbol}) updated at {label}: ${price}")
        except Exception as e:
            logging.error(f"Audit update failed for {symbol}: {e}")

    async def run_loop(self):
        while True:
            await self.run_audit_cycle()
            await asyncio.sleep(600) # Check every 10 mins
