import asyncio
from datetime import datetime, timedelta
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_manager import DatabaseManager
from backtester.engine import EventDrivenBacktester

async def test_sim():
    db = DatabaseManager()
    await db.connect()
    
    engine = EventDrivenBacktester(db)
    
    # バックテスト期間を直近1週間に設定（デモ用）
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=7)
    
    symbols = ["PEPEUSDT"]
    logging.info("Starting simulation runner...")
    await engine.run_simulation(symbols, start_date, end_date)

if __name__ == "__main__":
    asyncio.run(test_sim())
