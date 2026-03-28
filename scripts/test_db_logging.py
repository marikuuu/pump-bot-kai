import asyncio
import logging
from datetime import datetime, timezone
from database.db_manager import DatabaseManager
from database.data_logger import DataLogger

async def test_logging():
    logging.basicConfig(level=logging.INFO)
    db = DatabaseManager()
    await db.connect()
    logger = DataLogger(db)
    
    test_ticks = [
        (datetime.now(timezone.utc), 'binance', 'TEST/USDT', 1.2345, 100.0, 'buy', False),
        (datetime.now(timezone.utc), 'binance', 'TEST/USDT', 1.2346, 200.0, 'sell', True)
    ]
    
    print("Testing bulk log insertion...")
    try:
        await logger.log_ticks_batch(test_ticks)
        print("✅ Bulk insertion SUCCESS!")
        
        # Verify
        res = await db.fetch_all("SELECT * FROM ticks WHERE symbol = 'TEST/USDT';")
        print(f"Verification: Found {len(res)} rows for TEST/USDT.")
        
        # Cleanup
        await db.execute("DELETE FROM ticks WHERE symbol = 'TEST/USDT';")
        print("Cleanup done.")
        
    except Exception as e:
        print(f"❌ Bulk insertion FAILED: {e}")
    
    await db.close()

if __name__ == "__main__":
    asyncio.run(test_logging())
