import asyncio
import logging
from dotenv import load_dotenv
from database.db_manager import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def reset_db():
    load_dotenv()
    db = DatabaseManager()
    await db.connect()
    
    # Drop existing tables with CASCADE
    tables = [
        "ticks", "ohlcv", "open_interest", "dex_swaps", 
        "pump_events", "wallet_labels", "tokens", 
        "whale_trades", "candles", "signal_audits"
    ]
    
    logging.info("Dropping existing tables...")
    for table in tables:
        try:
            await db.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        except Exception as e:
            logging.warning(f"Failed to drop {table}: {e}")
            
    logging.info("Applying full schema from database/schema.sql...")
    with open("database/schema.sql", "r", encoding="utf-8") as f:
        sql = f.read()
    
    # Simple split by ';' (Wait, some statements might have ';' inside strings, 
    # but schema.sql is simple enough here)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for s in statements:
        try:
            await db.execute(s)
        except Exception as e:
            logging.error(f"Failed statement: {s[:50]}... Error: {e}")

    logging.info("✅ Database reset and schema applied successfully!")
    await db.close()

if __name__ == "__main__":
    asyncio.run(reset_db())
