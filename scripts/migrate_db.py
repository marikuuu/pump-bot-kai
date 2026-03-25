import asyncio
import os
import logging
from dotenv import load_dotenv
from database.db_manager import DatabaseManager

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def migrate():
    load_dotenv()
    db = DatabaseManager()
    
    try:
        logging.info("Connecting to database...")
        await db.connect()
        
        schema_path = os.path.join("database", "schema.sql")
        if not os.path.exists(schema_path):
            logging.error(f"Schema file not found at {schema_path}")
            return

        logging.info(f"Reading schema from {schema_path}...")
        with open(schema_path, "r", encoding="utf-8") as f:
            sql = f.read()

        logging.info("Applying schema (this will create 'ticks' table if missing)...")
        # Split by ';' to execute statements one by one if necessary, 
        # but asyncpg allows executing multiple statements.
        await db.execute(sql)
        
        logging.info("✅ Database migration completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Migration failed: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(migrate())
