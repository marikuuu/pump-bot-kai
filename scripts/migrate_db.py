import asyncio
import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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

        logging.info("Applying schema statement by statement...")
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        
        for statement in statements:
            try:
                # ログを短縮して表示
                snippet = statement[:50].replace('\n', ' ') + "..."
                logging.info(f"Executing: {snippet}")
                await db.execute(statement)
            except Exception as e:
                logging.error(f"❌ Statement failed: {e}")
                # 一部のエラー（既存のインデックス等）は無視して進める
        
        logging.info("✅ Database migration process completed!")
        
    except Exception as e:
        logging.error(f"❌ Migration failed: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(migrate())
