import asyncio
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_manager import DatabaseManager

async def test_db():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    print(f"Testing connection to: {db_url}")
    db = DatabaseManager()
    try:
        await db.connect()
        print("✅ Database connection successful!")
        res = await db.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        print(f"Tables found: {[r['table_name'] for r in res]}")
        await db.close()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_db())
