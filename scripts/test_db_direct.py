import asyncpg
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")

async def run():
    print(f"Connecting to {dsn}...")
    try:
        conn = await asyncio.wait_for(asyncpg.connect(dsn), timeout=10)
        print("Connected!")
        r = await conn.fetchval("SELECT count(*) FROM ticks")
        print(f"Total Ticks: {r}")
        await conn.close()
    except Exception as e:
        print(f"Connection Failed: {e}")

if __name__ == "__main__":
    asyncio.run(run())
