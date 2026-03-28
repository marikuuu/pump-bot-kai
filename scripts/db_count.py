import asyncio
from database.db_manager import DatabaseManager
async def check():
    db = DatabaseManager()
    await db.connect()
    r = await db.fetch_all("SELECT count(*) FROM ticks;")
    print(f"Total Ticks in DB: {r[0][0]}")
    r2 = await db.fetch_all("SELECT symbol, count(*) FROM ticks GROUP BY symbol ORDER BY count(*) DESC LIMIT 10;")
    print("Top Symbols in DB:")
    for row in r2:
        print(f"  {row[0]}: {row[1]}")
    await db.close()

if __name__ == "__main__":
    asyncio.run(check())
