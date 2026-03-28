import asyncio
from database.db_manager import DatabaseManager
async def check():
    db = DatabaseManager()
    await db.connect()
    r = await db.fetch_all("SELECT * FROM signal_audits ORDER BY alert_time DESC LIMIT 20;")
    print("Recent Signal Audits:")
    for row in r:
        print(f"  {row[1]}: {row[2]} at ${row[3]}")
    await db.close()

if __name__ == "__main__":
    asyncio.run(check())
