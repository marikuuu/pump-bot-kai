import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect("postgresql://admin:password@127.0.0.1:5433/pump_ai")
    print("Columns in 'candles' table:")
    rows = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = 'candles'")
    for r in rows:
        print(f" - {r['column_name']}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
