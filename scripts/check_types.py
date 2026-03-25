import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect("postgresql://admin:password@127.0.0.1:5433/pump_ai")
    rows = await conn.fetch("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'candles'")
    for r in rows:
        print(f"{r['column_name']}: {r['data_type']}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
