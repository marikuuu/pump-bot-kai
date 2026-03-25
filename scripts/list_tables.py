import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect("postgresql://admin:password@127.0.0.1:5433/pump_ai")
    rows = await conn.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    print("Tables in DB:")
    for r in rows:
        print(f" - {r['table_name']}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
