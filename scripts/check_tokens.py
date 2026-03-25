import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect("postgresql://admin:password@127.0.0.1:5433/pump_ai")
    rows = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens'")
    print("Columns in 'tokens':")
    for r in rows:
        print(f" - {r['column_name']}")
    
    print("\nSample data from 'tokens':")
    data = await conn.fetch("SELECT * FROM tokens LIMIT 3")
    for d in data:
        print(dict(d))
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
