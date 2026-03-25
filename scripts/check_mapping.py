import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect("postgresql://admin:password@127.0.0.1:5433/pump_ai")
    
    print("Checking 'tokens' table if exists...")
    try:
        rows = await conn.fetch("SELECT * FROM tokens LIMIT 5")
        for r in rows:
            print(dict(r))
    except Exception as e:
        print(f"Error reading tokens: {e}")

    print("\nChecking 'candles' mapping sample...")
    rows = await conn.fetch("SELECT DISTINCT token_id FROM candles LIMIT 5")
    for r in rows:
        print(f"Token ID: {r['token_id']}")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
