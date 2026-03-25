import asyncio
import ccxt.pro as ccxtpro
import logging
import traceback

async def test_exchange(name, options):
    print(f"\n--- Testing {name} ---")
    exchange = getattr(ccxtpro, name)(options)
    try:
        # 1. Fetch Markets
        print(f"[{name}] Fetching markets...")
        markets = await exchange.load_markets()
        print(f"[{name}] Found {len(markets)} markets.")
        
        # 2. Filter for Perpetual/Swap
        perps = [s for s, m in markets.items() if m.get('linear') or m.get('type') in ['swap', 'future']]
        print(f"[{name}] Found {len(perps)} Perpetual/Swap markets.")
        if perps:
            print(f"[{name}] Sample: {perps[0]}")
            
        # 3. Fetch Tickers (Top 2)
        if perps:
            print(f"[{name}] Fetching tickers for {perps[:2]}...")
            tickers = await exchange.fetch_tickers(perps[:2])
            print(f"[{name}] Tickers fetched successfully.")
            
    except Exception as e:
        print(f"[{name}] ERROR: {type(e).__name__}: {e}")
        # traceback.print_exc()
    finally:
        await exchange.close()

async def main():
    # Bybit
    await test_exchange('bybit', {'options': {'defaultType': 'linear'}})
    # Binance
    await test_exchange('binance', {'options': {'defaultType': 'future'}})
    # MEXC
    await test_exchange('mexc', {'options': {'defaultType': 'swap'}})

if __name__ == "__main__":
    asyncio.run(main())
