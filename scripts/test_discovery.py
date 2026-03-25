import asyncio
import ccxt.pro as ccxtpro
import logging

async def test_bybit():
    print("Testing Bybit Linear Discovery...")
    exchange = ccxtpro.bybit({'options': {'defaultType': 'linear'}})
    try:
        # Try raw market fetch
        markets = await exchange.load_markets()
        linear_markets = [m for m in markets.values() if m.get('linear')]
        print(f"Bybit: Found {len(linear_markets)} linear markets.")
        if linear_markets:
            print(f"Sample: {linear_markets[0]['symbol']}")
    except Exception as e:
        print(f"Bybit Error: {e}")
    finally:
        await exchange.close()

async def test_binance():
    print("\nTesting Binance FAPI Discovery...")
    # Try both CCXT modes
    for mode in ['future', 'swap']:
        print(f"Mode: {mode}")
        exchange = ccxtpro.binance({'options': {'defaultType': mode}})
        try:
            markets = await exchange.load_markets()
            print(f"Binance ({mode}): Found {len(markets)} markets.")
        except Exception as e:
            print(f"Binance ({mode}) Error: {e}")
        finally:
            await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_bybit())
    asyncio.run(test_binance())
