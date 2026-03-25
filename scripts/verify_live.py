import asyncio
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from database.db_manager import DatabaseManager

async def verify_live():
    load_dotenv()
    db = DatabaseManager()
    await db.connect()
    
    print("\n=== SYSTEM HEALTH CHECK (LIVE DATA) ===")
    
    # Check Ticks (MainBot)
    ticks = await db.fetch("SELECT time, symbol, price FROM ticks ORDER BY time DESC LIMIT 5")
    print(f"\n--- Latest 5 Ticks (MainBot) ---")
    if ticks:
        for t in ticks:
            print(f"[{t['time']}] {t['symbol']} @ ${t['price']}")
        now = datetime.now(timezone.utc)
        diff = (now - ticks[0]['time']).total_seconds()
        print(f"Latency from latest tick: {diff:.1f} seconds")
    else:
        print("No ticks found.")

    # Check Swaps (Nansen Monitor)
    swaps = await db.fetch("SELECT time, tx_hash, amount_usd FROM dex_swaps ORDER BY time DESC LIMIT 5")
    print(f"\n--- Latest 5 Swaps (Nansen Monitor) ---")
    if swaps:
        for s in swaps:
            print(f"[{s['time']}] TX: {s['tx_hash'][:15]}... | USD: ${s['amount_usd']}")
        now = datetime.now(timezone.utc)
        diff = (now - swaps[0]['time']).total_seconds()
        print(f"Latency from latest swap: {diff:.1f} seconds")
    else:
        print("No swaps found.")

    await db.close()

if __name__ == "__main__":
    asyncio.run(verify_live())
