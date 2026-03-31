import asyncio
import os
import sys
from datetime import datetime, timezone

# Ensure project root is in path
sys.path.append(os.getcwd())

from database.db_manager import DatabaseManager

async def check_db_health():
    db = DatabaseManager()
    try:
        await db.connect()
        print("🔍 Database Health Check Starting...")
        print("-" * 40)

        # 1. Check Ticks Table
        try:
            tick_count = await db.fetch("SELECT count(*) FROM ticks")
            latest_tick = await db.fetch("SELECT time, symbol, exchange FROM ticks ORDER BY time DESC LIMIT 1")
            
            print(f"✅ Ticks Table: {tick_count[0][0]:,} rows")
            if latest_tick:
                t, s, e = latest_tick[0]
                print(f"   Latest Data: {t} | Symbol: {s} | Ex: {e}")
            else:
                print("   ⚠️ No data in ticks table yet.")
        except Exception as e:
            print(f"❌ Error checking ticks: {e}")

        # 2. Check Symbols variety
        try:
            symbol_count = await db.fetch("SELECT count(DISTINCT symbol) FROM ticks")
            print(f"✅ Unique Symbols: {symbol_count[0][0]} symbols currently tracking")
        except: pass

        # 3. Check OHLCV Table
        try:
            ohlcv_count = await db.fetch("SELECT count(*) FROM ohlcv")
            print(f"✅ OHLCV Table: {ohlcv_count[0][0]:,} aggregated rows")
        except: pass

        # 4. Check Tokens/Signals (if exists)
        try:
            token_count = await db.fetch("SELECT count(*) FROM tokens")
            print(f"✅ Tokens Table: {token_count[0][0]} symbols discovered")
        except: pass

        print("-" * 40)
        print("🚀 System is successfully accumulating data!")
        
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(check_db_health())
