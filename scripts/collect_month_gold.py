import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import asyncio
import time

async def collect_one_month_gold_data():
    """
    1ヶ月分の「1.5倍パンプ」実績を全銘柄から洗い出し、
    v5.1 の学習用「黄金データセット」を作成します。
    """
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    
    # 期間: 2/24 - 3/24
    start_dt = datetime(2026, 2, 24, tzinfo=timezone.utc)
    end_dt = datetime(2026, 3, 24, tzinfo=timezone.utc)
    since = int(start_dt.timestamp() * 1000)
    
    markets = await exchange.load_markets()
    symbols = [s for s in markets if s.endswith('/USDT:USDT')]
    
    gold_events = []
    
    print(f"Scanning {len(symbols)} symbols for 30-day history...")
    
    for s in symbols:
        try:
            # 4h足で大まかなパンプを特定
            candles = await exchange.fetch_ohlcv(s, timeframe='4h', since=since, limit=1000)
            if not candles: continue
            
            df = pd.DataFrame(candles, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            for i in range(len(df) - 18): # 3 days (18 * 4h)
                p0 = df.iloc[i]['c']
                max_p = df.iloc[i+1:i+19]['h'].max()
                
                if (max_p - p0) / p0 >= 0.4: # 1.4x (Diverse threshold)
                    gold_events.append({
                        'symbol': s,
                        'start_ts': df.iloc[i]['ts'],
                        'max_gain': (max_p - p0) / p0
                    })
                    print(f"GOLD FOUND: {s} | {datetime.fromtimestamp(df.iloc[i]['ts']/1000)} | +{((max_p-p0)/p0)*100:.1f}%")
                    break # One event per symbol for training diversity
            
            await asyncio.sleep(0.05)
        except Exception:
            pass
            
    print(f"\nTotal Gold Events Found: {len(gold_events)}")
    pd.DataFrame(gold_events).to_csv('one_month_gold_events.csv', index=False)
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(collect_one_month_gold_data())
