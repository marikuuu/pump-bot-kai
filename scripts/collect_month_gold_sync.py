import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import time

def collect_one_month_gold_data():
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    
    # 期間: 2/24 - 3/24
    start_dt = datetime(2026, 2, 24, tzinfo=timezone.utc)
    since = int(start_dt.timestamp() * 1000)
    
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith('/USDT:USDT')]
    
    gold_events = []
    
    print(f"Scanning {len(symbols)} symbols for 30-day history (Sync)...")
    
    for s in symbols:
        try:
            # 1h candles
            candles = exchange.fetch_ohlcv(s, timeframe='1h', since=since, limit=1000)
            if not candles: continue
            
            df = pd.DataFrame(candles, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            # Find max gain in 72h window
            for i in range(len(df) - 72):
                p0 = df.iloc[i]['c']
                max_h = df.iloc[i+1 : i+73]['h'].max()
                gain = (max_h - p0) / p0
                
                if gain >= 0.45: # 1.45x
                    gold_events.append({
                        'symbol': s,
                        'jst': (datetime.fromtimestamp(df.iloc[i]['ts']/1000, tz=timezone.utc) + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M'),
                        'start_ts': df.iloc[i]['ts'],
                        'max_gain': gain
                    })
                    print(f"GOLD: {s} | {gold_events[-1]['jst']} | +{gain*100:.1f}%")
                    break # Next symbol
            
            time.sleep(0.02)
        except Exception:
            pass
            
    print(f"\nTotal Gold Events Found: {len(gold_events)}")
    pd.DataFrame(gold_events).to_csv('one_month_gold_events.csv', index=False)

if __name__ == "__main__":
    collect_one_month_gold_data()
