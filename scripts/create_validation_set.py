import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
import random
import time

def extract_random_anomaly_points():
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    
    # Range: 2/24 - 3/24
    start_dt = datetime(2026, 2, 24, tzinfo=timezone.utc)
    since = int(start_dt.timestamp() * 1000)
    
    markets = exchange.load_markets()
    symbols = [s for s in markets if s.endswith('/USDT:USDT')]
    
    anomalies = [] # (symbol, timestamp, max_gain)
    
    num_to_sample = 50 # random symbols to find anomalies in
    sampled_symbols = random.sample(symbols, min(num_to_sample, len(symbols)))
    
    print(f"Sampling anomalies from {len(sampled_symbols)} symbols over 30 days...")
    
    for s in sampled_symbols:
        try:
            # 1h candles
            candles = exchange.fetch_ohlcv(s, timeframe='1h', since=since, limit=1000)
            if not candles: continue
            
            df = pd.DataFrame(candles, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            # Find points where volume spike happened (Stage 2 simulation)
            avg_vol = df['v'].mean()
            spikes = df[df['v'] > avg_vol * 5] # 5x volume spike
            
            for i, row in spikes.iterrows():
                # Check outcome (must be NOT 1.5x)
                future_c = df[df['ts'] > row['ts']].head(72)
                if future_c.empty: continue
                gain = (future_c['h'].max() - row['c']) / row['c']
                
                if gain < 0.3: # Clearly not a monster pump
                    anomalies.append({
                        'symbol': s,
                        'timestamp': row['ts'],
                        'max_gain': gain,
                        'label': 0
                    })
                    if len(anomalies) % 50 == 0:
                        print(f"Found {len(anomalies)} failures...")
            
            time.sleep(0.02)
        except Exception:
            pass
            
    # Combine with Gold Events (label=1)
    gold_df = pd.read_csv('one_month_gold_events.csv')
    gold_df['label'] = 1
    
    # Limit anomalies to ~300 for a test set
    failure_df = pd.DataFrame(anomalies).sample(min(len(anomalies), 300))
    
    full_test_set = pd.concat([gold_df[['symbol', 'timestamp', 'label']], failure_df[['symbol', 'timestamp', 'label']]])
    full_test_set.to_csv('one_month_validation_set.csv', index=False)
    print(f"Final Validation Set: {len(full_test_set)} points (Success: {len(gold_df)}, Failure: {len(failure_df)})")

if __name__ == "__main__":
    extract_random_anomaly_points()
