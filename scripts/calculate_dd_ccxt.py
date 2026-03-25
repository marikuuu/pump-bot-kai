import ccxt
import pandas as pd
import pickle
from datetime import datetime, timezone
import time

def calculate_dd_ccxt():
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    
    # 1. Load predicted signals
    df = pd.read_csv('tick_training_data.csv')
    with open('pump_ai/pump_model_v5_tick.pkl', 'rb') as f:
        model = pickle.load(f)

    FT = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
    df['conf'] = model.predict_proba(df[FT].fillna(0))[:, 1]
    # Hits (one per symbol for speed)
    hits = df[df['conf'] >= 0.85].sort_values(['symbol', 'timestamp']).drop_duplicates('symbol').copy()

    print(f"Calculating DD for {len(hits)} symbols using CCXT...")
    results = []

    for i, row in hits.iterrows():
        symbol = row['symbol'].split(':')[0] # conversion for ccxt
        try:
            # Fetch 1m candles around detection
            since = row['timestamp']
            candles = exchange.fetch_ohlcv(symbol, timeframe='1m', since=since, limit=1440) # 24h
            if not candles:
                results.append({'symbol': symbol, 'dd': 0.0, 'note': 'No data'})
                continue
            
            # Calculate floor price before hitting top (or just min low in window)
            # Entry price is price in the detection row
            entry_p = row['price']
            
            # We want max drawdown relative to entry price BEFORE it hits the moon
            # But simpler: just min low in the next few hours
            lows = [c[3] for c in candles]
            min_low = min(lows)
            
            dd = (min_low - entry_p) / entry_p * 100
            if dd > 0: dd = 0.0 # No DD if it never went below entry
            
            results.append({
                'symbol': symbol,
                'time': datetime.fromtimestamp(row['timestamp']/1000, tz=timezone.utc).strftime('%m/%d %H:%M'),
                'dd': dd
            })
            time.sleep(0.1) # rate limit
        except Exception as e:
            results.append({'symbol': symbol, 'dd': 0.0, 'note': str(e)})

    res_df = pd.DataFrame(results)
    print("\n=== Signal Max Drawdown (DD) Report (24h window) ===")
    print(res_df.sort_values('dd')[['symbol', 'time', 'dd']].to_string(index=False))
    print(f"\nAverage DD: {res_df['dd'].mean():.2f}%")

if __name__ == "__main__":
    calculate_dd_ccxt()
