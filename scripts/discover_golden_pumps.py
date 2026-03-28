import ccxt
import pandas as pd
import time
from datetime import datetime, timedelta

def discover_pumps():
    binance = ccxt.binance({'options': {'defaultType': 'future'}})
    print("Fetching symbols...")
    markets = binance.load_markets()
    symbols = [s for s in markets if s.endswith('/USDT')]
    
    results = []
    three_months_ago = binance.parse8601((datetime.now() - timedelta(days=90)).isoformat())
    
    print(f"Scanning {len(symbols)} symbols for 1.5x+ gains in the last 90 days...")
    
    for i, symbol in enumerate(symbols):
        try:
            # Fetch daily data for the last 90 days
            ohlcv = binance.fetch_ohlcv(symbol, timeframe='1d', since=three_months_ago)
            if not ohlcv: continue
            
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            
            # Find the max gain compared to the minimum price in the preceding 7 days
            for j in range(7, len(df)):
                min_price = df.iloc[j-7:j]['low'].min()
                max_price = df.iloc[j]['high']
                gain = max_price / min_price if min_price > 0 else 1.0
                
                if gain >= 1.5:
                    results.append({
                        'symbol': symbol,
                        'date': datetime.fromtimestamp(df.iloc[j]['ts']/1000).strftime('%Y-%m-%d'),
                        'gain_x': round(gain, 2),
                        'peak_price': max_price,
                        'timestamp': df.iloc[j]['ts']
                    })
                    break # Only log the first occurrence for now
            
            if i % 20 == 0:
                print(f"Progress: {i}/{len(symbols)} symbols checked...")
                
        except Exception as e:
            pass # Skip errors for specific symbols
            
    if not results:
        print("No matches found.")
        return
        
    res_df = pd.DataFrame(results).sort_values('gain_x', ascending=False)
    print("\n🚀 GOLDEN PUMP DISCOVERY (Last 90 Days):")
    print(res_df.to_string(index=False))
    res_df.to_csv("golden_pumps_list.csv", index=False)

if __name__ == "__main__":
    discover_pumps()
