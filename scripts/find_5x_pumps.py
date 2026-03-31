import ccxt
import pandas as pd
import os
import time
from datetime import datetime, timedelta

def scan_exchange(exchange, days_back=180, min_gain_x=5.0):
    try:
        exchange.load_markets()
    except Exception as e:
        print(f"Failed to load markets for {exchange.id}: {e}")
        return []

    symbols = [s for s in exchange.symbols if s.endswith('/USDT') or s.endswith(':USDT')]
    # Take a diverse subset if it's too huge, or run through all. Let's do all valid quote pairs.
    print(f"Scanning {len(symbols)} symbols on {exchange.id} for {min_gain_x}x+ pumps...")

    results = []
    since = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)

    count = 0
    for symbol in symbols:
        count += 1
        if count % 100 == 0:
            print(f"  ...scanned {count}/{len(symbols)} on {exchange.id}")
            
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, '1d', since=since, limit=365)
            if not ohlcv or len(ohlcv) < 10:
                continue
                
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            lowest = df['l'].min()
            highest = df['h'].max()
            
            if lowest > 0:
                gain_x = highest / lowest
                if gain_x >= min_gain_x:
                    # Find when the peak happened and when the bottom happened
                    peak_idx = df['h'].idxmax()
                    peak_dt = datetime.fromtimestamp(df.iloc[peak_idx]['ts'] / 1000)
                    
                    pre_peak_df = df.iloc[:peak_idx+1]
                    bottom_idx = pre_peak_df['l'].idxmin()
                    bottom_dt = datetime.fromtimestamp(df.iloc[bottom_idx]['ts'] / 1000)
                    
                    days_to_peak = (peak_dt - bottom_dt).days
                    
                    results.append({
                        'exchange': exchange.id,
                        'symbol': symbol,
                        'gain_x': gain_x,
                        'bottom_price': lowest,
                        'peak_price': highest,
                        'bottom_date': bottom_dt.strftime('%Y-%m-%d'),
                        'peak_date': peak_dt.strftime('%Y-%m-%d'),
                        'days_to_peak': days_to_peak
                    })
        except Exception:
            pass
            
    return results

def main():
    binance = ccxt.binance({'options': {'defaultType': 'future'}})
    mexc = ccxt.mexc({'options': {'defaultType': 'swap'}})
    
    all_results = []
    
    print("=== STARTING 5x PUMP DISCOVERY (FUTURES ONLY) ===")
    
    # Scan Binance Futures 
    print("\\n[1/2] Scanning Binance Futures...")
    binance_res = scan_exchange(binance, days_back=180, min_gain_x=5.0)
    all_results.extend(binance_res)
    
    # Scan MEXC Swap (Futures)
    print("\\n[2/2] Scanning MEXC Swap (Futures)...")
    mexc_res = scan_exchange(mexc, days_back=180, min_gain_x=5.0)
    all_results.extend(mexc_res)
    
    if all_results:
        df = pd.DataFrame(all_results)
        # Sort by highest gain
        df = df.sort_values('gain_x', ascending=False)
        
        # Save to CSV
        df.to_csv("5x_pumpers_list_futures.csv", index=False)
        print("\\n🏆 FOUND 5x+ PUMPERS (FUTURES):")
        print(f"Total targets found: {len(df)}")
    else:
        print("No 5x pumpers found in the specified timeframe.")

if __name__ == "__main__":
    main()
