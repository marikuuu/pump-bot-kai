import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def analyze_naked_dna(symbol, pump_end_ts):
    exchanges = [
        ccxt.binance({'options': {'defaultType': 'future'}}),
        ccxt.mexc({'options': {'defaultType': 'swap'}}),
        ccxt.bitget({'options': {'defaultType': 'swap'}})
    ]
    
    selected_ex = None
    target_market_sym = None
    
    for ex in exchanges:
        try:
            markets = ex.load_markets()
            for m_sym, m_info in markets.items():
                if symbol == m_sym or f"{symbol}:USDT" == m_sym or symbol.replace("/", "") == m_sym.split(':')[0]:
                    selected_ex = ex
                    target_market_sym = m_sym
                    break
            if selected_ex: break
        except: continue
        
    if not selected_ex:
        return None, 0
    
    symbol = target_market_sym
    print(f"Using exchange: {selected_ex.id} | Full Symbol: {symbol}")
    
    start_ts = int(pump_end_ts - (7 * 24 * 60 * 60 * 1000))
    end_ts = int(pump_end_ts + (6 * 60 * 60 * 1000))
    
    # 1. Fetch 1m OHLCV
    all_ohlcv = []
    current_ts = start_ts
    try:
        while current_ts < end_ts:
            ohlcv = selected_ex.fetch_ohlcv(symbol, timeframe='1m', since=current_ts, limit=1000)
            if not ohlcv: break
            all_ohlcv.extend(ohlcv)
            current_ts = ohlcv[-1][0] + 60000
    except Exception as e:
        print(f"Fetch error for {symbol}: {e}")
        return None, 0
    
    if not all_ohlcv: return None, 0
    
    df = pd.DataFrame(all_ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    
    # 2. Fetch Funding Rate History
    print("Fetching Funding Rate history...")
    try:
        funding = binance.fetch_funding_rate_history(symbol, since=start_ts)
        f_df = pd.DataFrame(funding)
    except:
        f_df = pd.DataFrame()

    # 3. Fetch Open Interest (if multiple data points available)
    # Note: Historical OI for specific minutes is hard via standard CCXT, 
    # but we can try to find trends.

    # 4. VPVR (Volume Profile Visible Range) Calculation
    print("Calculating VPVR (Volume Profile)...")
    price_min, price_max = df['low'].min(), df['high'].max()
    bins = 50
    price_bins = np.linspace(price_min, price_max, bins+1)
    df['bin'] = pd.cut((df['open'] + df['close'])/2, bins=price_bins)
    vpvr = df.groupby('bin', observed=True)['vol'].sum()
    
    poc_bin = vpvr.idxmax()
    poc_price = (poc_bin.left + poc_bin.right) / 2
    
    # Identify Vacuum Zones (Bins with vol < 10% of POC)
    vacuum_threshold = vpvr.max() * 0.1
    vacuum_zones = vpvr[vpvr < vacuum_threshold]
    
    print(f"POC (Solid Foundation): ${poc_price:.4f}")
    print(f"Vacuum Zones detected: {len(vacuum_zones)} buckets")

    # 5. Categorization Logic
    # Accumulation Check: Is volume in the first 5 days significantly higher than baseline?
    first_5_days_vol = df[df['ts'] < start_ts + (5 * 24 * 60 * 60 * 1000)]['vol'].mean()
    last_2_days_vol = df[df['ts'] >= start_ts + (5 * 24 * 60 * 60 * 1000)]['vol'].mean()
    acc_ratio = last_2_days_vol / (first_5_days_vol + 1e-9)
    
    category = "PRE-ACCUMULATION (Predictable)" if acc_ratio > 2.0 else "FLASH/EVENT (Unpredictable)"
    
    print(f"Accumulation Ratio (Last 2d / First 5d): {acc_ratio:.2f}")
    print(f"FINAL CATEGORY: {category}")

    # Save to CSV for deep visualization
    out_dir = f"analysis/naked_{symbol.replace('/', '_')}"
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(f"{out_dir}/1m_data.csv", index=False)
    vpvr.to_csv(f"{out_dir}/vpvr_profile.csv")
    
    return category, acc_ratio

def run_bulk_analysis():
    df_golden = pd.read_csv("golden_pumps_list.csv").head(50)
    
    for idx, row in df_golden.iterrows():
        try:
            category, ratio = analyze_naked_dna(row['symbol'], row['timestamp'])
            print(f"Result for {row['symbol']}: {category} (Ratio: {ratio:.2f})")
        except Exception as e:
            print(f"Failed to analyze {row['symbol']}: {e}")
            continue

if __name__ == "__main__":
    run_bulk_analysis()
