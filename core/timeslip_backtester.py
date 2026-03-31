import pandas as pd
import numpy as np
import time
from datetime import datetime
import sys
import os

if sys.stdout.encoding.lower() != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

def run_timeslip(symbol):
    print(f"\\n=======================================================", flush=True)
    print(f"!!! [TIME SLIP] Booting IZANAGI v5 Backtester for {symbol}", flush=True)
    print(f"=======================================================\\n", flush=True)
    
    # 1. We use UM (Futures) data since SOON's accumulation phase predates its Spot listing
    spot_ticks = f"data/binance_dna/{symbol}/um/trades_tick.csv"
    if not os.path.exists(spot_ticks):
        print(f"--- Missing Spot ticks at {spot_ticks}", flush=True)
        return
        
    whale_threshold = 20000  # $20k USD Taker Buy minimum
    
    print(f"--- Loading historical Target: {spot_ticks}", flush=True)
    
    # 2. Live Memory Buffers (To perfectly simulate Real-Time conditions with ZERO future lookahead)
    daily_taker_volume = {} 
    listing_start_time = None
    skip_window_hours = 48
    warmup_days_required = 3
    chunk_size = 500_000
    
    chunk_iter = pd.read_csv(spot_ticks, chunksize=chunk_size, 
                             names=['id','price','qty','quote_qty','timestamp','is_buyer_maker'],
                             on_bad_lines='skip', header=0, low_memory=False)
    
    total_trades = 0
    signals = []
    
    # Performance trackers
    start_time = time.time()
    
    for chunk in chunk_iter:
        chunk['timestamp'] = pd.to_numeric(chunk['timestamp'], errors='coerce')
        chunk.dropna(subset=['timestamp'], inplace=True)
        chunk['time'] = pd.to_datetime(chunk['timestamp'], unit='ms')
        
        # Safely parse boolean maker flag
        chunk['is_buyer_maker'] = chunk['is_buyer_maker'].astype(str).str.lower().map({'true': True, 'false': False, '1': True, '0': False}).fillna(False).astype(bool)
        chunk['quote_qty'] = pd.to_numeric(chunk['quote_qty'], errors='coerce')
        chunk['price'] = pd.to_numeric(chunk['price'], errors='coerce')
        
        chunk['day'] = chunk['time'].dt.floor('D')
        chunk['sec'] = chunk['time'].dt.floor('s') # We need per-second granularity for Vacuum Emulation
        
        # Track listing start
        if listing_start_time is None:
            listing_start_time = chunk['time'].iloc[0]
            print(f"--- Detected Listing Start: {listing_start_time}", flush=True)
        taker_buys = chunk[~chunk['is_buyer_maker']]
        day_sums = taker_buys.groupby('day')['quote_qty'].sum()
        
        for d, v in day_sums.items():
            dt_str = d.strftime('%Y-%m-%d')
            daily_taker_volume[dt_str] = daily_taker_volume.get(dt_str, 0) + v
            
        # Isolate Whale Strikes
        whales = taker_buys[taker_buys['quote_qty'] >= whale_threshold]
        
        if len(whales) > 0:
            for idx, whale_row in whales.iterrows():
                whale_day_str = whale_row['day'].strftime('%Y-%m-%d')
                
                # [IPO NOISE ELIMINATION] Skip first 48 hours of a new listing
                if whale_row['time'] < listing_start_time + pd.Timedelta(hours=skip_window_hours):
                    continue

                # Fetch rolling history ONLY prior to this exact day (Zero future knowledge bias)
                history = [vol for d, vol in daily_taker_volume.items() if d < whale_day_str]
                
                if len(history) < warmup_days_required: 
                    # We skip the very first few days to get at least a minimal baseline
                    continue
                    
                mean_vol = np.mean(history[-30:]) 
                std_vol = np.std(history[-30:])
                if std_vol == 0: continue
                
                # What is the Z-score of TODAY'S volume at this exact millisecond?
                current_today_vol = daily_taker_volume[whale_day_str]
                current_z = (current_today_vol - mean_vol) / std_vol
                
                if current_z >= 3.0:
                    # --- [STAGE 3]: VACUUM EMULATION (Slippage Proxy) ---
                    # The Z-Score spiked. But is the asking wall completely hollow?
                    # We check the price action within this EXACT ONE SECOND.
                    
                    target_sec = whale_row['sec']
                    sec_trades = chunk[chunk['sec'] == target_sec]
                    if len(sec_trades) < 2: continue
                    
                    p_start = sec_trades['price'].iloc[0]
                    p_end = sec_trades['price'].iloc[-1]
                    
                    # Calculate Warp (Slippage %)
                    # Formula: ((Max_Price_in_Sec - Min_Price_in_Sec) / Min_Price_in_Sec) * 100
                    p_max = sec_trades['price'].max()
                    p_min = sec_trades['price'].min()
                    
                    slippage = ((p_max - p_min) / p_min) * 100
                    
                    # Slippage >= 0.3% inside a single second means NO RESISTANCE EXISTED.
                    if slippage >= 0.3:
                        
                        # Prevent logging thousands of signals for the exact same second
                        if not signals or (whale_row['time'] - signals[-1]['time']).total_seconds() > 3600:
                            print(f"[GOD SIGNAL FIRED] {whale_row['time']} | Z-Score: {current_z:.2f} | Buy: ${whale_row['quote_qty']:,.0f} | Vacuum Warp: +{slippage:.3f}%", flush=True)
                            signals.append({
                                'time': whale_row['time'],
                                'price': whale_row['price'],
                                'z_score': current_z,
                                'warp': slippage
                            })
                        
        total_trades += len(chunk)
        elapsed = time.time() - start_time
        print(f"   ... Time Ticked: {total_trades:,} events processed ({(total_trades/elapsed):,.0f} ticks/sec) ...", flush=True)
        
    print(f"\\n--- Time Slip Execution Complete for {symbol}!", flush=True)
    print(f"--- Total Past Events Simluated: {total_trades:,}", flush=True)
    print(f"--- Total Flawless 'God Signals' Detected: {len(signals)}", flush=True)
    for s in signals:
         print(f"   -> TARGET TRIGGERED AT {s['time']} @ Price {s['price']:.6f} (Warp +{s['warp']:.2f}%)", flush=True)

if __name__ == '__main__':
    # Usage: python core/timeslip_backtester.py SOONUSDT
    target_symbol = "SOONUSDT"
    if len(sys.argv) > 1:
        target_symbol = sys.argv[1]
        
    run_timeslip(target_symbol)
