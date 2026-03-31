import pandas as pd
import os
import shutil
import time
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "."))

# Import our custom tools natively
from binance_archive_downloader import get_binance_vision_data
from naked_dna_analyzer_v4 import analyze_symbol

def run_batch_pipeline():
    print("🚀 === INITIALIZING UNIVERSAL DNA BATCH PIPELINE ===")
    
    csv_file = "top_binance_pumpers.csv"
    if not os.path.exists(csv_file):
        print(f"❌ Cannot find {csv_file}")
        return
        
    df = pd.read_csv(csv_file)
    # Get top 5
    top_targets = df.head(5)
    
    out_dir = "data/binance_dna"
    os.makedirs(out_dir, exist_ok=True)
    
    for idx, row in top_targets.iterrows():
        raw_symbol = row['symbol']
        if "/" in raw_symbol:
            symbol_clean = raw_symbol.split("/")[0] + "USDT" 
        else:
            symbol_clean = raw_symbol.replace(":USDT", "").replace(":", "")
        
        # We want to analyze from Bottom to Peak
        # Bottom date is the absolute low. We want to start a bit before the bottom if possible, but bottom is fine.
        start_date = row['bottom_date']
        
        # Peak date. Add 1 day to ensure full capture
        peak_dt = datetime.strptime(row['peak_date'], "%Y-%m-%d") + timedelta(days=1)
        end_date = peak_dt.strftime("%Y-%m-%d")
        
        gain = row['gain_x']
        
        print(f"\\n=======================================================")
        print(f"🎯 STARTING TARGET [{idx+1}/5]: {symbol_clean} ({gain:.1f}x Pumper)")
        print(f"📅 Period: {start_date} to {end_date}")
        print(f"=======================================================")
        
        # Step 1: Download the raw massive tick data (SPOT & FUTURES)
        try:
            get_binance_vision_data(symbol_clean, start_date, end_date, out_dir, market_type='spot')
            get_binance_vision_data(symbol_clean, start_date, end_date, out_dir, market_type='um')
        except Exception as e:
            print(f"❌ Failed downloading {symbol_clean}: {e}")
            continue
            
        # Step 2: Run the Z-Score Naked DNA Analyzer
        try:
            analyze_symbol(symbol_clean)
        except Exception as e:
            print(f"❌ Failed analyzing {symbol_clean}: {e}")
            continue
            
        # Step 3: Cleanup Massive CSVs to avoid breaking the user's hard drive
        print(f"\\n🧹 Cleaning up massive raw CSVs for {symbol_clean}...")
        for mt in ['spot', 'um']:
            ticks_path = os.path.join(out_dir, symbol_clean, mt, "trades_tick.csv")
            klines_path = os.path.join(out_dir, symbol_clean, mt, "ohlcv_1m.csv")
            
            if os.path.exists(ticks_path):
                os.remove(ticks_path)
                print(f"   ✓ Deleted {ticks_path}")
            if os.path.exists(klines_path):
                os.remove(klines_path)
                print(f"   ✓ Deleted {klines_path}")
            
        print(f"✅ Target {symbol_clean} completed! Moving to next...\\n")
        time.sleep(3)
        
    print("🎉 ALL TARGETS PROCESSED. Check data/binance_dna/ for the Z-Score validation images.")

if __name__ == "__main__":
    run_batch_pipeline()
