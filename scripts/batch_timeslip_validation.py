import pandas as pd
import os
import subprocess
import sys
from datetime import datetime

import sys
import os

if sys.stdout.encoding.lower() != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

# Correct import logic
sys.path.append(os.path.join(os.path.dirname(__file__), "."))
from binance_archive_downloader import get_binance_vision_data

def run_batch_validation():
    csv_path = "top_binance_pumpers.csv"
    out_dir = "data/binance_dna"
    
    if not os.path.exists(csv_path):
        print(f"❌ Could not find {csv_path}")
        return

    df = pd.read_csv(csv_path)
    
    # Symbols to clean and process
    results = []

    print("\n" + "="*60, flush=True)
    print("!!! STARTING GLOBAL BATCH TIME SLIP VALIDATION", flush=True)
    print("="*60 + "\n", flush=True)

    for _, row in df.iterrows():
        # Normalize symbol PIIPIN/USDT:USDT -> PIPPINUSDT
        raw_symbol = row['symbol']
        symbol = raw_symbol.split('/')[0] + "USDT"
        
        # Determine date range (Bottom to Peak, but let's extend it to get full context)
        # We start from bottom_date - 14 days for warmup if possible
        start_date = row['bottom_date']
        end_date = row['peak_date']
        
        print(f"--- [Target] {symbol} | Gain: {row['gain_x']:.2f}x", flush=True)
        print(f"   Range: {start_date} to {end_date}", flush=True)
        
        # 1. Ensure Data exists
        target_csv = f"{out_dir}/{symbol}/um/trades_tick.csv"
        if not os.path.exists(target_csv):
            print(f"   >>> Downloading UM Tick Data for {symbol}...", flush=True)
            # We fetch at least 30 days of data starting from bottom_date - 7 days
            try:
                # Calculate slightly earlier start date for warmup
                base_dt = datetime.strptime(start_date, '%Y-%m-%d')
                adjusted_start = (base_dt - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
                get_binance_vision_data(symbol, adjusted_start, end_date, out_dir, market_type="um")
            except Exception as e:
                print(f"   [Error] Download failed: {e}", flush=True)
                continue
        else:
            print(f"   /OK/ Data detected (Cached)", flush=True)

        # 2. Run Time Slip Backtest
        print(f"   ... Running Time Slip Backtest for {symbol}...", flush=True)
        try:
            # We call the backtester script as a subprocess to capture its output
            cmd = [sys.executable, "core/timeslip_backtester.py", symbol]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8')
            
            god_signals = []
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                if "[GOD SIGNAL FIRED]" in line:
                    print(f"      🔥 {line.strip()}", flush=True)
                    god_signals.append(line.strip())
                elif "--- Total Flawless 'God Signals' Detected:" in line:
                    print(f"      🏆 {line.strip()}", flush=True)
            
            process.wait()
            results.append({
                'symbol': symbol,
                'signals': len(god_signals),
                'gain': row['gain_x'],
                'status': 'SUCCESS' if process.returncode == 0 else 'FAILED'
            })
            
        except Exception as e:
            print(f"   [Error] Backtest execution error: {e}", flush=True)
            results.append({'symbol': symbol, 'signals': 0, 'gain': row['gain_x'], 'status': 'ERROR'})

    # 3. Final Summary Report
    print("\n" + "="*60, flush=True)
    print("📋 BATCH VALIDATION SUMMARY REPORT", flush=True)
    print("="*60, flush=True)
    summary_df = pd.DataFrame(results)
    print(summary_df.to_string(index=False), flush=True)
    
    if len(summary_df) > 0:
        valid_hits = summary_df[summary_df['signals'] > 0]
        accuracy = (len(valid_hits) / len(summary_df)) * 100
        print(f"\n✅ Logic Hit Rate: {accuracy:.1f}% across all pump symbols.", flush=True)
    print("="*60 + "\n", flush=True)

if __name__ == "__main__":
    run_batch_validation()
