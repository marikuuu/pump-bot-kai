import pandas as pd
import os
import sys
import ccxt
import random
from datetime import datetime, timedelta
import subprocess
from concurrent.futures import ProcessPoolExecutor

# Add current dir to path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from scripts.binance_archive_downloader import get_binance_vision_data

def run_single_noise_test(symbol, test_date, out_dir):
    """
    Downloads data for 1 symbol for 1 day and runs the backtester.
    Returns the number of signals found.
    """
    symbol_clean = symbol.replace("/", "").replace(":USDT", "")
    target_csv = f"{out_dir}/{symbol_clean}/um/trades_tick.csv"
    
    # 1. Download 1 day of data
    try:
        if not os.path.exists(target_csv):
            # We only need 1 day for noise test
            get_binance_vision_data(symbol_clean, test_date, test_date, out_dir, market_type="um")
    except Exception as e:
        return {"symbol": symbol, "status": "DOWNLOAD_ERROR", "signals": 0, "error": str(e)}

    # 2. Run Backtest
    try:
        # Call backtester as subprocess to isolate memory and avoid collisions
        cmd = [sys.executable, "core/timeslip_backtester.py", symbol_clean]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
        
        output = result.stdout
        # Count [GOD SIGNAL FIRED]
        signal_count = output.count("[GOD SIGNAL FIRED]")
        
        return {
            "symbol": symbol,
            "status": "SUCCESS",
            "signals": signal_count,
            "date": test_date
        }
    except Exception as e:
        return {"symbol": symbol, "status": "BACKTEST_ERROR", "signals": 0, "error": str(e)}

def global_noise_stress_test():
    out_dir = "data/noise_test"
    os.makedirs(out_dir, exist_ok=True)
    
    # 1. Get all symbols
    print("📡 Fetching all Binance UM symbols...")
    exchange = ccxt.binance({'options': {'defaultType': 'future'}})
    markets = exchange.load_markets()
    all_symbols = [s for s in markets.keys() if '/USDT' in s and ':' in s]
    
    # Limit for safety in first run? User said "all symbols", so let's try to be efficient.
    # We will pick a "Safe Date" - Like 2 weeks ago Tuesday.
    test_date = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    
    print(f"🚀 Starting Global Noise Stress Test for {len(all_symbols)} symbols at date {test_date}")
    print(f"   Target: 0 False Positives on normal market noise.")
    
    results = []
    # Using a smaller pool size for Windows stability
    with ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_single_noise_test, sym, test_date, out_dir) for sym in all_symbols]
        
        completed = 0
        for f in futures:
            res = f.result()
            results.append(res)
            completed += 1
            if res['signals'] > 0:
                print(f"⚠️ [FALSE POSITIVE] {res['symbol']} fired {res['signals']} signals!")
            
            if completed % 10 == 0:
                print(f"📊 Progress: {completed}/{len(all_symbols)} processed.")

    # 3. Final Report
    df_res = pd.DataFrame(results)
    total_signals = df_res['signals'].sum()
    success_count = len(df_res[df_res['status'] == 'SUCCESS'])
    
    print("\n" + "="*60)
    print("🛡️ GLOBAL NOISE STRESS TEST SUMMARY")
    print("="*60)
    print(f"Total Symbols Tested: {len(all_symbols)}")
    print(f"Successfully Processed: {success_count}")
    print(f"Total False Positives: {total_signals}")
    
    fpr = (len(df_res[df_res['signals'] > 0]) / success_count) * 100 if success_count > 0 else 0
    print(f"False Positive Rate (FPR): {fpr:.4f}%")
    print("="*60 + "\n")
    
    df_res.to_csv("noise_test_report.csv", index=False)
    print("📝 Detailed report saved to noise_test_report.csv")

if __name__ == "__main__":
    global_noise_stress_test()
