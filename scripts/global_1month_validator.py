import pandas as pd
import os
import sys
import ccxt
import shutil
from datetime import datetime, timedelta
import subprocess

# Add current dir to path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from scripts.binance_archive_downloader import get_binance_vision_data

def run_single_month_test(symbol, start_date, end_date, out_dir):
    """
    Downloads data for 1 symbol for 1 month and runs the backtester.
    Deletes data immediately after to save disk space.
    """
    symbol_clean = symbol.replace("/", "").replace(":USDT", "")
    target_dir = os.path.join(out_dir, symbol_clean)
    target_csv = os.path.join(target_dir, "um", "trades_tick.csv")
    
    # 1. Download 1 month of data
    try:
        if not os.path.exists(target_csv):
            print(f"\n📡 [1/3] Downloading 30 days for {symbol_clean} ({start_date} to {end_date})...")
            get_binance_vision_data(symbol_clean, start_date, end_date, out_dir, market_type="um")
    except Exception as e:
        return {"symbol": symbol, "status": "DOWNLOAD_ERROR", "signals": 0, "error": str(e)}

    # 2. Run Backtest
    try:
        print(f"⚙️ [2/3] Running Time Slip Backtest for {symbol_clean}...")
        # Call backtester as subprocess to isolate memory
        cmd = [sys.executable, "core/timeslip_backtester.py", symbol_clean]
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
        
        output = result.stdout
        signal_count = output.count("[GOD SIGNAL FIRED]")
        
        # 3. Cleanup (CRITICAL for 16GB disk limit)
        print(f"🧹 [3/3] Cleaning up data for {symbol_clean} to free space...")
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
            
        return {
            "symbol": symbol,
            "status": "SUCCESS",
            "signals": signal_count,
            "period": f"{start_date} to {end_date}"
        }
    except Exception as e:
        # Still try to cleanup on failure
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        return {"symbol": symbol, "status": "BACKTEST_ERROR", "signals": 0, "error": str(e)}

def global_1month_validator():
    out_dir = "data/temp_1month" # Using a dedicated temp dir
    os.makedirs(out_dir, exist_ok=True)
    report_file = "global_1month_report.csv"
    
    # 1. Get all symbols
    print("📡 Fetching all Binance UM symbols...")
    exchange = ccxt.binance({'options': {'defaultType': 'future'}})
    markets = exchange.load_markets()
    all_symbols = [s for s in markets.keys() if '/USDT' in s and ':' in s]
    
    # [NEW] Skip Top 50 High-Cap/Volume symbols
    print("🧹 Filtering out Top 50 High-Cap symbols (BTC, ETH, etc.) to focus on Mid/Small Caps...")
    tickers = exchange.fetch_tickers()
    # Sort by 24h Quote Volume as proxy for Market Cap
    sorted_tickers = sorted(tickers.items(), key=lambda x: x[1]['quoteVolume'] if x[1]['quoteVolume'] else 0, reverse=True)
    top_50_names = [t[0].replace("/", "").split(":")[0] for t in sorted_tickers[:50]]
    
    filtered_symbols = []
    for s in all_symbols:
        s_clean = s.replace("/", "").split(":")[0]
        if s_clean in top_50_names:
            continue
        filtered_symbols.append(s)
    
    all_symbols = filtered_symbols
    print(f"✅ Filtered down to {len(all_symbols)} targets (Skip Top 50).")
    
    # Define 1 month ago
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")
    
    print(f"🚀 Starting 1-Month Global Validation for {len(all_symbols)} symbols")
    print(f"   Period: {start_date} to {end_date}")
    print(f"   Mode: Sequential with Auto-Cleanup (Disk Safe)")
    
    results = []
    
    # Process sequentially to avoid disk overflow
    for i, symbol in enumerate(all_symbols):
        res = run_single_month_test(symbol, start_date, end_date, out_dir)
        results.append(res)
        
        # Save intermediate report to avoid losing data if script crashes
        pd.DataFrame(results).to_csv(report_file, index=False)
        
        if res['signals'] > 0:
            print(f"⚠️ [SIGNAL FOUND] {res['symbol']} triggered {res['signals']} times!")
        
        print(f"📊 Progress: {i+1}/{len(all_symbols)} processed.")

    print("\n" + "="*60)
    print("🛡️ 1-MONTH GLOBAL VALIDATION COMPLETE")
    print("="*60)
    print(f"Total Symbols Tested: {len(all_symbols)}")
    print(f"Report saved to {report_file}")
    print("="*60 + "\n")

if __name__ == "__main__":
    global_1month_validator()
