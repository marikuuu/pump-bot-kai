import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "."))

from binance_archive_downloader import get_binance_vision_data
from naked_dna_analyzer_v4 import analyze_symbol

def run_soon_dual():
    symbol = "SOONUSDT"
    start_date = "2025-10-01"
    end_date = "2025-11-15"
    out_dir = "data/binance_dna"
    
    print("\\n=======================================================")
    print("🎯 STARTING DUAL-MARKET DNA EXTRACTION: $SOON (49x)")
    print("=======================================================")
    
    # 1. Spot Download
    print("\\n[Target] Downloading SPOT Data for SOON...")
    try:
        get_binance_vision_data(symbol, start_date, end_date, out_dir, market_type="spot")
    except Exception as e:
        print(f"Spot logic failed: {e}")
        
    # 2. UM Download (Already cached 70 million rows in previous session to avoid 10 min downloading)
    print("\\n[Target] Using Cached FUTURES Data for SOON...")
    # try:
    #     get_binance_vision_data(symbol, start_date, end_date, out_dir, market_type="um")
    # except Exception as e:
    #     print(f"UM logic failed: {e}")
        
    # 3. Analyze Spot vs Futures
    analyze_symbol(symbol)

if __name__ == "__main__":
    run_soon_dual()
