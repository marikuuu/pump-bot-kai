import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def analyze_cross_ex(binance_file, bybit_file, pump_time_ms):
    """
    Analyzes price correlation and volume rushes around a specific event.
    """
    print(f"Loading Binance data: {binance_file}")
    df_bin = pd.read_csv(binance_file)
    # Ensure correct column assignment if header is missing or different
    # Check if 'transact_time' exists, else use index
    if 'transact_time' not in df_bin.columns:
        print("Error: transact_time column missing in Binance data")
        return

    print(f"Loading Bybit data: {bybit_file}")
    df_byb = pd.read_csv(bybit_file)
    df_byb['transact_time'] = (df_byb['timestamp'] * 1000).astype(np.int64)

    print(f"Binance Timestamp Range: {df_bin['transact_time'].min()} to {df_bin['transact_time'].max()}")
    print(f"Bybit   Timestamp Range: {df_byb['transact_time'].min()} to {df_byb['transact_time'].max()}")

    # Filter window: pump_time_ms +/- 10 minutes (600,000 ms)
    window = 600000 
    start_t = pump_time_ms - window
    end_t = pump_time_ms + window

    df_bin_w = df_bin[(df_bin['transact_time'] >= start_t) & (df_bin['transact_time'] <= end_t)].copy()
    df_byb_w = df_byb[(df_byb['transact_time'] >= start_t) & (df_byb['transact_time'] <= end_t)].copy()

    if df_bin_w.empty or df_byb_w.empty:
        print("Warning: Data empty in the specified window.")
        print(f"Binance range: {df_bin['transact_time'].min()} to {df_bin['transact_time'].max()}")
        print(f"Bybit range: {df_byb['transact_time'].min()} to {df_byb['transact_time'].max()}")
        return

    # Normalize prices to % change from first tick in window
    p0_bin = df_bin_w.iloc[0]['price']
    p0_byb = df_byb_w.iloc[0]['price']
    df_bin_w['price_pct'] = (df_bin_w['price'] / p0_bin - 1) * 100
    df_byb_w['price_pct'] = (df_byb_w['price'] / p0_byb - 1) * 100

    # Calculate Volume Acceleration
    # Define trigger: price > 0.3% in window
    trigger_bin = df_bin_w[df_bin_w['price_pct'].abs() >= 0.3].iloc[0] if not df_bin_w[df_bin_w['price_pct'].abs() >= 0.3].empty else None
    trigger_byb = df_byb_w[df_byb_w['price_pct'].abs() >= 0.3].iloc[0] if not df_byb_w[df_byb_w['price_pct'].abs() >= 0.3].empty else None

    print("\n--- Cross-Exchange Analysis Result (BTC) ---")
    if trigger_bin is not None:
        print(f"Binance 0.3% Signal: {trigger_bin['transact_time']}")
    if trigger_byb is not None:
        print(f"Bybit   0.3% Signal: {trigger_byb['transact_time']}")
    
    if trigger_bin is not None and trigger_byb is not None:
        diff = trigger_byb['transact_time'] - trigger_bin['transact_time']
        if diff > 0:
            print(f"🔥 LEAD-LAG: Binance LED Bybit by {diff} ms")
        elif diff < 0:
            print(f"🔥 LEAD-LAG: Bybit LED Binance by {abs(diff)} ms")
        else:
            print("🚀 Perfect sync detected.")

    # Visualization (Saved to file)
    plt.figure(figsize=(12, 6))
    plt.plot(df_bin_w['transact_time'], df_bin_w['price_pct'], label='Binance', color='orange', alpha=0.7)
    plt.plot(df_byb_w['transact_time'], df_byb_w['price_pct'], label='Bybit', color='cyan', alpha=0.7)
    plt.axhline(0.3, color='red', linestyle='--', alpha=0.3, label='0.3% Threshold')
    plt.axhline(-0.3, color='red', linestyle='--', alpha=0.3)
    plt.title(f"Cross-Exchange DNA Analysis: BTC (Window around {pump_time_ms})")
    plt.xlabel("Timestamp (ms)")
    plt.ylabel("Price Change %")
    plt.legend()
    plt.grid(True)
    plt.savefig("cross_ex_analysis_btc.png")
    print(f"Analysis plot saved to cross_ex_analysis_btc.png")

if __name__ == "__main__":
    # BTC Volatility Window at 1774137060000
    analyze_cross_ex(
        "data/BTCUSDT_binance_2026-03-21.csv",
        "data/BTCUSDT_2026-03-21.csv",
        1774137060000
    )
