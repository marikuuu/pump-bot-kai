import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

def analyze_oi_dna(symbol_dir):
    ohlcv_path = f"{symbol_dir}/ohlcv_1m.csv"
    oi_path = f"{symbol_dir}/oi_history.csv"
    
    if not os.path.exists(ohlcv_path) or not os.path.exists(oi_path):
        return None

    # Load and clean
    df = pd.read_csv(ohlcv_path)
    oi_df = pd.read_csv(oi_path)
    
    # Merge on timestamp (Approximate to nearest minute if needed)
    df['dt'] = pd.to_datetime(df['ts'], unit='ms')
    oi_df['dt'] = pd.to_datetime(oi_df['timestamp'], unit='ms').dt.floor('min')
    
    # Group OI to 1m (since it might be 5m origin) and fill
    oi_df_1m = oi_df.groupby('dt')['openInterestAmount'].last().resample('1min').ffill()
    
    merged = pd.merge(df, oi_df_1m, on='dt', how='left').ffill()
    
    # --- DNA ANALYSIS ---
    # 1. Look for OI Divergence (OI increasing while Price is flat/down)
    merged['oi_pct'] = merged['openInterestAmount'].pct_change()
    merged['price_pct'] = merged['c'].pct_change()
    
    # Accumulation Score: High OI growth with low price movement
    merged['acc_score'] = (merged['oi_pct'] > 0.01) & (merged['price_pct'].abs() < 0.005)
    
    # 2. Identify the PRE-PUMP ZONE (6 hours before peak)
    peak_idx = merged['c'].idxmax()
    pre_pump = merged.iloc[max(0, peak_idx - 360) : peak_idx]
    
    total_acc_hits = pre_pump['acc_score'].sum()
    
    print(f"--- Analysis for {os.path.basename(symbol_dir)} ---")
    print(f"Pre-Pump Acc Hits (6h before peak): {total_acc_hits}")
    
    return total_acc_hits

def run_all_dna_analysis():
    base_dir = "data/naked_dna"
    dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    
    report = []
    for d in dirs:
        hits = analyze_oi_dna(d)
        if hits is not None:
            report.append({'symbol': os.path.basename(d), 'acc_hits': hits})
            
    rep_df = pd.DataFrame(report).sort_values('acc_hits', ascending=False)
    print("\n📊 NAKED DNA: OI ACCUMULATION SUMMARY")
    print(rep_df.to_string(index=False))

if __name__ == "__main__":
    run_all_dna_analysis()
