import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

# Optional: Disable matplotlib GUI showing for server scripts
import matplotlib
matplotlib.use('Agg')

def analyze_v3_dna(symbol_dir):
    ohlcv_path = f"{symbol_dir}/ohlcv_1m.csv"
    if not os.path.exists(ohlcv_path):
        return None

    df = pd.read_csv(ohlcv_path)
    df['dt'] = pd.to_datetime(df['ts'], unit='ms')
    
    symbol_name = os.path.basename(symbol_dir)
    print(f"\n🧬 Analyzing DNA for {symbol_name} (Data points: {len(df)})")
    
    # 1. Identify Peak and Bottom within this dataset
    peak_idx = df['c'].idxmax()
    peak_price = df.iloc[peak_idx]['c']
    peak_dt = df.iloc[peak_idx]['dt']
    
    # Bottom before peak
    pre_peak_df = df.iloc[:peak_idx+1]
    bottom_idx = pre_peak_df['l'].idxmin()
    bottom_price = df.iloc[bottom_idx]['l']
    bottom_dt = df.iloc[bottom_idx]['dt']
    
    gain_x = peak_price / bottom_price
    days_to_peak = (peak_dt - bottom_dt).total_seconds() / (24 * 3600)
    
    print(f"  Bottom: {bottom_price:.6f} at {bottom_dt}")
    print(f"  Peak:   {peak_price:.6f} at {peak_dt}")
    print(f"  Performance: {gain_x:.2f}x over {days_to_peak:.1f} days")
    
    # 2. Extract the Accumulation Phase (From bottom until breaking out of bottom range)
    # Let's say bottom range is bottom_price + 20%
    bottom_ceiling = bottom_price * 1.20
    
    # 3. VPVR (Volume Profile Visible Range) Calculation over the Pre-Peak period
    # Let's use 50 bins for the VPVR
    price_bins = np.linspace(pre_peak_df['l'].min(), pre_peak_df['h'].max(), 50)
    hist, _ = np.histogram(pre_peak_df['c'], bins=price_bins, weights=pre_peak_df['v'])
    
    poc_idx = np.argmax(hist)
    poc_price = (price_bins[poc_idx] + price_bins[min(len(price_bins)-1, poc_idx+1)]) / 2
    
    # Check if POC is near the bottom (Rock Bottom Accumulation)
    is_rock_bottom_poc = poc_price <= bottom_ceiling
    
    print(f"  VPVR POC (Rock Bottom Core): {poc_price:.6f} (Volume: {hist[poc_idx]:.0f})")
    if is_rock_bottom_poc:
        print("  🪨 STRONG ROCK BOTTOM DETECTED: Most volume traded near the absolute bottom.")
        
    # 4. Vacuum State Analysis (Is the area above POC frictionless?)
    # We look at bins above POC. If volume is extremely low compared to POC, it's a vacuum.
    vacuum_zones = []
    for i in range(poc_idx + 1, len(hist) - 5): # Exclude the very top peak bins
        if hist[i] < hist[poc_idx] * 0.1: # Less than 10% of POC volume
            vacuum_zones.append(price_bins[i])
            
    has_vacuum = len(vacuum_zones) > (len(hist) * 0.2) # If 20% of the price range is a vacuum
    print(f"  🌌 Vacuum Range Detected: {'YES' if has_vacuum else 'NO'} ({len(vacuum_zones)} price tiers empty)")
    
    # 5. Visual Output
    os.makedirs('analysis/v3_dna', exist_ok=True)
    out_img = f"analysis/v3_dna/{symbol_name}_dna.png"
    
    plt.figure(figsize=(14, 8))
    
    # Plot 1: Chart
    ax1 = plt.subplot2grid((1, 4), (0, 0), colspan=3)
    # Sub-sample for plotting speed (resample to 1H or 4H)
    df_plot = df.set_index('dt').resample('4h').agg({'o':'first','h':'max','l':'min','c':'last','v':'sum'}).dropna()
    ax1.plot(df_plot.index, df_plot['c'], color='cyan', linewidth=1.5)
    ax1.axhline(poc_price, color='magenta', linestyle='--', alpha=0.8, label='POC (Rock Bottom)')
    ax1.scatter([bottom_dt, peak_dt], [bottom_price, peak_price], color='yellow', zorder=5)
    ax1.set_title(f"{symbol_name} - Journey to {gain_x:.1f}x")
    ax1.set_facecolor('#111111')
    ax1.legend()
    
    # Plot 2: VPVR
    ax2 = plt.subplot2grid((1, 4), (0, 3), sharey=ax1)
    ax2.barh(price_bins[:-1], hist, height=(price_bins[1]-price_bins[0])*0.8, color='orange', alpha=0.7)
    ax2.axhline(poc_price, color='magenta', linestyle='--', alpha=0.8)
    ax2.set_facecolor('#111111')
    ax2.set_title("VPVR DNA")
    
    plt.tight_layout()
    plt.savefig(out_img, facecolor='#222222', dpi=100)
    plt.close()
    
    print(f"  📸 Saved DNA Fingerprint -> {out_img}")
    
    return {
        'symbol': symbol_name,
        'gain_x': gain_x,
        'days_to_peak': days_to_peak,
        'rock_bottom_poc': is_rock_bottom_poc,
        'vacuum_breakout': has_vacuum
    }

def main():
    base_dir = "data/naked_dna_long"
    if not os.path.exists(base_dir):
        print(f"Directory {base_dir} does not exist.")
        return
        
    dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    
    results = []
    for d in dirs:
        res = analyze_v3_dna(d)
        if res:
            results.append(res)
            
    if results:
        res_df = pd.DataFrame(results).sort_values('gain_x', ascending=False)
        print("\n\n=== 👑 SUMMARY OF PREDICTABILITY ===")
        print(res_df.to_string(index=False))

if __name__ == "__main__":
    main()
