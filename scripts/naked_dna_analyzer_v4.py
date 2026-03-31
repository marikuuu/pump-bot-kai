import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

def process_klines_for_vpvr(klines_path, bins=100):
    print(f"--- 📚 Parsing 1m Klines for VPVR ({klines_path}) ---")
    df = pd.read_csv(klines_path)
    for col in ['high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=['close'], inplace=True)
    
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    min_p = df['low'].min()
    max_p = df['high'].max()
    if pd.isna(min_p) or pd.isna(max_p):
        return None, None, None
        
    price_bins = np.linspace(min_p, max_p, bins)
    vpvr = np.zeros(bins - 1)
    
    for idx, row in df.iterrows():
        bin_idx = np.searchsorted(price_bins, row['typical_price']) - 1
        if bin_idx >= bins - 1: bin_idx = bins - 2
        if bin_idx < 0: bin_idx = 0
        vpvr[bin_idx] += row['volume']
        
    poc_idx = np.argmax(vpvr)
    poc_price = (price_bins[poc_idx] + price_bins[poc_idx+1]) / 2
    return price_bins[:-1], vpvr, poc_price

def process_ticks_for_whale_agression(ticks_path, chunk_size=1000000):
    print(f"--- 🐳 Parsing Tick Data for Whale Z-Score ({ticks_path}) ---")
    whale_threshold_usd = 10000
    hourly_whale_buys = {}
    
    chunk_iter = pd.read_csv(ticks_path, chunksize=chunk_size, 
                             names=['id','price','qty','quote_qty','time','is_buyer_maker'],
                             on_bad_lines='skip', header=0)
    count = 0
    for chunk in chunk_iter:
        chunk['time'] = pd.to_numeric(chunk['time'], errors='coerce')
        chunk.dropna(subset=['time'], inplace=True)
        chunk['hour'] = pd.to_datetime(chunk['time'], unit='ms').dt.floor('h')
        
        chunk['is_buyer_maker'] = chunk['is_buyer_maker'].astype(str).str.lower().map({'true': True, 'false': False, '1': True, '0': False}).fillna(False).astype(bool)
        chunk['quote_qty'] = pd.to_numeric(chunk['quote_qty'], errors='coerce')
        
        whales = chunk[(~chunk['is_buyer_maker']) & (chunk['quote_qty'] >= whale_threshold_usd)]
        whale_buys = whales.groupby('hour')['quote_qty'].sum()
        for hr, val in whale_buys.items():
            if hr not in hourly_whale_buys: hourly_whale_buys[hr] = 0
            hourly_whale_buys[hr] += val
            
        count += len(chunk)
    
    df_aggr = pd.DataFrame({'whale_buy_vol': pd.Series(hourly_whale_buys)}).fillna(0).sort_index()
    if df_aggr.empty:
        return None
        
    df_daily = df_aggr.resample('D').sum()
    
    mean_30d = df_daily['whale_buy_vol'].rolling(window=30, min_periods=3).mean()
    std_30d = df_daily['whale_buy_vol'].rolling(window=30, min_periods=3).std()
    
    df_daily['whale_buy_zscore'] = (df_daily['whale_buy_vol'] - mean_30d) / std_30d
    df_daily['whale_buy_zscore'] = df_daily['whale_buy_zscore'].replace([np.inf, -np.inf], 0).fillna(0)
    
    return df_daily


def plot_dual_naked_dna(symbol, base_dir, spot_k, spot_pb, spot_vpvr, spot_poc, spot_df, 
                        um_k, um_pb, um_vpvr, um_poc, um_df):
                            
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 14), gridspec_kw={'height_ratios': [2, 1]})
    
    main_k = um_k if um_k else spot_k
    df_k = pd.read_csv(main_k)
    df_k['open_time'] = pd.to_numeric(df_k['open_time'], errors='coerce')
    df_k.dropna(subset=['open_time'], inplace=True)
    df_k['time'] = pd.to_datetime(df_k['open_time'], unit='ms')
    
    ax1.plot(df_k['time'], df_k['close'], color='black', linewidth=1, label=f'Price ({ "Futures" if um_k else "Spot" })')
    ax1.set_ylabel('Price', color='black')
    
    ax1_vpvr = ax1.twiny()
    
    if um_pb is not None and um_vpvr is not None:
        ax1_vpvr.barh(um_pb, um_vpvr, height=(um_pb[1]-um_pb[0])*0.8, color='red', alpha=0.3, align='center', label='FUTURES VPVR')
        ax1_vpvr.axhline(um_poc, color='red', linestyle='--', linewidth=2, label=f'Futures POC: {um_poc:.4f}')
        
    if spot_pb is not None and spot_vpvr is not None:
        ax1_vpvr.barh(spot_pb, spot_vpvr, height=(spot_pb[1]-spot_pb[0])*0.8, color='blue', alpha=0.5, align='center', label='SPOT VPVR')
        ax1_vpvr.axhline(spot_poc, color='blue', linestyle='--', linewidth=2, label=f'Spot POC: {spot_poc:.4f}')

    ax1.set_title(f"[{symbol}] Cross-Market Whale DNA (Spot vs Futures)", fontsize=18, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1_vpvr.legend(loc='lower right')
    ax1.legend(loc='upper left')
    
    has_df = False
    
    if spot_df is not None and um_df is not None:
        idx = spot_df.index.union(um_df.index)
        spot_df = spot_df.reindex(idx).fillna(0)
        um_df = um_df.reindex(idx).fillna(0)
        has_df = True
    elif spot_df is not None:
        idx = spot_df.index
        um_df = spot_df.copy()
        um_df['whale_buy_zscore'] = 0
        has_df = True
    elif um_df is not None:
        idx = um_df.index
        spot_df = um_df.copy()
        spot_df['whale_buy_zscore'] = 0
        has_df = True

    if has_df:
        width = 0.4
        x = np.arange(len(idx))
        
        ax2.bar(x - width/2, spot_df['whale_buy_zscore'], width, color='blue', alpha=0.6, label='SPOT Whale Z-Score')
        ax2.bar(x + width/2, um_df['whale_buy_zscore'], width, color='red', alpha=0.6, label='FUTURES Whale Z-Score')
        
        # Highlight extreme anomalies (> 3 Sigma)
        for i, (date, row) in enumerate(spot_df.iterrows()):
            if row['whale_buy_zscore'] >= 3.0:
                ax2.bar(x[i] - width/2, row['whale_buy_zscore'], width, color='cyan', alpha=0.9, edgecolor='black')
        for i, (date, row) in enumerate(um_df.iterrows()):
            if row['whale_buy_zscore'] >= 3.0:
                ax2.bar(x[i] + width/2, row['whale_buy_zscore'], width, color='orange', alpha=0.9, edgecolor='black')
                
        ax2.set_xticks(x[::max(1, len(idx)//20)])
        ax2.set_xticklabels([d.strftime('%m-%d') for d in idx[::max(1, len(idx)//20)]], rotation=45)
    
    ax2.axhline(3.0, color='black', linestyle=':', linewidth=2, label='3-Sigma Threshold (Authentic Signal)')
    ax2.set_ylabel('Whale Taker Buy (Z-Score)')
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc='upper left')
    
    plt.tight_layout()
    out_img = os.path.join(base_dir, f"{symbol}_cross_market_dna.png")
    plt.savefig(out_img, dpi=150)
    print(f"\\n🏆 Dual-Market DNA Signature Image saved to {out_img}")

def analyze_symbol(symbol):
    print(f"\\n=== BEGNNING CROSS-MARKET NAKED DNA ANALYSIS v4: {symbol} ===")
    base_dir = f"data/binance_dna/{symbol}"
    
    spot_klines = os.path.join(base_dir, "spot", "ohlcv_1m.csv")
    spot_ticks = os.path.join(base_dir, "spot", "trades_tick.csv")
    um_klines = os.path.join(base_dir, "um", "ohlcv_1m.csv")
    um_ticks = os.path.join(base_dir, "um", "trades_tick.csv")
    
    spot_pb, spot_vpvr, spot_poc, spot_df = None, None, None, None
    um_pb, um_vpvr, um_poc, um_df = None, None, None, None
    
    if os.path.exists(spot_klines) and os.path.exists(spot_ticks):
        print("\\n[SPOT] Processing...")
        spot_pb, spot_vpvr, spot_poc = process_klines_for_vpvr(spot_klines)
        spot_df = process_ticks_for_whale_agression(spot_ticks)
    else:
        print("⚠️ [SPOT] Data not found.")
        
    if os.path.exists(um_klines) and os.path.exists(um_ticks):
        print("\\n[FUTURES] Processing...")
        um_pb, um_vpvr, um_poc = process_klines_for_vpvr(um_klines)
        um_df = process_ticks_for_whale_agression(um_ticks)
    else:
        print("⚠️ [FUTURES] Data not found.")
        
    if (spot_pb is None) and (um_pb is None):
        print("❌ No data found at all for this symbol.")
        return
        
    plot_dual_naked_dna(symbol, base_dir,
                        spot_klines if os.path.exists(spot_klines) else None, spot_pb, spot_vpvr, spot_poc, spot_df,
                        um_klines if os.path.exists(um_klines) else None, um_pb, um_vpvr, um_poc, um_df)

if __name__ == "__main__":
    analyze_symbol("SOONUSDT")
