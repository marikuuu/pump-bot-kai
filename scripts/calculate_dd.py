import pandas as pd
import glob
import os
from datetime import datetime, timezone

# ─── 定数 ──────────────────────────────────────────
START_TS = int(datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
END_TS   = int(datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
DATA_DIR = "data/history/multiverse"

def calculate_dd_for_top_signals():
    # Targets (The 14 signals we identified)
    targets = [
        ('AIN/USDT', '03/16 07:19'), ('AIN/USDT', '03/17 08:58'), ('G/USDT', '03/16 00:17'),
        ('JCT/USDT', '03/17 15:12'), ('ANIME/USDT', '03/17 00:10'), ('ENJ/USDT', '03/17 12:46'),
        ('IRYS/USDT', '03/19 06:49'), ('ARC/USDT', '03/18 17:23'), ('KAS/USDT', '03/16 05:43'),
        ('POLYX/USDT', '03/17 09:00'), ('B/USDT', '03/16 03:20'), ('BTR/USDT', '03/20 14:27'),
        ('ANKR/USDT', '03/20 20:28'), ('F/USDT', '03/17 00:15')
    ]
    
    results = []
    for symbol_pair, time_str in targets:
        symbol = symbol_pair.split('/')[0]
        file_path = os.path.join(DATA_DIR, f"{symbol}_USDT_USDT_1m.csv")
        if not os.path.exists(file_path): continue
        
        df = pd.read_csv(file_path)
        sig_dt = datetime.strptime(f"2026/{time_str}", "%Y/%m/%d %H:%M").replace(tzinfo=timezone.utc)
        sig_ts = int(sig_dt.timestamp() * 1000)
        
        # Entry row
        entry_rows = df[df['timestamp'] >= sig_ts]
        if entry_rows.empty: continue
        entry_price = float(entry_rows.iloc[0]['close'])
        
        # Future 3 days window
        future = df[(df['timestamp'] > sig_ts) & (df['timestamp'] <= sig_ts + 3 * 86400000)]
        if future.empty: continue
        
        # Max Gain & Max DD (Before or at the peak? Usually users want overall window DD)
        max_high = future['high'].max()
        # DD is the lowest low before hit the peak or overall? 
        # For pump bots, "Max DD during the 3-day hold" is standard.
        min_low = future['low'].min()
        
        gain = (max_high / entry_price - 1.0) * 100
        dd = (min_low / entry_price - 1.0) * 100
        
        results.append({
            'symbol': symbol_pair,
            'time': time_str,
            'gain%': round(gain, 1),
            'dd%': round(dd, 1)
        })
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    rdf = calculate_dd_for_top_signals()
    rdf['time_jst'] = rdf['time'].apply(lambda x: (datetime.strptime(f"2026/{x}", "%Y/%m/%d %H:%M") + pd.Timedelta(hours=9)).strftime('%m/%d %H:%M JST'))
    print("-" * 60)
    print("📈 FINAL REPORT: GAIN vs MAX DRAWDOWN (3-DAY WINDOW)")
    print("-" * 60)
    print(rdf[['symbol', 'time_jst', 'gain%', 'dd%']].to_string(index=False))
    print("-" * 60)
