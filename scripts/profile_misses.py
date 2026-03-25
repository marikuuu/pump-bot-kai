import pandas as pd
import numpy as np
import os

def profile_pumps():
    base_dir = 'data/history/multiverse'
    results = []
    
    files = [f for f in os.listdir(base_dir) if f.endswith('_1m.csv')]
    print(f"Profiling {len(files)} files...")
    
    for f in files:
        try:
            df = pd.read_csv(os.path.join(base_dir, f))
            if len(df) < 4320: continue
            
            # 1. Define Stage 2 Metrics
            df['vol_ma'] = df['volume'].ewm(span=7200, adjust=False).mean()
            df['vol_std'] = df['volume'].rolling(7200, min_periods=60).std().replace(0, 1e-6)
            df['vol_z'] = (df['volume'] - df['vol_ma']) / df['vol_std']

            df['price_change'] = (df['close'] - df['open']).abs()
            df['pc_ma'] = df['price_change'].rolling(720, min_periods=60).mean()
            df['pc_std'] = df['price_change'].rolling(720, min_periods=60).std().replace(0, 1e-6)
            df['pc_z'] = (df['price_change'] - df['pc_ma']) / df['pc_std']

            # 2. Find True Pump Moment
            df['max_future_close'] = df['close'].rolling(4320, min_periods=1).max().shift(-4320)
            df['gain'] = (df['max_future_close'] - df['close']) / df['close']
            
            hits = df[df['gain'] >= 0.50]
            if not hits.empty:
                trigger_row = hits.iloc[0]
                ts = trigger_row['timestamp']
                
                # Check Z-scores in the 1 hour surrounding the trigger
                context = df[(df['timestamp'] >= ts - 3600000) & (df['timestamp'] <= ts + 3600000)]
                
                res = {
                    'symbol': f.replace('_1m.csv',''),
                    'max_vol_z': context['vol_z'].max(),
                    'max_pc_z': context['pc_z'].max(),
                    'ts': ts
                }
                results.append(res)
                print(f"Found Pump: {res['symbol']} | Z_Vol: {res['max_vol_z']:.2f} | Z_PC: {res['max_pc_z']:.2f}")
        except Exception as e:
            print(f"Error in {f}: {e}")
            
    return results

res = profile_pumps()
df_res = pd.DataFrame(res)
df_res.to_csv('profile_results.csv', index=False)
print("Saved profile_results.csv")
