import json
import pandas as pd
import os
from datetime import datetime

def find_true_pumps():
    true_pumps = []
    base_dir = 'data/history/multiverse'
    for f in os.listdir(base_dir):
        if not f.endswith('_1m.csv'): continue
        df = pd.read_csv(os.path.join(base_dir, f))
        if len(df) < 4320: continue
        
        # We look for a >=50% gain in close price within 3 days (4320 mins)
        df['max_future_close'] = df['close'].rolling(4320, min_periods=1).max().shift(-4320)
        df['gain'] = (df['max_future_close'] - df['close']) / df['close']
        
        hits = df[df['gain'] >= 0.50]
        if not hits.empty:
            # We take the first moment it became a 'pre-pump' candidate
            first_hit = hits.iloc[0]
            true_pumps.append({
                'symbol': f.replace('_1m.csv','').replace('_','/'),
                'ts': int(first_hit['timestamp']),
                'gain': float(first_hit['gain'])
            })
    return true_pumps

def main():
    true_pumps = find_true_pumps()
    print(f"Total True 1.5x Pumps (Close): {len(true_pumps)}")
    
    with open('multiverse_candidates.json', 'r') as f:
        candidates = json.load(f)
    print(f"Total Stage 2 Candidates: {len(candidates)}")

    missing = []
    found = 0
    for tp in true_pumps:
        # Check if any candidate for this symbol exists around the same time (+/- 2 hours)
        # Normalize symbol name (e.g. BAND/USDT vs BAND/USDT:USDT)
        tp_sym = tp['symbol'].split('/')[0]
        
        matches = []
        for c in candidates:
            c_sym = c['symbol'].split('/')[0]
            if tp_sym == c_sym:
                if abs(c['start_ts'] - tp['ts']) < 7200000: # 2 hour window
                    matches.append(c)
        
        if matches:
            found += 1
        else:
            missing.append(tp)

    print(f"\n--- RECALL SUMMARY ---")
    print(f"Pumps reaching Stage 3: {found}/{len(true_pumps)} ({found/len(true_pumps):.1%})")
    
    if missing:
        print("\n--- MISSING PUMPS (FILTERED OUT BY STAGE 2) ---")
        for m in missing:
            print(f"Symbol: {m['symbol']} | TS: {m['ts']} | Gain: {m['gain']:.2%}")

if __name__ == '__main__':
    main()
