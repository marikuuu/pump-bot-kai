import os
import json
from datetime import datetime

def check():
    with open('multiverse_candidates.json', 'r') as f:
        cand = json.load(f)
    
    ticks = os.listdir('data/history/multiverse/ticks')
    
    print(f"Total Candidates: {len(cand)}")
    print(f"Total Ticks: {len(ticks)}")
    
    c = cand[0]
    print(f"\nSample Candidate: {c['symbol']} @ {datetime.fromtimestamp(c['start_ts']/1000)}")
    
    # Filter ticks for this symbol
    base = c['symbol'].split('/')[0].lower()
    matches = [t for t in ticks if base in t.lower()]
    print(f"Tick files for {base}: {len(matches)}")
    if matches:
        print(f"First match: {matches[0]}")
        # Extract TS from filename: AERO_USDT_USDT_1774213740000_ticks.csv
        parts = matches[0].split('_')
        for p in parts:
            if len(p) == 13 and p.isdigit():
                file_ts = int(p)
                diff = abs(file_ts - c['start_ts'])
                print(f"Diff: {diff/1000/60:.1f} minutes")

if __name__ == '__main__':
    check()
