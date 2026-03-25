import os
import json

def debug():
    with open('multiverse_candidates.json', 'r') as f:
        candidates = json.load(f)
    tick_dir = 'data/history/multiverse/ticks'
    all_ticks = os.listdir(tick_dir)
    print(f"Total candidates: {len(candidates)}")
    print(f"Total tick files: {len(all_ticks)}")

    for c in candidates[:100]:
        symbol = c['symbol']
        # The logic from train_multiverse_xgb.py
        clean = symbol.replace('/', '_').replace(':', '_')
        matches = [f for f in all_ticks if clean in f]
        if matches:
            print(f"MATCH: {symbol} (Clean: {clean}) -> {matches[0]}")
            return
    
    print("ZERO MATCHES in first 100")
    if candidates:
        print(f"Sample Candidate Symbol: {candidates[0]['symbol']}")
    if all_ticks:
        print(f"Sample Tick File: {all_ticks[0]}")

if __name__ == '__main__':
    debug()
