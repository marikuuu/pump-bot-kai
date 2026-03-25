import pandas as pd
import numpy as np
import os
import json
import logging
from datetime import datetime, timedelta
import sys

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pump_ai.detector import PumpDetector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class MultiverseAnalyzer:
    def __init__(self):
        self.tick_dir = 'data/history/multiverse/ticks'
        self.ohlcv_dir = 'data/history/multiverse'
        self.candidates_file = 'multiverse_candidates.json'
        # Detector calibrated for 90%+ Precision (Adjusting for initial benchmark)
        self.detector = PumpDetector(thresholds={'std_rush': 5.0, 'oi_z': 2.0, 'vol_z': 2.0})
        
    def check_pump_outcome(self, symbol, start_ts, window_m=15, threshold=0.04):
        """
        Verify if a real pump occurred within window_m after start_ts.
        """
        clean = symbol.replace('/', '_').replace(':', '_').replace('__', '_')
        # Handle double underscore if any
        ohlcv_file = os.path.join(self.ohlcv_dir, f"{clean}_1m.csv")
        if not os.path.exists(ohlcv_file): return False, 0.0
        
        df = pd.read_csv(ohlcv_file)
        # Search for max price in next window_m
        future = df[df['timestamp'] >= start_ts].head(window_m)
        if future.empty: return False, 0.0
        
        start_price = future['open'].iloc[0]
        max_price = future['high'].max()
        gain = (max_price - start_price) / start_price
        
        return gain >= threshold, gain

    def run(self):
        if not os.path.exists(self.candidates_file): return
        with open(self.candidates_file, 'r') as f:
            candidates = json.load(f)
            
        print(f"Analyzing {len(candidates)} event candidates...")
        
        results = []
        # Filter to only those with tick data available
        processed = 0
        for c in candidates:
            symbol = c['symbol']
            start_ts = c['start_ts']
            
            clean = symbol.replace('/', '_').replace(':', '_')
            tick_file = os.path.join(self.tick_dir, f"{clean}_{start_ts}_ticks.csv")
            
            if not os.path.exists(tick_file): continue
            
            # 1. Run Detector
            trades = pd.read_csv(tick_file)
            if trades.empty: continue
            
            # Simple Rush Calculation for the event window
            trades['ts_dt'] = pd.to_datetime(trades['timestamp'], unit='ms')
            # Look at a 1m slice at the peak of the event
            # (In a real bot, this is continuous. Here we check the burst).
            trades['sec'] = trades['ts_dt'].dt.second
            rush = trades.groupby('sec').size().reindex(range(0, 60), fill_value=0).std()
            
            data = {
                'std_rush': rush,
                'vol_z': c['peak_vol_z'],
                'pc_z': c['peak_pc_z']
            }
            is_pump, score, stage = self.detector.check_event(data)
            
            # 2. Check Outcome
            outcome, max_gain = self.check_pump_outcome(symbol, start_ts)
            
            results.append({
                'symbol': symbol,
                'signal': is_pump,
                'outcome': outcome,
                'max_gain': max_gain,
                'rush': rush
            })
            processed += 1
            if processed % 100 == 0: print(f"Processed {processed}...")

        # 3. Stats
        df = pd.DataFrame(results)
        if df.empty:
            print("No data processed.")
            return

        tp = len(df[(df['signal'] == True) & (df['outcome'] == True)])
        fp = len(df[(df['signal'] == True) & (df['outcome'] == False)])
        fn = len(df[(df['signal'] == False) & (df['outcome'] == True)])
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        print("\n--- MULTIVERSE STRESS TEST (PHASE 1) ---")
        print(f"Total Events Analyzed: {len(df)}")
        print(f"True Positives  (TP): {tp}")
        print(f"False Positives (FP): {fp}")
        print(f"False Negatives (FN): {fn}")
        print(f"PRECISION: {precision:.2%}")
        print(f"RECALL:    {recall:.2%}")
        
        print(f"\nPEAK METRICS ACROSS MULTIVERSE:")
        print(f"Max Rush Observed: {df['rush'].max():.2f}")
        print(f"Max Gain Observed: {df['max_gain'].max():.2%}")
        
        # Save results
        df.to_csv('multiverse_results.csv', index=False)

if __name__ == "__main__":
    analyzer = MultiverseAnalyzer()
    analyzer.run()
