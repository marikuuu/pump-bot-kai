import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class MultiverseFilter:
    def __init__(self):
        self.data_dir = 'data/history/multiverse'
        self.output_file = 'multiverse_candidates.json'
        
    def find_anomalies(self, symbol, df, vol_threshold=2.5, price_threshold=2.0):
        """
        Stage 1 & 2 Filtering: Find 1m windows with statistical spikes.
        """
        if len(df) < 60: return []
        
        # Calculate Z-Scores for Volume and Price Change using long-term baselines (Karbalaii 2025 finding)
        # Volume Baseline: 5-Day EWMA (7200 minutes) - using max available in 7-day dataset
        df['vol_ma'] = df['volume'].ewm(span=7200, adjust=False).mean()
        df['vol_std'] = df['volume'].rolling(7200, min_periods=60).std().replace(0, 1e-6)
        df['vol_z'] = (df['volume'] - df['vol_ma']) / df['vol_std'].fillna(1e-6)
        
        # Price Baseline: 12-Hour MA (720 minutes)
        df['price_change'] = (df['close'] - df['open']).abs()
        df['pc_ma'] = df['price_change'].rolling(720, min_periods=60).mean()
        df['pc_std'] = df['price_change'].rolling(720, min_periods=60).std().replace(0, 1e-6)
        df['pc_z'] = (df['price_change'] - df['pc_ma']) / df['pc_std'].fillna(1e-6)
        
        # Stage 2 Thresholds
        hits = df[(df['vol_z'] > vol_threshold) | (df['pc_z'] > price_threshold)].copy()
        
        # Grouping logic: if anomalies are within 30 mins, treat as one event
        events = []
        if not hits.empty:
            hits = hits.sort_values('timestamp')
            current_event = None
            
            for _, row in hits.iterrows():
                ts = int(row['timestamp'])
                if current_event is None or ts > current_event['end_ts'] + (30 * 60 * 1000):
                    # Start new event
                    if current_event: events.append(current_event)
                    current_event = {
                        'symbol': symbol,
                        'start_ts': ts - (15 * 60 * 1000), # Include 15m lead time
                        'end_ts': ts + (15 * 60 * 1000),
                        'peak_vol_z': float(row['vol_z']),
                        'peak_pc_z': float(row['pc_z'])
                    }
                else:
                    # Extend current event
                    current_event['end_ts'] = ts + (15 * 60 * 1000)
                    current_event['peak_vol_z'] = max(current_event['peak_vol_z'], float(row['vol_z']))
                    current_event['peak_pc_z'] = max(current_event['peak_pc_z'], float(row['pc_z']))
            
            if current_event: events.append(current_event)
            
        return events

    def run(self):
        files = [f for f in os.listdir(self.data_dir) if f.endswith('_1m.csv')]
        print(f"Grouping {len(files)} symbols into discrete events...")
        
        all_events = []
        for f in files:
            symbol = f.replace('_1m.csv', '').replace('_', '/')
            if '/' not in symbol: continue 
            # Fix symbol
            if symbol.count('/') == 2:
                parts = symbol.split('/')
                symbol = f"{parts[0]}/{parts[1]}:{parts[2]}"

            df = pd.read_csv(os.path.join(self.data_dir, f))
            events = self.find_anomalies(symbol, df)
            all_events.extend(events)
            if events:
                logging.info(f"GROUPS: {len(events)} events in {symbol}")

        with open(self.output_file, 'w') as f:
            json.dump(all_events, f, indent=2)
        print(f"DONE. Saved {len(all_events)} event groups to {self.output_file}")

if __name__ == "__main__":
    filter_engine = MultiverseFilter()
    filter_engine.run()
