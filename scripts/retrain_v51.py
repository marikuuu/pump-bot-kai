import ccxt
import pandas as pd
import pickle
from datetime import datetime, timezone, timedelta
import asyncio
import time

async def fix_labels_and_retrain():
    # 1. Load the "broken" training data
    df = pd.read_csv('tick_training_data.csv')
    print(f"Original labels count: {df['label'].value_counts().to_dict()}")
    
    exchange = ccxt.binance({'options': {'defaultType': 'swap'}})
    
    # 2. Re-verify EACH symbol's success (1.5x in 3 days)
    # This time, we use CCXT directly to be 100% sure of the TRUTH
    unique_events = df[['symbol', 'timestamp']].drop_duplicates()
    
    true_labels = {} # key: (symbol, timestamp), value: 1 or 0
    
    print(f"Re-verifying {len(unique_events)} occurrences across symbols...")
    
    for i, row in unique_events.iterrows():
        symbol = row['symbol'].split(':')[0]
        ts_ms = int(row['timestamp'])
        
        # Check if already verified for this symbol/time window (to save API calls)
        found_label = 0
        try:
            # 3 days window
            candles = exchange.fetch_ohlcv(symbol, timeframe='1h', since=ts_ms, limit=72)
            if candles:
                p0 = candles[0][1] # entry open
                max_p = max([c[2] for c in candles])
                gain = (max_p - p0) / p0
                if gain >= 0.45: # Using 1.45x as a safer "monster" threshold
                    found_label = 1
                    print(f"CORRECTED: {symbol} at {ts_ms} -> SUCCESS (Gain: {gain*100:.1f}%)")
            await asyncio.sleep(0.05)
        except Exception as e:
            print(f"Error checking {symbol}: {e}")
            
        true_labels[(row['symbol'], row['timestamp'])] = found_label

    # 3. Update the dataframe with REAL labels
    def update_label(r):
        return true_labels.get((r['symbol'], r['timestamp']), 0)
        
    df['label'] = df.apply(update_label, axis=1)
    print(f"New labels count: {df['label'].value_counts().to_dict()}")
    
    # Save the corrected data
    df.to_csv('tick_training_data_v51.csv', index=False)
    
    # 4. Retrain the model (v5.1 Diverse Engine)
    import xgboost as xgb
    FEATURES = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
    X = df[FEATURES].fillna(0)
    y = df['label']
    
    # Account for imbalance
    pos = (y == 1).sum()
    neg = (y == 0).sum()
    weight = neg / pos if pos > 0 else 1
    
    model = xgb.XGBClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, scale_pos_weight=weight, eval_metric='logloss')
    model.fit(X, y)
    
    with open('pump_ai/pump_model_v51_diverse.pkl', 'wb') as f:
        pickle.dump(model, f)
        
    print("Diverse Mode v5.1 retrained successfully.")
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(fix_labels_and_retrain())
