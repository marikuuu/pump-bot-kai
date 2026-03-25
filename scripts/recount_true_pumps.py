import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor

def analyze_symbol(filename):
    df = pd.read_csv(os.path.join('data/history/multiverse', filename))
    if len(df) < 4320: return None
    
    # We want to find if there was ANY 3-day window where the price went 1.5x (based on CLOSE price)
    # A fast way: Rolling 3-day (4320 mins) max close / current close
    df['max_future_close'] = df['close'].rolling(4320, min_periods=1).max().shift(-4320)
    df['gain'] = (df['max_future_close'] - df['close']) / df['close']
    
    max_gain = df['gain'].max()
    if max_gain >= 0.50:
        # Find when it happened
        idx = df['gain'].idxmax()
        start_ts = df.loc[idx, 'timestamp']
        return {'symbol': filename.replace('_1m.csv',''), 'max_gain': max_gain, 'start_ts': start_ts}
    return None

def main():
    files = [f for f in os.listdir('data/history/multiverse') if f.endswith('_1m.csv')]
    print(f"Analyzing {len(files)} symbols for STRICT 1.5x close-price pumps...")
    
    true_pumps = []
    with ProcessPoolExecutor() as exe:
        for res in exe.map(analyze_symbol, files):
            if res: true_pumps.append(res)
            
    print(f"\nFOUND {len(true_pumps)} TRUE 1.5x PUMPS:")
    for p in sorted(true_pumps, key=lambda x: x['max_gain'], reverse=True):
        print(f"{p['symbol']} -> {p['max_gain']:.2%} gain")

if __name__ == '__main__':
    main()
