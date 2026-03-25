import asyncio
import asyncpg
import pandas as pd
import pickle
import os
from datetime import datetime, timezone

# Database URL from common config
DATABASE_URL = "postgresql://admin:password@127.0.0.1:5433/pump_ai"

async def get_drawdown(conn, symbol, start_ts_ms, entry_p):
    # 72 hours window
    end_ts_ms = start_ts_ms + (72 * 3600 * 1000)
    
    # Query candles
    query = """
        SELECT low FROM candles 
        WHERE symbol = $1 
        AND timestamp >= $2 
        AND timestamp <= $3 
        ORDER BY timestamp ASC
    """
    rows = await conn.fetch(query, symbol, start_ts_ms, end_ts_ms)
    if not rows:
        return 0.0
    
    # Calculate Max DD (minimum price reached relative to entry)
    min_p_reached = min([r['low'] for r in rows])
    dd = (min_p_reached - entry_p) / entry_p
    return dd * 100

async def main():
    # Load hits
    df = pd.read_csv('tick_training_data.csv')
    with open('pump_ai/pump_model_v5_tick.pkl', 'rb') as f:
        model = pickle.load(f)

    FT = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
    df['conf'] = model.predict_proba(df[FT].fillna(0))[:, 1]
    hits = df[df['conf'] >= 0.85].sort_values(['symbol', 'timestamp']).drop_duplicates('symbol').copy()

    print(f"Connecting to Postgres at {DATABASE_URL}...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    results = []
    print("Calculating DD for unique hits...")
    for i, row in hits.iterrows():
        dd = await get_drawdown(conn, row['symbol'], row['timestamp'], row['price'])
        results.append({
            'symbol': row['symbol'],
            'time': datetime.fromtimestamp(row['timestamp']/1000, tz=timezone.utc).strftime('%m/%d %H:%M'),
            'dd': dd
        })
    
    await conn.close()
    
    res_df = pd.DataFrame(results)
    print("\n=== Signal Max Drawdown (DD) Report ===")
    # Sort by DD (most negative first)
    print(res_df.sort_values('dd')[['symbol', 'time', 'dd']].to_string(index=False))
    print(f"\nAverage DD: {res_df['dd'].mean():.2f}%")
    print(f"Worst DD: {res_df['dd'].min():.2f}%")

if __name__ == "__main__":
    asyncio.run(main())
