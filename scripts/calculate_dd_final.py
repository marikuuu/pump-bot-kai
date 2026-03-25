import asyncio
import asyncpg
import pandas as pd
import pickle
from datetime import datetime, timezone

DATABASE_URL = "postgresql://admin:password@127.0.0.1:5433/pump_ai"

async def calculate_dd_all():
    conn = await asyncpg.connect(DATABASE_URL)
    
    # 1. Load predicted signals
    df = pd.read_csv('tick_training_data.csv')
    with open('pump_ai/pump_model_v5_tick.pkl', 'rb') as f:
        model = pickle.load(f)

    FT = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
    df['conf'] = model.predict_proba(df[FT].fillna(0))[:, 1]
    # Filter for signals
    hits = df[df['conf'] >= 0.85].sort_values(['symbol', 'timestamp']).drop_duplicates('symbol').copy()

    results = []
    print(f"Analyzing DD for {len(hits)} symbols...")

    for i, row in hits.iterrows():
        symbol = row['symbol']
        # Try to find the token_id format
        # common formats in this bot: 'AIN/USDT:USDT' or 'AIN-USDT' or 'AIN'
        # We'll search by LIKE
        token_id_row = await conn.fetchrow("SELECT DISTINCT token_id FROM candles WHERE token_id LIKE $1 LIMIT 1", f"%{symbol.split('/')[0]}%")
        
        if not token_id_row:
            results.append({'symbol': symbol, 'dd': 0.0, 'note': 'No data'})
            continue
            
        tid = token_id_row['token_id']
        start_ts = datetime.fromtimestamp(row['timestamp']/1000, tz=timezone.utc)
        end_ts = start_ts + pd.Timedelta(hours=72)
        
        # Query lows
        candles = await conn.fetch("SELECT low FROM candles WHERE token_id = $1 AND time >= $2 AND time <= $3", tid, start_ts, end_ts)
        
        if not candles:
            results.append({'symbol': symbol, 'dd': 0.0, 'note': 'No candles'})
            continue
            
        min_low = min([c['low'] for c in candles])
        dd = (min_low - row['price']) / row['price'] * 100
        results.append({'symbol': symbol, 'time': start_ts.strftime('%m/%d %H:%M'), 'dd': dd, 'note': 'OK'})

    await conn.close()
    
    res_df = pd.DataFrame(results)
    print("\n=== Signal Drawdown (DD) Report ===")
    if 'time' in res_df.columns:
        print(res_df.sort_values('dd')[['symbol', 'time', 'dd']].to_string(index=False))
    else:
        print(res_df)

if __name__ == "__main__":
    asyncio.run(calculate_dd_all())
