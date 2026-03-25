import asyncio
import ccxt
import pandas as pd
import pickle
from datetime import datetime, timezone, timedelta
import time

async def generate_v51_report():
    # 1. Load data and NEW v5.1 model
    df = pd.read_csv('tick_training_data_v51.csv')
    with open('pump_ai/pump_model_v51_diverse.pkl', 'rb') as f:
        model = pickle.load(f)

    FEATURES = ['vol_z', 'pc_z', 'pre_accum_z', 'std_rush', 'avg_trade_size', 'max_trade_size', 'median_trade_size', 'buy_ratio', 'acceleration', 'price_impact']
    df['confidence'] = model.predict_proba(df[FEATURES].fillna(0))[:, 1]
    
    # Threshold for v5.1 (diverse model might need slightly different thresh, but let's try 0.80)
    THRESHOLD = 0.80
    hits = df[df['confidence'] >= THRESHOLD].sort_values('timestamp').copy()
    
    print(f"Total raw hits: {len(hits)}")
    
    # 2. Group by Event (Deduplication: 1 signal per coin per 12 hours)
    processed_hits = []
    cooldowns = {} # symbol -> last_signal_time
    
    for i, row in hits.iterrows():
        symbol = row['symbol']
        ts = row['timestamp']
        
        if symbol in cooldowns:
            if ts - cooldowns[symbol] < (12 * 3600 * 1000): # 12h cooldown
                continue
                
        cooldowns[symbol] = ts
        
        # JST conversion
        dt_jst = datetime.fromtimestamp(ts/1000, tz=timezone.utc) + timedelta(hours=9)
        time_jst = dt_jst.strftime('%Y-%m-%d %H:%M:%S')
        
        processed_hits.append({
            'Symbol': symbol.split(':')[0],
            'Time (JST)': time_jst,
            'Price': f"{row['price']:.6f}",
            'Conf': f"{row['confidence']:.3f}",
            'Outcome': "✅ 1.5x+ ACHIEVED" if row['label'] == 1 else "❌ FAILED"
        })

    # 3. Create MD Report
    report_df = pd.DataFrame(processed_hits)
    
    headers = ['Symbol', 'Time (JST)', 'Price', 'Conf', 'Outcome']
    md_table = "| " + " | ".join(headers) + " |\n"
    md_table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    for d in processed_hits:
        md_table += "| " + " | ".join([str(d[h]) for h in headers]) + " |\n"
    
    report_content = f"""# IZANAGI v5.1 Universal Pump Report (DIVERSE MODE)

## Overview
- **Model**: XGBoost v5.1 (Diverse DNA Engine)
- **Constraint**: deduplicated (1 signal per coin / 12h)
- **Success Criteria**: 1.5x in 3 days

## Detection Performance (3/14 - 3/21)
{md_table}

---
*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} JST*
"""
    
    with open('v51_diverse_report.md', 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print("Report generated: v51_diverse_report.md")

if __name__ == "__main__":
    asyncio.run(generate_v51_report())
