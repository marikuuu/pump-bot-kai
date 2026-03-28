import asyncio
import pandas as pd
from database.db_manager import DatabaseManager
from pump_ai.detector import PumpDetector

async def reverify():
    db = DatabaseManager()
    await db.connect()
    
    # 🚨 Target Symbols reported by user as "Noise"
    target_symbols = ["POL/USDT", "PYTH/USDT", "DYDX/USDT", "BSB/USDT", "ENSO/USDT"]
    
    print("🧪 IZANAGI Live Re-Verification: Comparing OLD vs NEW settings...")
    print("="*80)
    
    detector_old = PumpDetector(thresholds={'vol_z': 2.0, 'market_cap': 1_000_000_000})
    detector_new = PumpDetector(thresholds={'vol_z': 3.0, 'market_cap': 200_000_000})
    
    for symbol in target_symbols:
        try:
            # Fetch last 24h data from DB
            query = """
                SELECT time, price, amount, side, is_buyer_maker 
                FROM ticks 
                WHERE symbol = %s 
                ORDER BY time ASC;
            """
            rows = await db.fetch_all(query, (symbol,))
            if not rows:
                print(f"[-] {symbol}: No tick data found in DB.")
                continue
                
            df = pd.DataFrame(rows, columns=['time', 'price', 'amount', 'side', 'is_buyer_maker'])
            df['price'] = df['price'].astype(float)
            df['amount'] = df['amount'].astype(float)
            df['received_at'] = df['time'].apply(lambda x: x.timestamp())
            
            # Group into 30s chunks to simulate real-time process_chunk
            df.set_index('time', inplace=True)
            resampled = df.resample('30S')
            
            # Simple simulation of process_chunk logic
            history = pd.DataFrame()
            old_hit = 0
            new_hit = 0
            
            for interval, chunk in resampled:
                if chunk.empty: continue
                
                vol = chunk['amount'].sum()
                price_end = chunk['price'].iloc[-1]
                price_start = chunk['price'].iloc[0]
                price_change = (price_end - price_start) / (price_start + 1e-9)
                
                new_row = {'volume': vol, 'price_change': price_change}
                history = pd.concat([history, pd.DataFrame([new_row])]).iloc[-120:]
                
                if len(history) < 20: continue
                
                vol_z = (vol - history['volume'].mean()) / (history['volume'].std() or 1)
                pc_z = (price_change - history['price_change'].mean()) / (history['price_change'].std() or 1)
                
                # Mock features
                features = {
                    'symbol': symbol,
                    'vol_z': vol_z,
                    'pc_z': pc_z,
                    'market_cap': 500_000_000 if "POL" in symbol or "PYTH" in symbol or "DYDX" in symbol else 50_000_000,
                    'std_rush': 10.0 # Placeholder
                }
                
                if detector_old.check_event(features)[0]: old_hit += 1
                if detector_new.check_event(features)[0]: new_hit += 1
            
            print(f"Result for {symbol}:")
            print(f"  - OLD Setting Hits: {old_hit}")
            print(f"  - NEW Setting Hits: {new_hit}")
            print(f"  - EFFECT: {'✅ Filtered Out' if old_hit > 0 and new_hit == 0 else '⚠️ Still Hits (Gem?)' if new_hit > 0 else 'No hits in this sample'}")
            print("-" * 40)
            
        except Exception as e:
            print(f"[!] Error processing {symbol}: {e}")

    await db.close()

if __name__ == "__main__":
    asyncio.run(reverify())
