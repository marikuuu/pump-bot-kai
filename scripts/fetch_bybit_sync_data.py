import asyncio
import os
import logging
import pandas as pd
from datetime import datetime, timezone
import ccxt.async_support as ccxt
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 設定 ---
EXCHANGE_ID = 'bybit'
# 先ほど Binance で TP (本物) だった銘柄 + いくつかの FP (ノイズ) を中心に取得
TARGET_SYMBOLS = [
    'G/USDT:USDT', 'B/USDT:USDT', 'KAS/USDT:USDT', 'AIN/USDT:USDT', 
    'ANIME/USDT:USDT', 'POLYX/USDT:USDT', 'ENJ/USDT:USDT', 'JCT/USDT:USDT', 
    'ARC/USDT:USDT', 'IRYS/USDT:USDT',
    'MAV/USDT:USDT', 'DYM/USDT:USDT', 'HEI/USDT:USDT' # これらは Binance 側のノイズ(FP)候補
]
START_DATE = datetime(2026, 3, 15, 15, 0, 0, tzinfo=timezone.utc)
END_DATE   = datetime(2026, 3, 21, 15, 0, 0, tzinfo=timezone.utc)
DATA_DIR   = "data/history/bybit"

os.makedirs(DATA_DIR, exist_ok=True)

async def fetch_bybit_data():
    exchange = getattr(ccxt, EXCHANGE_ID)({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    
    await exchange.load_markets() # <--- Verify symbols
    
    since = int(START_DATE.timestamp() * 1000)
    end_ts = int(END_DATE.timestamp() * 1000)
    
    try:
        for symbol in TARGET_SYMBOLS:
            if symbol not in exchange.markets:
                logging.warning(f"❌ Bybit does NOT have {symbol}. (Likely Binance-exclusive Gem)")
                continue
                
            logging.info(f"🚀 Fetching {symbol} from Bybit...")
            all_ohlcv = []
            current_since = since
            
            while current_since < end_ts:
                ohlcv = await exchange.fetch_ohlcv(symbol, '1m', current_since, limit=1000)
                if not ohlcv: break
                
                # 範囲外のデータが含まれる場合はカット
                valid_ohlcv = [x for x in ohlcv if x[0] <= end_ts]
                all_ohlcv.extend(valid_ohlcv)
                
                if ohlcv[-1][0] >= end_ts: break
                current_since = ohlcv[-1][0] + 1
                await asyncio.sleep(exchange.rateLimit / 1000)
            
            if all_ohlcv:
                df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                clean_name = symbol.replace('/', '_').replace(':', '_')
                save_path = f"{DATA_DIR}/{clean_name}_bybit_1m.csv"
                df.to_csv(save_path, index=False)
                logging.info(f"✅ Saved {len(df)} rows to {save_path}")
            else:
                logging.warning(f"⚠️ No data found for {symbol} on Bybit within range.")
                
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(fetch_bybit_data())
