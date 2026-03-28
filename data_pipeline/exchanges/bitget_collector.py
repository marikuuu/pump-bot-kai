import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import websockets
from dotenv import load_dotenv

from database.db_manager import DatabaseManager
from pump_ai.detector import PumpDetector
from database.data_logger import DataLogger

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BitgetCollector:
    """
    Collects real-time Bitget trade data using Direct API (No CCXT).
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'bitget'
        self.symbols = symbols or []
        self.db = db_manager or DatabaseManager()
        self.logger = DataLogger(self.db)
        self.detector = None # Will be injected from main.py
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.history: Dict[str, pd.DataFrame] = {}
        self.oi_data: Dict[str, float] = {} # 🚀 Added OI Storage
        self.tick_buffer: List[tuple] = []

    async def initialize(self):
        import aiohttp
        if not self.db.pool:
            await self.db.connect()
        
        # Native Bitget V2 Discovery
        if os.getenv("CEX_SYMBOLS") == "AUTO":
            logging.info("Native Bitget Discovery: hitting api.bitget.com directly...")
            try:
                url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        tickers = data.get('data', [])
                        candidates = []
                        
                        stocks_to_exclude = ['NVDA', 'AAPL', 'TSLA', 'MSTR', 'MU', 'AMZN', 'GOOG', 'META', 'NFLX', 'MSFT', 'COIN', 'HOOD']
                        
                        for t in tickers:
                            sym = t.get('symbol', '') # Bitget Native: BTCUSDT
                            if not sym.endswith('USDT'): continue
                            
                            base = sym.replace('USDT', '')
                            if base in stocks_to_exclude: continue
                            
                            qv = float(t.get('quoteVolume') or 0)
                            if 500_000 < qv < 20_000_000:
                                # Internal format: BASE/USDT
                                candidates.append((f"{base}/USDT", qv))
                        
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        
                        # 🎯 Targeted Discovery: Skip top 30 (high caps), Take next 100 gems
                        discovery_range = candidates[30:130]
                        self.symbols = [s for s, _ in discovery_range]
                        logging.info(f"Native Bitget Discovery: {len(self.symbols)} gems (Rank 30-130) found.")
            except Exception as e:
                logging.error(f"Native Bitget Discovery failed: {e}")
                self.symbols = ["BTCUSDT"]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.history[s] = pd.DataFrame()
            
        logging.info(f"BitgetCollector initialized with {len(self.symbols)} symbols.")

    async def watch_combined_trades(self):
        """
        Watches all symbols using a single shared WebSocket connection.
        """
        import json
        import websockets
        ws_url = "wss://ws.bitget.com/v2/ws/public"
        
        # Build subscription arguments
        args = []
        sym_map = {} # BTCUSDT -> BTC/USDT
        for s in self.symbols:
            clean = s.replace('/', '').upper()
            args.append({
                "instType": "USDT-FUTURES",
                "channel": "trade",
                "instId": clean
            })
            sym_map[clean] = s
            
        logging.info(f"Starting Combined Bitget WS for {len(self.symbols)} symbols")
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    
                    # 🚀 Dedicated Ping Loop for Bitget (Literal string "ping")
                    async def ping_loop():
                        while True:
                            try:
                                await asyncio.sleep(20)
                                await ws.send("ping")
                            except:
                                break
                    
                    ping_task = asyncio.create_task(ping_loop())
                    
                    # Subscribe to all at once
                    sub_msg = json.dumps({"op": "subscribe", "args": args})
                    await ws.send(sub_msg)
                    
                    try:
                        while True:
                            msg = await ws.recv()
                            
                            # 🚀 Handle literal "pong" BEFORE JSON parsing
                            if msg == "pong": continue
                            
                            res = json.loads(msg)
                            if 'data' not in res or 'arg' not in res: continue
                            
                            native_sym = res['arg'].get('instId')
                            unified_sym = sym_map.get(native_sym, native_sym)
                            
                            for d in res['data']:
                                trade = {
                                    'price': float(d['price']),
                                    'amount': float(d['size']),
                                    'side': d['side'].lower(),
                                    'timestamp': int(d['ts']),
                                    'received_at': time.time()
                                }
                                self.trade_buffers[unified_sym].append(trade)
                                
                                # 🚀 DB Logging (Buffering)
                                self.tick_buffer.append((
                                    datetime.now(timezone.utc),
                                    self.exchange_id, unified_sym, trade['price'], trade['amount'], trade['side'], d.get('m', False)
                                ))
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logging.error(f"Combined Bitget WS Error: {e}")
                await asyncio.sleep(5)

    async def flush_ticks_loop(self):
        """Periodically flushes the tick buffer to DB in bulk."""
        while True:
            await asyncio.sleep(2)
            if self.tick_buffer:
                batch = self.tick_buffer[:]
                self.tick_buffer = []
                await self.logger.log_ticks_batch(batch)

    async def scheduler_loop(self):
        """Fires process_chunk for every symbol every 5 seconds (Protocol GHOST Speed)."""
        import aiohttp
        logging.info(f"Bitget Scheduler started: GHOST MODE (5s interval)")
        while True:
            # 🚀 Bulk fetch OI for all symbols from Bitget V2
            try:
                url = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        for t in data.get('data', []):
                            sym = t.get('symbol', '') # e.g. BTCUSDT
                            unified = next((s for s in self.symbols if s.replace('/','').upper() == sym), None)
                            if unified:
                                self.oi_data[unified] = float(t.get('openInterest', 0))
            except Exception as e:
                pass

            await asyncio.sleep(5)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on Bitget: {e}")

    async def process_chunk(self, symbol: str):
        trades = self.trade_buffers[symbol]
        self.trade_buffers[symbol] = []
        if not trades: return

        df = pd.DataFrame(trades)
        price_end = df['price'].iloc[-1]
        vol = df['amount'].sum()
        
        # === FEATURE: std_rush ===
        df['ts_dt'] = pd.to_datetime(df['received_at'], unit='s')
        df['sec'] = df['ts_dt'].dt.second
        buy_df = df[df['side'] == 'buy']
        rush_counts = buy_df.groupby('sec').size().reindex(range(0, 60), fill_value=0)
        std_rush = float(rush_counts.std())

        # Features
        current_oi = self.oi_data.get(symbol, 0)
        prev_row = self.history[symbol].iloc[-1] if not self.history[symbol].empty else None
        
        prev_oi = prev_row['oi'] if prev_row is not None else current_oi
        prev_price = prev_row['price_end'] if prev_row is not None else price_end
        
        oi_change = (current_oi - prev_oi) / (prev_oi + 1e-9)
        pc_change = (price_end - prev_price) / (prev_price + 1e-9)

        # Update history (Protocol GHOST Memory Expansion)
        new_row = {
            'time': datetime.now(timezone.utc),
            'volume': vol,
            'price_end': price_end,
            'oi': current_oi,
            'oi_change': oi_change,
            'pc_change': pc_change,
            'std_rush': std_rush
        }
        # Keep 24 hours of 5s data (17,280 samples)
        self.history[symbol] = pd.concat([self.history[symbol], pd.DataFrame([new_row])]).iloc[-17280:]
        
        if len(self.history[symbol]) < 20: return
        hist_full = self.history[symbol]

        # === FEATURE: VPVR Vacuum Score (24 hour window) ===
        vacuum_score = 0.0
        price_min, price_max = hist_full['price_end'].min(), hist_full['price_end'].max()
        if price_max > price_min:
            bins = np.linspace(price_min, price_max, 20)
            counts, _ = np.histogram(hist_full['price_end'], bins=bins, weights=hist_full['volume'])
            poc_idx, curr_idx = np.argmax(counts), np.digitize(price_end, bins) - 1
            if 0 <= curr_idx < len(counts):
                vacuum_score = 1.0 - (counts[curr_idx] / (counts[poc_idx] + 1e-9))

        # === Long-term Stats (4 hour window for Z-Scores) ===
        hist_stats = hist_full.iloc[-2880:] 
        
        pre_accum_z = 0.0
        if len(hist_stats) >= 40:
             pre_vol_mid = hist_stats['volume'].iloc[:len(hist_stats)//2].mean()
             pre_vol_late = hist_stats['volume'].iloc[len(hist_stats)//2:].mean()
             pre_accum_z = (pre_vol_late - pre_vol_mid) / (hist_stats['volume'].std() + 1e-9)

        vol_z  = (vol       - hist_stats['volume'].mean())    / (hist_stats['volume'].std()    or 1)
        pc_z   = (pc_change - hist_stats['pc_change'].mean()) / (hist_stats['pc_change'].std() or 1)
        oi_z   = (oi_change - hist_stats['oi_change'].mean()) / (hist_stats['oi_change'].std() or 1)

        features = {
            'symbol': symbol, 'exchange': 'BITGET', 'price': price_end,
            'vol_z': vol_z, 'pc_z': pc_z, 'oi_z': oi_z, 'pre_accum_z': pre_accum_z,
            'std_rush': std_rush, 'oi_change': oi_change, 'pc_change': pc_change,
            'vacuum_score': vacuum_score, 'buy_ratio': len(buy_df)/(len(df)+1e-9),
            'market_cap': 10_000_000 # Placeholder
        }

        if self.detector and self.detector.check_event(features)[0]:
            logging.warning(f"👻 BITGET GHOST SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")

    async def run(self):
        await self.initialize()
        watchers = [
            asyncio.create_task(self.watch_combined_trades()),
            asyncio.create_task(self.scheduler_loop()),
            asyncio.create_task(self.flush_ticks_loop())
        ]
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    c = BitgetCollector()
    asyncio.run(c.run())
