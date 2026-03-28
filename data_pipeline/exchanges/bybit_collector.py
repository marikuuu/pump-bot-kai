import asyncio
import logging
import os
import time
import json
import websockets
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from database.db_manager import DatabaseManager
from pump_ai.detector import PumpDetector
from database.data_logger import DataLogger

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BybitCollector:
    """
    Collects real-time Bybit trade data using Direct V5 API (No CCXT).
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'bybit'
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
        
        # Native Bybit V5 Discovery
        # Native Bybit V5 Discovery with Volume Ranking
        try:
            logging.info("Native Bybit V5 Discovery: fetching tickers for volume ranking...")
            async with aiohttp.ClientSession() as session:
                # 1. Get tickers to rank by volume
                async with session.get("https://api.bybit.com/v5/market/tickers?category=linear") as t_resp:
                    t_data = await t_resp.json()
                    tickers = t_data.get('result', {}).get('list', [])
                    vol_map = {t['symbol']: float(t['turnover24h']) for t in tickers}
                
                # 2. Get instrument metadata
                url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
                async with session.get(url) as resp:
                    data = await resp.json()
                    res = data.get('result', {}).get('list', [])
                    
                    candidate_data = [] # (unified_sym, volume)
                    for item in res:
                        if item.get('status') == 'Trading':
                            raw_sym = item.get('symbol')
                            base = item.get('baseCoin')
                            unified_sym = f"{base}/USDT"
                            v = vol_map.get(raw_sym, 0)
                            candidate_data.append((unified_sym, v))
                    
                    # Sort by Volume Descending
                    candidate_data.sort(key=lambda x: x[1], reverse=True)
                    
                    # 🎯 Targeted Discovery: Skip top 50 (Major Assets), Take next 100
                    discovery_range = candidate_data[50:150]
                    self.symbols = [s for s, _ in discovery_range]
                    
                    logging.info(f"Native Bybit Discovery Success: {len(self.symbols)} gems (Rank 50-150 by Volume).")
        except Exception as e:
            logging.error(f"Native Bybit Discovery FAILED: {e}")

        if not self.symbols:
             logging.warning("Bybit Discovery returned 0 symbols. Bridge might be needed.")
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.history[s] = pd.DataFrame()
            
        logging.info(f"BybitCollector initialized with {len(self.symbols)} symbols.")

    async def watch_combined_trades(self):
        """
        Watches all symbols using a single shared WebSocket connection.
        """
        import json
        import websockets
        ws_url = "wss://stream.bybit.com/v5/public/linear"
        
        logging.info(f"Starting Combined Bybit WS for {len(self.symbols)} symbols")
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    
                    # 🚀 Dedicated Ping Loop for Bybit
                    async def ping_loop():
                        while True:
                            try:
                                await asyncio.sleep(20)
                                await ws.send(json.dumps({"op": "ping"}))
                            except:
                                break
                    
                    ping_task = asyncio.create_task(ping_loop())
                    
                    # Subscribe in batches of 10 to avoid any message size limits
                    batch_size = 10
                    for i in range(0, len(self.symbols), batch_size):
                        batch = self.symbols[i:i+batch_size]
                        args = [f"publicTrade.{s.replace('/', '').upper()}" for s in batch]
                        sub_msg = json.dumps({"op": "subscribe", "args": args})
                        await ws.send(sub_msg)
                        await asyncio.sleep(0.1)
                    
                    try:
                        while True:
                            msg = await ws.recv()
                            try:
                                res = json.loads(msg)
                            except json.JSONDecodeError:
                                continue
                            
                            if 'ret_msg' in res and res['ret_msg'] == 'pong': continue
                            if 'data' not in res or 'topic' not in res: continue
                            
                            # Topic: publicTrade.BTCUSDT
                            topic = res['topic']
                            native_sym = topic.replace('publicTrade.', '')
                            
                            # Find unified symbol
                            unified_sym = next((s for s in self.symbols if s.replace('/', '').upper() == native_sym), native_sym)
                            
                            for d in res['data']:
                                trade = {
                                    'price': float(d['p']),
                                    'amount': float(d['v']),
                                    'side': d['S'].lower(),
                                    'timestamp': int(d['T']),
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
                logging.error(f"Combined Bybit WS Error: {e}")
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
        logging.info(f"Bybit Scheduler started: GHOST MODE (5s interval)")
        while True:
            # 🚀 Bulk fetch OI for all symbols from Bybit V5
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://api.bybit.com/v5/market/tickers?category=linear") as resp:
                        data = await resp.json()
                        for t in data.get('result', {}).get('list', []):
                            sym = t['symbol']
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
                    logging.error(f"Error processing {symbol} on Bybit: {e}")

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
        price_change = (price_end - df['price'].iloc[0]) / (df['price'].iloc[0] + 1e-9)
        
        # Rolling Metrics
        current_oi = self.oi_data.get(symbol, 0)
        prev_row = self.history[symbol].iloc[-1] if not self.history[symbol].empty else None
        
        prev_oi = prev_row['oi'] if prev_row is not None else current_oi
        prev_price = prev_row['price_end'] if prev_row is not None else price_end
        
        oi_change = (current_oi - prev_oi) / (prev_oi + 1e-9)
        pc_change = (price_end - prev_price) / (prev_price + 1e-9)

        # Update history
        new_row = {
            'time': datetime.now(timezone.utc),
            'volume': vol,
            'price_end': price_end,
            'oi': current_oi,
            'oi_change': oi_change,
            'pc_change': pc_change,
            'std_rush': std_rush
        }
        self.history[symbol] = pd.concat([self.history[symbol], pd.DataFrame([new_row])]).iloc[-240:]
        
        if len(self.history[symbol]) < 20: return
        hist = self.history[symbol]

        # === FEATURE: VPVR Vacuum Score ===
        vacuum_score = 0.0
        price_min, price_max = hist['price_end'].min(), hist['price_end'].max()
        if price_max > price_min:
            bins = np.linspace(price_min, price_max, 20)
            counts, _ = np.histogram(hist['price_end'], bins=bins, weights=hist['volume'])
            poc_idx, curr_idx = np.argmax(counts), np.digitize(price_end, bins) - 1
            if 0 <= curr_idx < len(counts):
                vacuum_score = 1.0 - (counts[curr_idx] / (counts[poc_idx] + 1e-9))

        # === Z-Scores & Pre-accum ===
        pre_accum_z = 0.0
        if len(hist) >= 40:
             pre_vol_mid = hist['volume'].iloc[:len(hist)//2].mean()
             pre_vol_late = hist['volume'].iloc[len(hist)//2:].mean()
             pre_accum_z = (pre_vol_late - pre_vol_mid) / (hist['volume'].std() + 1e-9)

        vol_z  = (vol       - hist['volume'].mean())    / (hist['volume'].std()    or 1)
        pc_z   = (pc_change - hist['pc_change'].mean()) / (hist['pc_change'].std() or 1)
        oi_z   = (oi_change - hist['oi_change'].mean()) / (hist['oi_change'].std() or 1)

        features = {
            'symbol': symbol, 'exchange': 'BYBIT', 'price': price_end,
            'vol_z': vol_z, 'pc_z': pc_z, 'oi_z': oi_z, 'pre_accum_z': pre_accum_z,
            'std_rush': std_rush, 'oi_change': oi_change, 'pc_change': pc_change,
            'vacuum_score': vacuum_score, 'buy_ratio': len(buy_df)/(len(df)+1e-9),
            'market_cap': 50_000_000 # Placeholder for Bybit Discovery
        }

        if self.detector and self.detector.check_event(features)[0]:
            logging.warning(f"👻 BYBIT GHOST SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")

    async def run(self):
        await self.initialize()
        watchers = [
            asyncio.create_task(self.watch_combined_trades()),
            asyncio.create_task(self.scheduler_loop()),
            asyncio.create_task(self.flush_ticks_loop())
        ]
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    c = BybitCollector()
    asyncio.run(c.run())
