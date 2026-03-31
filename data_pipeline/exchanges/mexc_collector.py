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

class MexcCollector:
    """
    Collects real-time MEXC trade data using Direct API (No CCXT).
    """
    def __init__(self, symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = 'mexc'
        self.symbols = symbols or []
        self.db = db_manager or DatabaseManager()
        self.logger = DataLogger(self.db)
        self.detector = PumpDetector()
        self.exchange = ccxt.mexc({'options': {'defaultType': 'swap'}}) # [FIXED] Initialize exchange for discovery
        
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.last_chunk_time: Dict[str, float] = {}
        self.history: Dict[str, pd.DataFrame] = {}
        self.market_caps: Dict[str, float] = {}
        self.tick_buffer: List[tuple] = []

    async def initialize(self):
        import aiohttp
        if not self.db.pool:
            await self.db.connect()
        
        # Native MEXC Discovery (Bypassing CCXT for contract stability)
        try:
            # 🚀 MEXC Smart Discovery: Target low-cap gems
            # Skip top 30 (high caps) and pick next 100 gems
            async with aiohttp.ClientSession() as session:
                # 1. Get all ticker data for volume ranking
                async with session.get("https://contract.mexc.com/api/v1/contract/ticker") as t_resp:
                    t_data = await t_resp.json()
                    tickers = t_data.get('data', [])
                    # [FIXED] Using .get() defensively to avoid KeyError: 'volume24h'
                    vol_map = {}
                    for t in tickers:
                        vol = float(t.get('lastPrice', 0)) * float(t.get('volume24h', 0))
                        vol_map[t.get('symbol')] = vol
                
                # 2. Get contract details for metadata
                async with session.get("https://contract.mexc.com/api/v1/contract/detail") as c_resp:
                    c_data = await c_resp.json()
                    contracts = c_data.get('data', [])
                    
                ticker_data = [] # (unified_sym, volume)
                for c in contracts:
                    raw_sym = c.get('symbol', '')
                    if c.get('state') == 0 and raw_sym.endswith('_USDT'):
                         base = raw_sym.replace('_USDT', '')
                         unified_sym = f"{base}/USDT"
                         v = vol_map.get(raw_sym, 0)
                         ticker_data.append((unified_sym, v))
                
                # Sort by Volume Descending
                ticker_data.sort(key=lambda x: x[1], reverse=True)
                # 🎯 Ultra Targeted Discovery: Skip top 100, Take next 100
                discovery_range = ticker_data[100:200]
                self.symbols = [s for s, _ in discovery_range]
                for s, v in discovery_range:
                     self.market_caps[s] = v * 30.0 # Strict Proxy
                
                # Ensure specific low-cap targets are always monitored
                u_targets = ["SIREN/USDT", "TRIA/USDT", "JCT/USDT", "LYN/USDT", "LIGHT/USDT"]
                for ut in u_targets:
                    if ut not in self.symbols:
                        self.symbols.append(ut)
                        self.market_caps[ut] = 500_000
                        
                logging.info(f"💎 MEXC Native Discovery: Targeted {len(self.symbols)} gems (Rank 30-130).")
        except Exception as e:
            logging.error(f"Native MEXC Discovery FAILED: {e}")

        if not self.symbols or self.symbols == ["AUTO"]:
            try:
                # Force MEXC Contract API (swap)
                markets = await self.exchange.fetch_markets(params={'type': 'swap'})
                candidates = []
                
                # Handle both list and dict returns from CCXT
                market_items = markets.values() if isinstance(markets, dict) else markets
                
                for m in market_items:
                    sym = m.get('symbol', '')
                    if m.get('active') and '_' in sym: # Native MEXC style
                        qv = float(m.get('info', {}).get('quoteVolume', 0)) or 0
                        if 100_000 < qv:
                             candidates.append((sym, qv))
                             self.market_caps[sym] = qv
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                self.symbols = [s for s, _ in candidates[:30]]
                
                # Standard core targets (Native format)
                u_targets = ["SIREN_USDT", "TRIA_USDT", "JCT_USDT", "LYN_USDT", "LIGHT_USDT"]
                for ut in u_targets:
                    if ut not in self.symbols: self.symbols.append(ut)
                
                logging.info(f"MEXC Discovery Success: {len(self.symbols)} symbols.")
            except Exception as e:
                logging.error(f"MEXC Discovery FAILED: {e}")
                self.symbols = ["SIREN_USDT", "TRIA_USDT", "JCT_USDT", "LYN_USDT"]
        
        for s in self.symbols:
            self.trade_buffers[s] = []
            self.last_chunk_time[s] = time.time()
            self.history[s] = pd.DataFrame()
            if s not in self.market_caps: self.market_caps[s] = 1_000_000
            
        logging.info(f"MexcCollector initialized ({len(self.symbols)} symbols).")

    async def watch_combined_trades(self):
        """
        Watches all symbols using a single shared WebSocket connection.
        """
        import json
        import websockets
        ws_url = "wss://contract.mexc.com/edge"
        
        logging.info(f"Starting Combined MEXC WS for {len(self.symbols)} symbols")
        while True:
            try:
                # Use longer timeout for MEXC
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    
                    # 🚀 Dedicated Ping Loop for MEXC (JSON-based)
                    async def ping_loop():
                        while True:
                            try:
                                await asyncio.sleep(20)
                                await ws.send(json.dumps({"method": "ping"}))
                            except:
                                break
                    
                    ping_task = asyncio.create_task(ping_loop())
                    
                    sym_map = {} # BTC_USDT -> BTC/USDT or BTC_USDT (if no slash)
                    # Subscribe to all symbols
                    for symbol in self.symbols:
                        native_sym = symbol.replace('/', '_') if '/' in symbol else symbol
                        sym_map[native_sym] = symbol
                        sub_msg = json.dumps({"method": "sub.deal", "param": {"symbol": native_sym}})
                        await ws.send(sub_msg)
                        await asyncio.sleep(0.05) # Tiny stagger for subscriptions
                    
                    try:
                        while True:
                            msg = await ws.recv()
                            try:
                                res = json.loads(msg)
                            except json.JSONDecodeError:
                                continue # Skip non-JSON messages (like raw strings)
                            
                            # Bulletproof handling for MEXC WS data structure quirks
                            def flatten(node):
                                if isinstance(node, list):
                                    for n in node: yield from flatten(n)
                                elif isinstance(node, dict):
                                    yield node

                            for item in flatten(res):
                                if not isinstance(item, dict) or item.get('channel') != 'push.deal': continue
                                
                                native_sym = item.get('symbol')
                                unified_sym = sym_map.get(native_sym, native_sym)
                                
                                data_node = item.get('data', {})
                                trades_to_process = data_node if isinstance(data_node, list) else [data_node]
                                
                                for d in trades_to_process:
                                    if not isinstance(d, dict): continue
                                    try:
                                        trade = {
                                            'price': float(d.get('p', 0)),
                                            'amount': float(d.get('v', 0)),
                                            'side': 'buy' if d.get('T') == 1 else 'sell',
                                            'timestamp': d.get('t', 0),
                                            'received_at': time.time()
                                        }
                                        self.trade_buffers[unified_sym].append(trade)
                                        
                                        # 🚀 DB Logging (Buffering)
                                        self.tick_buffer.append((
                                            datetime.now(timezone.utc),
                                            self.exchange_id, unified_sym, trade['price'], trade['amount'], trade['side'], d.get('M', False)
                                        ))
                                    except (ValueError, TypeError):
                                        continue
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logging.error(f"Combined MEXC WS Error: {e}")
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
        logging.info(f"MEXC Scheduler started: GHOST MODE (5s interval)")
        while True:
            await asyncio.sleep(5)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on MEXC: {e}")

    async def process_chunk(self, symbol: str):
        trades = self.trade_buffers[symbol]
        self.trade_buffers[symbol] = []
        if not trades: return

        df = pd.DataFrame(trades)
        price_end = df['price'].iloc[-1]
        vol = df['amount'].sum()
        
        # === v5.1 Tick-DNA features ===
        df['ts_dt'] = pd.to_datetime(df['received_at'], unit='s')
        df['sec'] = df['ts_dt'].dt.second
        buy_df = df[df['side'] == 'buy']
        rush_counts = buy_df.groupby('sec').size().reindex(range(0, 60), fill_value=0)
        std_rush = float(rush_counts.std())

        costs = df['price'] * df['amount']
        avg_trade_size = float(costs.mean()) if not costs.empty else 0.0
        max_trade_size = float(costs.max()) if not costs.empty else 0.0
        median_trade_size = float(costs.median()) if not costs.empty else 0.0
        buy_ratio = len(buy_df) / (len(df) + 1e-9)
        
        mid_ts = df['received_at'].min() + 15
        acceleration = (len(df[df['received_at'] >= mid_ts]) + 1) / (len(df[df['received_at'] < mid_ts]) + 1)
        
        prices = df['price']
        price_impact = (prices.max() - prices.iloc[0]) / (prices.iloc[0] + 1e-9) if len(prices) >= 2 else 0.0

        # Features
        current_oi = 0.0 # MEXC Bulk OI is hard, placeholder
        prev_row = self.history[symbol].iloc[-1] if not self.history[symbol].empty else None
        prev_price = prev_row['price_end'] if prev_row is not None else price_end
        
        oi_change = 0.0
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
        oi_z   = 0.0 

        features = {
            'symbol': symbol, 'exchange': 'MEXC', 'price': price_end,
            'vol_z': vol_z, 'pc_z': pc_z, 'oi_z': oi_z, 'pre_accum_z': pre_accum_z,
            'std_rush': std_rush, 'oi_change': oi_change, 'pc_change': pc_change,
            'vacuum_score': vacuum_score, 'buy_ratio': buy_ratio,
            'avg_trade_size': avg_trade_size, 'max_trade_size': max_trade_size,
            'median_trade_size': median_trade_size, 'acceleration': acceleration,
            'price_impact': price_impact,
            'market_cap': self.market_caps.get(symbol, 1_000_000)
        }

        if self.detector and self.detector.check_event(features)[0]:
            logging.warning(f"👻 MEXC GHOST SIGNAL: {symbol} | vol_z={vol_z:.2f} pc_z={pc_z:.2f}")
            
            # Save the raw ticks for future training (The DNA of this pump)
            save_path = f"data/ticks_mexc_{symbol.replace('/','_').replace(':','_')}_{int(time.time())}.csv"
            os.makedirs("data", exist_ok=True)
            df.to_csv(save_path, index=False)
            logging.info(f"💾 MEXC Tick Data saved to {save_path}")

    async def run(self):
        await self.initialize()
        watchers = [
            asyncio.create_task(self.watch_combined_trades()),
            asyncio.create_task(self.scheduler_loop()),
            asyncio.create_task(self.flush_ticks_loop())
        ]
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    collector = MexcCollector()
    asyncio.run(collector.run())
