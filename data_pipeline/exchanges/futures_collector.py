import asyncio
import json
import logging
import os
import time
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

class FuturesCollector:
    """
    Collects real-time Futures data using Native APIs (No CCXT).
    """
    def __init__(self, exchange_id: str = 'binance', symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = exchange_id
        self.symbols = symbols or []
        self.db = db_manager or DatabaseManager()
        self.logger = DataLogger(self.db)
        self.detector = PumpDetector()
        
        self.trade_buffers: Dict[str, List[Dict]] = {s: [] for s in self.symbols}
        self.last_chunk_time: Dict[str, float] = {s: time.time() for s in self.symbols}
        self.history: Dict[str, pd.DataFrame] = {s: pd.DataFrame() for s in self.symbols}
        self.oi_data: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.volumes_24h: Dict[str, float] = {}
        self.tick_buffer: List[tuple] = [] # 🚀 Buffer for high-speed logging

    async def initialize(self):
        import aiohttp
        if not self.db.pool:
            await self.db.connect()
        
        # Native API Discovery (Bypassing CCXT)
        if os.getenv("CEX_SYMBOLS") == "AUTO" and self.exchange_id == 'binance':
            try:
                logging.info(f"Native Binance Discovery: hitting https://fapi.binance.com/fapi/v1/exchangeInfo directly...")
                async with aiohttp.ClientSession() as session:
                    # 1. Fetch all symbols to get metadata (status, assets)
                    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
                        exchange_data = await resp.json()
                        symbols_raw = exchange_data.get('symbols', [])
                        
                    # 2. Fetch 24h ticker data to get Volume
                    async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as t_resp:
                        tickers_all = await t_resp.json()
                        vol_map = {t['symbol']: float(t['quoteVolume']) for t in tickers_all}
                        
                    # 3. Filter and Rank
                    ticker_data = [] # List of (symbol, quote_volume)
                    for s in symbols_raw:
                        if s.get('status') == 'TRADING' and s.get('quoteAsset') == 'USDT':
                            raw_sym = s.get('symbol')
                            base = s.get('baseAsset')
                            unified_sym = f"{base}/USDT"
                            qv = vol_map.get(raw_sym, 0)
                            ticker_data.append((unified_sym, qv))
                    
                    # Sort by Volume Descending (High to Low)
                    ticker_data.sort(key=lambda x: x[1], reverse=True)
                    
                    # 🎯 Ultra-Targeted Discovery: Skip top 300, Take next 100 (Deep Tail Gems)
                    # Skipping up to Rank 300 virtually guarantees that mid-high caps like SKY/AAVE/BAT are GONE.
                    discovery_range = ticker_data[300:400] 
                    self.symbols = [s for s, _ in discovery_range]
                    for s, v in discovery_range:
                        # Conservative MC proxy: Volume * 50
                        self.volumes_24h[s] = v * 50.0 
                        
                    logging.info(f"💎 Ultra Binance Discovery: Targeted {len(self.symbols)} tail gems (Rank 300-400 by Volume).")
                    logging.info(f"Sample Symbols: {self.symbols[:10]}")
            except Exception as e:
                logging.error(f"Native Binance Discovery FAILED: {e}")
                
        # Original logic fallback if native fails
        if not self.symbols and os.getenv("CEX_SYMBOLS") == "AUTO":
            try:
                # Use load_markets which handles rate limits better
                markets = await self.exchange.load_markets()
                candidates = []
                
                market_items = markets.values() if isinstance(markets, dict) else markets
                
                for m in market_items:
                    sym = m.get('symbol', '')
                    if m.get('active') and m.get('linear') and sym.endswith('/USDT:USDT'):
                        # Support multiple info keys
                        info = m.get('info', {})
                        qv = float(info.get('quoteVolume') or info.get('volume') or 0)
                        if 1_000_000 < qv:
                            candidates.append((sym, qv))
                            self.volumes_24h[sym] = qv
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                # 🎯 [FIX] Fallback must also skip top coins
                self.symbols = [s for s, _ in candidates[100:150]]
                for s, v in candidates[100:150]:
                    self.volumes_24h[s] = v 
            except Exception as e:
                logging.error(f"Binance Discovery FAILED: {e}")
                self.symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "HBAR/USDT:USDT", "JASMY/USDT:USDT"]
                logging.info("Binance Fallback to bare minimum. Check region blocks.")
        else:
            for s in self.symbols:
                self.volumes_24h[s] = 10_000_000 # Default fallback
        
        # Re-initialize state containers for final symbol list
        self.trade_buffers   = {s: [] for s in self.symbols}
        self.last_chunk_time = {s: time.time() for s in self.symbols}
        self.history         = {s: pd.DataFrame() for s in self.symbols}
        self.oi_data         = {s: 0.0 for s in self.symbols}
        
        logging.info(f"FuturesCollector initialized for {self.exchange_id} ({len(self.symbols)} symbols).")

    async def watch_combined_trades(self):
        """
        Watches all symbols using a single Combined Stream connection.
        """
        import json
        import websockets
        
        # Build streams: btcusdt@aggTrade/ethusdt@aggTrade/...
        streams = []
        sym_map = {} # BTCUSDT -> BTC/USDT
        for s in self.symbols:
            clean = s.replace('/', '').lower()
            streams.append(f"{clean}@aggTrade")
            sym_map[clean.upper()] = s
            
        ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
        
        logging.info(f"Starting Combined Binance WS for {len(self.symbols)} symbols")
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    while True:
                        msg = await ws.recv()
                        res = json.loads(msg)
                        if 'data' not in res: continue
                        
                        data = res['data']
                        native_sym = data['s'] # e.g. BTCUSDT
                        unified_sym = sym_map.get(native_sym, native_sym)
                        
                        # p: price, q: quantity, m: isBuyerMaker, E: event time
                        trade = {
                            'price': float(data['p']),
                            'amount': float(data['q']),
                            'side': 'sell' if data['m'] else 'buy',
                            'timestamp': data['E'],
                            'received_at': time.time()
                        }
                        self.trade_buffers[unified_sym].append(trade)
                        
                        # 🚀 DB Logging (Buffering for speed)
                        self.tick_buffer.append((
                            datetime.now(timezone.utc),
                            self.exchange_id, unified_sym, trade['price'], trade['amount'], trade['side'], data.get('m', False)
                        ))
            except Exception as e:
                logging.error(f"Combined Binance WS Error: {e}")
                await asyncio.sleep(5)

    async def scheduler_loop(self):
        """Fires process_chunk for every symbol every 5 seconds (Protocol GHOST Speed)."""
        logging.info(f"Scheduler started: GHOST MODE (5s interval)")
        while True:
            await asyncio.sleep(5)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on Binance: {e}")

    async def watch_oi(self, symbol: str):
        """Watch Open Interest for the symbol."""
        logging.info(f"Starting OI watcher for {symbol}")
        while True:
            try:
                import aiohttp
                clean_sym = symbol.replace('/', '').upper().replace(':USDT', '')
                url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={clean_sym}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        self.oi_data[symbol] = float(data.get('openInterest', 0))
                await asyncio.sleep(30) # Increased speed
            except Exception as e:
                # logging.error(f"Error in {symbol} OI loop: {e}")
                await asyncio.sleep(30)

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

        # === NEW FEATURES for v5 (Tick-Level DNA) ===
        costs = df['price'] * df['amount']
        avg_trade_size = float(costs.mean()) if not costs.empty else 0.0
        max_trade_size = float(costs.max()) if not costs.empty else 0.0
        median_trade_size = float(costs.median()) if not costs.empty else 0.0
        
        buy_ratio = len(buy_df) / (len(df) + 1e-9)
        
        # Acceleration: (trades in last 15s) / (trades in first 15s)
        mid_ts = df['received_at'].min() + 15
        acceleration = (len(df[df['received_at'] >= mid_ts]) + 1) / (len(df[df['received_at'] < mid_ts]) + 1)
        
        prices = df['price']
        price_impact = (prices.max() - prices.iloc[0]) / (prices.iloc[0] + 1e-9) if len(prices) >= 2 else 0.0

        # === FEATURE: OI + Price Change (Protocol GHOST) ===
        current_oi = self.oi_data[symbol]
        prev_row = self.history[symbol].iloc[-1] if not self.history[symbol].empty else None
        
        prev_oi = prev_row['oi'] if prev_row is not None else current_oi
        prev_price = prev_row['price_end'] if prev_row is not None else price_end
        
        oi_change = (current_oi - prev_oi) / (prev_oi + 1e-9)
        pc_change = (price_end - prev_price) / (prev_price + 1e-9)

        # === FEATURE: VPVR Vacuum Score ===
        vacuum_score = 0.0
        hist_df = self.history[symbol]
        if len(hist_df) >= 30:
            price_min = hist_df['price_end'].min()
            price_max = hist_df['price_end'].max()
            if price_max > price_min:
                bins = np.linspace(price_min, price_max, 20)
                counts, _ = np.histogram(hist_df['price_end'], bins=bins, weights=hist_df['volume'])
                poc_idx = np.argmax(counts)
                
                # Check where current price sits in histogram
                curr_idx = np.digitize(price_end, bins) - 1
                if 0 <= curr_idx < len(counts):
                    # Vacuum = relative emptiness vs POC
                    vacuum_score = 1.0 - (counts[curr_idx] / (counts[poc_idx] + 1e-9))

        # === Update rolling history ===
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

        # Need at least 20 candles to compute meaningful Z-scores
        if len(self.history[symbol]) < 20: return

        # === Z-Scores ===
        hist = self.history[symbol]
        
        # Pre-accumulation (using 30-min window from history)
        if len(hist) >= 20:
             pre_vol_mid = hist['volume'].iloc[:len(hist)//2].mean()
             pre_vol_late = hist['volume'].iloc[len(hist)//2:].mean()
             pre_accum_z = (pre_vol_late - pre_vol_mid) / (hist['volume'].std() + 1e-9)
        else:
             pre_accum_z = 0.0

        vol_z  = (vol       - hist['volume'].mean())    / (hist['volume'].std()    or 1)
        pc_z   = (pc_change - hist['pc_change'].mean()) / (hist['pc_change'].std() or 1)
        oi_z   = (oi_change - hist['oi_change'].mean()) / (hist['oi_change'].std() or 1)

        # ── 30秒スキャンサマリー ──
        vol_24h = self.volumes_24h.get(symbol, 10_000_000)
        s1_ok = vol_24h <= self.detector.MAX_MARKET_CAP
        s2_ok = s1_ok and (vol_z > self.detector.VOL_Z_THRESHOLD and pc_z > self.detector.PC_Z_THRESHOLD)

        logging.info(
            f"📊 30s [{symbol}] $={price_end:.4f} "
            f"vol_z={vol_z:+.2f} pc_z={pc_z:+.2f} rush={std_rush:.1f} buy_r={buy_ratio:.2f} "
            f"| STAGE 1: {'OK' if s1_ok else 'SKIP'} | STAGE 2: {'ANOMALY' if s2_ok else 'normal'}"
        )

        # === Build feature dict for v6 (Protocol GHOST) ===
        features = {
            'symbol': symbol,
            'exchange': self.exchange_id,
            'price': price_end,
            'vol_z': vol_z,
            'pc_z': pc_z,
            'pre_accum_z': pre_accum_z,
            'std_rush': std_rush,
            'avg_trade_size': avg_trade_size,
            'max_trade_size': max_trade_size,
            'median_trade_size': median_trade_size,
            'buy_ratio': buy_ratio,
            'acceleration': acceleration,
            'price_impact': price_impact,
            'market_cap': vol_24h,
            'oi_z': oi_z,
            'oi_change': oi_change,
            'pc_change': pc_change,
            'vacuum_score': vacuum_score
        }

        result = self.detector.check_event(features)
        is_pump = result[0] if isinstance(result, tuple) else result
        if is_pump:
            logging.warning(
                f"🚀 PUMP SIGNAL: {symbol} | "
                f"vol_z={vol_z:.2f} pc_z={pc_z:.2f} "
                f"oi_z={oi_z:.2f} rush={std_rush:.2f}"
            )
            if hasattr(self, 'auditor'):
                asyncio.create_task(self.auditor.add_signal(symbol, price_end))

    async def flush_ticks_loop(self):
        """Periodically flushes the tick buffer to DB in bulk."""
        while True:
            await asyncio.sleep(2) # Flush every 2 seconds
            if self.tick_buffer:
                batch = self.tick_buffer[:]
                self.tick_buffer = []
                await self.logger.log_ticks_batch(batch)

    async def run(self):
        await self.initialize()
        watchers = []
        
        # 🚀 Use Combined WebSocket instead of individual ones
        watchers.append(asyncio.create_task(self.watch_combined_trades()))
        watchers.append(asyncio.create_task(self.flush_ticks_loop())) # 🚀 Start flusher
        
        # OI watchers still need to poll per-symbol (Binance FAPI restriction)
        for s in self.symbols:
            watchers.append(asyncio.create_task(self.watch_oi(s)))
            await asyncio.sleep(0.1)
        
        # Independent 30s scheduler
        watchers.append(asyncio.create_task(self.scheduler_loop()))
        
        # 5-min status logger
        async def status_logger():
            while True:
                await asyncio.sleep(300)
                logging.info(f"📊 CEX MONITOR: Actively watching {len(self.symbols)} symbols on {self.exchange_id}...")
        
        watchers.append(asyncio.create_task(status_logger()))
        await asyncio.gather(*watchers)

if __name__ == "__main__":
    # symbols = ['BTC/USDT:USDT', 'ETH/USDT:USDT']
    collector = FuturesCollector(exchange_id='binance', symbols=['BTC/USDT:USDT'])
    asyncio.run(collector.run())
