import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ccxt.pro as ccxtpro
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
    Collects real-time Futures (Swap) data including Open Interest.
    """
    def __init__(self, exchange_id: str = 'binance', symbols: List[str] = None, db_manager: DatabaseManager = None):
        self.exchange_id = exchange_id
        self.symbols = symbols or []
        self.exchange = getattr(ccxtpro, exchange_id)({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'} # USDT-M Futures (fapi)
        })
        self.db = db_manager or DatabaseManager()
        self.logger = DataLogger(self.db)
        self.detector = PumpDetector()
        
        self.trade_buffers: Dict[str, List[Dict]] = {s: [] for s in self.symbols}
        self.last_chunk_time: Dict[str, float] = {s: time.time() for s in self.symbols}
        self.history: Dict[str, pd.DataFrame] = {s: pd.DataFrame() for s in self.symbols}
        self.oi_data: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.market_caps: Dict[str, float] = {}

    async def initialize(self):
        if not self.db.pool:
            await self.db.connect()
        
        # Auto-Discovery Logic
        if os.getenv("CEX_SYMBOLS") == "AUTO":
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
                            self.market_caps[sym] = qv
                
                candidates.sort(key=lambda x: x[1], reverse=True)
                self.symbols = [s for s, _ in candidates[:50]]
                logging.info(f"Binance Discovery Success: {len(self.symbols)} symbols found.")
            except Exception as e:
                logging.error(f"Binance Discovery FAILED: {e}")
                self.symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "HBAR/USDT:USDT", "JASMY/USDT:USDT"]
                logging.info("Binance Fallback to bare minimum. Check region blocks.")
        else:
            for s in self.symbols:
                self.market_caps[s] = 30_000_000
        
        # Re-initialize state containers for final symbol list
        self.trade_buffers   = {s: [] for s in self.symbols}
        self.last_chunk_time = {s: time.time() for s in self.symbols}
        self.history         = {s: pd.DataFrame() for s in self.symbols}
        self.oi_data         = {s: 0.0 for s in self.symbols}
        
        logging.info(f"FuturesCollector initialized for {self.exchange_id} ({len(self.symbols)} symbols).")

    async def watch_trades(self, symbol: str):
        """Pure data collection — just append trades to buffer. Scheduler handles processing."""
        logging.info(f"Starting Futures trade watcher for {symbol}")
        while True:
            try:
                trades = await self.exchange.watch_trades(symbol)
                for trade in trades:
                    trade['received_at'] = time.time()
                    self.trade_buffers[symbol].append(trade)
                    # 🚀 Real-time tick logging to DB
                    asyncio.create_task(self.logger.log_tick(
                        self.exchange_id, 
                        symbol, 
                        trade['price'], 
                        trade['amount'], 
                        trade['side'], 
                        trade.get('info', {}).get('m', False) # is_buyer_maker for Binance
                    ))
            except Exception as e:
                logging.error(f"Error in {symbol} trade loop: {e}")
                await asyncio.sleep(5)

    async def scheduler_loop(self):
        """Fires process_chunk for every symbol every 30 seconds, independent of trade flow."""
        logging.info(f"Scheduler started: will scan {len(self.symbols)} symbols every 30s")
        while True:
            await asyncio.sleep(30)
            for symbol in self.symbols:
                try:
                    await self.process_chunk(symbol)
                except Exception as e:
                    logging.error(f"Error processing {symbol} on Binance: {e}")

    async def watch_oi(self, symbol: str):
        """
        Watch Open Interest for the symbol.
        Note: CCXT Pro support for watch_open_interest varies by exchange.
        Fallback to polling if sub-second updates aren't available.
        """
        logging.info(f"Starting OI watcher for {symbol}")
        while True:
            try:
                # Optimized for Binance/Bybit
                oi_raw = await self.exchange.fetch_open_interest(symbol)
                self.oi_data[symbol] = float(oi_raw['openInterestAmount'])
                await asyncio.sleep(10) # OI doesn't move as fast as trades
            except Exception as e:
                logging.error(f"Error in {symbol} OI loop: {e}")
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

        # === FEATURE: OI Change ===
        current_oi = self.oi_data[symbol]
        prev_oi = self.history[symbol]['oi'].iloc[-1] if not self.history[symbol].empty else current_oi
        oi_change = (current_oi - prev_oi) / (prev_oi + 1e-9)

        # === Update rolling history ===
        price_change = (price_end - df['price'].iloc[0]) / df['price'].iloc[0]
        
        # Pre-accumulation (using 30-min window from history)
        hist_v = self.history[symbol]['volume'] if not self.history[symbol].empty else pd.Series()
        if len(hist_v) >= 20:
             pre_vol_mid = hist_v.iloc[:len(hist_v)//2].mean()
             pre_vol_late = hist_v.iloc[len(hist_v)//2:].mean()
             pre_accum_z = (pre_vol_late - pre_vol_mid) / (hist_v.std() + 1e-9)
        else:
             pre_accum_z = 0.0

        new_row = {
            'time': datetime.now(timezone.utc),
            'volume': vol,
            'price_change': price_change,
            'oi': current_oi,
            'oi_change': oi_change,
            'std_rush': std_rush
        }
        self.history[symbol] = pd.concat([self.history[symbol], pd.DataFrame([new_row])]).iloc[-120:]

        # Need at least 20 candles to compute meaningful Z-scores
        if len(self.history[symbol]) < 20: return

        # === Z-Scores ===
        hist = self.history[symbol]
        vol_z  = (vol         - hist['volume'].mean())       / (hist['volume'].std()       or 1)
        pc_z   = (price_change - hist['price_change'].mean()) / (hist['price_change'].std() or 1)
        oi_z   = (oi_change   - hist['oi_change'].mean())    / (hist['oi_change'].std()    or 1)

        # ── 30秒スキャンサマリー ──
        market_cap = self.market_caps.get(symbol, 30_000_000)
        s1_ok = market_cap <= self.detector.MAX_MARKET_CAP
        s2_ok = s1_ok and (vol_z > self.detector.VOL_Z_THRESHOLD and pc_z > self.detector.PC_Z_THRESHOLD)

        logging.info(
            f"📊 30s [{symbol}] $={price_end:.4f} "
            f"vol_z={vol_z:+.2f} pc_z={pc_z:+.2f} rush={std_rush:.1f} buy_r={buy_ratio:.2f} "
            f"| STAGE 1: {'OK' if s1_ok else 'SKIP'} | STAGE 2: {'ANOMALY' if s2_ok else 'normal'}"
        )

        # === Build feature dict for v5 ===
        features = {
            'symbol': symbol,
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
            'market_cap': market_cap,
            'oi_z': oi_z
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

    async def run(self):
        await self.initialize()
        watchers = []
        for s in self.symbols:
            watchers.append(asyncio.create_task(self.watch_trades(s)))
            watchers.append(asyncio.create_task(self.watch_oi(s)))
            await asyncio.sleep(1.0)
        
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
