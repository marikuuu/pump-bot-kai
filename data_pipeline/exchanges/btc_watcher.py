import asyncio
import logging
import pandas as pd
import ccxt.pro as ccxt
from datetime import datetime, timezone
from pump_ai.notifier import DiscordNotifier

class BTCWatcher:
    """
    Monitors BTC price for sudden crashes and sends Discord alerts.
    """
    def __init__(self, interval_sec: int = 60):
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        self.notifier = DiscordNotifier()
        self.history = pd.DataFrame(columns=['timestamp', 'price'])
        self.interval_sec = interval_sec
        self.shared_detector = None # Will be injected from main.py
        # Optimized Thresholds: 
        # Warning: -1.0% in 15m
        # Crash: -2.0% in 15m OR -3.5% in 1h
        self.CRASH_15M = -0.02
        self.CRASH_1H = -0.035
        self.WARN_15M = -0.01

    async def start(self):
        logging.info("BTC Watcher started (Crash monitoring active).")
        while True:
            try:
                ticker = await self.exchange.fetch_ticker('BTC/USDT:USDT', params={'type': 'perpetual'})
                price = ticker['last']
                now = datetime.now(timezone.utc)
                
                new_row = {'timestamp': now, 'price': price}
                self.history = pd.concat([self.history, pd.DataFrame([new_row])]).iloc[-360:] # Store last 6h if 1m interval
                
                await self.check_for_crash(price)
                await asyncio.sleep(self.interval_sec)
            except Exception as e:
                logging.error(f"Error in BTC Watcher: {e}")
                await asyncio.sleep(10)

    async def check_for_crash(self, current_price: float):
        if len(self.history) < 15: return
        
        # 15 min check
        price_15m_ago = self.history.iloc[-15]['price']
        change_15m = (current_price - price_15m_ago) / price_15m_ago
        
        # 1h check
        if len(self.history) >= 60:
            price_1h_ago = self.history.iloc[-60]['price']
            change_1h = (current_price - price_1h_ago) / price_1h_ago
        else:
            change_1h = 0
            
        # Update Shared Detector status
        if self.shared_detector:
            self.shared_detector.set_btc_status(change_15m, change_1h)

        if change_15m <= self.CRASH_15M:
            await self.notifier.send_crash_alert("BTC/USDT", f"{change_15m:.2%}", "Last 15m")
            logging.warning(f"BTC 15M CRASH detected: {change_15m:.2%}")
        elif change_1h <= self.CRASH_1H:
            await self.notifier.send_crash_alert("BTC/USDT", f"{change_1h:.2%}", "Last 1h")
            logging.warning(f"BTC 1H CRASH detected: {change_1h:.2%}")
        elif change_15m <= self.WARN_15M:
            logging.info(f"BTC Weakness detected: {change_15m:.2%} (Last 15m)")

if __name__ == "__main__":
    watcher = BTCWatcher()
    asyncio.run(watcher.start())
