import asyncio
import numpy as np
import pandas as pd
from datetime import datetime
import sys
import os
from collections import deque

from websocket_streamer import BinanceStreamer
from core.notifier import IzanagiNotifier

# Global Memory State
class IzanagiState:
    def __init__(self):
        self.ask_depth = {'spot': {}, 'futures': {}} # Live vacuum state
        self.trade_history = {} # sym -> deque of volumes for Z-Score
        self.history_window = 1000 # Number of trades to keep for Z-Score
        self.whale_threshold = 20000  # $20k per tick to count as extreme anomaly
        self.notifier = IzanagiNotifier()
        
        # v6.1: Signal Throttling
        self.signal_counts = {} # sym -> Level 3 fire count
        self.base_prices = {} # sym -> first detection price
        self.MAX_GOD_SIGNALS = 5
        self.PRICE_LIMIT_FACTOR = 1.3 # 30% price limit from start
        
state = IzanagiState()

async def process_stream_data(market_type, msg):
    """
    Callback function that processes every tick and orderbook update from Binance.
    """
    if 'data' not in msg: return
    data = msg['data']
    stream = msg['stream']
    
    # --- STAGE 1: ORDERBOOK VACUUM MONITORING (Depth) ---
    if 'depth20' in stream:
        sym = data['s']
        asks = data['a'] # list of [price, qty]
        if len(asks) == 0: return
        
        best_ask = float(asks[0][0])
        limit_price = best_ask * 1.01 # Top 1% of the ask ceiling
        
        total_ask_vol = 0
        for a in asks:
            ap = float(a[0])
            if ap <= limit_price:
                aq = float(a[1])
                total_ask_vol += (ap * aq) # Value in USDT of the sell wall
                
        # Store current vacuum state
        state.ask_depth[market_type][sym] = total_ask_vol
        
        # Level 2 Signal: Physical Vacuum Warning
        if total_ask_vol < 50000: # Threshold for "extremely hollow"
            state.notifier.notify(2, sym, "板の真空状態を検知。急騰準備完了の可能性あり。", {
                "Ask Depth (1%)": f"${total_ask_vol:,.0f}",
                "Market": market_type.upper()
            })

    # --- STAGE 2: TAKER BUY ARCHITECTURE (Trades) ---
    elif 'aggTrade' in stream:
        sym = data['s']
        is_buyer_maker = data['m'] # False = Taker Buy
        price = float(data['p'])
        qty = float(data['q'])
        quote_qty = price * qty
        
        # Track volume in history for Z-Score calculation
        if sym not in state.trade_history:
            state.trade_history[sym] = deque(maxlen=state.history_window)
        
        if not is_buyer_maker: # Taker Buy
            state.trade_history[sym].append(quote_qty)
            
            # Level 1 Signal: Whale Accumulation Trace
            if quote_qty >= state.whale_threshold:
                # Calculate Z-Score if we have enough data
                if len(state.trade_history[sym]) > 100:
                    vols = np.array(state.trade_history[sym])
                    mu = vols.mean()
                    std = vols.std()
                    z_score = (quote_qty - mu) / std if std > 0 else 0
                    
                    if z_score > 3.0:
                        state.notifier.notify(1, sym, "有力な買い集めの痕跡。クジラが動き出しました。", {
                            "Trade Size": f"${quote_qty:,.0f}",
                            "Z-Score": f"{z_score:.2f}",
                            "Price": price
                        })
                        
                        # TRIGGER CHECK: Does this whale strike hit during a Market Vacuum?
                        check_god_signal(market_type, sym, price, z_score, quote_qty)

def check_god_signal(market_type, sym, trg_price, z_score, volume):
    """
    Level 3: THE GOD SIGNAL
    Fires when a high Z-Score buy hits a Hollow Orderbook.
    v6.1: Limits notification frequency and price range.
    """
    depth = state.ask_depth[market_type].get(sym, None)
    if depth is None: return
    
    # 1. Base Criteria
    if depth < 100000 and z_score > 4.0:
        # 2. Level 6.1: Throttling & Price Filtering
        count = state.signal_counts.get(sym, 0)
        base_price = state.base_prices.get(sym, trg_price)
        
        # Only notify if we haven't exceeded count OR if we are still in the "Low Price Range"
        price_threshold = base_price * state.PRICE_LIMIT_FACTOR
        
        if count < state.MAX_GOD_SIGNALS and trg_price <= price_threshold:
            # Set base price on first signal
            if count == 0:
                state.base_prices[sym] = trg_price
            
            state.signal_counts[sym] = count + 1
            
            state.notifier.notify(3, sym, f"👼【神のシグナル発火】(通知 {count+1}/{state.MAX_GOD_SIGNALS})👼", {
                "Z-Score": f"{z_score:.2f}",
                "Ask Depth Remaining": f"${depth:,.0f}",
                "Entry Signal Price": trg_price,
                "Base Price": f"${base_price:,.4f}",
                "Trade Volume": f"${volume:,.0f}"
            })
        else:
            # Silent logging only
            pass

async def main():
    print("\n" + "="*50)
    print(" 👁️  PROJECT IZANAGI v6.1: Throttled Alert Engine")
    print("="*50 + "\n")
    
    # Potential Targets (Real-time monitoring)
    targets = ["btcusdt", "ethusdt", "solusdt", "dogeusdt", "xrpudst", "pepeusdt", "pippinusdt"]
    print(f"📡 Initializing Real-Time Websockets for {len(targets)} Targets...")
    
    # Initialize streamer with multi-market support if needed
    streamer = BinanceStreamer(targets, process_stream_data)
    await streamer.start()

if __name__ == "__main__":
    try:
         asyncio.run(main())
    except KeyboardInterrupt:
         print("\n[IZANAGI] Shutting down.")
