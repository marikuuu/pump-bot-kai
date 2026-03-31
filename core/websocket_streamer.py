import asyncio
import json
import websockets
from collections import defaultdict
from datetime import datetime

class BinanceStreamer:
    def __init__(self, symbols, callback):
        """
        Connects to Binance Spot & Futures WebSockets.
        Streams: `aggTrade` (for Taker Buys) and `depth20@100ms` (for Vacuum Depth).
        """
        self.symbols = [s.lower() for s in symbols]
        self.callback = callback
        
        # Spot: wss://stream.binance.com:9443/ws
        # Futures: wss://fstream.binance.com/ws
        self.spot_url = "wss://stream.binance.com:9443/stream?streams="
        self.futures_url = "wss://fstream.binance.com/stream?streams="
        
    def build_url(self, base, symbols):
        streams = []
        for s in symbols:
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@depth20@100ms")
        return base + "/".join(streams)

    async def _listen(self, url, market_type):
        while True:
            try:
                print(f"[{market_type.upper()}] Connecting to Stream...")
                async with websockets.connect(url, max_size=None) as ws:
                    print(f"[{market_type.upper()}] Connected successfully! Streaming {len(self.symbols)} pairs.")
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        await self.callback(market_type, data)
            except Exception as e:
                print(f"[{market_type.upper()}] WS Disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def start(self):
        spot_ws = self.build_url(self.spot_url, self.symbols)
        futures_ws = self.build_url(self.futures_url, self.symbols)
        
        await asyncio.gather(
            self._listen(spot_ws, 'spot'),
            self._listen(futures_ws, 'futures')
        )
