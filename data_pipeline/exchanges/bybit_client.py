import asyncio
import json
import logging
import httpx
import websockets
from datetime import datetime
from database.db_manager import DatabaseManager

class BybitClient:
    def __init__(self, db: DatabaseManager, symbols: list):
        self.db = db
        # Bybit uses USDT instead of USD for linear perps
        self.symbols = [s.upper() for s in symbols]
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.rest_url = "https://api.bybit.com/v5"

    async def fetch_open_interest(self):
        """REST APIから未決済建玉(OI)を取得して保存"""
        async with httpx.AsyncClient() as client:
            while True:
                for symbol in self.symbols:
                    try:
                        response = await client.get(
                            f"{self.rest_url}/market/open-interest",
                            params={"category": "linear", "symbol": symbol, "intervalTime": "5min"}
                        )
                        if response.status_code == 200:
                            data = response.json()
                            if data.get("retCode") == 0 and data["result"].get("list"):
                                oi_data = data["result"]["list"][0]
                                oi_asset = float(oi_data["openInterest"])
                                time_ts = datetime.fromtimestamp(int(oi_data["timestamp"]) / 1000.0)
                                
                                query = """
                                    INSERT INTO open_interest (time, exchange, symbol, oi_asset)
                                    VALUES ($1, 'Bybit', $2, $3)
                                    ON CONFLICT DO NOTHING
                                """
                                await self.db.execute(query, time_ts, symbol, oi_asset)
                                logging.debug(f"Bybit {symbol} OI: {oi_asset}")
                    except Exception as e:
                        logging.error(f"Error fetching Bybit OI for {symbol}: {e}")
                
                await asyncio.sleep(300)

    async def connect_websocket(self):
        """WebSocketで1分足のOHLCVデータを受信して保存"""
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    args = [f"kline.1.{s}" for s in self.symbols]
                    req = {"op": "subscribe", "args": args}
                    await ws.send(json.dumps(req))
                    
                    logging.info(f"Connected to Bybit WebSocket for {len(self.symbols)} symbols.")
                    async for message in ws:
                        await self._handle_ws_message(message)
            except websockets.ConnectionClosed:
                logging.warning("Bybit WebSocket disconnected. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Bybit WS error: {e}")
                await asyncio.sleep(5)

    async def _handle_ws_message(self, message):
        data = json.loads(message)
        if "topic" in data and data["topic"].startswith("kline"):
            symbol = data["topic"].split(".")[-1]
            for kline in data.get("data", []):
                # Save when closed
                if kline.get("confirm"):
                    time_ts = datetime.fromtimestamp(int(kline["start"]) / 1000.0)
                    open_p = float(kline["open"])
                    high_p = float(kline["high"])
                    low_p = float(kline["low"])
                    close_p = float(kline["close"])
                    volume = float(kline["volume"])

                    query = """
                        INSERT INTO ohlcv (time, exchange, symbol, open, high, low, close, volume)
                        VALUES ($1, 'Bybit', $2, $3, $4, $5, $6, $7)
                    """
                    await self.db.execute(query, time_ts, symbol, open_p, high_p, low_p, close_p, volume)
                    logging.debug(f"Saved 1m kline for {symbol} (Bybit)")

    async def start(self):
        await asyncio.gather(
            self.connect_websocket(),
            self.fetch_open_interest()
        )
