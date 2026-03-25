import asyncio
import json
import logging
import httpx
import websockets
from datetime import datetime
from database.db_manager import DatabaseManager

class MexcClient:
    def __init__(self, db: DatabaseManager, symbols: list):
        self.db = db
        # MEXC uses an underscore format for contracts like BTC_USDT
        self.symbols = [s.replace('USDT', '_USDT').upper() if 'USDT' in s and '_' not in s else s.upper() for s in symbols]
        self.ws_url = "wss://contract.mexc.com/ws"
        self.rest_url = "https://contract.mexc.com/api/v1/contract"

    async def fetch_open_interest(self):
        """MEXCのREST APIからOIを取得して保存"""
        async with httpx.AsyncClient() as client:
            while True:
                for symbol in self.symbols:
                    try:
                        response = await client.get(
                            f"{self.rest_url}/open_interest/{symbol}"
                        )
                        if response.status_code == 200:
                            data = response.json()
                            if data.get("success") and "data" in data:
                                oi_asset = float(data["data"].get("openInterest", 0))
                                time_ts = datetime.utcnow()
                                
                                query = """
                                    INSERT INTO open_interest (time, exchange, symbol, oi_asset)
                                    VALUES ($1, 'MEXC', $2, $3)
                                    ON CONFLICT DO NOTHING
                                """
                                await self.db.execute(query, time_ts, symbol.replace('_', ''), oi_asset)
                    except Exception as e:
                        logging.error(f"Error fetching MEXC OI for {symbol}: {e}")
                
                await asyncio.sleep(300)

    async def connect_websocket(self):
        """WebSocketで1分足のOHLCVデータを受信して保存"""
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    req = {
                        "method": "sub.kline",
                        "param": {
                            "symbol": ",".join(self.symbols),
                            "interval": "Min1"
                        }
                    }
                    await ws.send(json.dumps(req))
                    
                    logging.info(f"Connected to MEXC WebSocket for {len(self.symbols)} symbols.")
                    async for message in ws:
                        await self._handle_ws_message(message)
            except websockets.ConnectionClosed:
                logging.warning("MEXC WebSocket disconnected. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"MEXC WS error: {e}")
                await asyncio.sleep(5)

    async def _handle_ws_message(self, message):
        data = json.loads(message)
        if "channel" in data and data["channel"] == "push.kline":
            kline = data.get("data", {})
            symbol = data.get("symbol", "").replace('_', '')
            
            # MEXC streams continuous updates. We need to handle them carefully.
            # Assuming we save every minute's open timestamp
            time_ts = datetime.fromtimestamp(kline.get("t", 0))
            open_p, high_p, low_p, close_p, volume, quote_volume = (
                float(kline.get("o", 0)), float(kline.get("h", 0)), float(kline.get("l", 0)),
                float(kline.get("c", 0)), float(kline.get("v", 0)), float(kline.get("a", 0))
            )
            
            query = """
                INSERT INTO ohlcv (time, exchange, symbol, open, high, low, close, volume, quote_volume)
                VALUES ($1, 'MEXC', $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT DO NOTHING
            """
            await self.db.execute(query, time_ts, symbol, open_p, high_p, low_p, close_p, volume, quote_volume)

    async def start(self):
        await asyncio.gather(
            self.connect_websocket(),
            self.fetch_open_interest()
        )
