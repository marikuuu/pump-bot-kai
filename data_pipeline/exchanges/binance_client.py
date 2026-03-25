import asyncio
import json
import logging
import httpx
import websockets
from datetime import datetime
from database.db_manager import DatabaseManager

class BinanceClient:
    def __init__(self, db: DatabaseManager, symbols: list):
        self.db = db
        self.symbols = [s.upper() for s in symbols]
        self.ws_url = "wss://fstream.binance.com/ws"
        self.rest_url = "https://fapi.binance.com/fapi/v1"

    async def fetch_open_interest(self):
        """定期的にREST APIからOI（未決済建玉）を取得してDBに保存"""
        async with httpx.AsyncClient() as client:
            while True:
                for symbol in self.symbols:
                    try:
                        response = await client.get(
                            f"{self.rest_url}/openInterest", 
                            params={"symbol": symbol}
                        )
                        if response.status_code == 200:
                            data = response.json()
                            oi_asset = float(data.get("openInterest", 0))
                            time_ts = datetime.fromtimestamp(data.get("time", 0) / 1000.0)
                            
                            query = """
                                INSERT INTO open_interest (time, exchange, symbol, oi_asset)
                                VALUES ($1, 'Binance', $2, $3)
                                ON CONFLICT DO NOTHING
                            """
                            await self.db.execute(query, time_ts, symbol, oi_asset)
                            logging.debug(f"Binance {symbol} OI: {oi_asset}")
                    except Exception as e:
                        logging.error(f"Error fetching Binance OI for {symbol}: {e}")
                
                # 5分ごとにOIを更新
                await asyncio.sleep(300)

    async def connect_websocket(self):
        """WebSocketで1分足(kline_1m)をリアルタイム受信"""
        streams = "/".join([f"{s.lower()}@kline_1m" for s in self.symbols])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        
        while True:
            try:
                async with websockets.connect(url) as ws:
                    logging.info(f"Connected to Binance WebSocket for {len(self.symbols)} symbols.")
                    async for message in ws:
                        await self._handle_ws_message(message)
            except websockets.ConnectionClosed:
                logging.warning("Binance WebSocket disconnected. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Binance WS error: {e}")
                await asyncio.sleep(5)

    async def _handle_ws_message(self, message):
        data = json.loads(message)
        if "data" in data and "k" in data["data"]:
            kline = data["data"]["k"]
            # klineが確定(is_closed=True)したとき、または毎秒の更新を保存
            is_closed = kline["x"]
            if is_closed:
                symbol = data["data"]["s"]
                time_ts = datetime.fromtimestamp(kline["t"] / 1000.0)
                open_p = float(kline["o"])
                high_p = float(kline["h"])
                low_p = float(kline["l"])
                close_p = float(kline["c"])
                volume = float(kline["v"])
                quote_volume = float(kline["q"])
                trades = int(kline["n"])

                query = """
                    INSERT INTO ohlcv (time, exchange, symbol, open, high, low, close, volume, quote_volume, trades_count)
                    VALUES ($1, 'Binance', $2, $3, $4, $5, $6, $7, $8, $9)
                """
                await self.db.execute(
                    query, time_ts, symbol, open_p, high_p, low_p, close_p, volume, quote_volume, trades
                )
                logging.debug(f"Saved 1m kline for {symbol}")

    async def start(self):
        """WebsocketとRESTポーリングを並行して実行"""
        await asyncio.gather(
            self.connect_websocket(),
            self.fetch_open_interest()
        )
