import asyncio
import logging
from aiohttp import web
import json
from datetime import datetime
from database.db_manager import DatabaseManager

class AlchemyWebhookServer:
    def __init__(self, db: DatabaseManager, port: int = 8080):
        self.db = db
        self.port = port
        self.app = web.Application()
        self.app.router.add_post("/alchemy-webhook", self.handle_webhook)

    async def handle_webhook(self, request):
        try:
            data = await request.json()
            logging.info(f"Received Alchemy webhook: {json.dumps(data)[:200]}...")
            
            # 通常、AlchemyのCustom Webhook (Graphql) または Address Activity が送られてくる
            # ここではAddress Activityのモック処理を実装
            if "event" in data and "activity" in data["event"]:
                activities = data["event"]["activity"]
                for act in activities:
                    chain = data.get("event", {}).get("network", "ETH_MAINNET")
                    tx_hash = act.get("hash")
                    sender = act.get("fromAddress")
                    recipient = act.get("toAddress")
                    amount = float(act.get("value", 0))
                    token_address = act.get("rawContract", {}).get("address", "ETH")
                    time_ts = datetime.utcnow() # Webhook受信時刻
                    
                    query = """
                        INSERT INTO dex_swaps (time, chain, tx_hash, sender, recipient, token_in_address, amount_in, is_smart_money)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
                    """
                    await self.db.execute(query, time_ts, chain, tx_hash, sender, recipient, token_address, amount)
            
            return web.Response(text="OK", status=200)
        except Exception as e:
            logging.error(f"Error processing Alchemy webhook: {e}")
            return web.Response(text="Error", status=500)

    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        logging.info(f"Alchemy Webhook Server started on port {self.port}")
        
        # Keep running
        while True:
            await asyncio.sleep(3600)
