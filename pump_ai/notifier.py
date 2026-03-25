import aiohttp
import json
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

class DiscordNotifier:
    """
    Handles sending alerts to Discord via Webhooks.
    """
    def __init__(self, webhook_url: str = None):
        self.webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
        if not self.webhook_url:
            logging.error("Discord Webhook URL not found.")

    async def send_alert(self, title: str, message: str, color: int = 0xFF0000, fields: list = None):
        if not self.webhook_url:
            return

        embed = {
            "title": title,
            "description": message,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "Project IZANAGI | Engine v4.0"}
        }
        if fields:
            embed["fields"] = fields

        payload = {"embeds": [embed]}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.webhook_url, json=payload) as resp:
                    if resp.status != 204:
                        logging.error(f"Failed to send Discord alert: {resp.status}")
        except Exception as e:
            logging.error(f"Error sending Discord alert: {e}")

    async def send_pump_alert(self, symbol: str, lead_time: str, move: str,
                               price: float = 0, vol_z: float = 0,
                               pc_z: float = 0, oi_z: float = 0, rush: float = 0):
        title = f"🚀 PUMP DETECTED: {symbol}"
        desc = (
            f"**バックテスト精度90%+** のシグナルパターンが一致しました。\n"
            f"**エントリーウィンドウ（目安）**: 検知後 30〜120 秒以内"
        )
        fields = [
            {"name": "💲 現在価格",         "value": f"`${price:.5f}`",          "inline": True},
            {"name": "📈 Vol Z-score",       "value": f"`{vol_z:+.2f}`",          "inline": True},
            {"name": "💹 Price Z-score",     "value": f"`{pc_z:+.2f}`",           "inline": True},
            {"name": "📡 OI Z-score",        "value": f"`{oi_z:+.2f}`",           "inline": True},
            {"name": "⚡ Rush Orders (σ)",   "value": f"`{rush:.1f}`",            "inline": True},
            {"name": "🎯 想定リターン",      "value": f"`{move}`",                "inline": True},
            {"name": "🛡️ Stage 1",          "value": "✅ 銘柄フィルター通過",    "inline": True},
            {"name": "📊 Stage 2",          "value": "✅ 統計的異常 検知",       "inline": True},
            {"name": "🤖 Stage 3/4",        "value": "✅ Rush + ML 確定",        "inline": True},
        ]
        await self.send_alert(title, desc, color=0x00FF7F, fields=fields)

    async def send_crash_alert(self, symbol: str, price_drop: str, window: str):
        title = f"⚠️ MARKET CRASH WARNING: {symbol}"
        msg = f"**Price Drop:** {price_drop}\n**Window:** {window}\n**Status:** BTC 急落中！！"
        await self.send_alert(title, msg, color=0xFF0000)
