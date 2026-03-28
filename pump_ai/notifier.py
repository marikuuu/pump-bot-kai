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
                               pc_z: float = 0, oi_z: float = 0, rush: float = 0,
                               whale_stack: int = 0, vacuum_score: float = 0.0,
                               is_ghost: bool = False):
        
        # UI Elements
        tv_symbol = symbol.replace("/", "").upper()
        tv_link = f"https://www.tradingview.com/chart/?symbol=BINANCE:{tv_symbol}PERP"
        
        if is_ghost:
            title = f"👻 [PROTOCOL GHOST] {symbol} DETECTED"
            color = 0x8A2BE2 # BlueViolet
            status_text = "💎 **GHOST TRIGGER (Whale Accumulation + Vacuum)**"
        else:
            title = f"🚀 [STANDARD PUMP] {symbol} DETECTED"
            color = 0x00FF7F # SpringGreen
            status_text = "💹 **STANDARD MOMENTUM BREAKOUT**"

        # Progress / Stars
        stars = "⭐" * min(whale_stack, 5) if whale_stack > 0 else "🌑 (No Stack)"
        v_bars = int(vacuum_score * 10)
        v_gauge = f"[`{'■' * v_bars}{'-' * (10 - v_bars)}`] {int(vacuum_score * 100)}%"

        desc = (
            f"{status_text}\n"
            f"**Action**: 30〜120秒以内に指値検討\n"
            f"**Chart**: [TradingViewで開く]({tv_link})"
        )
        
        fields = [
            {"name": "💲 現在価格",         "value": f"`${price:.5f}`",          "inline": True},
            {"name": "🐋 Whale DNA",        "value": f"{stars}",                 "inline": True},
            {"name": "🌌 Vacuum Score",     "value": f"{v_gauge}",               "inline": False},
            {"name": "📈 Vol Z-score",       "value": f"`{vol_z:+.2f}`",          "inline": True},
            {"name": "💹 Price Z-score",     "value": f"`{pc_z:+.2f}`",           "inline": True},
            {"name": "📡 OI Z-score",        "value": f"`{oi_z:+.2f}`",           "inline": True},
            {"name": "⚡ Rush Orders (σ)",   "value": f"`{rush:.1f}`",            "inline": True},
            {"name": "🎯 想定リターン",      "value": f"`{move}`",                "inline": True},
            {"name": "🔍 先読み精度",        "value": "`90%+ (v6.1)`",            "inline": True},
        ]
        
        await self.send_alert(title, desc, color=color, fields=fields)

    async def send_crash_alert(self, symbol: str, price_drop: str, window: str):
        title = f"⚠️ MARKET CRASH WARNING: {symbol}"
        msg = f"**Price Drop:** {price_drop}\n**Window:** {window}\n**Status:** BTC 急落中！！"
        await self.send_alert(title, msg, color=0xFF0000)
