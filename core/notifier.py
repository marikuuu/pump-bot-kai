import requests
import json
import os
import platform
from datetime import datetime

class IzanagiNotifier:
    def __init__(self, discord_webhook_url=None, telegram_token=None, telegram_chat_id=None):
        self.discord_webhook_url = discord_webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
        self.telegram_token = telegram_token or os.getenv("TELEGRAM_TOKEN")
        self.telegram_chat_id = telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID")
        
    def notify(self, level, symbol, message, metadata=None):
        """
        Send notification across all enabled channels.
        Level 1: Accumulation (Info)
        Level 2: Vacuum (Warning)
        Level 3: GOD SIGNAL (Critical)
        """
        title = f"[{'🔥' * level}] IZANAGI Level {level}: {symbol}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        full_message = f"**{title}**\nTime: {timestamp}\n{message}"
        if metadata:
            meta_str = "\n".join([f"- {k}: {v}" for k, v in metadata.items()])
            full_message += f"\n\n📊 **Metrics:**\n{meta_str}"

        print(f"\n📢 [NOTIFICATION LEVEL {level}] {symbol}: {message}")
        
        # 1. Discord
        if self.discord_webhook_url:
            self._send_discord(title, full_message, level)
            
        # 2. Telegram
        if self.telegram_token and self.telegram_chat_id:
            self._send_telegram(full_message)
            
        # 3. Local Sound (Level 3 only)
        if level >= 3:
            self._play_alert_sound()

    def _send_discord(self, title, content, level):
        colors = {
            1: 3447003, # Blue
            2: 15105570, # Orange
            3: 15158332  # Red
        }
        payload = {
            "embeds": [{
                "title": title,
                "description": content,
                "color": colors.get(level, 3447003)
            }]
        }
        try:
            requests.post(self.discord_webhook_url, json=payload, timeout=5)
        except Exception as e:
            print(f"⚠️ Discord notification failed: {e}")

    def _send_telegram(self, content):
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": content,
            "parse_mode": "Markdown"
        }
        try:
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"⚠️ Telegram notification failed: {e}")

    def _play_alert_sound(self):
        """Play a beep or sound depending on OS"""
        try:
            if platform.system() == "Windows":
                import winsound
                winsound.Beep(1000, 500) # 1000Hz for 500ms
                winsound.Beep(1500, 500)
            else:
                print("\a") # Generic terminal bell
        except:
            pass

if __name__ == "__main__":
    # Test
    notifier = IzanagiNotifier()
    notifier.notify(1, "TESTUSDT", "仕込みの痕跡を検知しました。", {"Z-Score": 2.1})
    notifier.notify(3, "GODUSDT", "神のシグナル発火！", {"Z-Score": 12.5, "Vacuum": "0.8%"})
