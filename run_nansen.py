"""
run_nansen.py - Nansen DEX Monitor を単独で起動するファイル
メインボット (main.py) とは完全に分離されているため、
ログが混在せずDEXトレースのみをクリーンに確認できます。

使い方:
    python run_nansen.py
"""
import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [NANSEN] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nansen_log.txt"),
        logging.StreamHandler()
    ]
)

from data_pipeline.onchain.dex_monitor import DexMonitor

async def main():
    logging.info("🌊 Nansen DEX Monitor starting (standalone mode)...")
    monitor = DexMonitor()
    await monitor.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Nansen Monitor stopped.")
