import asyncio
import logging
import os
from dotenv import load_dotenv

# ── SSLなど外部ライブラリのノイズを最初に抑制（basicConfigより前に必須）──
logging.getLogger('telethon').setLevel(logging.WARNING)
logging.getLogger('telethon.network').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('ccxt').setLevel(logging.WARNING)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("bot_log.txt"), logging.StreamHandler()]
)

# Import our components (Cleaned for v5.3)
# legacy component removal

async def main():
    """
    Project IZANAGI Pump Detection System (CEX Only).
    DEXモニター（Nansen）は run_nansen.py で別プロセス起動してください。
    """
    logging.info("Starting High-Precision Pump Detection System (CEX Mode)...")
    logging.info("TIP: DEX/Nansen monitor -> run separately: python run_nansen.py")

    # --- Component Configuration ---

    # 1. CEX Collectors (Binance + MEXC)
    from pump_ai.detector import PumpDetector
    shared_detector = PumpDetector() # Shared logic and BTC state

    cex_symbols_env = os.getenv("CEX_SYMBOLS", "AIN/USDT:USDT")
    cex_symbols = [] if cex_symbols_env == "AUTO" else cex_symbols_env.split(",")
    
    from data_pipeline.exchanges.futures_collector import FuturesCollector
    from data_pipeline.exchanges.mexc_collector import MexcCollector
    from data_pipeline.exchanges.bybit_collector import BybitCollector
    from data_pipeline.exchanges.bitget_collector import BitgetCollector

    # 4. System Health Monitor & Auditor & Discord Bot
    from pump_ai.health_monitor import HealthMonitor
    from pump_ai.signal_auditor import SignalAuditor
    from pump_ai.discord_bot import run_bot
    from database.db_manager import DatabaseManager
    db = DatabaseManager()
    await db.connect()
    
    # Update collectors to use the SHARED DB instance to avoid "Too many connections"
    binance_collector = FuturesCollector(exchange_id='binance', symbols=cex_symbols, db_manager=db)
    binance_collector.detector = shared_detector
    
    mexc_collector = MexcCollector(db_manager=db)
    mexc_collector.detector = shared_detector

    bybit_collector = BybitCollector(db_manager=db)
    bybit_collector.detector = shared_detector

    bitget_collector = BitgetCollector(db_manager=db)
    bitget_collector.detector = shared_detector

    social_monitor = None

    # 3. BTC Crash Watcher (Linked to Shared Detector)
    from data_pipeline.exchanges.btc_watcher import BTCWatcher
    btc_watcher = BTCWatcher()
    btc_watcher.shared_detector = shared_detector # Inject detector to update its state

    health_monitor = HealthMonitor(db)
    signal_auditor = SignalAuditor(db)

    # 5. Inject auditor
    binance_collector.auditor = signal_auditor
    # mexc_collector.auditor = signal_auditor # Optional: add if MEXC signals should be audited

    # --- Run Loops ---
    tasks = [
        binance_collector.run(),
        mexc_collector.run(),
        bybit_collector.run(),
        bitget_collector.run(),
        btc_watcher.start(),
        health_monitor.run_loop(),
        signal_auditor.run_loop(),
        run_bot(db)
    ]

    # Social monitor tasks removed

    logging.info("All systems started. CEX Pump Detection active.")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("System gracefully shutting down...")
