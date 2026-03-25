import subprocess
import concurrent.futures

events = [
    ("IRUSDT", "2026-03-16"),
    ("AIAUSDT", "2026-03-17"),
    ("PTBUSDT", "2026-03-18"),
    ("RDNTUSDT", "2026-03-19"),
    ("BRUSDT", "2026-03-19"),
    ("MAGMAUSDT", "2026-03-19"),
    ("GUNUSDT", "2026-03-20"),
    ("A2ZUSDT", "2026-03-20"),
    ("BTRUSDT", "2026-03-20"),
    ("LIGHTUSDT", "2026-03-20"),
    ("ONTUSDT", "2026-03-21"),
    ("DUSKUSDT", "2026-03-21"),
]

def fetch_pair(symbol, date):
    print(f"--- Fetching {symbol} for {date} ---")
    # Fetch Binance
    subprocess.run(["python", "scripts/fetch_binance_ticks.py", "--symbol", symbol, "--date", date])
    # Fetch Bybit
    subprocess.run(["python", "scripts/fetch_bybit_ticks.py", "--symbol", symbol, "--date", date])

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(fetch_pair, s, d) for s, d in events]
    concurrent.futures.wait(futures)

print("Batch Fetch Completed.")
