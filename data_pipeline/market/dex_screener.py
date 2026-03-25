import requests
import logging
import asyncio
import os
import sys

# Add current directory for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# from database.db_manager import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')

class DexScreenerClient:
    """
    Tier 1 Nansen alternative: DEX Screener (Free API, no auth, 300req/min)
    Used for token discovery and universe filtering (Stage 1).
    """
    def __init__(self):
        self.base_url = "https://api.dexscreener.com/latest/dex"
        
    async def fetch_token_profile(self, chain_id: str, token_address: str):
        """Fetch pairs for a specific token"""
        url = f"{self.base_url}/tokens/{token_address}"
        try:
            # Running synchronous requests in a thread pool for simplicity
            response = await asyncio.to_thread(requests.get, url)
            response.raise_for_status()
            data = response.json()
            return data.get('pairs', [])
        except Exception as e:
            logging.error(f"Error fetching DEX Screener token profile: {e}")
            return []
            
    async def discover_new_pairs(self, chain_id: str = "ethereum"):
        """Fetch latest pairs across the platform or specific chain"""
        # Note: /latest/dex/search is versatile. A true 'new pairs' might require premium or scraping,
        # but we can search for trending or specific query keywords (e.g. WETH pairs).
        url = f"{self.base_url}/search?q={chain_id}"
        try:
            response = await asyncio.to_thread(requests.get, url)
            response.raise_for_status()
            data = response.json()
            pairs = data.get('pairs', [])
            
            # Filter low caps (e.g., market cap < 60M)
            low_caps = [
                p for p in pairs 
                if p.get('fdv', float('inf')) < 60_000_000 and p.get('volume', {}).get('h24', 0) > 100_000
            ]
            logging.info(f"Discovered {len(low_caps)} low-cap active pairs on {chain_id}")
            return low_caps
        except Exception as e:
            logging.error(f"Error searching DEX Screener: {e}")
            return []
            
async def main():
    client = DexScreenerClient()
    logging.info("Testing DEX Screener API...")
    
    # 1. Discover low cap pairs
    pairs = await client.discover_new_pairs("solana")
    for p in pairs[:5]:
        base = p.get('baseToken', {})
        logging.info(f"Target Token: {base.get('symbol')} ({base.get('address')}) - FDV: ${p.get('fdv')}")

if __name__ == "__main__":
    asyncio.run(main())
