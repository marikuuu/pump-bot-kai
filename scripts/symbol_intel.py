import asyncio
import sys
import os
from datetime import datetime, timedelta, timezone
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.db_manager import DatabaseManager
from pump_ai.notifier import DiscordNotifier

class SymbolIntel:
    """
    Generates a Nansen-style professional report for a given symbol.
    Combines CEX volume, DEX swaps, and Smart Money flows.
    """
    def __init__(self, db_manager):
        self.db = db_manager
        self.notifier = DiscordNotifier()

    async def generate_report(self, symbol: str):
        symbol = symbol.upper()
        if not symbol.endswith('/USDT:USDT') and not symbol.endswith('/USDT'):
            query_symbol = f"{symbol}/USDT:USDT"
        else:
            query_symbol = symbol

        print(f"🔍 Generating report for {query_symbol}...")
        
        # 1. Fetch Token Info
        token_info = await self.db.fetch("SELECT id, network, market_cap FROM tokens WHERE symbol = $1", symbol.split('/')[0])
        token_id = token_info[0][0] if token_info else None
        
        # 2. Analyze DEX Swaps (Last 24h)
        swaps_24h = await self.db.fetch("""
            SELECT count(*) as count, 
                   sum(amount_in) as vol_in, 
                   count(CASE WHEN is_smart_money THEN 1 END) as smart_count
            FROM dex_swaps 
            WHERE (sender ILIKE $1 OR recipient ILIKE $2 OR wallet_label ILIKE $3)
            AND time > NOW() - INTERVAL '24 hours'
        """, f"%{symbol}%", f"%{symbol}%", f"%{symbol}%")
        
        # 3. Analyze Smart Money Labels
        smart_labels = await self.db.fetch("""
            SELECT label, count(*) 
            FROM wallet_labels 
            WHERE label ILIKE $1
            GROUP BY label LIMIT 5
        """, f"%{symbol}%")

        # 4. Mock CEX Data (In real case, fetch from CCXT)
        price_now = 1.234 # Placeholder
        vol_change = "+45.2%" # Placeholder

        report = f"""
🕵️‍♂️ **PSEUDO-NANSEN INTELLIGENCE: {symbol}**
---
📊 **MARKET SUMMARY**
• Price: ${price_now} ({vol_change} 24h)
• Market Cap: ${token_info[0][2] if token_info else 'Unknown'}
• Network: {token_info[0][1] if token_info else 'Multi-chain'}

🌊 **ON-CHAIN FLOWS (DEX 24h)**
• Total Swaps: {swaps_24h[0][0]}
• Est. Volume: ${swaps_24h[0][1] or 0:,.0f}
• **Smart Money Activity**: {swaps_24h[0][2]} transactions

🧠 **WHALE INSIGHTS**
{chr(10).join([f"• {r[0]}: {r[1]} active wallets" for r in smart_labels]) if smart_labels else "• No specific smart money clusters detected yet."}

📝 **VERDICT**
{'🔥 HIGH CONVICTION: Significant smart money accumulation detected.' if (swaps_24h[0][2] or 0) > 5 else '⚖️ NEUTRAL: Normal retail activity.'}
        """
        
        await self.notifier.send_alert(f"🕵️‍♂️ Intel Report: {symbol}", report, color=0xF1C40F)
        return report

async def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/symbol_intel.py <SYMBOL>")
        return

    db = DatabaseManager()
    await db.connect()
    intel = SymbolIntel(db)
    await intel.generate_report(sys.argv[1])

if __name__ == "__main__":
    asyncio.run(main())
