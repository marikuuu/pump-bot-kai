import os
import glob
import logging
import pandas as pd
from typing import Optional, List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')

class WalletLabelManager:
    """
    Simulates Nansen's Smart Money and Entity labeling by parsing the `eth-labels` 
    dataset and maintaining an in-memory/cache index of addresses.
    """
    def __init__(self, db_manager=None):
        from database.db_manager import DatabaseManager
        self.db = db_manager or DatabaseManager()
        self.smart_money_labels = ['smart_money', 'fund', 'whale', 'high_conviction']

    async def get_wallet_info(self, address: str) -> Optional[Dict]:
        """Queries PostgreSQL for entity information"""
        query = "SELECT label, source, last_seen FROM wallet_labels WHERE address = $1"
        row = await self.db.fetch(query, address.lower())
        if row:
            return {
                'label': row[0]['label'],
                'source': row[0]['source'],
                'last_seen': row[0]['last_seen']
            }
        return None

    async def is_smart_money(self, address: str) -> bool:
        """Returns true if the label indicates smart money/whale"""
        info = await self.get_wallet_info(address)
        if info:
            label_lower = info['label'].lower()
            return any(sm in label_lower for sm in self.smart_money_labels)
        return False
        
    async def add_arkham_label(self, address: str, name: str, label_type: str = 'smart_money'):
        """Inserts a new label from Arkham/Manual discovery into the DB"""
        query = """
        INSERT INTO wallet_labels (address, label, source)
        VALUES ($1, $2, $3)
        ON CONFLICT (address) DO UPDATE SET label = EXCLUDED.label;
        """
        await self.db.execute(query, address.lower(), f"{label_type}: {name}", 'Arkham_Discovery')
        logging.info(f"DB Label Sync: {name} ({address})")

if __name__ == "__main__":
    manager = WalletLabelManager()
    manager.load_labels()
    # Mocking Nansen smart money discovery
    manager.add_arkham_label("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "vitalik.eth", "whale")
    print("Is Vitalik smart money?", manager.is_smart_money("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"))
