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
        query = "SELECT entity_name, label_type, source, updated_at FROM wallet_labels WHERE address = $1"
        rows = await self.db.fetch(query, address.lower())
        if rows:
            row = rows[0]
            return {
                'label': f"{row['entity_name']} ({row['label_type']})",
                'source': row['source'],
                'updated_at': row['updated_at']
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
        INSERT INTO wallet_labels (address, entity_name, label_type, source)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (address) DO UPDATE SET entity_name = EXCLUDED.entity_name, label_type = EXCLUDED.label_type;
        """
        await self.db.execute(query, address.lower(), name, label_type, 'Arkham_Discovery')
        logging.info(f"DB Label Sync: {name} ({address})")

if __name__ == "__main__":
    manager = WalletLabelManager()
    manager.load_labels()
    # Mocking Nansen smart money discovery
    manager.add_arkham_label("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "vitalik.eth", "whale")
    print("Is Vitalik smart money?", manager.is_smart_money("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"))
