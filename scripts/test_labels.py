import asyncio
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_pipeline.onchain.wallet_labels import WalletLabelManager

async def test_labels():
    manager = WalletLabelManager()
    await manager.db.connect()
    
    # Test manual injection
    test_addr = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" # Vitalik
    await manager.add_arkham_label(test_addr, "Vitalik Buterin", "whale")
    
    # Test lookup
    info = await manager.get_wallet_info(test_addr)
    print(f"Lookup Result: {info}")
    
    is_sm = await manager.is_smart_money(test_addr)
    print(f"Is Smart Money? {is_sm}")
    
    # Test eth-labels lookup (one from the 100k)
    # 0x07e594aa718bb872b526... from previous head check
    eth_addr = "0x000000000000000000000000000000000000dead" # NULL addr usually tagged
    info_eth = await manager.get_wallet_info(eth_addr)
    print(f"Eth-Labels Lookup: {info_eth}")

if __name__ == "__main__":
    asyncio.run(test_labels())
