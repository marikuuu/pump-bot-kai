import asyncio
import logging
import os
from web3 import AsyncWeb3, WebSocketProvider
from web3.utils.subscriptions import LogsSubscription, LogsSubscriptionContext
from eth_abi.abi import decode
from dotenv import load_dotenv

# Optional: integrate existing database manager if needed later
# from database.db_manager import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')

# Uniswap V2 Swap Event Signature
SWAP_V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

class AlchemyWSMonitor:
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self.w3 = AsyncWeb3(WebSocketProvider(self.rpc_url))
        
    async def swap_handler(self, ctx: LogsSubscriptionContext):
        log = ctx.result
        try:
            data = bytes.fromhex(log["data"][2:])
            # decode amount0In, amount1In, amount0Out, amount1Out
            amounts = decode(["uint256", "uint256", "uint256", "uint256"], data)
            
            # The sender is usually the first indexed topic after the event signature
            sender = decode(["address"], log["topics"][1])[0] if len(log["topics"]) > 1 else "Unknown"
            
            # Additional pump detection cross-referencing logic would go here
            # e.g., checking if `sender` is in our eth-labels/Smart Money database
            logging.info(f"Swap detected from {sender} - Amounts: {amounts}")
        except Exception as e:
            logging.error(f"Error decoding swap log: {e}")

    async def start(self):
        logging.info("Connecting to Alchemy WebSocket...")
        if not await self.w3.is_connected():
            logging.error("Failed to connect to Ethereum node.")
            return

        logging.info("Connected. Subscribing to Uniswap V2 Swaps...")
        
        # We listen to all Uniswap V2 swaps globally. For pump detection, 
        # we might want to filter by specific token addresses in `address` parameter.
        await self.w3.subscription_manager.subscribe([
            LogsSubscription(
                label="uniswap-v2-swaps",
                topics=[SWAP_V2_TOPIC],
                handler=self.swap_handler,
            ),
        ])
        
        # Keep the subscription running
        await self.w3.subscription_manager.handle_subscriptions()

async def main():
    load_dotenv()
    # E.g., wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
    alchemy_url = os.getenv("ALCHEMY_WS_URL")
    if not alchemy_url:
        logging.warning("ALCHEMY_WS_URL not found in environment. Using placeholder.")
        alchemy_url = "wss://eth-mainnet.g.alchemy.com/v2/DEMO_KEY"
        
    monitor = AlchemyWSMonitor(alchemy_url)
    await monitor.start()

if __name__ == "__main__":
    asyncio.run(main())
