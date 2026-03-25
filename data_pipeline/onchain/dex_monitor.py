import asyncio
import os
import time
import logging
from datetime import datetime, timezone
import asyncpg
from web3 import AsyncWeb3, WebSocketProvider
from web3.utils.subscriptions import LogsSubscription, LogsSubscriptionContext
from eth_abi.abi import decode
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Uniswap V2 Swap Event Topic
SWAP_V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

class DexMonitor:
    def __init__(self):
        self.ws_url = os.getenv("RPC_WS_URL", "wss://ethereum-rpc.publicnode.com").strip()
        from database.db_manager import DatabaseManager
        from data_pipeline.onchain.wallet_labels import WalletLabelManager
        self.db = DatabaseManager()
        self.label_manager = WalletLabelManager(db_manager=self.db)
        logging.info(f"Initializing Web3 with RPC: [{self.ws_url}]")
        self.w3 = AsyncWeb3(WebSocketProvider(self.ws_url))
        self.event_count = 0
        self.last_log_time = time.time()

    async def swap_handler(self, ctx: LogsSubscriptionContext):
        log = ctx.result
        try:
            # Decode data and topics
            # Handle HexBytes or bytes correctly
            data_bytes = log["data"]
            if isinstance(data_bytes, str):
                if data_bytes.startswith("0x"): data_bytes = data_bytes[2:]
                data_bytes = bytes.fromhex(data_bytes)
            elif hasattr(data_bytes, "hex"): # HexBytes
                data_bytes = bytes(data_bytes)
            
            # Uniswap V2 pair contract address (the one emitting the event)
            contract_address = self.w3.to_checksum_address(log["address"])
            tx_hash = log["transactionHash"].hex()
            
            # Need at least 2 topics for a valid swap
            if len(log["topics"]) < 3:
                return

            # topic[1] = sender, topic[2] = to
            sender = self.w3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
            recipient = self.w3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])

            # Decode the 4 amounts from data
            # Decode the 4 amounts from data
            try:
                amounts = decode(["uint256", "uint256", "uint256", "uint256"], data_bytes)
                amount_in = float(amounts[0] if amounts[0] > 0 else amounts[1])
                amount_out = float(amounts[2] if amounts[2] > 0 else amounts[3])
            except Exception as decode_err:
                logging.error(f"Decode error: {decode_err}")
                return

            # Label Lookup
            self.event_count += 1
            if time.time() - self.last_log_time > 300: # Every 5 minutes
                logging.info(f"📡 DEX MONITOR: Processed {self.event_count} swap events from Uniswap V2.")
                self.last_log_time = time.time()
                
            sender_info = await self.label_manager.get_wallet_info(sender)
            recip_info = await self.label_manager.get_wallet_info(recipient)
            
            is_smart = await self.label_manager.is_smart_money(sender) or \
                       await self.label_manager.is_smart_money(recipient)
            
            labels = []
            if sender_info: labels.append(f"Sender: {sender_info['label']}")
            if recip_info: labels.append(f"Recipient: {recip_info['label']}")
            label_str = ", ".join(labels)

            if is_smart:
                logging.warning(f"!!! PSEUDO-NANSEN SIGNAL: Smart Money ({label_str}) buying via {tx_hash} !!!")

            # Save to DB
            query = """
                INSERT INTO dex_swaps (
                    time, chain, tx_hash, sender, recipient, 
                    amount_in, amount_out, is_smart_money, wallet_label
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """
            await self.db.execute(query, datetime.now(timezone.utc), 'Ethereum', tx_hash, 
                                 sender, recipient, amount_in, amount_out, is_smart, label_str)

        except Exception as e:
            logging.error(f"Error processing swap log: {e}")

    async def start(self):
        await self.db.connect()
        logging.info(f"Connecting to RPC WebSocket: {self.ws_url}")
        
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                async with self.w3 as w3:
                    # Subscribe to Uniswap V2 Swap events across all pairs
                    await w3.subscription_manager.subscribe([
                        LogsSubscription(
                            label="uniswap-v2-swaps",
                            topics=[SWAP_V2_TOPIC],
                            handler=self.swap_handler,
                        ),
                    ])
                    logging.info("Subscribed to Uniswap V2 Swap events. Listening...")
                    retry_count = 0 # Reset on success
                    
                    # Keep connection alive
                    await w3.subscription_manager.handle_subscriptions()
            except Exception as e:
                retry_count += 1
                logging.error(f"WebSocket connection error (Attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    await asyncio.sleep(5 * retry_count) # Exponential backoff
                else:
                    logging.critical("Max retries reached for RPC. On-chain monitoring disabled.")
                    break

if __name__ == "__main__":
    monitor = DexMonitor()
    try:
        asyncio.run(monitor.start())
    except KeyboardInterrupt:
        logging.info("Monitor stopped by user.")
