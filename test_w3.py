from web3 import AsyncWeb3, WebSocketProvider
import asyncio
import logging

async def t():
    urls = [
        "wss://ethereum-rpc.publicnode.com",
        "wss://eth.public-rpc.com/ws",
        "wss://eth.drpc.org"
    ]
    for url in urls:
        print(f"TESTING URL: [{url}]")
        w3 = AsyncWeb3(WebSocketProvider(url))
        try:
            async with w3 as w:
                print(f"  SUCCESS: Connected to {url}")
                connected = await w.is_connected()
                print(f"  IS_CONNECTED: {connected}")
        except Exception as e:
            print(f"  FAILED: {e}")

if __name__ == "__main__":
    asyncio.run(t())
