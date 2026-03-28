import asyncio
import os
import sys

# Ensure the project root is in path
sys.path.append(os.getcwd())

from pump_ai.notifier import DiscordNotifier

async def test_rich_alerts():
    notifier = DiscordNotifier()
    
    print("Sending GHOST sample alert...")
    await notifier.send_pump_alert(
        symbol="ACX/USDT",
        lead_time="GHOST TRIGGER",
        move="🚀 1.5x - 2.0x (Est.)",
        price=0.0452,
        vol_z=4.2,
        pc_z=0.15,
        oi_z=5.8,
        rush=3.5,
        whale_stack=4,
        vacuum_score=0.92,
        is_ghost=True
    )
    
    await asyncio.sleep(2)
    
    print("Sending STANDARD sample alert...")
    await notifier.send_pump_alert(
        symbol="SOL/USDT",
        lead_time="STANDARD PUMP",
        move="📈 1.1x - 1.2x (Est.)",
        price=145.20,
        vol_z=6.5,
        pc_z=3.2,
        oi_z=1.2,
        rush=12.5,
        whale_stack=0,
        vacuum_score=0.25,
        is_ghost=False
    )

    print("✅ Samples sent! Check your Discord channel.")

if __name__ == "__main__":
    asyncio.run(test_rich_alerts())
