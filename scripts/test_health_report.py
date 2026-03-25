import asyncio
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pump_ai.health_monitor import HealthMonitor
from database.db_manager import DatabaseManager

async def test_health():
    db = DatabaseManager()
    await db.connect()
    
    monitor = HealthMonitor(db)
    # Simulate some stats
    monitor.scan_count = 1250
    monitor.alert_count = 2
    
    print("Testing 1h Heartbeat...")
    await monitor.send_heartbeat()
    
    print("Testing 4h Detailed Report...")
    await monitor.send_detailed_report()
    
    print("✅ Health reports sent to Discord!")

if __name__ == "__main__":
    asyncio.run(test_health())
