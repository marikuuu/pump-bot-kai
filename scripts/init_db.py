import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def init_db():
    dsn = os.getenv("DATABASE_URL", "postgresql://admin:password@127.0.0.1:5433/pump_ai")
    print(f"Connecting to {dsn}...")
    
    try:
        conn = await asyncpg.connect(dsn=dsn)
        
        # Read the schema file
        with open('database/init_db.sql', 'r') as f:
            schema = f.read()
            
        print("Executing schema...")
        await conn.execute(schema)
        print("✅ Database initialized successfully with TimescaleDB hypertables.")
        
        await conn.close()
    except Exception as e:
        print(f"❌ Error initializing database: {e}")

if __name__ == "__main__":
    asyncio.run(init_db())
