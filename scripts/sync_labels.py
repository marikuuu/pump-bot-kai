import asyncio
import os
import pandas as pd
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def sync_labels():
    dsn = os.getenv("DATABASE_URL", "postgresql://admin:password@127.0.0.1:5433/pump_ai")
    csv_path = "data_pipeline/eth-labels/data/csv/accounts.csv"
    
    if not os.path.exists(csv_path):
        print(f"❌ Error: {csv_path} not found.")
        return

    print(f"Reading labels from {csv_path}...")
    # Read first 100,000 for proof of concept (can be expanded)
    df = pd.read_csv(csv_path, nrows=100000)
    
    conn = await asyncpg.connect(dsn=dsn)
    print("Syncing to database...")
    
    # Batch insert logic
    data = []
    for _, row in df.iterrows():
        data.append((
            str(row['address']).lower(),
            f"{row.get('label', '')}: {row.get('name', '')}",
            'eth-labels'
        ))
    
    query = """
    INSERT INTO wallet_labels (address, label, source)
    VALUES ($1, $2, $3)
    ON CONFLICT (address) DO UPDATE SET
        label = EXCLUDED.label,
        last_seen = NOW();
    """
    
    await conn.executemany(query, data)
    print(f"✅ Successfully synced {len(data)} labels to PostgreSQL.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(sync_labels())
