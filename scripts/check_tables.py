import asyncio
import os
from dotenv import load_dotenv
from database.db_manager import DatabaseManager

async def check_tables():
    load_dotenv()
    db = DatabaseManager()
    await db.connect()
    
    # Check tables in public schema
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
    res = await db.fetch(query)
    print("\n--- Current Tables ---")
    for row in res:
        print(f"- {row['table_name']}")
    
    # Check columns of 'ticks' if it exists
    try:
        query_cols = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ticks' ORDER BY ordinal_position;"
        res_cols = await db.fetch(query_cols)
        if res_cols:
            print("\n--- Columns in 'ticks' ---")
            for col in res_cols:
                print(f"- {col['column_name']} ({col['data_type']})")
    except Exception as e:
        print(f"\nCould not check columns of 'ticks': {e}")

    await db.close()

if __name__ == "__main__":
    asyncio.run(check_tables())
