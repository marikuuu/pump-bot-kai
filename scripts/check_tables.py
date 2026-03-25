import asyncio
import os
import sys

# 🚀 VPS / Subdirectory execution fix
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from database.db_manager import DatabaseManager

async def check_tables():
    load_dotenv()
    db = DatabaseManager()
    await db.connect()
    
    # Check tables in public schema and their counts
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
    res = await db.fetch(query)
    print("\n--- Current Tables & Row Counts ---")
    for row in res:
        table_name = row['table_name']
        try:
            count_res = await db.fetch(f"SELECT count(*) FROM {table_name}")
            count = count_res[0][0]
            print(f"- {table_name}: {count} rows")
        except Exception:
            print(f"- {table_name}: (could not count)")
    
    # Check columns of 'ticks' and 'dex_swaps'
    for table in ['ticks', 'dex_swaps']:
        try:
            query_cols = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position;"
            res_cols = await db.fetch(query_cols)
            if res_cols:
                print(f"\n--- Columns in '{table}' ---")
                for col in res_cols:
                    print(f"- {col['column_name']} ({col['data_type']})")
        except Exception as e:
            print(f"\nCould not check columns of '{table}': {e}")

    await db.close()

if __name__ == "__main__":
    asyncio.run(check_tables())
