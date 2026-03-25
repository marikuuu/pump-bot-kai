import asyncpg
import logging
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    """
    Handles connection pooling and operations for PostgreSQL + TimescaleDB
    """
    def __init__(self):
        self.pool = None
        self.dsn = os.getenv("DATABASE_URL", "postgresql://admin:password@127.0.0.1:5433/pump_ai")

    async def connect(self):
        if self.pool:
            return
        try:
            logging.info(f"Connecting to DB: {self.dsn}")
            # Limit connections for VPS stability (Total CEX + DEX + Stats < 100)
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=1,
                max_size=20,
                command_timeout=60
            )
            logging.info("Connected to PostgreSQL database pool (max_size=20).")
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    async def execute(self, query: str, *args):
        if not self.pool:
            raise Exception("Database not connected. Call connect() first.")
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetch(self, query: str, *args):
        if not self.pool:
            raise Exception("Database not connected. Call connect() first.")
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def close(self):
        if self.pool:
            await self.pool.close()
            logging.info("Database connection pool closed.")
