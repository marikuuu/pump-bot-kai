import discord
from discord import app_commands
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

async def hard_reset():
    token = os.getenv("DISCORD_TOKEN")
    client = discord.Client(intents=discord.Intents.default())
    
    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        
        # 1. Clear Global Commands
        print("FORCING CLEAR GLOBAL COMMANDS...")
        await client.http.bulk_upsert_global_commands(client.application_id, [])
        print("Global commands cleared on API.")
        
        # 2. Clear Guild Commands
        guild_id = os.getenv("DISCORD_GUILD_ID")
        if guild_id:
            print(f"FORCING CLEAR GUILD COMMANDS for {guild_id}...")
            await client.http.bulk_upsert_guild_commands(client.application_id, int(guild_id), [])
            print("Guild commands cleared on API.")
            
        print("Hard Reset Complete. All commands are now deleted on the server.")
        await client.close()

if __name__ == "__main__":
    asyncio.run(hard_reset())
