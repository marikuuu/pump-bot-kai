import discord
from discord import app_commands
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

async def clear_global():
    token = os.getenv("DISCORD_TOKEN")
    client = discord.Client(intents=discord.Intents.default())
    tree = app_commands.CommandTree(client)
    
    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        print("Clearing global commands...")
        tree.clear(guild=None)
        await tree.sync()
        print("Global commands cleared successfully.")
        
        guild_id = os.getenv("DISCORD_GUILD_ID")
        if guild_id:
            print(f"Clearing guild commands for {guild_id}...")
            guild = discord.Object(id=int(guild_id))
            tree.clear(guild=guild)
            await tree.sync(guild=guild)
            print("Guild commands cleared.")
            
        await client.close()

if __name__ == "__main__":
    asyncio.run(clear_global())
