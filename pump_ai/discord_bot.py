import discord
from discord import app_commands
import os
import logging
from scripts.symbol_intel_pro import SymbolIntelPro

class IzanagiBot(discord.Client):
    def __init__(self, db_manager):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.db = db_manager

    async def setup_hook(self):
        guild_id = os.getenv("DISCORD_GUILD_ID")
        try:
            if guild_id:
                guild = discord.Object(id=int(guild_id))
                # 1. Clear existing guild commands to avoid duplicates
                self.tree.clear_commands(guild=guild)
                # 2. Copy global commands (currently only /intel) to guild
                self.tree.copy_global_to(guild=guild)
                # 3. Sync to guild
                synced = await self.tree.sync(guild=guild)
                logging.info(f"Discord Slash Commands consolidated to Guild {guild_id}: {len(synced)} commands.")
            else:
                # Fallback to global sync
                synced = await self.tree.sync()
                logging.info(f"Discord Slash Commands synced globally: {len(synced)} commands.")
        except Exception as e:
            logging.error(f"Failed to sync commands: {e}")

    async def on_ready(self):
        logging.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logging.info("------ Bot is ready: Use /intel <symbol> ------")

def setup_bot(db_manager):
    bot = IzanagiBot(db_manager)

    @bot.tree.command(name="intel", description="Get Intelligence Report for a specific symbol (e.g. AIN, BTC)")
    @app_commands.describe(symbol="The symbol to analyze (e.g. AIN, BTC, SOL)")
    async def _intel(interaction: discord.Interaction, symbol: str):
        symbol = symbol.upper().strip()
        logging.info(f"Command /intel {symbol} received from {interaction.user}")
        try:
            await interaction.response.defer(thinking=True)
            intel = SymbolIntelPro(bot.db)
            report = await intel.generate_report(symbol, send_webhook=False)
            
            embed = discord.Embed(
                title=f"🕵️‍♂️ {symbol} 市場分析レポート (V2.6 PRO)",
                description=report.split("市場分析レポート (V2.6 PRO)**")[-1].strip(),
                color=0x7146FF
            )
            embed.set_footer(text="Project IZANAGI | Pseudo-Nansen Intelligence")
            embed.set_thumbnail(url="https://coinmarketcap.com/apple-touch-icon.png")
            
            await interaction.followup.send(embed=embed)
            logging.info(f"Command /intel {symbol} processed successfully")
        except Exception as e:
            logging.error(f"Error in /intel command: {e}")
            error_msg = f"❌ エラーが発生しました: {e}"
            await interaction.followup.send(error_msg, ephemeral=True)

    return bot

async def run_bot(db_manager):
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        logging.error("DISCORD_TOKEN not found in .env. Skipping Discord Bot.")
        return
    
    bot = setup_bot(db_manager)
    try:
        await bot.start(token)
    except Exception as e:
        logging.error(f"Failed to start Discord Bot: {e}")
