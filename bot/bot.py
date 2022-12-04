import logging

import discord
from discord.ext import commands

from config import bot_config

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s] %(message)s"
)

# bot init
bot = commands.Bot(help_command=None,
                   case_insensitive=True,
                   activity=discord.Game("WATCHING YOU"),
                   intents=discord.Intents.all()
                   )

bot.load_extension("cogs.Admin")
bot.load_extension("cogs.CogManager")
bot.load_extension("cogs.Archiver")


bot.run(bot_config.TOKEN)
