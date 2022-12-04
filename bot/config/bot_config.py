from datetime import datetime
import os

import discord

TOKEN = os.environ.get("TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")


async def NOTIFY_TO_OWNER(bot, message: str):
    owner = await bot.fetch_user(OWNER_ID)
    dmCh = await owner.create_dm()
    await dmCh.send(
            content="Bot Status Notification",
            embed=discord.Embed().add_field(
                    name="Status",
                    value=message
            ).set_footer(
                    text=str(datetime.now())
            )
    )
