import logging
import os

import discord
import mysql.connector
from discord.commands import slash_command
from discord.ext import commands


class Archiver(commands.Cog):
    def __init__(self, bot):
        self.bot: discord.Bot = bot

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def create_db_connection(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                charset="utf8mb4",
                collation="utf8mb4_general_ci",
                autocommit=False
        )

    def update_guild_data(self, guilds: list[discord.Guild], connection: mysql.connector.MySQLConnection):
        try:
            with connection.cursor() as cur:
                for guild in guilds:
                    cur.execute(
                            "INSERT INTO Guilds (id, name, icon_url) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name), icon_url = VALUES(icon_url)",
                            (guild.id, guild.name, guild.icon.url if guild.icon is not None else None))
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e

    def update_channel_data(self, channels: list[discord.abc.GuildChannel], connection: mysql.connector.MySQLConnection):
        try:
            with connection.cursor() as cur:
                for channel in channels:
                    cur.execute(
                            "INSERT INTO Channels (id, name, type, guild_id, category) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name)",
                            (channel.id, channel.name, channel.__class__.__name__, channel.guild.id, channel.category.id if channel.category else None))
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e

    def update_thread_data(self, threads: list[discord.Thread], connection: mysql.connector.MySQLConnection):
        try:
            with connection.cursor() as cur:
                for thread in threads:
                    cur.execute(
                            "INSERT INTO Threads (id, name, guild_id, channel_id) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name)",
                            (thread.id, thread.name, thread.guild.id, thread.parent.id))
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e

    def update_message_data(self, messages: list[discord.Message], connection: mysql.connector.MySQLConnection):
        try:
            with connection.cursor() as cur:
                for message in messages:
                    cur.execute(
                            "INSERT INTO Messages (id, content, guild_id, channel_id, author_id, created_at, edited_at) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(content), edited_at=VALUES(edited_at)",
                            (message.id, message.content, message.guild.id, message.channel.id, message.author.id, message.created_at, message.edited_at))
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e

    def update_user_data(self, users: list[discord.abc.User], connection: mysql.connector.MySQLConnection):
        try:
            with connection.cursor() as cur:
                for user in users:
                    cur.execute(
                            "INSERT INTO Users (id, name, discriminator, avatar_url) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name), discriminator=VALUES(discriminator), avatar_url=VALUES(avatar_url)",
                            (user.id, user.name, user.discriminator, user.avatar.url if user.avatar is not None else None))
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e

    # def update_custom_emoji_data(self, emojis: list[discord.Emoji], connection: mysql.connector.MySQLConnection):
    #     try:
    #         with connection.cursor() as cur:
    #             for emoji in emojis:
    #                 cur.execute(
    #                         "INSERT INTO CustomEmojis (id, guild_id, content) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(content)",
    #                         (emoji.id, emoji.guild.id, await emoji.read()))
    #         connection.commit()
    #     except Exception as e:
    #         connection.rollback()
    #         raise e

    def update_message_attachments_data(self, data: list[tuple[int, discord.Attachment, bytes]], connection: mysql.connector.MySQLConnection):
        try:
            with connection.cursor() as cur:
                for message_id, attachment, filebyte in data:
                    cur.execute(
                            "INSERT INTO MessageAttachments (id, message_id, filename, content_type, content) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(content)",
                            (attachment.id, message_id, attachment.filename, attachment.content_type, filebyte))
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e

    @slash_command(name="get_all")
    async def archive_all_messages(self, ctx: discord.ApplicationContext):
        await ctx.respond("Archiving all messages...", ephemeral=True)
        self.logger.info(f"[Start] {ctx.guild.name}")
        guilds = [ctx.guild]
        self.logger.info("[Progress] Acquiring Channels....")
        channels = ctx.guild.channels
        self.logger.info("[Progress] Acquiring Threads....")
        threads = [thread for channel in channels if isinstance(channel, discord.TextChannel) for thread in channel.threads]
        self.logger.info("[Progress] Acquiring Messages....")
        messages = [message for channel in channels if isinstance(channel, discord.TextChannel) async for message in channel.history(limit=None)] + [message for thread in
                                                                                                                                                     threads async for message
                                                                                                                                                     in
                                                                                                                                                     thread.history(
                                                                                                                                                             limit=None)]
        self.logger.info("[Progress] Creating Attachment Data....")
        attachment_data = [(message.id, attachment, await attachment.read()) for message in messages for attachment in message.attachments]

        self.logger.info("[Progress] Acquiring Users....")
        users = [message.author for message in messages]

        self.logger.info("[Progress] Dumping....")
        con = self.create_db_connection()
        self.update_guild_data(guilds, con)
        self.update_channel_data(channels, con)
        self.update_thread_data(threads, con)
        self.update_message_data(messages, con)
        self.update_message_attachments_data(attachment_data, con)
        self.update_user_data(users, con)
        con.close()

        self.logger.info("[Progress] Done")

    async def update_single_message_data(self, message: discord.Message):
        guilds = [message.guild]
        if isinstance(message.channel, discord.abc.GuildChannel):
            channels = [message.channel]
            threads = []
        elif isinstance(message.channel, discord.Thread):
            threads = [message.channel]
            channels = [message.channel.parent]
        else:
            return
        messages = [message]
        attachment_data = [(message.id, attachment, await attachment.read()) for attachment in message.attachments]
        users = [message.author]

        con = self.create_db_connection()
        self.update_guild_data(guilds, con)
        self.update_channel_data(channels, con)
        self.update_thread_data(threads, con)
        self.update_message_data(messages, con)
        self.update_message_attachments_data(attachment_data, con)
        self.update_user_data(users, con)
        con.close()

    @commands.Cog.listener(name="on_message")
    async def on_message(self, message: discord.Message):
        await self.update_single_message_data(message)

    @commands.Cog.listener(name="on_raw_message_edit")
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        message = await self.bot.get_channel(payload.channel_id).fetch_message(payload.message_id)
        await self.update_single_message_data(message)


def setup(bot):
    return bot.add_cog(Archiver(bot))
