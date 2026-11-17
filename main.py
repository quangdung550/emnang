import asyncio
import logging
import os
import re
import sqlite3
import tempfile
import time
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
import edge_tts
import emoji

# ==================== CẤU HÌNH ====================
TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("Thiếu biến môi trường DISCORD_TOKEN hoặc TOKEN")

# (Tùy chọn) đặt SYNC_GUILD_ID để slash command hiện ngay lập tức trong 1 server khi test.
# Nếu không đặt, bot sync global (có thể mất tới ~1 giờ để hiện).
SYNC_GUILD_ID = os.getenv("SYNC_GUILD_ID")

TTS_VOICE = "vi-VN-HoaiMyNeural"
TTS_RATE = "+0%"
TTS_PITCH = "+0Hz"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tts-bot")

# ==================== DATABASE ====================
DB_PATH = "bot_data.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS voice_lock (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER,
            locked_at REAL
        )
        """
    )
    conn.commit()
    conn.close()


def get_locked_channel(guild_id: int) -> Optional[int]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT channel_id FROM voice_lock WHERE guild_id = ?", (guild_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def set_locked_channel(guild_id: int, channel_id: Optional[int]):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if channel_id is None:
        c.execute("DELETE FROM voice_lock WHERE guild_id = ?", (guild_id,))
    else:
        c.execute(
            "INSERT OR REPLACE INTO voice_lock (guild_id, channel_id, locked_at) VALUES (?, ?, ?)",
            (guild_id, channel_id, time.time()),
        )
    conn.commit()
    conn.close()


# ==================== TTS QUEUE ====================
speech_queues: dict[int, asyncio.Queue] = {}
speech_locks: dict[int, asyncio.Lock] = {}
speech_tasks: dict[int, asyncio.Task] = {}
active_audio_files: dict[int, str] = {}


def get_speech_queue(guild_id: int) -> asyncio.Queue:
    if guild_id not in speech_queues:
        speech_queues[guild_id] = asyncio.Queue()
    return speech_queues[guild_id]


def get_speech_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in speech_locks:
        speech_locks[guild_id] = asyncio.Lock()
    return speech_locks[guild_id]


def clear_queue(guild_id: int):
    queue = speech_queues.get(guild_id)
    if not queue:
        return
    while not queue.empty():
        try:
            queue.get_nowait()
            queue.task_done()
        except asyncio.QueueEmpty:
            break


def ensure_worker(guild_id: int):
    """Đảm bảo mỗi guild chỉ có đúng 1 speech_worker đang chạy."""
    task = speech_tasks.get(guild_id)
    if task is None or task.done():
        speech_tasks[guild_id] = asyncio.create_task(speech_worker(guild_id))


def delete_audio_file(path: str):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning("Không xóa được file %s: %s", path, e)


# ==================== TTS ====================
async def create_tts_file(text: str) -> str:
    fd, file_path = tempfile.mkstemp(prefix="discord_tts_", suffix=".mp3")
    os.close(fd)

    try:
        communicate = edge_tts.Communicate(
            text=text,
            voice=TTS_VOICE,
            rate=TTS_RATE,
            pitch=TTS_PITCH,
        )
        await communicate.save(file_path)

        size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        if size < 1000:
            raise RuntimeError(f"TTS tạo file quá nhỏ ({size} bytes)")

        logger.info("TTS file created: %s (%s bytes)", file_path, size)
        return file_path
    except Exception:
        delete_audio_file(file_path)
        raise


# ==================== TEXT CLEANING ====================
def clean_message(content: str) -> str:
    # Xóa mention
    content = re.sub(r"<@!?\d+>", "", content)
    content = re.sub(r"<@&\d+>", "", content)
    content = re.sub(r"<#\d+>", "", content)

    # Xóa URL
    content = re.sub(r"https?://\S+", "", content)

    # Xóa custom emoji
    content = re.sub(r"<a?:\w+:\d+>", "", content)

    # Xóa emoji thường
    content = emoji.replace_emoji(content, replace="")

    # Dọn khoảng trắng
    content = re.sub(r"\s+", " ", content).strip()
    return content


# ==================== SPEECH WORKER ====================
async def speech_worker(guild_id: int):
    queue = get_speech_queue(guild_id)
    lock = get_speech_lock(guild_id)

    while True:
        text = await queue.get()
        try:
            async with lock:
                guild = bot.get_guild(guild_id)
                if not guild:
                    continue

                voice_client = guild.voice_client
                if not voice_client or not voice_client.is_connected():
                    locked_ch_id = get_locked_channel(guild_id)
                    if not locked_ch_id:
                        continue
                    channel = guild.get_channel(locked_ch_id)
                    if not isinstance(channel, discord.VoiceChannel):
                        continue
                    try:
                        voice_client = await channel.connect()
                    except Exception as e:
                        logger.error("Không connect được voice: %s", e)
                        continue

                if not voice_client or not voice_client.is_connected():
                    continue

                # Tạo file TTS
                try:
                    file_path = await create_tts_file(text)
                except Exception as e:
                    logger.error("Không tạo được âm thanh TTS: %s", e)
                    continue

                logger.info(
                    "Playing TTS | guild=%s | size=%s bytes",
                    guild_id,
                    os.path.getsize(file_path),
                )

                active_audio_files[guild_id] = file_path
                try:
                    source = discord.FFmpegOpusAudio(file_path)
                    voice_client.play(source)
                    while voice_client.is_playing():
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error("Lỗi khi play: %s", e)
                finally:
                    delete_audio_file(file_path)
                    active_audio_files.pop(guild_id, None)

        except Exception as e:
            logger.error("Lỗi speech_worker: %s", e)
        finally:
            queue.task_done()


# ==================== BOT ====================
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True


class TTSBot(commands.Bot):
    def __init__(self):
        # Không dùng prefix command nữa, chỉ phản hồi khi bị mention.
        super().__init__(command_prefix=commands.when_mentioned, intents=intents)

    async def setup_hook(self):
        init_db()

        if SYNC_GUILD_ID:
            guild = discord.Object(id=int(SYNC_GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            synced = await self.tree.sync(guild=guild)
            logger.info("Đã sync %d slash command cho guild %s", len(synced), SYNC_GUILD_ID)
        else:
            synced = await self.tree.sync()
            logger.info("Đã sync %d slash command (global)", len(synced))


bot = TTSBot()


@bot.event
async def on_ready():
    logger.info("Bot đã sẵn sàng: %s", bot.user)
    for guild in bot.guilds:
        ensure_worker(guild.id)


@bot.event
async def on_guild_join(guild: discord.Guild):
    ensure_worker(guild.id)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    content = clean_message(message.content)
    if not content:
        return

    # Chỉ đọc khi bot đang ở trong voice channel
    vc = message.guild.voice_client
    if not vc or not vc.is_connected():
        return

    # Kiểm tra channel lock
    locked_id = get_locked_channel(message.guild.id)
    if locked_id and message.channel.id != locked_id:
        return

    ensure_worker(message.guild.id)
    await get_speech_queue(message.guild.id).put(content)


# ==================== SLASH COMMANDS ====================
@bot.tree.command(name="join", description="Bot vào voice channel của bạn và khóa vào đó")
@app_commands.guild_only()
async def join(interaction: discord.Interaction):
    user = interaction.user
    if not isinstance(user, discord.Member) or not user.voice or not user.voice.channel:
        await interaction.response.send_message(
            "Bạn phải ở trong voice channel.", ephemeral=True
        )
        return

    channel = user.voice.channel
    guild = interaction.guild
    vc = guild.voice_client

    if vc and vc.is_connected() and vc.channel == channel:
        await interaction.response.send_message(
            "Đã ở trong channel này rồi.", ephemeral=True
        )
        return

    # Kết nối voice có thể mất vài giây -> defer để tránh timeout 3s
    await interaction.response.defer()

    try:
        if vc and vc.is_connected():
            await vc.move_to(channel)
        else:
            await channel.connect()
    except Exception as e:
        logger.error("Join lỗi: %s", e)
        await interaction.followup.send(f"Không thể vào voice channel: {e}", ephemeral=True)
        return

    set_locked_channel(guild.id, channel.id)
    ensure_worker(guild.id)
    await interaction.followup.send(f"Đã join và khóa vào {channel.name}")


@bot.tree.command(name="leave", description="Bot rời voice channel và mở khóa")
@app_commands.guild_only()
async def leave(interaction: discord.Interaction):
    guild = interaction.guild
    vc = guild.voice_client

    clear_queue(guild.id)
    if vc:
        await vc.disconnect()
    set_locked_channel(guild.id, None)
    await interaction.response.send_message("Đã rời và mở khóa.")


@bot.tree.command(name="lock", description="Khóa bot vào voice channel hiện tại")
@app_commands.guild_only()
async def lock(interaction: discord.Interaction):
    vc = interaction.guild.voice_client
    if not vc or not vc.is_connected():
        await interaction.response.send_message("Bot chưa ở trong voice.", ephemeral=True)
        return

    set_locked_channel(interaction.guild.id, vc.channel.id)
    await interaction.response.send_message(f"Đã khóa vào {vc.channel.name}")


@bot.tree.command(name="unlock", description="Mở khóa channel")
@app_commands.guild_only()
async def unlock(interaction: discord.Interaction):
    set_locked_channel(interaction.guild.id, None)
    await interaction.response.send_message("Đã mở khóa.")


# ==================== RUN ====================
if __name__ == "__main__":
    bot.run(TOKEN)
