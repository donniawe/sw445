import os
import time
import math
import asyncio
import aiohttp
import yt_dlp
import logging
import traceback
import psutil
import sqlite3
from logging.handlers import RotatingFileHandler
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import FloodWait, MessageNotModified, UserIsBlocked, InputUserDeactivated

# ================================= CONFIG ================================= #
# --- Critical Settings ---
# مقادیر حساس از متغیرهای محیطی خوانده می‌شوند
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# --- Admin & Paths ---
# ادمین‌ها به صورت یک رشته با کاما از متغیرهای محیطی خوانده می‌شوند
ADMINS_RAW = os.environ.get("ADMINS", "").split(',')
ADMINS = [int(admin_id) for admin_id in ADMINS_RAW if admin_id.strip().isdigit()]

DOWNLOAD_PATH = "downloads/"
LOG_PATH = "logs/"
DB_FILE = "bot_database.db"

# --- Bot Behavior ---
MAX_CONCURRENT_DOWNLOADS = 3
SELF_DESTRUCT_TIMER = 30  # Seconds before a sent video is deleted. 0 to disable.

# ================================= LOGGING ================================= #
os.makedirs(LOG_PATH, exist_ok=True)
LOG_FILE = os.path.join(LOG_PATH, "bot.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=2),
        logging.StreamHandler()
    ]
)
LOGGER = logging.getLogger(__name__)

# ============================= DATABASE SETUP ============================== #
def init_db():
    """Initializes the SQLite database and tables."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                is_verified BOOLEAN NOT NULL DEFAULT 0,
                is_banned BOOLEAN NOT NULL DEFAULT 0,
                join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_status (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        cursor.execute("INSERT OR IGNORE INTO bot_status (key, value) VALUES ('is_active', '1')")
        conn.commit()

def get_user_status(user_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT is_verified, is_banned FROM users WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        return {"verified": bool(result[0]), "banned": bool(result[1])} if result else None

def add_user(user_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()

def update_user_verification(user_id: int, status: bool):
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("UPDATE users SET is_verified = ? WHERE user_id = ?", (int(status), user_id))
        conn.commit()

def set_user_ban_status(user_id: int, status: bool):
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (int(status), user_id))
        conn.commit()

def get_all_user_ids():
    with sqlite3.connect(DB_FILE) as conn:
        return [row[0] for row in conn.cursor().execute("SELECT user_id FROM users WHERE is_banned = 0").fetchall()]

def get_total_users():
    with sqlite3.connect(DB_FILE) as conn:
        return conn.cursor().execute("SELECT COUNT(*) FROM users").fetchone()[0]

def get_bot_status():
    with sqlite3.connect(DB_FILE) as conn:
        return conn.cursor().execute("SELECT value FROM bot_status WHERE key = 'is_active'").fetchone()[0] == '1'

def set_bot_status(is_active: bool):
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("UPDATE bot_status SET value = ? WHERE key = 'is_active'", (str(int(is_active)),))
        conn.commit()


# =============================== APP SETUP ================================= #
app = Client("professional_downloader_session_v6", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ============== GLOBAL STATE & HELPERS ===================================== #
link_cache = {}
active_downloads = set()
cancelled_downloads = set()
admin_states = {}
BOT_IS_ACTIVE = True

def humanbytes(size):
    if not size: return "0 B"
    power = 1024; n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < len(power_labels) -1 :
        size /= power; n += 1
    return f"{size:.2f} {power_labels[n]}"

def time_formatter(seconds: int) -> str:
    if not seconds: return "0s"
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return ' '.join(f"{value}{unit}" for value, unit in zip([hours, minutes, seconds], ['h', 'm', 's']) if value)

def get_progress_bar(percentage: float) -> str:
    """Creates a modern-looking progress bar."""
    filled = '🟩'; empty = '⬜️'; bar_length = 12
    filled_length = int(bar_length * percentage / 100)
    return f"{filled * filled_length}{empty * (bar_length - filled_length)}"

# ============== AGE VERIFICATION & START =================================== #
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if not BOT_IS_ACTIVE and message.from_user.id not in ADMINS:
        await message.reply_text("🤖 The bot is currently offline for maintenance. Please try again later.")
        return

    user_id = message.from_user.id
    add_user(user_id)
    status = get_user_status(user_id)

    if status and status["banned"]:
        await message.reply_text("🚫 You are banned from using this bot.")
        return

    if status and status["verified"]:
        await message.reply_text(f"👋 **Welcome back, {message.from_user.mention}!**\n\nI'm ready to download. Just send me a valid video link.")
    else:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ I am 18 or older", callback_data="verify_age_yes")],
            [InlineKeyboardButton("❌ I am not 18", callback_data="verify_age_no")]
        ])
        await message.reply_text(
            "**🔞 Age Verification Required**\n\nTo use this bot, please confirm that you are 18 years of age or older.",
            reply_markup=keyboard
        )

@app.on_callback_query(filters.regex("^verify_age_"))
async def age_verification_callback(client, callback_query):
    user_id = callback_query.from_user.id
    action = callback_query.data.split("_")[-1]

    if action == "yes":
        update_user_verification(user_id, True)
        await callback_query.message.edit_text(f"✅ **Verification Successful!**\n\nWelcome, {callback_query.from_user.mention}.\nYou can now send a link to start downloading.")
    else:
        set_user_ban_status(user_id, True)
        await callback_query.message.edit_text("❌ **Access Denied**\n\nYou must be 18 or older to use this service. Your access has been restricted.")
        await callback_query.answer("Access Denied.", show_alert=True)

# ============== AUTHORIZED USER FILTER ==================================== #
async def is_verified(_, __, message: Message):
    user_id = message.from_user.id
    if user_id in ADMINS: return True
    
    if not BOT_IS_ACTIVE:
        await message.reply_text("🤖 The bot is currently offline for maintenance. Please try again later.", quote=True)
        return False

    status = get_user_status(user_id)
    if status and status["verified"] and not status["banned"]: return True
    
    if isinstance(message, Message): await start_command(__, message)
    return False

verified_user_filter = filters.create(is_verified)

# ============== LINK HANDLING & DOWNLOAD ================================== #
@app.on_message(filters.regex(r'https?://[^\s]+') & filters.private & verified_user_filter)
async def link_handler(client, message):
    link, user_id = message.text, message.from_user.id
    
    if len(active_downloads) >= MAX_CONCURRENT_DOWNLOADS:
        await message.reply_text("Busy with other downloads. Please try again in a moment.")
        return
        
    processing_msg = await message.reply_text("🔎 `Extracting video information...`", quote=True)
    
    try:
        ydl_opts = {'noplaylist': True, 'quiet': True, 'no_warnings': True, 'forcejson': True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = await asyncio.to_thread(ydl.extract_info, link, download=False)
        
        title, thumb_url, duration = info_dict.get('title', 'N/A'), info_dict.get('thumbnail'), info_dict.get('duration', 0)
        short_title = (title[:70] + '...') if len(title) > 70 else title
        data_key = f"{user_id}:{processing_msg.id}"
        link_cache[data_key] = (link, title, duration, thumb_url, message.id)

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Download", callback_data=f"confirm_{data_key}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_op_{data_key}")],
        ])
        caption = f"**🏷️ Title:** `{short_title}`\n\n**⏱️ Duration:** `{time_formatter(duration)}`\n\nReady to download?"
        
        await processing_msg.delete()
        if thumb_url:
            await client.send_photo(user_id, photo=thumb_url, caption=caption, reply_markup=keyboard, has_spoiler=True)
        else:
            await client.send_message(user_id, text=caption, reply_markup=keyboard)
            
    except Exception as e:
        LOGGER.error(f"Error processing link {link}: {e}", exc_info=True)
        await processing_msg.edit_text(f"🚫 **Error:** Failed to process the link. It might be invalid, private, or from an unsupported site.")

@app.on_callback_query(filters.regex("^(confirm|cancel_op)_"))
async def confirmation_callback(client, callback_query):
    user_id = callback_query.from_user.id
    action, data_key = callback_query.data.split("_", 1)

    if action == "cancel_op":
        link_cache.pop(data_key, None)
        await callback_query.message.delete()
        return await callback_query.answer("Download cancelled.")

    if len(active_downloads) >= MAX_CONCURRENT_DOWNLOADS:
        return await callback_query.answer("Bot is currently busy with other downloads. Please try again in a moment.", show_alert=True)
        
    link_data = link_cache.pop(data_key, None)
    if not link_data:
        await callback_query.message.delete()
        return await callback_query.answer("This download confirmation has expired. Please send the link again.", show_alert=True)

    await callback_query.message.delete()
    status_message = await client.send_message(user_id, "🚀 `Your download is starting...`")
    asyncio.create_task(download_and_upload(user_id, link_data, status_message))

async def download_and_upload(user_id, link_data, message):
    link, title, duration, thumb_url, original_message_id = link_data
    download_id = f"{user_id}-{int(time.time())}"
    active_downloads.add(download_id)
    
    start_time, last_update_time = time.time(), 0
    download_filepath = os.path.join(DOWNLOAD_PATH, f"{download_id}.mp4")
    thumb_filepath = os.path.join(DOWNLOAD_PATH, f"{download_id}.jpg")
    short_title = (title[:60] + '...') if len(title) > 60 else title

    async def update_status_message(stage, current=0, total=1):
        nonlocal last_update_time
        now = time.time()
        if now - last_update_time < 2: return
        last_update_time = now
        
        percentage = (current / total) * 100 if total > 0 else 0
        progress_bar = get_progress_bar(percentage)
        elapsed_time = time.time() - start_time
        speed = current / elapsed_time if elapsed_time > 0 else 0
        eta = (total - current) / speed if speed > 0 else 0
        
        header = f"📥 **Downloading...**" if stage == 'download' else "☁️ **Uploading...**"
        
        details = (
            f"**Progress:** `{progress_bar} {percentage:.1f}%`\n"
            f"**Status:** `{humanbytes(current)}` of `{humanbytes(total)}`\n"
            f"**Speed:** `{humanbytes(speed)}/s`\n"
            f"**ETA:** `{time_formatter(int(eta))}`"
        )
        
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✖️ Cancel", callback_data=f"cancel_dl_{download_id}")
        ]])
        
        try:
            await message.edit_text(f"{header}\n\n**🏷️** `{short_title}`\n\n{details}", reply_markup=keyboard)
        except (MessageNotModified, FloodWait): pass
        except Exception as e: LOGGER.warning(f"Status update failed: {e}")

    def ydl_hook(d):
        if d['status'] == 'downloading':
            total, downloaded = d.get('total_bytes_estimate', 0), d['downloaded_bytes']
            if app.loop.is_running(): asyncio.run_coroutine_threadsafe(update_status_message('download', downloaded, total), app.loop)

    def upload_hook(current, total):
        if app.loop.is_running(): asyncio.run_coroutine_threadsafe(update_status_message('upload', current, total), app.loop)

    try:
        ydl_opts = {'outtmpl': download_filepath, 'progress_hooks': [ydl_hook], 'noplaylist': True, 'format': 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best', 'merge_output_format': 'mp4', 'nocheckcertificate': True}
        await asyncio.to_thread(yt_dlp.YoutubeDL(ydl_opts).extract_info, link, download=True)
            
        if download_id in cancelled_downloads:
            await message.edit_text(f"❌ **Download Canceled**\n\nYour request for `{short_title}` was successfully canceled.")
            return

        if not os.path.exists(download_filepath): raise FileNotFoundError("Downloaded file not found.")

        if thumb_url:
            async with aiohttp.ClientSession() as session, session.get(thumb_url) as resp:
                if resp.status == 200:
                    with open(thumb_filepath, "wb") as f: f.write(await resp.read())
                else: thumb_filepath = None
        else: thumb_filepath = None

        await message.edit_text("✅ `Download complete!`\n\n☁️ `Preparing to upload...`")
        
        caption_text = f"🎬 **{title}**"
        if SELF_DESTRUCT_TIMER > 0: caption_text += f"\n\n_🗑️ This video will be deleted in {time_formatter(SELF_DESTRUCT_TIMER)}._"
            
        sent_video = await app.send_video(user_id, video=download_filepath, caption=caption_text, thumb=thumb_filepath, duration=duration, progress=upload_hook, has_spoiler=True)
        
        await message.delete()
        LOGGER.info(f"Upload finished for user {user_id}.")
        
        if SELF_DESTRUCT_TIMER > 0:
            await asyncio.sleep(SELF_DESTRUCT_TIMER)
            await sent_video.delete()
            LOGGER.info(f"Self-destructed video for user {user_id}.")

    except Exception as e:
        LOGGER.error(f"Download/Upload error for {user_id}: {e}", exc_info=True)
        error_message = str(e).replace('ERROR: ', '')
        await message.edit_text(f"🚫 **An Error Occurred:**\n`{error_message}`\nPlease try another link.")
    finally:
        active_downloads.discard(download_id)
        cancelled_downloads.discard(download_id)
        for f in [download_filepath, thumb_filepath]:
            if f and os.path.exists(f): os.remove(f)

@app.on_callback_query(filters.regex("^cancel_dl_"))
async def cancel_download_handler(client, callback_query):
    download_id = callback_query.data.split("_", 2)[2]

    if download_id in active_downloads:
        cancelled_downloads.add(download_id)
        try:
            await callback_query.message.edit_text(f"**⚠️ Cancelling Download**\n\nThe process will be stopped after the current operation finishes. Please wait.")
        except MessageNotModified: pass
        await callback_query.answer("Cancellation request sent.", show_alert=False)
    else:
        await callback_query.answer("This download is already complete or has been cancelled.", show_alert=True)

# ============== ADMIN PANEL & ACTIONS ===================================== #
@app.on_message(filters.command("admin") & filters.user(ADMINS))
async def admin_panel(client, message):
    bot_status_text = "🟢 ON" if BOT_IS_ACTIVE else "🔴 OFF"
    await message.reply_text(
        "**👨‍💻 Admin Panel**\nWelcome to the control panel. Choose an option below.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"), InlineKeyboardButton("⚙️ System", callback_data="admin_sys")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("👥 Users", callback_data="admin_users")],
            [InlineKeyboardButton(f"🤖 Toggle Bot ({bot_status_text})", callback_data="admin_toggle_bot")],
            [InlineKeyboardButton("📄 Get Log File", callback_data="admin_get_log")]
        ])
    )

@app.on_callback_query(filters.regex("^admin_") & filters.user(ADMINS))
async def admin_callbacks(client, cb):
    action = cb.data.split("_", 1)[1]
    back_button = InlineKeyboardButton("⬅️ Back to Menu", callback_data="admin_back")
    
    if action == "stats":
        stats_text = f"**📊 Bot Statistics**\n\n👥 **Total Users:** `{get_total_users()}`\n⚡ **Active Downloads:** `{len(active_downloads)}`"
        await cb.message.edit_text(stats_text, reply_markup=InlineKeyboardMarkup([[back_button]]))
    
    elif action == "sys":
        cpu, mem, disk = psutil.cpu_percent(interval=0.5), psutil.virtual_memory(), psutil.disk_usage('/')
        stats = f"**⚙️ System**\n\n**CPU:** `{cpu}%`\n**RAM:** `{mem.percent}%` ({humanbytes(mem.used)}/{humanbytes(mem.total)})\n**Disk:** `{disk.percent}%` ({humanbytes(disk.used)}/{humanbytes(disk.total)})"
        await cb.message.edit_text(stats, reply_markup=InlineKeyboardMarkup([[back_button]]))

    elif action == "broadcast":
        admin_states[cb.from_user.id] = "broadcast"
        await cb.message.edit_text("Enter the message to broadcast to all users. Send /cancel to abort.", reply_markup=InlineKeyboardMarkup([[back_button]]))

    elif action == "users":
        await cb.message.edit_text("**👥 User Management**", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban"), InlineKeyboardButton("✅ Unban User", callback_data="admin_unban")],
            [back_button]
        ]))
    
    elif action in ["ban", "unban"]:
        admin_states[cb.from_user.id] = action
        await cb.message.edit_text(f"Enter the User ID to **{action}**. Send /cancel to abort.", reply_markup=InlineKeyboardMarkup([[back_button]]))

    elif action == "toggle_bot":
        global BOT_IS_ACTIVE
        BOT_IS_ACTIVE = not BOT_IS_ACTIVE
        set_bot_status(BOT_IS_ACTIVE)
        await cb.answer(f"Bot has been turned {'ON' if BOT_IS_ACTIVE else 'OFF'}.", show_alert=True)
        await admin_panel(client, cb.message) # Refresh the panel
        
    elif action == "get_log":
        await cb.answer()
        if os.path.exists(LOG_FILE): await client.send_document(cb.from_user.id, LOG_FILE, caption="Bot Log File")
        else: await client.send_message(cb.from_user.id, "Log file not found!")
    
    elif action == "back":
        admin_states.pop(cb.from_user.id, None)
        await admin_panel(client, cb.message)
    
    await cb.answer()

@app.on_message(filters.private & filters.user(ADMINS))
async def admin_action_handler(client, message):
    admin_id = message.from_user.id
    state = admin_states.get(admin_id)
    if not state: return
    if message.text == "/cancel":
        admin_states.pop(admin_id, None)
        return await message.reply_text("Action cancelled.")

    admin_states.pop(admin_id, None)
    if state == "broadcast":
        sent_count, failed_count = 0, 0
        user_ids = get_all_user_ids()
        await message.reply_text(f"📢 Broadcasting to `{len(user_ids)}` users... This may take a while.")
        for user_id in user_ids:
            try:
                await message.copy(user_id)
                sent_count += 1
                await asyncio.sleep(0.1)
            except (UserIsBlocked, InputUserDeactivated):
                failed_count += 1
            except Exception as e:
                failed_count += 1
                LOGGER.error(f"Broadcast failed for user {user_id}: {e}")
        await message.reply_text(f"✅ **Broadcast Complete!**\n\n- Sent: `{sent_count}`\n- Failed (Blocked/Deactivated): `{failed_count}`")

    elif state in ["ban", "unban"]:
        if not message.text.isdigit():
            return await message.reply_text("Invalid User ID. Please provide a numeric ID.")
        user_id_to_modify = int(message.text)
        is_banning = state == "ban"
        set_user_ban_status(user_id_to_modify, is_banning)
        action_text = "banned" if is_banning else "unbanned"
        await message.reply_text(f"✅ User `{user_id_to_modify}` has been successfully **{action_text}**.")
        
        try:
            if is_banning:
                await client.send_message(user_id_to_modify, "🚫 You have been **banned** from using this bot by an administrator.")
            else:
                await client.send_message(user_id_to_modify, "✅ You have been **unbanned**. You can now use the bot again.")
        except (UserIsBlocked, InputUserDeactivated):
            await message.reply_text("ℹ️ Could not notify the user as they may have blocked the bot.")
        except Exception as e:
            LOGGER.error(f"Could not notify user {user_id_to_modify} about status change: {e}")

# ============================ MAIN EXECUTION ============================= #
async def main():
    global BOT_IS_ACTIVE
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    init_db()
    BOT_IS_ACTIVE = get_bot_status()
    LOGGER.info(f"Bot starting... Initial status: {'ACTIVE' if BOT_IS_ACTIVE else 'INACTIVE'}")
    await app.start()
    LOGGER.info("Bot has started successfully!")
    await idle()
    await app.stop()
    LOGGER.info("Bot has been stopped.")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
