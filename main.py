import os
import asyncio
import random
import re
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any
from contextlib import asynccontextmanager

from dotenv import load_dotenv
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMember, ChatMemberUpdated, Message, User, InputMediaPhoto
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler,
    ContextTypes, ChatMemberHandler
)
import uvicorn

load_dotenv()

import os

# ---------- Configuration ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN missing")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing")

try:
    OWNER_ID = int(os.getenv("OWNER_ID", "7728424218"))
except ValueError:
    raise ValueError("OWNER_ID must be integer")

OWNER_USERNAME = os.getenv("OWNER_USERNAME", "YorichiiPrime")
OWNER_LINK = f"https://t.me/{OWNER_USERNAME}"

SUPPORT_LINK = os.getenv("SUPPORT_LINK", "https://t.me/kingchaos7")

# Conversation states for /addcharacter
(NAME, ANIME, IMG_URL, RARITY, PRICE) = range(5)

# ---------- Database Layer ----------
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(DATABASE_URL)

    async def init_tables(self):
        async with self.pool.acquire() as conn:
            # Groups
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    group_id BIGINT PRIMARY KEY,
                    welcome_img_id TEXT,
                    last_calladmins TIMESTAMP
                )
            ''')
            # Users per group (inventory tracking only)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT,
                    group_id BIGINT,
                    PRIMARY KEY (user_id, group_id)
                )
            ''')
            # Global user data (coins + cooldowns shared across all groups)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS global_users (
                    user_id BIGINT PRIMARY KEY,
                    coins INT DEFAULT 0,
                    last_daily TIMESTAMP,
                    last_claim TIMESTAMP
                )
            ''')
            # Characters global
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS characters (
                    char_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    anime TEXT NOT NULL,
                    img_url TEXT NOT NULL,
                    rarity TEXT NOT NULL,
                    rarity_tier INT NOT NULL,
                    price INT NOT NULL,
                    is_available BOOLEAN DEFAULT TRUE
                )
            ''')
            # Inventory per user per group
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS inventory (
                    user_id BIGINT,
                    group_id BIGINT,
                    char_id TEXT REFERENCES characters(char_id),
                    PRIMARY KEY (user_id, group_id, char_id)
                )
            ''')
            # Started users
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS started_users (
                    user_id BIGINT PRIMARY KEY,
                    started_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            # Bot config
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            # Rarity ranking
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS rarity_ranking (
                    rarity TEXT PRIMARY KEY,
                    tier INT UNIQUE,
                    emoji TEXT
                )
            ''')
            # Insert default rarities
            await conn.executemany('''
                INSERT INTO rarity_ranking (rarity, tier, emoji)
                VALUES ($1, $2, $3) ON CONFLICT (rarity) DO NOTHING
            ''', [
                ('Common', 1, '⚪'), ('Uncommon', 2, '🟢'), ('Elite', 3, '🔵'),
                ('Epic', 4, '🟣'), ('Mythic', 5, '🔴'), ('Waifu', 6, '💖'),
                ('Special Edition', 7, '✨'), ('Limited', 8, '⏳'),
                ('Event', 9, '🎉'), ('Legendary', 10, '🌟')
            ])

    # ---- User tracking ----
    async def user_has_started(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT 1 FROM started_users WHERE user_id = $1', user_id) is not None

    async def register_start(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO started_users (user_id) VALUES ($1) ON CONFLICT DO NOTHING', user_id)

    # ---- Groups ----
    async def get_group_welcome_img(self, group_id: int) -> Optional[str]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT welcome_img_id FROM groups WHERE group_id = $1', group_id)

    async def set_group_welcome_img(self, group_id: int, file_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO groups (group_id, welcome_img_id) VALUES ($1, $2) ON CONFLICT (group_id) DO UPDATE SET welcome_img_id = $2', group_id, file_id)

    async def get_last_calladmins(self, group_id: int) -> Optional[datetime]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT last_calladmins FROM groups WHERE group_id = $1', group_id)

    async def update_calladmins_time(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO groups (group_id, last_calladmins) VALUES ($1, NOW()) ON CONFLICT (group_id) DO UPDATE SET last_calladmins = NOW()', group_id)

    # ---- Global user data (coins + cooldowns, shared across all groups) ----
    async def get_user_coins(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            val = await conn.fetchval('SELECT coins FROM global_users WHERE user_id = $1', user_id)
            return val if val is not None else 0

    async def add_coins(self, user_id: int, amount: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO global_users (user_id, coins) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET coins = global_users.coins + $2
            ''', user_id, amount)

    async def remove_coins(self, user_id: int, amount: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO global_users (user_id, coins) VALUES ($1, 0)
                ON CONFLICT (user_id) DO UPDATE SET coins = GREATEST(0, global_users.coins - $2)
            ''', user_id, amount)

    async def can_claim_daily(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            last = await conn.fetchval('SELECT last_daily FROM global_users WHERE user_id = $1', user_id)
            if not last: return True
            return (datetime.utcnow() - last).total_seconds() >= 7200

    async def record_daily(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO global_users (user_id, last_daily) VALUES ($1, NOW())
                ON CONFLICT (user_id) DO UPDATE SET last_daily = NOW()
            ''', user_id)

    async def can_claim_character(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            last = await conn.fetchval('SELECT last_claim FROM global_users WHERE user_id = $1', user_id)
            if not last: return True
            return (datetime.utcnow() - last).total_seconds() >= 39600  # 11h

    async def record_claim(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO global_users (user_id, last_claim) VALUES ($1, NOW())
                ON CONFLICT (user_id) DO UPDATE SET last_claim = NOW()
            ''', user_id)

    async def get_user_char_count(self, user_id: int, group_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM inventory WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            ) or 0

    # ---- Characters ----
    async def add_character(self, char_id: str, name: str, anime: str, img_url: str, rarity: str, price: int):
        async with self.pool.acquire() as conn:
            tier = await conn.fetchval('SELECT tier FROM rarity_ranking WHERE rarity = $1', rarity)
            if tier is None: tier = 1
            await conn.execute('''
                INSERT INTO characters (char_id, name, anime, img_url, rarity, rarity_tier, price)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', char_id, name, anime, img_url, rarity, tier, price)

    async def remove_character(self, char_id: str):
        async with self.pool.acquire() as conn:
            # Soft-delete: hides from market but keeps existing player inventories intact
            await conn.execute('UPDATE characters SET is_available = FALSE WHERE char_id = $1', char_id)

    async def get_character_by_id(self, char_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM characters WHERE char_id = $1 AND is_available = true', char_id)
            return dict(row) if row else None

    async def character_exists(self, name: str, anime: str) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT 1 FROM characters WHERE name = $1 AND anime = $2', name, anime) is not None

    async def search_characters(self, query: str) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT char_id, name, anime, img_url, rarity, price, rarity_tier
                FROM characters WHERE is_available = true AND (name ILIKE $1 OR anime ILIKE $1)
                ORDER BY rarity_tier DESC LIMIT 20
            ''', f'%{query}%')
            return [dict(r) for r in rows]

    async def get_random_character(self) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM characters WHERE is_available = true ORDER BY RANDOM() LIMIT 1')
            return dict(row) if row else None

    async def get_random_unowned_character(self, user_id: int, group_id: int) -> Optional[dict]:
        """Returns a random available character the user does NOT already own in this group."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM characters
                WHERE is_available = true
                AND char_id NOT IN (
                    SELECT char_id FROM inventory
                    WHERE user_id = $1 AND group_id = $2
                )
                ORDER BY RANDOM() LIMIT 1
            ''', user_id, group_id)
            return dict(row) if row else None

    # ---- Inventory ----
    async def add_to_inventory(self, user_id: int, group_id: int, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            try:
                await conn.execute('INSERT INTO inventory (user_id, group_id, char_id) VALUES ($1, $2, $3)', user_id, group_id, char_id)
                return True
            except asyncpg.UniqueViolationError:
                return False

    async def remove_from_inventory(self, user_id: int, group_id: int, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute('DELETE FROM inventory WHERE user_id = $1 AND group_id = $2 AND char_id = $3', user_id, group_id, char_id)
            return result != "DELETE 0"

    async def user_owns_character(self, user_id: int, group_id: int, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT 1 FROM inventory WHERE user_id = $1 AND group_id = $2 AND char_id = $3', user_id, group_id, char_id) is not None

    async def get_user_inventory(self, user_id: int, group_id: int, limit: int = 7, offset: int = 0) -> Tuple[List[dict], int]:
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM inventory WHERE user_id = $1 AND group_id = $2', user_id, group_id)
            rows = await conn.fetch('''
                SELECT c.char_id, c.name, c.anime, c.img_url, c.rarity, c.price, c.rarity_tier
                FROM inventory i JOIN characters c ON i.char_id = c.char_id
                WHERE i.user_id = $1 AND i.group_id = $2
                ORDER BY c.rarity_tier DESC, c.name
                LIMIT $3 OFFSET $4
            ''', user_id, group_id, limit, offset)
            return [dict(r) for r in rows], total

    async def get_market_characters(self, limit: int = 7, offset: int = 0) -> Tuple[List[dict], int]:
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM characters WHERE is_available = true')
            rows = await conn.fetch('''
                SELECT char_id, name, anime, img_url, rarity, price, rarity_tier
                FROM characters WHERE is_available = true
                ORDER BY rarity_tier DESC, name
                LIMIT $1 OFFSET $2
            ''', limit, offset)
            return [dict(r) for r in rows], total

    async def get_start_video(self) -> Optional[str]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT value FROM bot_config WHERE key = $1', 'start_video')

    async def set_start_video(self, file_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO bot_config (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2', 'start_video', file_id)

    async def get_rarity_emoji(self, rarity: str) -> str:
        async with self.pool.acquire() as conn:
            emoji = await conn.fetchval('SELECT emoji FROM rarity_ranking WHERE rarity = $1', rarity)
            return emoji or '⭐'

db = Database()

# ---------- Helper Functions ----------
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID

async def ensure_started(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False
    if not await db.user_has_started(user.id):
        await update.message.reply_text("❌ Please start the bot first with /start")
        return False
    return True

def get_group_id(update: Update) -> int:
    chat = update.effective_chat
    if chat.type in ["group", "supergroup"]:
        return chat.id
    return 0  # private chat, no group-specific data

def get_effective_group_id(update: Update) -> int:
    # For commands that need group context (like /buy), use group if in group, else 0 (global? but we separate per group)
    chat = update.effective_chat
    if chat.type in ["group", "supergroup"]:
        return chat.id
    return 0  # private chats have no group, but we can treat as group 0? But spec says per group separated, so private chat maybe not allowed for collection? We'll allow but store under group_id=0.
    # Simpler: allow commands only in groups? But user may want to use in private. We'll store under group_id=0 for private.

async def format_character_card(char: dict, emoji: str) -> str:
    return (
        f"{emoji} *{char['name']}*\n"
        f"🎬 *Anime:* {char['anime']}\n"
        f"💎 *Rarity:* {emoji} {char['rarity']}\n"
        f"🆔 *ID:* `{char['char_id']}`\n"
        f"💰 *Price:* {char['price']} coins"
    )

# ---------- Bot Command Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db.register_start(user.id)

    video_id = await db.get_start_video()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add To Group", url=f"https://t.me/{context.bot.username}?startgroup=true")],
        [InlineKeyboardButton("Owner 🤪", url=OWNER_LINK),   # ✅ uses OWNER_LINK from config
         InlineKeyboardButton("Support chat 💝", url=SUPPORT_LINK)]
    ])
    caption = "Welcome to Anime Character Collector Bot!\nUse /help to see commands."

    try:
        if video_id:
            await update.message.reply_video(video_id, caption=caption, reply_markup=keyboard)
        else:
            raise Exception("no video")
    except Exception:
        # Fallback to text if video fails or doesn't exist
        await update.message.reply_text(caption, reply_markup=keyboard)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """🎮 *Available Commands*

*💰 Economy*
/daily - Claim coins (every 2h)
/claim - Claim random character (every 11h)
/wallet - Show your coins

*📦 Collection*
/vault - Your collected characters
/market - View buyable characters
/buy <id> [id2 ...] - Buy character(s)
/sell <id> - Sell character (70% refund)
/search <name> - Search characters

*👥 Group Admin*
/listadmins - List human admins
/calladmins - Mention all admins (10min cooldown)

*👑 Owner/Dev*
/addcharacter - Add new character (interactive)
/remove <id> - Remove character
/listchar - List all characters (IDs)
/addcoins (reply) - Add coins
/removecoins (reply) - Remove coins
/setstartvid (reply to video)
/setwelcomepic (reply to photo, in group)
/resetgrpdata - Reset group data (danger)

*Other*
/start - Start the bot
/help - This menu"""
    await update.message.reply_text(text, parse_mode="Markdown")

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await ensure_started(update, context): return
    if await db.can_claim_daily(user.id):
        reward = random.randint(50, 200)
        await db.add_coins(user.id, reward)
        await db.record_daily(user.id)
        coins = await db.get_user_coins(user.id)
        await update.message.reply_text(f"💸 You claimed *{reward} coins*!\n💰 Total balance: `{coins}` coins", parse_mode="Markdown")
    else:
        await update.message.reply_text("⏳ You already claimed daily coins. Try again in 2 hours.")

async def claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context): return
    if not await db.can_claim_character(user.id):
        await update.message.reply_text("⏳ You already claimed a character. Try again in 11 hours.")
        return

    char = await db.get_random_unowned_character(user.id, group_id)

    if char is None:
        # Check if the market has any characters at all
        any_char = await db.get_random_character()
        if not any_char:
            await update.message.reply_text("❌ No characters available. Contact the owner.")
            return
        # User owns every available character — reward coins
        await db.add_coins(user.id, 10000)
        await db.record_claim(user.id)
        coins = await db.get_user_coins(user.id)
        await update.message.reply_text(
            "🏆 *You own every character in the collection!*\n\n"
            "╭───────────────╮\n"
            "💰 *Reward:* +10,000 Coins\n"
            f"👜 *Balance:* {coins:,} Coins\n"
            "╰───────────────╯\n\n"
            "⏳ Come back in 11 hours for your next claim!",
            parse_mode="Markdown"
        )
        return

    await db.add_to_inventory(user.id, group_id, char['char_id'])
    await db.record_claim(user.id)
    emoji = await db.get_rarity_emoji(char['rarity'])

    caption = (
        f"{emoji} *{char['name']}*\n"
        f"╭───────────────╮\n"
        f"🎬 Anime: {char['anime']}\n"
        f"💎 Tier: {emoji} {char['rarity']}\n"
        f"🆔 Card ID: `{char['char_id']}`\n"
        f"💰 Value: {char['price']:,} Coins\n"
        f"╰───────────────╯\n\n"
        f"✨ A rare presence has been claimed…\n"
        f"🌸 Grace\\. Power\\. Mystery — all in one\\.\n\n"
        f"🔐 Status: Newly Claimed"
    )

    try:
        await update.message.reply_photo(photo=char['img_url'], caption=caption, parse_mode="MarkdownV2")
    except Exception:
        await update.message.reply_text(caption, parse_mode="MarkdownV2")

async def wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context): return
    coins = await db.get_user_coins(user.id)
    char_count = await db.get_user_char_count(user.id, group_id)
    text = (
        f"👤 *{user.first_name}'s Wallet*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💰 *Coins:* `{coins:,}`\n"
        f"🎴 *Characters:* `{char_count}`\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"_/daily to earn coins • /vault to see collection_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def send_vault_page(target, user_id: int, group_id: int, page: int, send_new: bool = True):
    """Show one character card per page with image. target = Message object."""
    items, total = await db.get_user_inventory(user_id, group_id, 1, page - 1)
    total_pages = max(1, total)

    if not items:
        text = "📭 Your vault is empty. Use /claim or /buy to get characters!"
        if send_new:
            await target.reply_text(text)
        else:
            await target.edit_text(text)
        return

    char = items[0]
    emoji = await db.get_rarity_emoji(char['rarity'])
    caption = (
        f"{emoji} *{char['name']}*\n"
        f"╭───────────────╮\n"
        f"🎬 Anime: {char['anime']}\n"
        f"💎 Tier: {emoji} {char['rarity']}\n"
        f"🆔 Card ID: `{char['char_id']}`\n"
        f"💰 Value: {char['price']:,} Coins\n"
        f"╰───────────────╯\n\n"
        f"_Card {page} of {total_pages}_"
    )

    row = []
    if page > 1:
        row.append(InlineKeyboardButton("◀", callback_data=f"vlt:{user_id}:{group_id}:{page - 1}"))
    row.append(InlineKeyboardButton(f"🗂 {page}/{total_pages}", callback_data="vlt:noop"))
    if page < total_pages:
        row.append(InlineKeyboardButton("▶", callback_data=f"vlt:{user_id}:{group_id}:{page + 1}"))
    keyboard = InlineKeyboardMarkup([row])

    if send_new:
        try:
            await target.reply_photo(char['img_url'], caption=caption, parse_mode="Markdown", reply_markup=keyboard)
        except Exception:
            await target.reply_text(caption, parse_mode="Markdown", reply_markup=keyboard)
    else:
        # Edit in place: swap the photo and caption together
        try:
            await target.edit_media(
                media=InputMediaPhoto(media=char['img_url'], caption=caption, parse_mode="Markdown"),
                reply_markup=keyboard
            )
        except Exception:
            try:
                await target.edit_caption(caption=caption, parse_mode="Markdown", reply_markup=keyboard)
            except Exception:
                await target.edit_text(caption, parse_mode="Markdown", reply_markup=keyboard)

async def vault(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context): return
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    await send_vault_page(update.message, user.id, group_id, page, send_new=True)

async def vault_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "vlt:noop":
        await query.answer()
        return
    parts = query.data.split(":")
    user_id, group_id, page = int(parts[1]), int(parts[2]), int(parts[3])
    # Only the vault owner can navigate their own vault
    if update.effective_user.id != user_id:
        await query.answer("⛔ This is not your vault!", show_alert=True)
        return
    await query.answer()
    await send_vault_page(query.message, user_id, group_id, page, send_new=False)

async def send_market_page(target, page: int, send_new: bool = True):
    """Render one page of the market. target is a Message (send_new=True) or Message from callback (send_new=False)."""
    PER_PAGE = 5
    offset = (page - 1) * PER_PAGE
    items, total = await db.get_market_characters(PER_PAGE, offset)
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = max(1, min(page, total_pages))

    if not items:
        text = "🛒 The market is currently empty. Check back later!"
        if send_new:
            await target.reply_text(text)
        else:
            await target.edit_text(text)
        return

    text = f"🛒 *Character Market — Page {page}/{total_pages}*\n"
    text += "━━━━━━━━━━━━━━━━\n\n"
    for c in items:
        emoji = await db.get_rarity_emoji(c['rarity'])
        text += f"{emoji} *{c['name']}* — _{c['anime']}_\n"
        text += f"   💰 `{c['price']}` coins  •  🆔 `{c['char_id']}`\n\n"
    text += "_Use /buy \\<id\\> to purchase a character_"

    row = []
    if page > 1:
        row.append(InlineKeyboardButton("◀ Prev", callback_data=f"mkt:{page - 1}"))
    row.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="mkt:noop"))
    if page < total_pages:
        row.append(InlineKeyboardButton("Next ▶", callback_data=f"mkt:{page + 1}"))
    keyboard = InlineKeyboardMarkup([row])

    if send_new:
        await target.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await target.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)

# Web store URL for /market button
WEB_STORE_URL = "https://anime-html-fsyc.vercel.app"

async def market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show market in bot + web store button"""
    # Send web store button first
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Open Web Store", url=WEB_STORE_URL)],
    ])
    await update.message.reply_text(
        "🛒 *Character Market*\n"
        "Browse in web store or use buttons below:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    # Then show paginated list
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    await send_market_page(update.message, page, send_new=True)

async def market_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "mkt:noop":
        return
    page = int(query.data.split(":")[1])
    await send_market_page(query.message, page, send_new=False)

async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context): return
    if not context.args:
        await update.message.reply_text("Usage: /buy <character_id> [id2 ...]")
        return
    char_ids = context.args
    bought = []
    failed = []
    total_cost = 0
    for cid in char_ids:
        char = await db.get_character_by_id(cid)
        if not char:
            failed.append(f"{cid} (not found)")
            continue
        if await db.user_owns_character(user.id, group_id, cid):
            failed.append(f"{cid} (already owned)")
            continue
        total_cost += char['price']
        bought.append(char)
    coins = await db.get_user_coins(user.id)
    if total_cost > coins:
        await update.message.reply_text(f"❌ Insufficient coins. Need {total_cost}, you have {coins}.")
        return
    # Deduct and add to inventory
    for char in bought:
        await db.remove_coins(user.id, char['price'])
        await db.add_to_inventory(user.id, group_id, char['char_id'])
    msg = f"✅ Bought {len(bought)} character(s).\n"
    if failed:
        msg += f"⚠️ Failed: {', '.join(failed)}"
    await update.message.reply_text(msg)

async def sell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context): return
    if not context.args:
        await update.message.reply_text("Usage: /sell <character_id>")
        return
    char_id = context.args[0]
    char = await db.get_character_by_id(char_id)
    if not char:
        await update.message.reply_text("❌ Character not found.")
        return
    if not await db.user_owns_character(user.id, group_id, char_id):
        await update.message.reply_text("❌ You don't own this character.")
        return
    sell_price = int(char['price'] * 0.7)
    await db.add_coins(user.id, sell_price)
    await db.remove_from_inventory(user.id, group_id, char_id)
    await update.message.reply_text(f"💰 Sold {char['name']} for {sell_price} coins (70% of base).")

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /search <character name>")
        return
    query = " ".join(context.args)
    results = await db.search_characters(query)
    if not results:
        await update.message.reply_text("No matching characters.")
        return
    for char in results[:5]:  # limit to 5
        emoji = await db.get_rarity_emoji(char['rarity'])
        caption = await format_character_card(char, emoji)
        if char.get('img_url'):
            await update.message.reply_photo(char['img_url'], caption=caption, parse_mode="Markdown")
        else:
            await update.message.reply_text(caption, parse_mode="Markdown")

# Owner/Dev commands
async def addcharacter_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner can add characters.")
        return ConversationHandler.END
    await update.message.reply_text("Send the character name (or /cancel to stop):")
    return NAME

async def addcharacter_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['char_name'] = update.message.text
    await update.message.reply_text("Send the anime name:")
    return ANIME

async def addcharacter_anime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['char_anime'] = update.message.text
    await update.message.reply_text("Send the image URL (catbox.moe):")
    return IMG_URL

async def addcharacter_img(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['char_img'] = update.message.text
    rarities = "Common, Uncommon, Elite, Epic, Mythic, Waifu, Special Edition, Limited, Event, Legendary"
    await update.message.reply_text(f"Send rarity (choose one):\n{rarities}")
    return RARITY

async def addcharacter_rarity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rarity = update.message.text
    valid = ['Common','Uncommon','Elite','Epic','Mythic','Waifu','Special Edition','Limited','Event','Legendary']
    if rarity not in valid:
        await update.message.reply_text(f"Invalid rarity. Choose from: {', '.join(valid)}")
        return RARITY
    context.user_data['char_rarity'] = rarity
    await update.message.reply_text("Send the price in coins:")
    return PRICE

async def addcharacter_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        price = int(update.message.text)
    except:
        await update.message.reply_text("Price must be a number.")
        return PRICE
    # Generate unique random ID #0000-#9999
    while True:
        rand_id = f"#{random.randint(0,9999):04d}"
        if not await db.get_character_by_id(rand_id):
            break
    await db.add_character(
        rand_id, context.user_data['char_name'], context.user_data['char_anime'],
        context.user_data['char_img'], context.user_data['char_rarity'], price
    )
    await update.message.reply_text(f"✅ Character added with ID {rand_id}")
    return ConversationHandler.END

async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

async def remove_character(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /remove <character_id>")
        return
    char_id = context.args[0]
    await db.remove_character(char_id)
    await update.message.reply_text(f"Removed {char_id} from market and inventories.")

async def listchar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    chars, _ = await db.get_market_characters(1000, 0)
    if not chars:
        await update.message.reply_text("No characters.")
        return
    text = "📋 *All Characters*\n"
    for c in chars[:30]:
        text += f"`{c['char_id']}` {c['name']} ({c['anime']})\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def addcoins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a user's message.")
        return
    target = update.message.reply_to_message.from_user
    group_id = get_effective_group_id(update)
    try:
        amount = int(context.args[0])
    except:
        await update.message.reply_text("Usage: /addcoins <amount> (reply to user)")
        return
    await db.add_coins(target.id, amount)
    await update.message.reply_text(f"Added {amount} coins to {target.first_name}.")

async def removecoins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a user.")
        return
    target = update.message.reply_to_message.from_user
    group_id = get_effective_group_id(update)
    try:
        amount = int(context.args[0])
    except:
        await update.message.reply_text("Usage: /removecoins <amount> (reply)")
        return
    await db.remove_coins(target.id, amount)
    await update.message.reply_text(f"Removed {amount} coins from {target.first_name}.")

async def setstartvid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    if not update.message.reply_to_message or not update.message.reply_to_message.video:
        await update.message.reply_text("Reply to a video with /setstartvid")
        return
    file_id = update.message.reply_to_message.video.file_id
    await db.set_start_video(file_id)
    await update.message.reply_text("Start video updated.")

async def setwelcomepic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Use this command in a group.")
        return
    if not update.message.reply_to_message or not update.message.reply_to_message.photo:
        await update.message.reply_text("Reply to a photo with /setwelcomepic")
        return
    file_id = update.message.reply_to_message.photo[-1].file_id
    await db.set_group_welcome_img(chat.id, file_id)
    await update.message.reply_text("Welcome image set for this group.")

async def resetgrpdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("Only owner.")
        return
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Use in group.")
        return
    # Delete all user data for this group
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM users WHERE group_id = $1', chat.id)
        await conn.execute('DELETE FROM inventory WHERE group_id = $1', chat.id)
    await update.message.reply_text("All user data reset for this group.")

# Group admin commands
async def listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command only works in groups.")
        return
    admins = await chat.get_administrators()
    human_admins = [a.user for a in admins if not a.user.is_bot]
    if not human_admins:
        await update.message.reply_text("No human admins found.")
        return
    text = "👥 *Human Admins*\n"
    for admin in human_admins:
        text += f"• {admin.full_name}\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def calladmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Only in groups.")
        return
    # Check rate limit
    last = await db.get_last_calladmins(chat.id)
    if last and (datetime.utcnow() - last).total_seconds() < 600:
        await update.message.reply_text("⏳ Rate limit: once per 10 minutes per group.")
        return
    admins = await chat.get_administrators()
    human_admins = [a.user for a in admins if not a.user.is_bot]
    if not human_admins:
        await update.message.reply_text("No human admins to call.")
        return
    mentions = []
    for admin in human_admins:
        mentions.append(f'<a href="tg://user?id={admin.id}">.</a>')
    text = "Hey admins come fast 🙃\n" + "".join(mentions)
    await update.message.reply_text(text, parse_mode="HTML")
    await db.update_calladmins_time(chat.id)

# Welcome new members
async def welcome_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        return
    for member in update.message.new_chat_members:
        if member.is_bot:
            continue
        # Clickable first name — shows the name, tapping opens their profile
        mention = f'<a href="tg://user?id={member.id}">{member.first_name}</a>'
        welcome_text = (
            f"👋 Welcome to <b>{chat.title}</b>, {mention}!\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🎮 Use /start to begin your adventure\n"
            f"💰 Claim daily coins with /daily\n"
            f"🎴 Get your first character with /claim\n"
            f"🛒 Browse the /market for more!"
        )
        welcome_img = await db.get_group_welcome_img(chat.id)
        if welcome_img:
            await update.message.reply_photo(welcome_img, caption=welcome_text, parse_mode="HTML")
        else:
            await update.message.reply_text(welcome_text, parse_mode="HTML")

# ---------- Main Entry Point ----------
async def run_bot():
    # Initialize DB and tables
    await db.connect()
    await db.init_tables()

    application = Application.builder().token(BOT_TOKEN).build()

    # User commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("daily", daily))
    application.add_handler(CommandHandler("claim", claim))
    application.add_handler(CommandHandler("wallet", wallet))
    application.add_handler(CommandHandler("vault", vault))
    application.add_handler(CommandHandler("mycollection", vault))
    application.add_handler(CommandHandler("market", market))
    application.add_handler(CommandHandler("buy", buy))
    application.add_handler(CommandHandler("sell", sell))
    application.add_handler(CommandHandler("search", search))

    # Group admin
    application.add_handler(CommandHandler("listadmins", listadmins))
    application.add_handler(CommandHandler("calladmins", calladmins))

    # Owner/dev commands
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("addcharacter", addcharacter_start)],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcharacter_name)],
            ANIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcharacter_anime)],
            IMG_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcharacter_img)],
            RARITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcharacter_rarity)],
            PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, addcharacter_price)],
        },
        fallbacks=[CommandHandler("cancel", cancel_add)],
    )
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("remove", remove_character))
    application.add_handler(CommandHandler("listchar", listchar))
    application.add_handler(CommandHandler("addcoins", addcoins))
    application.add_handler(CommandHandler("removecoins", removecoins))
    application.add_handler(CommandHandler("setstartvid", setstartvid))
    application.add_handler(CommandHandler("setwelcomepic", setwelcomepic))
    application.add_handler(CommandHandler("resetgrpdata", resetgrpdata))

    # Welcome new members
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_members))

    # Pagination callbacks (market + vault)
    application.add_handler(CallbackQueryHandler(market_callback, pattern=r'^mkt:'))
    application.add_handler(CallbackQueryHandler(vault_callback, pattern=r'^vlt:'))

    # Start bot polling
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    print("Bot started...")
    return application

async def main():
    await run_bot()

if __name__ == "__main__":
    asyncio.run(main())