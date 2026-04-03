import os
import asyncio
import random
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any

from dotenv import load_dotenv
import asyncpg
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler,
    ContextTypes, ChatMemberHandler
)
from telegram.constants import ChatMemberStatus as CMS
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import uvicorn

load_dotenv()

# ---------- Configuration ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN missing")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing")

OWNER_ID = int(os.getenv("OWNER_ID", "7728424218"))
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "YorichiiPrime")
OWNER_LINK = f"https://t.me/{OWNER_USERNAME}"
SUPPORT_LINK = os.getenv("SUPPORT_LINK", "https://t.me/kingchaos7")
WEB_STORE_URL = os.getenv("WEB_STORE_URL", "https://your-vercel-app.vercel.app")

# Conversation states
NAME, ANIME, IMG_URL, RARITY, PRICE = range(5)

# Global scheduler
scheduler = AsyncIOScheduler()

# Salty messages (no .format, we will replace manually)
SALTY_MESSAGES = [
    "No one guessed... what a fool group 🙄",
    "50 minutes and nothing? Embarrassing 💀",
    "The character was {name}... y'all really didn't know? 😂",
    "Zero correct guesses. Zero. 💀",
    "Even my grandma would have guessed that. Unbelievable.",
    "Dropped the ball on this one, didn't you? 🏀❌",
    "Mystery remains... because none of you tried hard enough 🤷",
    "Y'all need to watch more anime. That was easy."
]

# ---------- Database Layer ----------
class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(DATABASE_URL)

    async def init_tables(self):
        async with self.pool.acquire() as conn:
            # Original tables
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    group_id BIGINT PRIMARY KEY,
                    welcome_img_id TEXT,
                    last_calladmins TIMESTAMP
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT, group_id BIGINT, PRIMARY KEY (user_id, group_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS global_users (
                    user_id BIGINT PRIMARY KEY, coins INT DEFAULT 0
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS group_user_data (
                    user_id BIGINT, group_id BIGINT,
                    last_daily TIMESTAMP, last_claim TIMESTAMP,
                    daily_streak INT DEFAULT 0, last_daily_date DATE,
                    PRIMARY KEY (user_id, group_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS characters (
                    char_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL, anime TEXT NOT NULL, img_url TEXT NOT NULL,
                    rarity TEXT NOT NULL, rarity_tier INT NOT NULL,
                    price INT NOT NULL, is_available BOOLEAN DEFAULT TRUE
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS inventory (
                    user_id BIGINT, group_id BIGINT, char_id TEXT REFERENCES characters(char_id),
                    PRIMARY KEY (user_id, group_id, char_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS started_users (
                    user_id BIGINT PRIMARY KEY, started_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_config (
                    key TEXT PRIMARY KEY, value TEXT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS rarity_ranking (
                    rarity TEXT PRIMARY KEY, tier INT UNIQUE, emoji TEXT
                )
            ''')
            # Drop system tables
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS drops_active (
                    group_id BIGINT PRIMARY KEY, enabled BOOLEAN DEFAULT FALSE
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS current_drops (
                    group_id BIGINT PRIMARY KEY,
                    char_id TEXT, start_time TIMESTAMP, message_id BIGINT,
                    hint_shown BOOLEAN DEFAULT FALSE, winner_id BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS drop_winners (
                    group_id BIGINT, user_id BIGINT,
                    win_streak INT DEFAULT 0, last_win_time TIMESTAMP,
                    PRIMARY KEY (group_id, user_id)
                )
            ''')
            # Tasks system
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id SERIAL PRIMARY KEY,
                    type TEXT, target TEXT, reward INT,
                    description TEXT, created_by BIGINT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS task_completions (
                    user_id BIGINT, task_id INT,
                    completed_at TIMESTAMP, next_reset TIMESTAMP,
                    PRIMARY KEY (user_id, task_id)
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    referrer_id BIGINT, referred_id BIGINT PRIMARY KEY, joined_at TIMESTAMP
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS group_adds (
                    user_id BIGINT, group_id BIGINT PRIMARY KEY,
                    added_at TIMESTAMP, rewarded BOOLEAN DEFAULT FALSE
                )
            ''')
            # Fixed bonus_tasks: generic table, no dynamic columns
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bonus_tasks (
                    user_id BIGINT,
                    task_name TEXT,
                    completed BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (user_id, task_name)
                )
            ''')
            # Guess cooldown per drop
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS guess_cooldown (
                    user_id BIGINT, group_id BIGINT,
                    last_guess TIMESTAMP,
                    PRIMARY KEY (user_id, group_id)
                )
            ''')
            # Default rarities
            await conn.executemany('''
                INSERT INTO rarity_ranking (rarity, tier, emoji)
                VALUES ($1, $2, $3) ON CONFLICT (rarity) DO NOTHING
            ''', [
                ('Common', 1, '⚪'), ('Uncommon', 2, '🟢'), ('Elite', 3, '🔵'),
                ('Epic', 4, '🟣'), ('Mythic', 5, '🔴'), ('Waifu', 6, '💖'),
                ('Special Edition', 7, '✨'), ('Limited', 8, '⏳'),
                ('Event', 9, '🎉'), ('Legendary', 10, '🌟')
            ])

    # ---------- User tracking ----------
    async def user_has_started(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT 1 FROM started_users WHERE user_id = $1', user_id) is not None

    async def register_start(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO started_users (user_id) VALUES ($1) ON CONFLICT DO NOTHING', user_id)

    # ---------- Groups ----------
    async def get_group_welcome_img(self, group_id: int) -> Optional[str]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT welcome_img_id FROM groups WHERE group_id = $1', group_id)

    async def set_group_welcome_img(self, group_id: int, file_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO groups (group_id, welcome_img_id) VALUES ($1, $2) '
                'ON CONFLICT (group_id) DO UPDATE SET welcome_img_id = $2',
                group_id, file_id
            )

    async def get_last_calladmins(self, group_id: int) -> Optional[datetime]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT last_calladmins FROM groups WHERE group_id = $1', group_id)

    async def update_calladmins_time(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO groups (group_id, last_calladmins) VALUES ($1, NOW()) '
                'ON CONFLICT (group_id) DO UPDATE SET last_calladmins = NOW()',
                group_id
            )

    # ---------- Coins ----------
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

    # ---------- Daily & Claim ----------
    async def can_claim_daily(self, user_id: int, group_id: int) -> bool:
        async with self.pool.acquire() as conn:
            last = await conn.fetchval(
                'SELECT last_daily FROM group_user_data WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            )
            if not last:
                return True
            return (datetime.utcnow() - last).total_seconds() >= 7200

    async def record_daily(self, user_id: int, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO group_user_data (user_id, group_id, last_daily)
                VALUES ($1, $2, NOW())
                ON CONFLICT (user_id, group_id) DO UPDATE SET last_daily = NOW()
            ''', user_id, group_id)

    async def update_daily_streak(self, user_id: int, group_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT daily_streak, last_daily_date FROM group_user_data WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            )
            today = datetime.utcnow().date()
            if row and row['last_daily_date']:
                last_date = row['last_daily_date']
                streak = row['daily_streak'] or 0
                if (today - last_date).days == 1:
                    streak += 1
                elif (today - last_date).days > 1:
                    streak = 1
            else:
                streak = 1
            await conn.execute('''
                INSERT INTO group_user_data (user_id, group_id, daily_streak, last_daily_date)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id, group_id) DO UPDATE
                SET daily_streak = $3, last_daily_date = $4
            ''', user_id, group_id, streak, today)
            return streak

    async def get_daily_streak(self, user_id: int, group_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT daily_streak FROM group_user_data WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            ) or 0

    async def can_claim_character(self, user_id: int, group_id: int) -> bool:
        async with self.pool.acquire() as conn:
            last = await conn.fetchval(
                'SELECT last_claim FROM group_user_data WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            )
            if not last:
                return True
            return (datetime.utcnow() - last).total_seconds() >= 39600

    async def record_claim(self, user_id: int, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO group_user_data (user_id, group_id, last_claim)
                VALUES ($1, $2, NOW())
                ON CONFLICT (user_id, group_id) DO UPDATE SET last_claim = NOW()
            ''', user_id, group_id)

    async def get_user_char_count(self, user_id: int, group_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM inventory WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            ) or 0

    # ---------- Characters ----------
    async def add_character(self, char_id: str, name: str, anime: str, img_url: str, rarity: str, price: int):
        async with self.pool.acquire() as conn:
            tier = await conn.fetchval('SELECT tier FROM rarity_ranking WHERE rarity = $1', rarity)
            if tier is None:
                tier = 1
            await conn.execute('''
                INSERT INTO characters (char_id, name, anime, img_url, rarity, rarity_tier, price)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', char_id, name, anime, img_url, rarity, tier, price)

    async def remove_character(self, char_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE characters SET is_available = FALSE WHERE char_id = $1', char_id)

    async def char_id_exists(self, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT 1 FROM characters WHERE char_id = $1', char_id) is not None

    async def get_character_by_id(self, char_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM characters WHERE char_id = $1 AND is_available = true', char_id)
            return dict(row) if row else None

    async def get_character_by_id_any(self, char_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM characters WHERE char_id = $1', char_id)
            return dict(row) if row else None

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
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM characters
                WHERE is_available = true
                AND char_id NOT IN (SELECT char_id FROM inventory WHERE user_id = $1 AND group_id = $2)
                ORDER BY RANDOM() LIMIT 1
            ''', user_id, group_id)
            return dict(row) if row else None

    # ---------- Inventory ----------
    async def add_to_inventory(self, user_id: int, group_id: int, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    'INSERT INTO inventory (user_id, group_id, char_id) VALUES ($1, $2, $3)',
                    user_id, group_id, char_id
                )
                return True
            except asyncpg.UniqueViolationError:
                return False

    async def remove_from_inventory(self, user_id: int, group_id: int, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                'DELETE FROM inventory WHERE user_id = $1 AND group_id = $2 AND char_id = $3',
                user_id, group_id, char_id
            )
            return result != "DELETE 0"

    async def user_owns_character(self, user_id: int, group_id: int, char_id: str) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT 1 FROM inventory WHERE user_id = $1 AND group_id = $2 AND char_id = $3',
                user_id, group_id, char_id
            ) is not None

    async def get_user_inventory(self, user_id: int, group_id: int, limit: int = 1, offset: int = 0) -> Tuple[List[dict], int]:
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

    async def get_market_characters(self, limit: int = 5, offset: int = 0) -> Tuple[List[dict], int]:
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
            await conn.execute(
                'INSERT INTO bot_config (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2',
                'start_video', file_id
            )

    async def get_rarity_emoji(self, rarity: str) -> str:
        async with self.pool.acquire() as conn:
            emoji = await conn.fetchval('SELECT emoji FROM rarity_ranking WHERE rarity = $1', rarity)
            return emoji or '⭐'

    # ---------- Drop System ----------
    async def is_drops_enabled(self, group_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT enabled FROM drops_active WHERE group_id = $1', group_id) or False

    async def enable_drops(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO drops_active (group_id, enabled) VALUES ($1, TRUE) ON CONFLICT (group_id) DO UPDATE SET enabled = TRUE',
                group_id
            )

    async def disable_drops(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO drops_active (group_id, enabled) VALUES ($1, FALSE) ON CONFLICT (group_id) DO UPDATE SET enabled = FALSE',
                group_id
            )

    async def create_drop(self, group_id: int, char_id: str, message_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO current_drops (group_id, char_id, start_time, message_id, hint_shown, winner_id)
                VALUES ($1, $2, NOW(), $3, FALSE, NULL)
                ON CONFLICT (group_id) DO UPDATE SET
                    char_id = $2, start_time = NOW(), message_id = $3, hint_shown = FALSE, winner_id = NULL
            ''', group_id, char_id, message_id)

    async def get_current_drop(self, group_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM current_drops WHERE group_id = $1', group_id)
            return dict(row) if row else None

    async def show_hint(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE current_drops SET hint_shown = TRUE WHERE group_id = $1', group_id)

    async def end_drop(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM current_drops WHERE group_id = $1', group_id)

    async def set_drop_winner(self, group_id: int, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE current_drops SET winner_id = $2 WHERE group_id = $1', group_id, user_id)

    async def get_win_streak(self, group_id: int, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT win_streak FROM drop_winners WHERE group_id = $1 AND user_id = $2',
                group_id, user_id
            ) or 0

    async def update_win_streak(self, group_id: int, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT win_streak, last_win_time FROM drop_winners WHERE group_id = $1 AND user_id = $2',
                group_id, user_id
            )
            if row and row['last_win_time'] and (datetime.utcnow() - row['last_win_time']).total_seconds() < 7200:
                new_streak = (row['win_streak'] or 0) + 1
            else:
                new_streak = 1
            await conn.execute('''
                INSERT INTO drop_winners (group_id, user_id, win_streak, last_win_time)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (group_id, user_id) DO UPDATE
                SET win_streak = $3, last_win_time = NOW()
            ''', group_id, user_id, new_streak)
            return new_streak

    async def reset_win_streak(self, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('UPDATE drop_winners SET win_streak = 0 WHERE group_id = $1', group_id)

    async def get_groups_with_drops(self) -> List[int]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT group_id FROM drops_active WHERE enabled = TRUE')
            return [r['group_id'] for r in rows]

    # ---------- Guess cooldown ----------
    async def can_guess(self, user_id: int, group_id: int, cooldown_sec: int = 10) -> bool:
        async with self.pool.acquire() as conn:
            last = await conn.fetchval(
                'SELECT last_guess FROM guess_cooldown WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            )
            if not last:
                return True
            return (datetime.utcnow() - last).total_seconds() >= cooldown_sec

    async def record_guess(self, user_id: int, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO guess_cooldown (user_id, group_id, last_guess)
                VALUES ($1, $2, NOW())
                ON CONFLICT (user_id, group_id) DO UPDATE SET last_guess = NOW()
            ''', user_id, group_id)

    # ---------- Tasks System ----------
    async def add_task(self, task_type: str, target: str, reward: int, description: str, created_by: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('''
                INSERT INTO tasks (type, target, reward, description, created_by)
                VALUES ($1, $2, $3, $4, $5) RETURNING task_id
            ''', task_type, target, reward, description, created_by)

    async def remove_task(self, task_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('DELETE FROM tasks WHERE task_id = $1', task_id)

    async def get_tasks(self, task_type: str = None) -> List[dict]:
        async with self.pool.acquire() as conn:
            if task_type:
                rows = await conn.fetch('SELECT * FROM tasks WHERE type = $1', task_type)
            else:
                rows = await conn.fetch('SELECT * FROM tasks')
            return [dict(r) for r in rows]

    async def get_task(self, task_id: int) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM tasks WHERE task_id = $1', task_id)
            return dict(row) if row else None

    async def is_task_completed(self, user_id: int, task_id: int) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT next_reset FROM task_completions WHERE user_id = $1 AND task_id = $2',
                user_id, task_id
            )
            if not row:
                return False
            next_reset = row['next_reset']
            if next_reset and datetime.utcnow() > next_reset:
                await conn.execute('DELETE FROM task_completions WHERE user_id = $1 AND task_id = $2', user_id, task_id)
                return False
            return True

    async def complete_task(self, user_id: int, task_id: int, weekly: bool = False):
        async with self.pool.acquire() as conn:
            next_reset = datetime.utcnow() + timedelta(days=7) if weekly else None
            await conn.execute('''
                INSERT INTO task_completions (user_id, task_id, completed_at, next_reset)
                VALUES ($1, $2, NOW(), $3)
                ON CONFLICT (user_id, task_id) DO UPDATE SET completed_at = NOW(), next_reset = $3
            ''', user_id, task_id, next_reset)

    # ---------- Referrals ----------
    async def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        async with self.pool.acquire() as conn:
            if referrer_id == referred_id:
                return False
            existing = await conn.fetchval('SELECT 1 FROM referrals WHERE referred_id = $1', referred_id)
            if existing:
                return False
            await conn.execute(
                'INSERT INTO referrals (referrer_id, referred_id, joined_at) VALUES ($1, $2, NOW())',
                referrer_id, referred_id
            )
            return True

    async def get_referral_count(self, user_id: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT COUNT(*) FROM referrals WHERE referrer_id = $1', user_id) or 0

    async def get_referral_stats(self, user_id: int) -> Tuple[int, int]:
        cnt = await self.get_referral_count(user_id)
        return cnt, cnt * 1000

    # ---------- Group Adds ----------
    async def record_group_add(self, user_id: int, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO group_adds (user_id, group_id, added_at, rewarded)
                VALUES ($1, $2, NOW(), FALSE) ON CONFLICT (group_id) DO NOTHING
            ''', user_id, group_id)

    async def is_group_add_rewarded(self, group_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT rewarded FROM group_adds WHERE group_id = $1', group_id) or False

    async def reward_group_add(self, user_id: int, group_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE group_adds SET rewarded = TRUE WHERE user_id = $1 AND group_id = $2',
                user_id, group_id
            )

    # ---------- Bonus Tasks (fixed) ----------
    async def is_bonus_completed(self, user_id: int, task_name: str) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT completed FROM bonus_tasks WHERE user_id = $1 AND task_name = $2',
                user_id, task_name
            ) or False

    async def complete_bonus(self, user_id: int, task_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO bonus_tasks (user_id, task_name, completed)
                VALUES ($1, $2, TRUE) ON CONFLICT (user_id, task_name) DO UPDATE SET completed = TRUE
            ''', user_id, task_name)

    # ---------- Stats ----------
    async def get_total_users(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval('SELECT COUNT(*) FROM started_users') or 0

    async def get_active_today(self) -> int:
        async with self.pool.acquire() as conn:
            today = datetime.utcnow().date()
            return await conn.fetchval(
                'SELECT COUNT(DISTINCT user_id) FROM group_user_data WHERE last_daily::date = $1',
                today
            ) or 0

    async def get_all_users(self) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT user_id FROM started_users ORDER BY user_id')
            return [dict(r) for r in rows]

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

def get_effective_group_id(update: Update) -> int:
    chat = update.effective_chat
    if chat.type in ["group", "supergroup"]:
        return chat.id
    return 0

async def format_character_card(char: dict, emoji: str) -> str:
    return (
        f"{emoji} *{char['name']}*\n"
        f"🎬 *Anime:* {char['anime']}\n"
        f"💎 *Rarity:* {emoji} {char['rarity']}\n"
        f"🆔 *ID:* `{char['char_id']}`\n"
        f"💰 *Price:* {char['price']} coins"
    )

async def check_collector_bonus(user_id: int, group_id: int, update: Update):
    """If user reaches 10 unique chars in this group, give 2500 coins (once)."""
    if await db.is_bonus_completed(user_id, 'collector'):
        return
    count = await db.get_user_char_count(user_id, group_id)
    if count >= 10:
        await db.complete_bonus(user_id, 'collector')
        await db.add_coins(user_id, 2500)
        await update.message.reply_text("🎉 *Collector Bonus!* You own 10 characters in this group! +2500 coins!", parse_mode="Markdown")

# ---------- Drop System Jobs ----------
async def perform_drop(bot, group_id: int):
    if not await db.is_drops_enabled(group_id):
        return
    if await db.get_current_drop(group_id):
        return  # already active
    char = await db.get_random_character()
    if not char:
        return
    try:
        msg = await bot.send_photo(
            chat_id=group_id,
            photo=char['img_url'],
            caption="🎭 Guess the character!"
        )
        try:
            await bot.pin_chat_message(group_id, msg.message_id)
        except Exception:
            pass  # no permission
        await db.create_drop(group_id, char['char_id'], msg.message_id)

        # Schedule hint and expiry
        scheduler.add_job(
            show_drop_hint, 'date',
            run_date=datetime.utcnow() + timedelta(minutes=30),
            args=[bot, group_id],
            id=f"hint_{group_id}",
            replace_existing=True
        )
        scheduler.add_job(
            expire_drop, 'date',
            run_date=datetime.utcnow() + timedelta(minutes=50),
            args=[bot, group_id],
            id=f"expire_{group_id}",
            replace_existing=True
        )
    except Exception as e:
        print(f"Drop error in {group_id}: {e}")
        await db.disable_drops(group_id)  # auto-disable on fatal error

async def show_drop_hint(bot, group_id: int):
    drop = await db.get_current_drop(group_id)
    if not drop or drop['winner_id'] or drop['hint_shown']:
        return
    char = await db.get_character_by_id_any(drop['char_id'])
    if not char:
        return
    try:
        await bot.edit_message_caption(
            chat_id=group_id,
            message_id=drop['message_id'],
            caption=f"🎭 Guess the character!\n\n💡 Hint: From *{char['anime']}*",
            parse_mode="Markdown"
        )
        await db.show_hint(group_id)
    except Exception:
        pass

async def expire_drop(bot, group_id: int):
    drop = await db.get_current_drop(group_id)
    if not drop or drop['winner_id']:
        return
    char = await db.get_character_by_id_any(drop['char_id'])
    if not char:
        await db.end_drop(group_id)
        return
    try:
        await bot.unpin_chat_message(group_id, drop['message_id'])
    except Exception:
        pass
    msg = random.choice(SALTY_MESSAGES).replace("{name}", char['name'])
    await bot.send_message(group_id, msg)
    await db.end_drop(group_id)
    await db.reset_win_streak(group_id)

async def start_drop_scheduler(bot):
    async def hourly_drops():
        groups = await db.get_groups_with_drops()
        for gid in groups:
            await perform_drop(bot, gid)
    scheduler.add_job(hourly_drops, IntervalTrigger(hours=1), id='hourly_drops', replace_existing=True)
    scheduler.start()

# ---------- Bot Command Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Referral handling
    if context.args and context.args[0].startswith('ref_'):
        try:
            referrer_id = int(context.args[0].split('_')[1])
            if referrer_id != user.id:
                success = await db.add_referral(referrer_id, user.id)
                if success:
                    await db.add_coins(referrer_id, 1000)
                    await db.add_coins(user.id, 500)
                    try:
                        await context.bot.send_message(referrer_id, f"🎉 *New Referral!*\n@{user.username or user.first_name} joined using your link!\n💰 You earned 1000 coins!", parse_mode="Markdown")
                    except:
                        pass
        except:
            pass
    await db.register_start(user.id)
    video_id = await db.get_start_video()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add To Group", url=f"https://t.me/{context.bot.username}?startgroup=true")],
        [InlineKeyboardButton("Owner 🤪", url=OWNER_LINK), InlineKeyboardButton("Support chat 💝", url=SUPPORT_LINK)]
    ])
    if video_id:
        await update.message.reply_video(video_id, caption="Welcome to Anime Character Collector Bot!\nUse /help to see commands.", reply_markup=keyboard)
    else:
        await update.message.reply_text("Welcome to Anime Character Collector Bot!\nUse /help to see commands.", reply_markup=keyboard)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """🎮 *Available Commands*

*💰 Economy*
/daily - Claim coins (every 2h, per group)
/claim - Claim random character (every 11h, per group)
/wallet - Show your coins

*📦 Collection*
/vault - Your collected characters (per group)
/market - View buyable characters
/buy <id> [id2 ...] - Buy character(s)
/sell <id> - Sell character (70% refund)
/search <n> - Search characters

*🎭 Drop System (Groups)*
/guess <name> - Guess the dropped character
/enabledrops - Enable drops (admin/owner)
/disabledrops - Disable drops (admin/owner)

*📋 Tasks (DM Only)*
/tasks - View and complete tasks
/refer - Get your referral link

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
/stats - Bot statistics

*Other*
/start - Start the bot
/help - This menu"""
    await update.message.reply_text(text, parse_mode="Markdown")

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context):
        return
    if await db.can_claim_daily(user.id, group_id):
        reward = random.randint(50, 200)
        await db.add_coins(user.id, reward)
        await db.record_daily(user.id, group_id)
        streak = await db.update_daily_streak(user.id, group_id)
        coins = await db.get_user_coins(user.id)
        streak_text = f"\n🔥 Daily Streak: {streak} days" if streak > 1 else ""
        if streak >= 7 and streak % 7 == 0:
            await db.add_coins(user.id, 3000)
            streak_text += "\n🎉 *7-Day Streak Bonus!* +3000 coins!"
        await update.message.reply_text(f"💸 You claimed *{reward} coins*!\n💰 Total balance: `{coins}` coins{streak_text}", parse_mode="Markdown")
    else:
        await update.message.reply_text("⏳ You already claimed daily coins here. Try again in 2 hours.")

async def claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context):
        return
    if not await db.can_claim_character(user.id, group_id):
        await update.message.reply_text("⏳ You already claimed a character here. Try again in 11 hours.")
        return
    char = await db.get_random_unowned_character(user.id, group_id)
    if char is None:
        any_char = await db.get_random_character()
        if not any_char:
            await update.message.reply_text("❌ No characters available. Contact the owner.")
            return
        await db.add_coins(user.id, 10000)
        await db.record_claim(user.id, group_id)
        coins = await db.get_user_coins(user.id)
        await update.message.reply_text(
            "🏆 *You own every character in this group's collection!*\n\n╭───────────────╮\n💰 *Reward:* +10,000 Coins\n"
            f"👜 *Balance:* {coins:,} Coins\n╰───────────────╯\n\n⏳ Come back in 11 hours for your next claim!",
            parse_mode="Markdown"
        )
        return
    await db.add_to_inventory(user.id, group_id, char['char_id'])
    await db.record_claim(user.id, group_id)
    emoji = await db.get_rarity_emoji(char['rarity'])
    caption = (
        f"{emoji} *{char['name']}*\n╭───────────────╮\n🎬 Anime: {char['anime']}\n💎 Tier: {emoji} {char['rarity']}\n"
        f"🆔 Card ID: `{char['char_id']}`\n💰 Value: {char['price']:,} Coins\n╰───────────────╯\n\n✨ A rare presence has been claimed…\n"
        f"🌸 Grace\\. Power\\. Mystery — all in one\\.\n\n🔐 Status: Newly Claimed"
    )
    try:
        await update.message.reply_photo(photo=char['img_url'], caption=caption, parse_mode="MarkdownV2")
    except Exception:
        await update.message.reply_text(caption, parse_mode="MarkdownV2")
    await check_collector_bonus(user.id, group_id, update)

async def wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context):
        return
    coins = await db.get_user_coins(user.id)
    char_count = await db.get_user_char_count(user.id, group_id)
    text = (
        f"👤 *{user.first_name}'s Wallet*\n━━━━━━━━━━━━━━━━\n💰 *Coins:* `{coins:,}` _(global)_\n"
        f"🎴 *Characters here:* `{char_count}`\n━━━━━━━━━━━━━━━━\n_/daily to earn coins • /vault to see collection_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def send_vault_page(target, user_id: int, group_id: int, page: int, send_new: bool = True):
    items, total = await db.get_user_inventory(user_id, group_id, 1, page - 1)
    total_pages = max(1, total)
    if not items:
        text = "📭 Your vault is empty here. Use /claim or /buy to get characters!"
        if send_new:
            await target.reply_text(text)
        else:
            await target.edit_text(text)
        return
    char = items[0]
    emoji = await db.get_rarity_emoji(char['rarity'])
    caption = (
        f"{emoji} *{char['name']}*\n╭───────────────╮\n🎬 Anime: {char['anime']}\n💎 Tier: {emoji} {char['rarity']}\n"
        f"🆔 Card ID: `{char['char_id']}`\n💰 Value: {char['price']:,} Coins\n╰───────────────╯\n\n_Card {page} of {total_pages}_"
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
        try:
            await target.edit_media(media=InputMediaPhoto(media=char['img_url'], caption=caption, parse_mode="Markdown"), reply_markup=keyboard)
        except Exception:
            try:
                await target.edit_caption(caption=caption, parse_mode="Markdown", reply_markup=keyboard)
            except Exception:
                await target.edit_text(caption, parse_mode="Markdown", reply_markup=keyboard)

async def vault(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context):
        return
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    await send_vault_page(update.message, user.id, group_id, page, send_new=True)

async def vault_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "vlt:noop":
        await query.answer()
        return
    parts = query.data.split(":")
    user_id, group_id, page = int(parts[1]), int(parts[2]), int(parts[3])
    if update.effective_user.id != user_id:
        await query.answer("⛔ This is not your vault!", show_alert=True)
        return
    await query.answer()
    await send_vault_page(query.message, user_id, group_id, page, send_new=False)

async def send_market_page(target, page: int, send_new: bool = True):
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
    text = f"🛒 *Character Market — Page {page}/{total_pages}*\n━━━━━━━━━━━━━━━━\n\n"
    for c in items:
        emoji = await db.get_rarity_emoji(c['rarity'])
        text += f"{emoji} *{c['name']}* — _{c['anime']}_\n   💰 `{c['price']}` coins  •  🆔 `{c['char_id']}`\n\n"
    text += "_Use /buy \\</id\\> to purchase a character_"
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

async def market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🌐 Open Web Store", url=WEB_STORE_URL)]])
    await update.message.reply_text("🛒 *Character Market*\nBrowse in web store or use buttons below:", parse_mode="Markdown", reply_markup=keyboard)
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
    if not await ensure_started(update, context):
        return
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
    for char in bought:
        await db.remove_coins(user.id, char['price'])
        await db.add_to_inventory(user.id, group_id, char['char_id'])
    if not await db.is_bonus_completed(user.id, 'first_buy'):
        await db.complete_bonus(user.id, 'first_buy')
        await db.add_coins(user.id, 1500)
        await update.message.reply_text("🎉 *First Buy Bonus!* +1500 coins!", parse_mode="Markdown")
    msg = f"✅ Bought {len(bought)} character(s).\n"
    if failed:
        msg += f"⚠️ Failed: {', '.join(failed)}"
    await update.message.reply_text(msg)
    await check_collector_bonus(user.id, group_id, update)

async def sell(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    group_id = get_effective_group_id(update)
    if not await ensure_started(update, context):
        return
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
    for char in results[:5]:
        emoji = await db.get_rarity_emoji(char['rarity'])
        caption = await format_character_card(char, emoji)
        if char.get('img_url'):
            await update.message.reply_photo(char['img_url'], caption=caption, parse_mode="Markdown")
        else:
            await update.message.reply_text(caption, parse_mode="Markdown")

async def guess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("🎭 This command only works in groups!")
        return
    if not context.args:
        await update.message.reply_text("Usage: /guess <character name>")
        return
    user = update.effective_user
    group_id = chat.id
    # Rate limit per user per group (10 sec)
    if not await db.can_guess(user.id, group_id, 10):
        await update.message.reply_text("⏳ Please wait 10 seconds before guessing again.")
        return
    await db.record_guess(user.id, group_id)
    guess_name = " ".join(context.args).lower()
    drop = await db.get_current_drop(group_id)
    if not drop:
        await update.message.reply_text("❌ No active drop right now. Wait for the next one!")
        return
    if drop['winner_id']:
        await update.message.reply_text("🏆 Someone already guessed this character!")
        return
    char = await db.get_character_by_id_any(drop['char_id'])
    if not char:
        await update.message.reply_text("❌ Error loading character data.")
        return
    char_name = char['name'].lower()
    char_first = char_name.split()[0]
    if guess_name == char_name or guess_name == char_first:
        await db.set_drop_winner(group_id, user.id)
        streak = await db.update_win_streak(group_id, user.id)
        already_owned = await db.user_owns_character(user.id, group_id, char['char_id'])
        if already_owned:
            reward = 5000
            reward_text = "💰 You already own this character, so you get 5000 coins instead!"
        else:
            reward = 500
            await db.add_to_inventory(user.id, group_id, char['char_id'])
            reward_text = f"🎴 {char['name']} added to your vault!"
        await db.add_coins(user.id, reward)
        try:
            await context.bot.unpin_chat_message(group_id, drop['message_id'])
        except:
            pass
        mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
        streak_text = f"\n🔥 x{streak} Streak!" if streak >= 3 else ""
        await update.message.reply_text(
            f"✨ *CORRECT!* {mention} guessed *{char['name']}*!{streak_text}\n\n💰 Reward: +{reward} coins\n{reward_text}",
            parse_mode="Markdown"
        )
        if streak >= 3:
            try:
                streak_msg = await context.bot.send_message(group_id, f"🔥 @{user.username or user.first_name} x{streak} streak!", parse_mode="Markdown")
                await context.bot.pin_chat_message(group_id, streak_msg.message_id)
            except:
                pass
        await db.end_drop(group_id)
        if not await db.is_bonus_completed(user.id, 'first_guess'):
            await db.complete_bonus(user.id, 'first_guess')
            await db.add_coins(user.id, 1000)
            await update.message.reply_text("🎉 *First Guess Win Bonus!* +1000 coins!", parse_mode="Markdown")
        await check_collector_bonus(user.id, group_id, update)
    else:
        await update.message.reply_text("❌ Wrong guess! Try again!")

async def enabledrops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command only works in groups!")
        return
    is_admin = is_owner(user.id)
    if not is_admin:
        try:
            member = await chat.get_member(user.id)
            is_admin = member.status in [CMS.ADMINISTRATOR, CMS.OWNER]
        except:
            pass
    if not is_admin:
        await update.message.reply_text("⛔ Only group admins or the bot owner can enable drops.")
        return
    await db.enable_drops(chat.id)
    await update.message.reply_text("🎭 *Drops Enabled!*\n\nCharacters will now drop every hour.\nUse /guess <name> to guess the character!", parse_mode="Markdown")

async def disabledrops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command only works in groups!")
        return
    is_admin = is_owner(user.id)
    if not is_admin:
        try:
            member = await chat.get_member(user.id)
            is_admin = member.status in [CMS.ADMINISTRATOR, CMS.OWNER]
        except:
            pass
    if not is_admin:
        await update.message.reply_text("⛔ Only group admins or the bot owner can disable drops.")
        return
    await db.disable_drops(chat.id)
    await update.message.reply_text("🚫 *Drops Disabled!*\n\nNo more automatic drops in this group.", parse_mode="Markdown")

async def tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat.type != "private":
        await update.message.reply_text("📋 Please use /tasks in DM (private chat) with me!")
        return
    ref_count, ref_earned = await db.get_referral_stats(user.id)
    text = f"📋 *Your Tasks*\n━━━━━━━━━━━━━━━━\n\n📨 *Referral Task*\n   ✅ Refer friends and earn!\n   📊 Referred: {ref_count} users | Earned: {ref_earned} coins\n   💰 Reward: 1000 coins per referral\n\n"
    channel_tasks = await db.get_tasks("channel")
    text += "📢 *Channel Join Tasks*\n"
    if channel_tasks:
        for t in channel_tasks:
            completed = await db.is_task_completed(user.id, t['task_id'])
            status = "✅ Completed" if completed else "⏳ Pending"
            text += f"   {status}: {t['description']} (+{t['reward']} coins)\n"
    else:
        text += "   No channel tasks available\n"
    text += "\n➕ *Add Bot to Group*\n   ⏳ Add me to a group as admin (+5000 coins)\n\n🎁 *Bonus Tasks*\n"
    streak = await db.get_daily_streak(user.id, 0)
    streak_status = "✅" if streak >= 7 else "⏳"
    text += f"   {streak_status} 7-Day Streak ({streak}/7) (+3000 coins)\n"
    first_buy = await db.is_bonus_completed(user.id, 'first_buy')
    text += f"   {'✅' if first_buy else '⏳'} First Buy (+1500 coins)\n"
    char_count = await db.get_user_char_count(user.id, 0)
    collector_done = await db.is_bonus_completed(user.id, 'collector')
    text += f"   {'✅' if collector_done else '⏳'} Collector ({char_count}/10 chars) (+2500 coins)\n"
    first_guess = await db.is_bonus_completed(user.id, 'first_guess')
    text += f"   {'✅' if first_guess else '⏳'} First Guess Win (+1000 coins)\n"
    buttons = []
    for t in channel_tasks:
        if not await db.is_task_completed(user.id, t['task_id']):
            buttons.append([InlineKeyboardButton(f"✅ Done: {t['description'][:30]}...", callback_data=f"task:{t['task_id']}")])
    keyboard = InlineKeyboardMarkup(buttons) if buttons else None
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)

async def tasks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    await query.answer()
    if not query.data.startswith("task:"):
        return
    task_id = int(query.data.split(":")[1])
    task = await db.get_task(task_id)
    if not task:
        await query.edit_message_text("❌ Task not found.")
        return
    if await db.is_task_completed(user.id, task_id):
        await query.edit_message_text("✅ You already completed this task!")
        return
    try:
        channel = task['target']
        if 't.me/' in channel:
            username = channel.split('t.me/')[-1].split('/')[0]
            if username.startswith('@'):
                username = username[1:]
            channel = f"@{username}"
        member = await context.bot.get_chat_member(channel, user.id)
        if member.status in [CMS.MEMBER, CMS.ADMINISTRATOR, CMS.OWNER, CMS.CREATOR]:
            await db.complete_task(user.id, task_id, weekly=True)
            await db.add_coins(user.id, task['reward'])
            await query.edit_message_text(f"🎉 *Task Complete!*\n\n✅ Joined {task['description']}\n💰 Reward: +{task['reward']} coins!", parse_mode="Markdown")
        else:
            await query.answer("❌ You haven't joined the channel yet!", show_alert=True)
    except Exception:
        await query.answer("❌ Error verifying membership. Make sure the bot is admin in the channel!", show_alert=True)

async def refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat.type != "private":
        await update.message.reply_text("📨 Please use /refer in DM with me!")
        return
    ref_link = f"https://t.me/{context.bot.username}?start=ref_{user.id}"
    ref_count, ref_earned = await db.get_referral_stats(user.id)
    text = f"📨 *Your Referral Link*\n━━━━━━━━━━━━━━━━\n\n🔗 {ref_link}\n\n📊 *Stats:*\n   👥 Referred: {ref_count} users\n   💰 Total earned: {ref_earned} coins\n\n💡 Share this link with friends!\n   • You get 1000 coins per referral\n   • They get 500 coins for joining"
    await update.message.reply_text(text, parse_mode="Markdown")

# ---------- Owner/Dev Commands ----------
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
    valid = ['Common', 'Uncommon', 'Elite', 'Epic', 'Mythic', 'Waifu', 'Special Edition', 'Limited', 'Event', 'Legendary']
    if rarity not in valid:
        await update.message.reply_text(f"Invalid rarity. Choose from: {', '.join(valid)}")
        return RARITY
    context.user_data['char_rarity'] = rarity
    await update.message.reply_text("Send the price in coins:")
    return PRICE

async def addcharacter_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        price = int(update.message.text)
    except ValueError:
        await update.message.reply_text("Price must be a number.")
        return PRICE
    while True:
        rand_id = f"#{random.randint(0, 9999):04d}"
        if not await db.char_id_exists(rand_id):
            break
    await db.add_character(rand_id, context.user_data['char_name'], context.user_data['char_anime'], context.user_data['char_img'], context.user_data['char_rarity'], price)
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
    await update.message.reply_text(f"Removed {char_id} from the market (existing inventories unaffected).")

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
    try:
        amount = int(context.args[0])
    except (IndexError, ValueError):
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
    try:
        amount = int(context.args[0])
    except (IndexError, ValueError):
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
    async with db.pool.acquire() as conn:
        await conn.execute('DELETE FROM users WHERE group_id = $1', chat.id)
        await conn.execute('DELETE FROM inventory WHERE group_id = $1', chat.id)
        await conn.execute('DELETE FROM group_user_data WHERE group_id = $1', chat.id)
    await update.message.reply_text("All user data reset for this group.")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Owner only command.")
        return
    total_users = await db.get_total_users()
    active_today = await db.get_active_today()
    text = f"📊 *Bot Statistics*\n━━━━━━━━━━━━━━━━\nTotal Users: {total_users}\nActive Today: {active_today}\n\n👤 *User List:*\n"
    users = await db.get_all_users()
    for i, u in enumerate(users[:50], 1):
        text += f"{i}. User ID: `{u['user_id']}`\n"
    if len(users) > 50:
        text += f"\n... and {len(users) - 50} more users"
    await update.message.reply_text(text, parse_mode="Markdown")

async def addtask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Owner only.")
        return
    if len(context.args) < 4:
        await update.message.reply_text("Usage: /addtask channel @username reward_coins description")
        return
    task_type = context.args[0]
    target = context.args[1]
    try:
        reward = int(context.args[2])
    except ValueError:
        await update.message.reply_text("Reward must be a number.")
        return
    description = " ".join(context.args[3:])
    task_id = await db.add_task(task_type, target, reward, description, update.effective_user.id)
    await update.message.reply_text(f"✅ Task added! ID: {task_id}")

async def removetask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Owner only.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removetask <task_id>")
        return
    try:
        task_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Task ID must be a number.")
        return
    await db.remove_task(task_id)
    await update.message.reply_text(f"✅ Task {task_id} removed.")

async def listtasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("⛔ Owner only.")
        return
    tasks = await db.get_tasks()
    if not tasks:
        await update.message.reply_text("No tasks found.")
        return
    text = "📋 *All Tasks*\n━━━━━━━━━━━━━━━━\n"
    for t in tasks:
        text += f"ID: `{t['task_id']}` | Type: {t['type']}\nTarget: {t['target']}\nReward: {t['reward']} coins\nDesc: {t['description']}\n\n"
    await update.message.reply_text(text, parse_mode="Markdown")

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
    text = "👥 *Human Admins*\n" + "\n".join(f"• {a.full_name}" for a in human_admins)
    await update.message.reply_text(text, parse_mode="Markdown")

async def calladmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("Only in groups.")
        return
    last = await db.get_last_calladmins(chat.id)
    if last and (datetime.utcnow() - last).total_seconds() < 600:
        await update.message.reply_text("⏳ Rate limit: once per 10 minutes per group.")
        return
    admins = await chat.get_administrators()
    human_admins = [a.user for a in admins if not a.user.is_bot]
    if not human_admins:
        await update.message.reply_text("No human admins to call.")
        return
    mentions = [f'<a href="tg://user?id={a.id}">.</a>' for a in human_admins]
    text = "Hey admins come fast 🙃\n" + "".join(mentions)
    await update.message.reply_text(text, parse_mode="HTML")
    await db.update_calladmins_time(chat.id)

async def welcome_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ["group", "supergroup"]:
        return
    for member in update.message.new_chat_members:
        if member.is_bot:
            continue
        mention = f'<a href="tg://user?id={member.id}">{member.first_name}</a>'
        welcome_text = f"👋 Welcome to <b>{chat.title}</b>, {mention}!\n━━━━━━━━━━━━━━━━\n🎮 Use /start to begin your adventure\n💰 Claim daily coins with /daily\n🎴 Get your first character with /claim\n🛒 Browse the /market for more!"
        welcome_img = await db.get_group_welcome_img(chat.id)
        if welcome_img:
            await update.message.reply_photo(welcome_img, caption=welcome_text, parse_mode="HTML")
        else:
            await update.message.reply_text(welcome_text, parse_mode="HTML")

async def bot_added_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member = update.chat_member
    if chat_member.new_chat_member.user.id == context.bot.id and chat_member.new_chat_member.status == CMS.ADMINISTRATOR:
        adder_id = chat_member.from_user.id
        group_id = chat_member.chat.id
        await db.record_group_add(adder_id, group_id)
        if not await db.is_group_add_rewarded(group_id):
            await db.reward_group_add(adder_id, group_id)
            await db.add_coins(adder_id, 5000)
            try:
                await context.bot.send_message(adder_id, f"🎉 *Thanks for adding me to {chat_member.chat.title}!*\n\n💰 You earned 5000 coins!\nUse /tasks to see more ways to earn!", parse_mode="Markdown")
            except:
                pass

# ---------- Web Preview (FastAPI) ----------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>Anime Character Store</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            background: linear-gradient(145deg, #0b0b1a 0%, #1a1a2e 100%);
            font-family: 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 20px;
        }
        .store-container { max-width: 500px; width: 100%; margin: 0 auto; }
        .card {
            background: rgba(20, 20, 40, 0.7);
            backdrop-filter: blur(12px);
            border-radius: 32px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 20px 35px -10px rgba(0,0,0,0.5);
            border: 1px solid rgba(255,255,255,0.1);
            transition: transform 0.2s ease;
        }
        .card:hover { transform: translateY(-5px); }
        .character-img {
            width: 100%; border-radius: 28px; object-fit: cover;
            aspect-ratio: 1 / 1; background: #111;
            box-shadow: 0 8px 20px rgba(0,0,0,0.3);
        }
        .info { margin-top: 16px; }
        .name {
            font-size: 1.8rem; font-weight: bold;
            background: linear-gradient(135deg, #FFD966, #FF8C42);
            -webkit-background-clip: text; background-clip: text;
            color: transparent; margin-bottom: 6px;
        }
        .anime { font-size: 1rem; color: #ccc; margin-bottom: 8px; }
        .rarity {
            display: inline-block; background: rgba(255,215,0,0.2);
            padding: 4px 12px; border-radius: 40px;
            font-size: 0.8rem; font-weight: bold; margin-bottom: 12px;
        }
        .price { font-size: 1.3rem; font-weight: bold; color: #7CFC00; }
        .id { font-family: monospace; font-size: 0.8rem; color: #aaa; margin-top: 8px; }
        .slider-controls {
            display: flex; justify-content: center;
            gap: 20px; margin-top: 20px; margin-bottom: 20px;
        }
        button {
            background: #2c2c54; border: none; color: white;
            font-size: 1.5rem; padding: 10px 24px;
            border-radius: 60px; cursor: pointer;
            transition: all 0.2s; box-shadow: 0 4px 10px rgba(0,0,0,0.3);
        }
        button:active { transform: scale(0.96); }
        .page-indicator { text-align: center; color: #aaa; margin-top: 10px; }
        @media (max-width: 500px) { .name { font-size: 1.4rem; } .card { padding: 14px; } }
    </style>
</head>
<body>
<div class="store-container">
    <div id="card-container" class="card-container"></div>
    <div class="slider-controls">
        <button id="prevBtn">◀</button>
        <button id="nextBtn">▶</button>
    </div>
    <div class="page-indicator" id="pageIndicator"></div>
</div>
<script>
    let characters = [];
    let currentIndex = 0;
    let container = document.getElementById('card-container');
    let prevBtn = document.getElementById('prevBtn');
    let nextBtn = document.getElementById('nextBtn');
    let indicator = document.getElementById('pageIndicator');

    function renderCard(index) {
        let char = characters[index];
        if (!char) return;
        let rarityEmoji = {
            'Common':'⚪','Uncommon':'🟢','Elite':'🔵','Epic':'🟣','Mythic':'🔴',
            'Waifu':'💖','Special Edition':'✨','Limited':'⏳','Event':'🎉','Legendary':'🌟'
        }[char.rarity] || '⭐';
        container.innerHTML = `
            <div class="card">
                <img class="character-img" src="${char.img_url}" alt="${char.name}" loading="lazy">
                <div class="info">
                    <div class="name">${rarityEmoji} ${char.name}</div>
                    <div class="anime">🎬 ${char.anime}</div>
                    <div class="rarity">💎 ${char.rarity}</div>
                    <div class="price">💰 ${char.price} coins</div>
                    <div class="id">🆔 ${char.char_id}</div>
                </div>
            </div>`;
        indicator.innerText = `Character ${currentIndex+1} of ${characters.length}`;
    }

    function loadCharacters() {
        fetch('/api/characters')
            .then(res => res.json())
            .then(data => {
                characters = data;
                if (characters.length === 0) {
                    container.innerHTML = '<div class="card"><div class="info">No characters available yet.</div></div>';
                    prevBtn.disabled = true;
                    nextBtn.disabled = true;
                    indicator.innerText = '0 characters';
                    return;
                }
                currentIndex = 0;
                renderCard(0);
            });
    }

    prevBtn.addEventListener('click', () => {
        if (!characters.length) return;
        currentIndex = (currentIndex - 1 + characters.length) % characters.length;
        renderCard(currentIndex);
    });
    nextBtn.addEventListener('click', () => {
        if (!characters.length) return;
        currentIndex = (currentIndex + 1) % characters.length;
        renderCard(currentIndex);
    });
    loadCharacters();
</script>
</body>
</html>
"""

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/", response_class=HTMLResponse)
async def store_page():
    return HTML_TEMPLATE

@app.get("/api/characters")
async def api_characters():
    chars, _ = await db.get_market_characters(limit=500, offset=0)
    return [{"char_id": c["char_id"], "name": c["name"], "anime": c["anime"], "img_url": c["img_url"], "rarity": c["rarity"], "price": c["price"]} for c in chars]

# ---------- Main Entry Point ----------
async def run_bot():
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

    # Drop system
    application.add_handler(CommandHandler("guess", guess))
    application.add_handler(CommandHandler("enabledrops", enabledrops))
    application.add_handler(CommandHandler("disabledrops", disabledrops))

    # Tasks
    application.add_handler(CommandHandler("tasks", tasks))
    application.add_handler(CommandHandler("refer", refer))
    application.add_handler(CallbackQueryHandler(tasks_callback, pattern=r'^task:'))

    # Group admin
    application.add_handler(CommandHandler("listadmins", listadmins))
    application.add_handler(CommandHandler("calladmins", calladmins))

    # Owner
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
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("addtask", addtask))
    application.add_handler(CommandHandler("removetask", removetask))
    application.add_handler(CommandHandler("listtasks", listtasks))

    # Welcome & group add
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_members))
    application.add_handler(ChatMemberHandler(bot_added_to_group, ChatMemberHandler.CHAT_MEMBER))

    # Pagination callbacks
    application.add_handler(CallbackQueryHandler(market_callback, pattern=r'^mkt:'))
    application.add_handler(CallbackQueryHandler(vault_callback, pattern=r'^vlt:'))

    await start_drop_scheduler(application.bot)
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    print("Bot started...")
    return application

async def run_web():
    port = int(os.getenv("PORT", 8000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    bot_task = asyncio.create_task(run_bot())
    web_task = asyncio.create_task(run_web())
    await asyncio.gather(bot_task, web_task)

if __name__ == "__main__":
    asyncio.run(main())
