import logging
import sqlite3
import random
import pymysql
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# 新增：讀取環境變量庫
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)

# ==================== 🔐 安全配置區域 ====================

# 1. 加載 .env 文件
load_dotenv()

# 2. 從環境變量獲取配置 (代碼裡不再有密碼)
BOT_TOKEN = os.getenv("BOT_TOKEN")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# 3. 檢查配置是否讀取成功
if not BOT_TOKEN or not DB_CONFIG["password"]:
    print("❌ 錯誤：無法讀取 .env 文件中的配置。請確保 .env 文件存在且已填寫。")
    exit(1)

# ==================== ⚙️ 業務配置 ====================

# 流量獎勵範圍 (MB)
BASE_MIN = 100
BASE_MAX = 500

# 套餐限制 (留空 [] 代表不限制)
ALLOWED_PLAN_IDS = [] 

# 暴擊配置
NORMAL_CRIT_RATE = 0.1
NORMAL_CRIT_MULT = 1.5

# ==================== 🗄 本地緩存數據庫 (Bot用) ====================

LOCAL_DB_FILE = "bot_data.db"

def init_local_db():
    with sqlite3.connect(LOCAL_DB_FILE) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS bindings (
            tg_id INTEGER PRIMARY KEY,
            email TEXT UNIQUE,
            streak INTEGER DEFAULT 0,
            last_checkin_date TEXT
        )''')

def get_binding(tg_id):
    with sqlite3.connect(LOCAL_DB_FILE) as conn:
        cursor = conn.execute("SELECT email, streak, last_checkin_date FROM bindings WHERE tg_id=?", (tg_id,))
        return cursor.fetchone()

def save_binding(tg_id, email):
    try:
        with sqlite3.connect(LOCAL_DB_FILE) as conn:
            conn.execute("INSERT OR REPLACE INTO bindings (tg_id, email, streak, last_checkin_date) VALUES (?, ?, 0, '')", (tg_id, email))
        return True
    except sqlite3.IntegrityError:
        return False 

def update_streak(tg_id, streak, date_str):
    with sqlite3.connect(LOCAL_DB_FILE) as conn:
        conn.execute("UPDATE bindings SET streak=?, last_checkin_date=? WHERE tg_id=?", (streak, date_str, tg_id))

# ==================== ⚡ MySQL 直連核心邏輯 ====================

class V2BoardDB:
    def __init__(self, config):
        self.config = config

    def get_connection(self):
        try:
            return pymysql.connect(**self.config)
        except Exception as e:
            logging.error(f"數據庫連接失敗: {e}")
            return None

    def get_user_by_email(self, email):
        conn = self.get_connection()
        if not conn: return None
        try:
            with conn.cursor() as cursor:
                # 兼容不同的表前綴
                sql = "SELECT id, email, transfer_enable, u, d, plan_id, expired_at FROM v2_user WHERE email = %s"
                cursor.execute(sql, (email,))
                return cursor.fetchone()
        finally:
            conn.close()

    def get_plan_name(self, plan_id):
        conn = self.get_connection()
        if not conn: return f"套餐ID: {plan_id}"
        try:
            with conn.cursor() as cursor:
                sql = "SELECT name FROM v2_plan WHERE id = %s"
                cursor.execute(sql, (plan_id,))
                result = cursor.fetchone()
                return result['name'] if result else f"套餐ID: {plan_id}"
        finally:
            conn.close()

    def add_traffic(self, user_id, add_bytes):
        conn = self.get_connection()
        if not conn: return False
        try:
            with conn.cursor() as cursor:
                sql = "UPDATE v2_user SET transfer_enable = transfer_enable + %s WHERE id = %s"
                cursor.execute(sql, (add_bytes, user_id))
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"寫入流量失敗: {e}")
            return False
        finally:
            conn.close()

# 初始化 DB
v2_db = V2BoardDB(DB_CONFIG)
executor = ThreadPoolExecutor(max_workers=3)

# ==================== 🛠 工具函數 ====================

def format_bytes(size):
    if not size: size = 0
    power = 2**10
    n = 0
    power_labels = {0 : '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f}{power_labels[n]}"

def get_progress_bar(used, total, length=10):
    if total == 0: return "⬜" * length
    percent = used / total
    if percent > 1: percent = 1
    filled = int(length * percent)
    return "🟦" * filled + "⬜" * (length - filled) + f" ({percent*100:.1f}%)"

def calculate_reward(streak):
    if streak == 7: return 2.0, "🔥 連簽7天雙倍！", True
    if streak == 14: return 3.0, "💎 連簽14天三倍！", True
    if streak == 21: return 4.0, "👑 連簽21天四倍！", True
    
    if random.random() < NORMAL_CRIT_RATE:
        return NORMAL_CRIT_MULT, "✨ 幸運暴擊", True
    return 1.0, "日常簽到", False

# ==================== 🤖 Bot 指令 (安全+隱私版) ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type
    if chat_type == ChatType.PRIVATE:
        msg = (
            "👋 <b>歡迎使用智能簽到助手 (私聊模式)</b>\n\n"
            "👇 <b>請直接發送指令：</b>\n"
            "<code>/bind 您的郵箱</code>"
        )
    else:
        msg = ("👋 <b>歡迎使用智能簽到助手</b>\n\n建議點擊下方按鈕去私聊綁定。")
        
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔒 點擊去私聊綁定", url=f"https://t.me/{context.bot.username}?start=bind")]
    ])
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)

async def bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    chat_type = update.effective_chat.type
    message_id = update.message.message_id
    
    if not context.args:
        if chat_type != ChatType.PRIVATE:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔒 點擊去私聊綁定", url=f"https://t.me/{context.bot.username}?start=bind")]
            ])
            reply = await update.message.reply_text("🚫 為了隱私，請點擊去私聊綁定：", reply_markup=keyboard)
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=message_id)
                await asyncio.sleep(5)
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=reply.message_id)
            except: pass
            return
        else:
            await update.message.reply_text("❌ 請發送：<code>/bind 你的註冊郵箱</code>", parse_mode=ParseMode.HTML)
            return

    email = context.args[0]
    
    if chat_type != ChatType.PRIVATE:
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=message_id)
        except:
            await update.message.reply_text("⚠️ 為了隱私，請撤回您的消息！")

    loading_msg = await update.message.reply_text("🔍 正在查詢...")
    
    loop = asyncio.get_running_loop()
    user = await loop.run_in_executor(executor, v2_db.get_user_by_email, email)
    
    if user:
        if save_binding(tg_id, email):
            plan_name = await loop.run_in_executor(executor, v2_db.get_plan_name, user.get('plan_id'))
            
            success_text = (
                f"✅ <b>綁定成功！</b>\n\n"
                f"👤 賬號：{email}\n"
                f"📦 套餐：{plan_name}\n"
                f"📊 當前流量：{format_bytes(user['transfer_enable'])}\n\n"
                f"現在您可以在群組發送 <code>簽到</code> 了！"
            )
            
            if chat_type == ChatType.PRIVATE:
                await loading_msg.edit_text(success_text, parse_mode=ParseMode.HTML)
            else:
                await loading_msg.edit_text(f"✅ <b>綁定成功！</b>\n(為了隱私，本消息將在 5 秒後銷毀)", parse_mode=ParseMode.HTML)
                await asyncio.sleep(5)
                try:
                    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=loading_msg.message_id)
                except: pass
        else:
            await loading_msg.edit_text("🚫 綁定失敗：該郵箱已被其他 Telegram 賬號綁定。")
    else:
        await loading_msg.edit_text("🚫 綁定失敗：數據庫中找不到該郵箱。")

async def checkin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    binding = get_binding(tg_id)

    if not binding:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔒 去私聊綁定", url=f"https://t.me/{context.bot.username}?start=bind")]
        ])
        await update.message.reply_text("⚠️ 您還未綁定賬號，請點擊下方按鈕去綁定：", reply_markup=keyboard)
        return

    email, streak, last_date = binding
    today = datetime.now().strftime("%Y-%m-%d")

    if last_date == today:
        await update.message.reply_text(f"📅 <b>今天已簽到</b>\n連簽：{streak} 天，明天繼續！", parse_mode=ParseMode.HTML)
        return

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    current_streak = streak + 1 if last_date == yesterday else 1

    process_msg = await update.message.reply_text("🎲 正在祈禱運勢...")

    loop = asyncio.get_running_loop()
    user_data = await loop.run_in_executor(executor, v2_db.get_user_by_email, email)

    if not user_data:
        await process_msg.edit_text("❌ 錯誤：無法讀取用戶數據。")
        return

    if ALLOWED_PLAN_IDS and user_data['plan_id'] not in ALLOWED_PLAN_IDS:
        await process_msg.edit_text("🚫 您的套餐不支持簽到獎勵。", parse_mode=ParseMode.HTML)
        return

    base_mb = random.randint(BASE_MIN, BASE_MAX)
    multiplier, reason, is_crit = calculate_reward(current_streak)
    final_bytes = int(base_mb * multiplier * 1024 * 1024)

    success = await loop.run_in_executor(
        executor, 
        v2_db.add_traffic, 
        user_data['id'], final_bytes
    )

    if success:
        update_streak(tg_id, current_streak, today)
        plan_name = await loop.run_in_executor(executor, v2_db.get_plan_name, user_data.get('plan_id'))
        
        new_total = user_data['transfer_enable'] + final_bytes
        used = user_data['u'] + user_data['d']
        expire_date = datetime.fromtimestamp(user_data['expired_at']).strftime('%Y-%m-%d') if user_data['expired_at'] else "無限期"
        
        header = "🎰 <b>歐皇附體！</b>" if is_crit else "🎉 <b>簽到成功！</b>"
        
        msg = f"""
{header}
👤 用戶：{update.effective_user.first_name}
🔥 連續簽到：<b>{current_streak}</b> 天 {reason}

📦 套餐：{plan_name}
⏳ 到期：{expire_date}
🎁 獎勵：x{multiplier} (<b>{format_bytes(final_bytes)}</b>)
🌊 當前流量 {format_bytes(new_total)}

📊 流量使用：{format_bytes(used)} / {format_bytes(new_total)}
{get_progress_bar(used, new_total)}

📉 已下載：{format_bytes(user_data['d'])}
📈 已上傳：{format_bytes(user_data['u'])}
📜 <b>規則：</b>
• 7天:2倍 | 14天:3倍 | 21天:4倍
• 斷簽重置，每日隨機暴擊
"""
        await process_msg.edit_text(msg, parse_mode=ParseMode.HTML)
    else:
        await process_msg.edit_text("⚠️ 簽到失敗，數據庫寫入錯誤。")

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    init_local_db()
    
    # 這裡會檢查環境變量是否加載成功
    if not BOT_TOKEN or not DB_CONFIG['password']:
        print("❌ 請確保 .env 文件存在並且已填寫 BOT_TOKEN 和 DB_PASSWORD")
        exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("bind", bind))
    app.add_handler(CommandHandler("checkin", checkin_handler))
    app.add_handler(MessageHandler(filters.Regex(r"^簽到$"), checkin_handler))
    
    print("🚀 V2Board SQL安全版機器人運行中...")
    app.run_polling()
