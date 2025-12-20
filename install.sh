#!/bin/bash

# =================配置区域=================
WORK_DIR="/root/v2bot"
SERVICE_NAME="v2bot"
# =========================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
PLAIN='\033[0m'

if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}错误：请使用 root 用户运行此脚本！${PLAIN}"
   exit 1
fi

# 1. 安装系统环境
function install_env() {
    echo -e "${YELLOW}>>> 正在停止旧服务...${PLAIN}"
    systemctl stop $SERVICE_NAME >/dev/null 2>&1

    echo -e "${YELLOW}>>> 安装系统依赖...${PLAIN}"
    if [[ -f /etc/redhat-release ]]; then
        yum -y update
        yum -y install python3 python3-pip wget redis
        systemctl enable --now redis
    elif [[ -f /etc/debian_version ]]; then
        apt-get update -y
        apt-get -y install python3 python3-pip python3-venv wget redis-server
        systemctl enable --now redis-server
    fi

    mkdir -p $WORK_DIR
    cd $WORK_DIR
}

# 2. 写入 Bot 代码
function write_bot_code() {
    echo -e "${YELLOW}>>> 写入依赖...${PLAIN}"
    cat > requirements.txt <<EOF
python-telegram-bot
pymysql
python-dotenv
redis
requests
EOF

    echo -e "${YELLOW}>>> 配置虚拟环境...${PLAIN}"
    if [ ! -d "venv" ]; then python3 -m venv venv; fi
    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r requirements.txt

    # 配置 .env
    if [ ! -f ".env" ]; then
        echo -e "${GREEN}>>> 配置 Bot 参数 <<<${PLAIN}"
        read -p "请输入 Bot Token: " input_token
        read -p "请输入 网站域名 (如 https://vpn.com): " input_domain
        
        echo -e "${YELLOW}配置数据库连接 (通常是 127.0.0.1)${PLAIN}"
        read -p "数据库地址 (默认 127.0.0.1): " input_db_host
        input_db_host=${input_db_host:-127.0.0.1}
        read -p "数据库名 (默认 v2board): " input_db_name
        input_db_name=${input_db_name:-v2board}
        read -p "数据库用户 (默认 root): " input_db_user
        input_db_user=${input_db_user:-root}
        read -p "请输入 数据库密码: " input_db_pass
        
        input_domain=${input_domain%/}
        
        cat > .env <<EOF
BOT_TOKEN=$input_token
V2BOARD_DOMAIN=$input_domain
DB_HOST=$input_db_host
DB_PORT=3306
DB_DATABASE=$input_db_name
DB_USERNAME=$input_db_user
DB_PASSWORD=$input_db_pass
DB_TABLE_PREFIX=v2_
REDIS_URL=redis://localhost:6379/0
EOF
    fi

    echo -e "${YELLOW}>>> 正在写入 bot.py (紧急修复登录问题版)...${PLAIN}"

cat > bot.py << 'EOF'
import logging
import random
import pymysql
import asyncio
import os
import string
import json
import redis.asyncio as redis
import requests
import traceback
import uuid
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatType
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, CallbackQueryHandler

# ==================== 🛠 配置 ====================
load_dotenv()
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# 签到配置
BASE_MIN, BASE_MAX, NORMAL_CRIT_RATE, NORMAL_CRIT_MULT = 100, 500, 0.1, 1.5

BOT_TOKEN = os.getenv("BOT_TOKEN")
V2BOARD_DOMAIN = (os.getenv("V2BOARD_DOMAIN") or "").rstrip('/')
TABLE_PREFIX = os.getenv("DB_TABLE_PREFIX", "v2_")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# 表名
TBL_USER = f"{TABLE_PREFIX}user"
TBL_PLAN = f"{TABLE_PREFIX}plan"
TBL_ORDER = f"{TABLE_PREFIX}order"
TBL_PAYMENT = f"{TABLE_PREFIX}payment"
TBL_SETTING = f"{TABLE_PREFIX}settings"

executor = ThreadPoolExecutor(max_workers=10)

# ==================== 🧠 数据核心层 ====================

class DataManager:
    @staticmethod
    def get_db_conn(): return pymysql.connect(**DB_CONFIG)

    @classmethod
    async def get_user_by_email(cls, email):
        cache_key = f"v2bot:cache:user:{email}"
        cached = await redis_client.get(cache_key)
        if cached: return json.loads(cached)
        def _q():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    cur.execute(f"SELECT id, email, token, transfer_enable, u, d, plan_id, expired_at FROM {TBL_USER} WHERE email = %s", (email,))
                    return cur.fetchone()
        user = await asyncio.get_event_loop().run_in_executor(executor, _q)
        if user: await redis_client.set(cache_key, json.dumps(user, default=str), ex=30)
        return user

    @classmethod
    async def get_plan_name(cls, plan_id):
        if not plan_id: return "无套餐"
        cache_key = f"v2bot:cache:plan_name:{plan_id}"
        cached = await redis_client.get(cache_key)
        if cached: return cached
        def _q():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    cur.execute(f"SELECT name FROM {TBL_PLAN} WHERE id = %s", (plan_id,))
                    res = cur.fetchone()
                    return res['name'] if res else "未知套餐"
        name = await asyncio.get_event_loop().run_in_executor(executor, _q)
        if name: await redis_client.set(cache_key, name, ex=3600)
        return name

    @classmethod
    async def get_active_plans(cls):
        def _q():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    cur.execute(f"SELECT id, name, month_price FROM {TBL_PLAN} WHERE `show`=1 AND `renew`=1 ORDER BY sort ASC")
                    return cur.fetchall()
        return await asyncio.get_event_loop().run_in_executor(executor, _q)

    @classmethod
    async def get_payment_methods(cls):
        def _q():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    cur.execute(f"SELECT id, name, payment FROM {TBL_PAYMENT} WHERE `enable`=1")
                    return cur.fetchall()
        return await asyncio.get_event_loop().run_in_executor(executor, _q)

    @classmethod
    async def get_pending_order(cls, user_id):
        def _q():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    sql = f"SELECT trade_no, total_amount, plan_id FROM {TBL_ORDER} WHERE user_id=%s AND status=0 ORDER BY created_at DESC LIMIT 1"
                    cur.execute(sql, (user_id,))
                    return cur.fetchone()
        return await asyncio.get_event_loop().run_in_executor(executor, _q)

    @classmethod
    async def cancel_order(cls, trade_no, user_id):
        def _up():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    sql = f"UPDATE {TBL_ORDER} SET status=2 WHERE trade_no=%s AND user_id=%s AND status=0"
                    cur.execute(sql, (trade_no, user_id))
                    c.commit()
        await asyncio.get_event_loop().run_in_executor(executor, _up)

    @classmethod
    async def create_order(cls, user_id, plan_id, amount, cycle, email):
        trade_no = ''.join(random.choices(string.ascii_lowercase + string.digits, k=20))
        now = int(datetime.now().timestamp())
        def _ins():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    sql = f"INSERT INTO {TBL_ORDER} (user_id, plan_id, type, period, trade_no, total_amount, status, created_at, updated_at) VALUES (%s, %s, 1, %s, %s, %s, 0, %s, %s)"
                    cur.execute(sql, (user_id, plan_id, cycle, trade_no, amount, now, now))
                    c.commit()
            return trade_no
        tn = await asyncio.get_event_loop().run_in_executor(executor, _ins)
        await redis_client.delete(f"v2bot:cache:user:{email}")
        return tn

    @classmethod
    async def add_traffic(cls, uid, flow, email):
        def _up():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    cur.execute(f"UPDATE {TBL_USER} SET transfer_enable = transfer_enable + %s WHERE id = %s", (flow, uid))
                    c.commit()
        await asyncio.get_event_loop().run_in_executor(executor, _up)
        await redis_client.delete(f"v2bot:cache:user:{email}")

    @classmethod
    async def get_orders(cls, uid):
        def _q():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    cur.execute(f"SELECT trade_no, total_amount, status, created_at FROM {TBL_ORDER} WHERE user_id=%s ORDER BY created_at DESC LIMIT 5", (uid,))
                    return cur.fetchall()
        return await asyncio.get_event_loop().run_in_executor(executor, _q)

    @classmethod
    async def get_sub_domain(cls):
        cache_key = "v2bot:cache:sub_domains_v6" 
        cached_list = await redis_client.get(cache_key)
        domains = []
        if cached_list:
            domains = json.loads(cached_list)
        else:
            def _q():
                with cls.get_db_conn() as c:
                    with c.cursor() as cur:
                        sql = f"SELECT `value` FROM {TBL_SETTING} WHERE `name` = 'subscribe_url' LIMIT 1"
                        cur.execute(sql)
                        return cur.fetchone()
            try:
                row = await asyncio.get_event_loop().run_in_executor(executor, _q)
                if row and row['value']:
                    raw = row['value'].split(',')
                    domains = [d.strip().rstrip('/') for d in raw if d.strip()]
            except: pass
            if not domains: domains = [V2BOARD_DOMAIN]
            await redis_client.set(cache_key, json.dumps(domains), ex=60) 

        return random.choice(domains)

    @staticmethod
    def call_checkout_api(trade_no, method_id, token):
        url = f"{V2BOARD_DOMAIN}/api/v1/user/order/checkout"
        payload = {"trade_no": trade_no, "method": method_id}
        headers = {"Authorization": token, "User-Agent": "V2BoardBot/1.0"}
        try:
            resp = requests.post(url, data=payload, headers=headers, timeout=10)
            data = resp.json()
            if 'data' in data: return data['data']
            return None
        except: return None

    # [紧急修复] 只重置 Token 和 UUID，绝对不碰 Password (网站登录密码)
    @classmethod
    async def reset_security_direct(cls, user_id, email):
        # 1. 生成新数据
        new_token = ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))
        new_uuid = str(uuid.uuid4())
        now = int(time.time())
        
        # 2. 更新数据库 (仅 Token + UUID + Timestamp)
        def _up():
            with cls.get_db_conn() as c:
                with c.cursor() as cur:
                    # 严禁修改 password 字段！
                    sql = f"UPDATE {TBL_USER} SET token=%s, uuid=%s, updated_at=%s WHERE id=%s"
                    cur.execute(sql, (new_token, new_uuid, now, user_id))
                    c.commit()
        await asyncio.get_event_loop().run_in_executor(executor, _up)
        
        # 3. 清除缓存
        await redis_client.delete(f"v2bot:cache:user:{email}")
        
        return new_token

# ==================== 📐 工具函数 ====================
def safe_int(val):
    try:
        if val is None: return 0
        return int(float(val))
    except: return 0

def format_bytes(size):
    size = float(size or 0)
    power = 1024; n = 0
    labels = {0:'', 1:'KB', 2:'MB', 3:'GB', 4:'TB'}
    while size > power and n < 4:
        size /= power
        n += 1
    return f"{size:.2f}{labels[n]}"

def get_progress_bar(used, total, length=10):
    try:
        if not total or float(total) == 0: return "⬜" * length
        p = min(float(used)/float(total), 1.0)
        filled = int(length * p)
        return "🟦" * filled + "⬜" * (length - filled) + f" ({p*100:.1f}%)"
    except:
        return "⬜" * length + " (0%)"

# ==================== 🛡️ 隐私权限检查 ====================
async def check_priv(u, c):
    if u.effective_chat.type == ChatType.PRIVATE:
        return True
    try:
        bot_user = await c.bot.get_me()
        url = f"https://t.me/{bot_user.username}?start=help"
        kb = [[InlineKeyboardButton("🔒 点击进入私聊", url=url)]]
        msg = await u.message.reply_text("⚠️ <b>此功能涉及隐私，请私聊使用</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        asyncio.create_task(del_msg(msg, 10))
        try: await u.message.delete()
        except: pass
    except Exception as e:
        logger.error(f"Priv Check Error: {e}")
    return False

async def del_msg(m, d):
    await asyncio.sleep(d)
    try: await m.delete()
    except: pass

# ==================== 🤖 Bot 指令 ====================

async def start(u, c):
    if not await check_priv(u, c): return

    msg = (
        "🚀 <b>V2Board 智能助手</b>\n\n"
        "💳 <b>购买:</b> /shop\n"
        "🧾 <b>订单:</b> /orders\n"
        "🔗 <b>订阅:</b> /sub\n"
        "🔄 <b>重置:</b> /reset_sub\n"
        "👤 <b>查询:</b> /info\n"
        "📧 <b>绑定:</b> /bind 邮箱\n\n"
        "✨ <b>群组:</b> 发送「签到」"
    )
    await u.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def bind(u, c):
    if not await check_priv(u, c): return
    if not c.args: return await u.message.reply_text("❌ 格式: `/bind 邮箱`", parse_mode=ParseMode.MARKDOWN)
    email = c.args[0]
    user = await DataManager.get_user_by_email(email)
    if user:
        await redis_client.set(f"v2bot:bind:{u.effective_user.id}", email)
        await u.message.reply_text(f"✅ 绑定成功: {email}")
    else: await u.message.reply_text("🚫 邮箱不存在")

async def info(u, c):
    if not await check_priv(u, c): return
    try:
        email = await redis_client.get(f"v2bot:bind:{u.effective_user.id}")
        if not email: return await u.message.reply_text("⚠️ 请先绑定 /bind")
        
        user = await DataManager.get_user_by_email(email)
        if not user:
            await u.message.reply_text("🚫 无法获取用户信息，请重新绑定")
            return

        plan_name = await DataManager.get_plan_name(user.get('plan_id'))
        
        u_traffic = safe_int(user.get('u'))
        d_traffic = safe_int(user.get('d'))
        transfer_enable = safe_int(user.get('transfer_enable'))
        used = u_traffic + d_traffic
        
        expire_ts = safe_int(user.get('expired_at'))
        if expire_ts > 0:
            expire_str = datetime.fromtimestamp(expire_ts).strftime('%Y-%m-%d')
        else:
            expire_str = "长期有效"

        msg = (
            f"👤 <b>账户信息</b>\n"
            f"📧 {email}\n"
            f"📦 {plan_name}\n"
            f"⏳ 到期: {expire_str}\n"
            f"🌊 流量: {format_bytes(used)} / {format_bytes(transfer_enable)}\n"
            f"{get_progress_bar(used, transfer_enable)}"
        )
        await u.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Info Crash: {traceback.format_exc()}")
        await u.message.reply_text(f"❌ 查询出错: {str(e)}")

async def sub(u, c):
    if not await check_priv(u, c): return
    email = await redis_client.get(f"v2bot:bind:{u.effective_user.id}")
    if not email: return
    user = await DataManager.get_user_by_email(email)
    
    sub_domain = await DataManager.get_sub_domain()
    url = f"{sub_domain}/api/v1/client/subscribe?token={user['token']}"
    
    await u.message.reply_text(f"🔗 <b>订阅链接 (随机节点):</b>\n<code>{url}</code>", parse_mode=ParseMode.HTML)

# [重置订阅] 仅重置 Token 和 UUID，不影响登录
async def reset_sub(u, c):
    if not await check_priv(u, c): return
    email = await redis_client.get(f"v2bot:bind:{u.effective_user.id}")
    if not email: return
    user = await DataManager.get_user_by_email(email)

    msg = await u.message.reply_text("🔄 正在重置订阅链接...")
    
    try:
        new_token = await DataManager.reset_security_direct(user['id'], email)
        sub_domain = await DataManager.get_sub_domain()
        new_url = f"{sub_domain}/api/v1/client/subscribe?token={new_token}"
        
        await msg.edit_text(f"✅ <b>重置成功！</b>\n\n新链接：\n<code>{new_url}</code>\n\n⚠️ 旧订阅链接和节点配置已失效，请更新客户端。", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Reset Error: {e}")
        await msg.edit_text("❌ 系统错误，请查看日志")

# ========== 💰 购买与订单 ==========

async def shop(u, c):
    if not await check_priv(u, c): return
    plans = await DataManager.get_active_plans()
    if not plans: return await u.message.reply_text("📭 暂无套餐")
    kb = []
    for p in plans:
        kb.append([InlineKeyboardButton(f"📦 {p['name']} - {p['month_price']/100}元", callback_data=f"step1:{p['id']}:month_price")])
    await u.message.reply_text("🛒 <b>请选择套餐：</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

async def show_payment_methods(trade_no, amount_str, update):
    methods = await DataManager.get_payment_methods()
    if not methods:
        await update.callback_query.edit_message_text(f"✅ 订单 {trade_no} 存在，但无支付方式。")
        return
    kb = []
    for m in methods:
        kb.append([InlineKeyboardButton(f"💳 {m['name']}", callback_data=f"step2:{trade_no}:{m['id']}")])
    kb.append([InlineKeyboardButton("❌ 取消订单", callback_data=f"cancel:{trade_no}")])
    
    await update.callback_query.edit_message_text(
        f"🧾 <b>订单确认</b>\n单号：<code>{trade_no}</code>\n金额：{amount_str}\n\n👇 <b>请选择支付方式：</b>",
        parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb)
    )

async def btn_handler(u, c):
    q = u.callback_query
    await q.answer()
    data = q.data.split(":")
    action = data[0]

    tg_id = q.from_user.id
    email = await redis_client.get(f"v2bot:bind:{tg_id}")
    if not email: return await q.message.reply_text("⚠️ 请先绑定")
    user = await DataManager.get_user_by_email(email)

    if action == "step1":
        try:
            plan_id, cycle = int(data[1]), data[2]
            pending = await DataManager.get_pending_order(user['id'])
            if pending:
                await show_payment_methods(pending['trade_no'], f"{pending['total_amount']/100} 元", u)
                await q.answer("✋ 发现未支付订单，请先处理", show_alert=True)
                return

            plans = await DataManager.get_active_plans()
            plan = next((p for p in plans if p['id'] == plan_id), None)
            if not plan: return await q.edit_message_text("❌ 套餐已下架")
            
            trade_no = await DataManager.create_order(user['id'], plan_id, plan['month_price'], cycle, email)
            await show_payment_methods(trade_no, f"{plan['month_price']/100} 元", u)
        except Exception as e:
            logger.error(f"Step1 Error: {e}")
            await q.edit_message_text("❌ 系统错误")

    elif action == "repay":
        try:
            trade_no, amount = data[1], data[2]
            await show_payment_methods(trade_no, f"{int(amount)/100} 元", u)
        except: await q.edit_message_text("❌ 无法加载订单")

    elif action == "step2":
        try:
            trade_no, method_id = data[1], int(data[2])
            pay_url = await asyncio.get_event_loop().run_in_executor(executor, DataManager.call_checkout_api, trade_no, method_id, user['token'])
            if not pay_url: pay_url = f"{V2BOARD_DOMAIN}/#/order/{trade_no}"
            
            await redis_client.sadd("v2bot:pending_orders", trade_no)
            await redis_client.set(f"v2bot:order_owner:{trade_no}", tg_id, ex=7200)
            
            kb = [
                [InlineKeyboardButton("🚀 点击跳转支付", url=pay_url)],
                [InlineKeyboardButton("⬅️ 返回", callback_data="back_to_shop")]
            ]
            await q.edit_message_text(f"✅ <b>支付链接已生成</b>\n\n单号：<code>{trade_no}</code>\n请点击下方按钮完成支付，Bot 会自动检测结果。", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        except: await q.edit_message_text("❌ 获取支付链接失败")
    
    elif action == "cancel":
        try:
            await DataManager.cancel_order(data[1], user['id'])
            await q.edit_message_text("🗑️ 订单已取消，您可以重新购买。")
        except: await q.edit_message_text("❌ 取消失败")

    elif action == "back_to_shop":
        await q.message.delete()
        await shop(u, c)

async def orders(u, c):
    if not await check_priv(u, c): return
    email = await redis_client.get(f"v2bot:bind:{u.effective_user.id}")
    if not email: return
    user = await DataManager.get_user_by_email(email)
    ords = await DataManager.get_orders(user['id'])
    
    st_map = {0:"⏳ 待支付", 1:"🔄 开通中", 2:"❌ 已取消", 3:"✅ 已完成"}
    msg = "🧾 <b>最近订单</b>\n━━━━━━━━\n"
    kb = []
    has_pending = False
    
    if not ords: msg += "无记录"
    else:
        for o in ords:
            d = datetime.fromtimestamp(o['created_at']).strftime('%m-%d %H:%M')
            msg += f"<code>{o['trade_no']}</code>\n💰 {o['total_amount']/100}元 | {st_map.get(o['status'],'未知')}\n📅 {d}\n\n"
            if o['status'] == 0 and not has_pending:
                kb.append([InlineKeyboardButton(f"💳 支付待付订单 ({o['total_amount']/100}元)", callback_data=f"repay:{o['trade_no']}:{o['total_amount']}")])
                kb.append([InlineKeyboardButton("❌ 取消该订单", callback_data=f"cancel:{o['trade_no']}")])
                has_pending = True

    await u.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb) if kb else None)

# ========== ✨ 签到 ==========
async def checkin(u, c):
    tg_id = u.effective_user.id
    email = await redis_client.get(f"v2bot:bind:{tg_id}")
    if not email:
        kb = [[InlineKeyboardButton("🔒 去私聊绑定", url=f"https://t.me/{c.bot.username}")]]
        msg = await u.message.reply_text("⚠️ 还没绑定账号，请去私聊绑定：", reply_markup=InlineKeyboardMarkup(kb))
        asyncio.create_task(del_msg(msg, 10))
        return

    today = datetime.now().strftime("%Y-%m-%d")
    if await redis_client.get(f"v2bot:checkin:{tg_id}:{today}"):
        msg = await u.message.reply_text("📅 <b>今天已签到</b>\n明天继续保持哦！", parse_mode=ParseMode.HTML)
        asyncio.create_task(del_msg(msg, 5))
        return

    process_msg = await u.message.reply_text("🎲 正在祈祷运势...")

    last_date = await redis_client.get(f"v2bot:last_date:{tg_id}")
    streak = 1
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if last_date == yesterday: streak = int(await redis_client.get(f"v2bot:streak:{tg_id}") or 0) + 1

    mult = 1.0; reason = "日常签到"; is_crit = False
    if streak % 21 == 0: mult = 4.0; reason = "👑 连签21天四倍！"
    elif streak % 14 == 0: mult = 3.0; reason = "💎 连签14天三倍！"
    elif streak % 7 == 0: mult = 2.0; reason = "🔥 连签7天双倍！"
    
    if random.random() < NORMAL_CRIT_RATE:
        mult = max(mult, NORMAL_CRIT_MULT); reason += " | ✨ 幸运暴击"; is_crit = True

    base_mb = random.randint(BASE_MIN, BASE_MAX)
    final_bytes = int(base_mb * mult * 1024 * 1024)

    user = await DataManager.get_user_by_email(email)
    if not user: return await process_msg.edit_text("❌ 数据异常")

    await DataManager.add_traffic(user['id'], final_bytes, email)
    await redis_client.set(f"v2bot:checkin:{tg_id}:{today}", 1, ex=86400)
    await redis_client.set(f"v2bot:last_date:{tg_id}", today)
    await redis_client.set(f"v2bot:streak:{tg_id}", streak)

    # 刷新并安全获取数据
    user = await DataManager.get_user_by_email(email)
    plan_name = await DataManager.get_plan_name(user.get('plan_id'))
    
    u_traffic = safe_int(user.get('u'))
    d_traffic = safe_int(user.get('d'))
    trans = safe_int(user.get('transfer_enable'))
    used = u_traffic + d_traffic
    
    expire_ts = safe_int(user.get('expired_at'))
    expire = datetime.fromtimestamp(expire_ts).strftime('%Y-%m-%d') if expire_ts > 0 else "无限期"
    
    header = "🎰 <b>欧皇附体！</b>" if is_crit else "🎉 <b>签到成功！</b>"
    
    msg_text = (
        f"{header}\n"
        f"👤 用户：{u.effective_user.first_name}\n"
        f"🔥 连续签到：<b>{streak}</b> 天\n"
        f"💡 {reason}\n\n"
        f"📦 套餐：{plan_name}\n"
        f"⏳ 到期：{expire}\n"
        f"🎁 奖励：x{mult} (<b>{format_bytes(final_bytes)}</b>)\n"
        f"🌊 当前流量：{format_bytes(trans)}\n"
        f"📊 使用：{format_bytes(used)}\n"
        f"{get_progress_bar(used, trans)}"
    )
    await process_msg.edit_text(msg_text, parse_mode=ParseMode.HTML)

# ========== 📡 监听 ==========
async def payment_monitor(bot):
    while True:
        try:
            pending = await redis_client.smembers("v2bot:pending_orders")
            if pending:
                p_list = list(pending)
                def _chk():
                    with DataManager.get_db_conn() as c:
                        with c.cursor() as cur:
                            fmt = ','.join(['%s']*len(p_list))
                            cur.execute(f"SELECT trade_no, total_amount, status FROM {TBL_ORDER} WHERE trade_no IN ({fmt})", tuple(p_list))
                            return cur.fetchall()
                ords = await asyncio.get_event_loop().run_in_executor(executor, _chk)
                for o in ords:
                    if o['status'] == 3:
                        tn = o['trade_no']
                        tg_id = await redis_client.get(f"v2bot:order_owner:{tn}")
                        if tg_id:
                            try: await bot.send_message(tg_id, f"🎉 <b>支付成功！</b>\n单号：<code>{tn}</code>\n金额：{o['total_amount']/100}元", parse_mode=ParseMode.HTML)
                            except: pass
                        await redis_client.srem("v2bot:pending_orders", tn)
                        await redis_client.delete(f"v2bot:order_owner:{tn}")
                    elif o['status'] == 2:
                        await redis_client.srem("v2bot:pending_orders", o['trade_no'])
        except: pass
        await asyncio.sleep(15)

async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("bind", bind))
    app.add_handler(CommandHandler("info", info))
    app.add_handler(CommandHandler("sub", sub))
    app.add_handler(CommandHandler("reset_sub", reset_sub))
    app.add_handler(CommandHandler("shop", shop))
    app.add_handler(CommandHandler("orders", orders))
    app.add_handler(CommandHandler("checkin", checkin))
    app.add_handler(MessageHandler(filters.Regex("^签到$"), checkin))
    app.add_handler(CallbackQueryHandler(btn_handler))
    await app.initialize(); await app.start(); asyncio.create_task(payment_monitor(app.bot)); await app.updater.start_polling()
    while True: await asyncio.sleep(1)

if __name__ == '__main__': asyncio.run(main())
EOF
}

# 3. 创建系统服务
function create_service() {
    cat > /etc/systemd/system/$SERVICE_NAME.service <<EOF
[Unit]
Description=V2Board Telegram Bot
After=network.target mysql.service redis.service

[Service]
Type=simple
User=root
WorkingDirectory=$WORK_DIR
ExecStart=$WORK_DIR/venv/bin/python3 $WORK_DIR/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
}

function check_status() {
    if systemctl is-active --quiet $SERVICE_NAME; then
        echo -e "状态: ${GREEN}运行中${PLAIN}"
    else
        echo -e "状态: ${RED}未运行${PLAIN}"
    fi
}
function install_bot() { install_env; write_bot_code; create_service; systemctl enable $SERVICE_NAME; systemctl restart $SERVICE_NAME; echo -e "${GREEN}✅ 安装完成${PLAIN}"; }
function start_bot() { systemctl start $SERVICE_NAME; echo -e "${GREEN}已启动${PLAIN}"; }
function stop_bot() { systemctl stop $SERVICE_NAME; echo -e "${GREEN}已停止${PLAIN}"; }
function restart_bot() { systemctl restart $SERVICE_NAME; echo -e "${GREEN}已重启${PLAIN}"; }
function view_logs() { journalctl -u $SERVICE_NAME -f; }
function uninstall_bot() { systemctl stop $SERVICE_NAME; systemctl disable $SERVICE_NAME; rm -f /etc/systemd/system/$SERVICE_NAME.service; rm -rf $WORK_DIR; systemctl daemon-reload; echo -e "${GREEN}卸载完成${PLAIN}"; }

clear
echo -e "${GREEN} V2Board Bot (安全重置修复版) ${PLAIN}"; check_status
echo " 1. 安装 (更新)"; echo " 2. 启动"; echo " 3. 停止"; echo " 4. 重启"; echo " 5. 日志"; echo " 6. 卸载"; echo " 0. 退出"
read -p " 请输入: " n
case "$n" in
    1) install_bot ;; 2) start_bot ;; 3) stop_bot ;; 4) restart_bot ;; 5) view_logs ;; 6) uninstall_bot ;; 0) exit 0 ;; *) echo "无效" ;;
esac
