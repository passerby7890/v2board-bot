import asyncio
import base64
import json
import logging
import os
from pathlib import Path
import random
import hmac
import hashlib
import smtplib
import string
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import datetime, timedelta
from email.header import Header
from email.message import EmailMessage
from email.utils import formataddr
from html import escape
import re
from urllib.parse import urlencode, urlparse

import pymysql
import phpserialize
import redis as redis_sync
import redis.asyncio as redis
import requests
from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
_V2BOARD_RUNTIME_CONFIG_CACHE = {"expires_at": 0.0, "data": {}}
_AUTH_HELPER_FALLBACK_WARNED: set[str] = set()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r, fallback to %s", name, raw, default)
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r, fallback to %s", name, raw, default)
        return default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def env_int_list(name: str, default_values):
    raw = os.getenv(name)
    if raw in (None, ""):
        return list(default_values)

    values = []
    for item in str(raw).split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError:
            logger.warning("Invalid integer list item for %s=%r", name, item)

    return values or list(default_values)


BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
V2BOARD_DOMAIN = (os.getenv("V2BOARD_DOMAIN") or "").rstrip("/")
V2BOARD_CONFIG_PATH = (os.getenv("V2BOARD_CONFIG_PATH") or "").strip()
TABLE_PREFIX = os.getenv("DB_TABLE_PREFIX", "v2_")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SITE_NAME = (os.getenv("SITE_NAME") or "Duty123 用户中心").strip()

CHECKIN_MIN = env_int("CHECKIN_MIN", 100)
CHECKIN_MAX = env_int("CHECKIN_MAX", 250)
CRIT_RATE = max(0.0, min(env_float("CRIT_RATE", 0.1), 1.0))
CRIT_MULT = max(1.0, env_float("CRIT_MULT", 1.5))
PAYMENT_POLL_INTERVAL = max(5, env_int("PAYMENT_POLL_INTERVAL", 15))
RETENTION_POLL_INTERVAL = max(60, env_int("RETENTION_POLL_INTERVAL", 300))
BIND_CODE_TTL = max(60, env_int("BIND_CODE_TTL", 300))
BIND_MAX_ATTEMPTS = max(3, env_int("BIND_MAX_ATTEMPTS", 5))
BIND_SEND_COOLDOWN_SECONDS = max(10, env_int("BIND_SEND_COOLDOWN_SECONDS", 60))
INPUT_STATE_TTL = max(120, env_int("INPUT_STATE_TTL", 900))
REGISTER_SEND_COOLDOWN_SECONDS = max(10, env_int("REGISTER_SEND_COOLDOWN_SECONDS", 60))
EXPIRE_REMIND_DAYS = sorted(set(env_int_list("EXPIRE_REMIND_DAYS", [7, 3, 1, 0])), reverse=True)
TRAFFIC_ALERT_THRESHOLDS = sorted(set(env_int_list("TRAFFIC_ALERT_THRESHOLDS", [70, 85, 95])))
UNPAID_RECALL_MINUTES = sorted(set(env_int_list("UNPAID_RECALL_MINUTES", [10, 60, 1440])))
EMAIL_NOTIFY_EXPIRE_DAYS = sorted(set(env_int_list("EMAIL_NOTIFY_EXPIRE_DAYS", [1, 0])), reverse=True)
EMAIL_NOTIFY_TRAFFIC_THRESHOLDS = sorted(set(env_int_list("EMAIL_NOTIFY_TRAFFIC_THRESHOLDS", [95])))
EMAIL_NOTIFY_UNPAID_MINUTES = sorted(set(env_int_list("EMAIL_NOTIFY_UNPAID_MINUTES", [1440])))
QUIET_HOURS_START = env_int("QUIET_HOURS_START", 0)
QUIET_HOURS_END = env_int("QUIET_HOURS_END", 8)
EXPIRE_REMINDER_COOLDOWN_MINUTES = max(0, env_int("EXPIRE_REMINDER_COOLDOWN_MINUTES", 720))
TRAFFIC_ALERT_COOLDOWN_MINUTES = max(0, env_int("TRAFFIC_ALERT_COOLDOWN_MINUTES", 1440))
UNPAID_RECALL_COOLDOWN_MINUTES = max(0, env_int("UNPAID_RECALL_COOLDOWN_MINUTES", 180))
COMMISSION_NOTICE_COOLDOWN_MINUTES = max(0, env_int("COMMISSION_NOTICE_COOLDOWN_MINUTES", 0))
COMMISSION_EMAIL_MIN_AMOUNT = max(0, env_int("COMMISSION_EMAIL_MIN_AMOUNT", 1000))
COMMISSION_EMAIL_MIN_ORDER_AMOUNT = max(0, env_int("COMMISSION_EMAIL_MIN_ORDER_AMOUNT", 5000))
ADMIN_TELEGRAM_IDS = sorted(set(env_int_list("ADMIN_TELEGRAM_IDS", [])))
ADMIN_REPORT_HOUR = max(0, min(env_int("ADMIN_REPORT_HOUR", 23), 23))
ADMIN_REPORT_MINUTE = max(0, min(env_int("ADMIN_REPORT_MINUTE", 59), 59))
ADMIN_WEEKLY_REPORT_WEEKDAY = max(0, min(env_int("ADMIN_WEEKLY_REPORT_WEEKDAY", 0), 6))
AFF_RANK_LIMIT = max(3, env_int("AFF_RANK_LIMIT", 10))
INVITE_CODE_STATS_LIMIT = max(3, env_int("INVITE_CODE_STATS_LIMIT", 10))
TASK_INFO_REWARD_MB = max(0, env_int("TASK_INFO_REWARD_MB", 20))
TASK_SHOP_REWARD_MB = max(0, env_int("TASK_SHOP_REWARD_MB", 20))
TASK_SUB_REWARD_MB = max(0, env_int("TASK_SUB_REWARD_MB", 20))
TASK_ORDERS_REWARD_MB = max(0, env_int("TASK_ORDERS_REWARD_MB", 20))
TASK_INVITE_REWARD_MB = max(0, env_int("TASK_INVITE_REWARD_MB", 20))
TASK_STREAK_7_REWARD_MB = max(0, env_int("TASK_STREAK_7_REWARD_MB", 128))
TASK_STREAK_14_REWARD_MB = max(0, env_int("TASK_STREAK_14_REWARD_MB", 256))
TASK_STREAK_30_REWARD_MB = max(0, env_int("TASK_STREAK_30_REWARD_MB", 512))
CHECKIN_BROADCAST_ENABLED = env_bool("CHECKIN_BROADCAST_ENABLED", True)
CHECKIN_BROADCAST_CHAT_ID = env_int("CHECKIN_BROADCAST_CHAT_ID", 0)
CHECKIN_BROADCAST_PRIVATE_SYNC = env_bool("CHECKIN_BROADCAST_PRIVATE_SYNC", True)
CHECKIN_BROADCAST_GROUP_SYNC = env_bool("CHECKIN_BROADCAST_GROUP_SYNC", True)
GROUP_HOURLY_PUSH_ENABLED = env_bool("GROUP_HOURLY_PUSH_ENABLED", False)
GROUP_HOURLY_PUSH_CHAT_ID = env_int("GROUP_HOURLY_PUSH_CHAT_ID", CHECKIN_BROADCAST_CHAT_ID)
GROUP_HOURLY_PUSH_INTERVAL_MINUTES = max(10, env_int("GROUP_HOURLY_PUSH_INTERVAL_MINUTES", 60))
GROUP_HOURLY_PUSH_ANCHOR_MINUTE = max(0, min(env_int("GROUP_HOURLY_PUSH_ANCHOR_MINUTE", 0), 59))
GROUP_HOURLY_PUSH_TEXT = (os.getenv("GROUP_HOURLY_PUSH_TEXT") or "").strip()
GROUP_HOURLY_PUSH_BUTTON_1_TEXT = (os.getenv("GROUP_HOURLY_PUSH_BUTTON_1_TEXT") or "").strip()
GROUP_HOURLY_PUSH_BUTTON_1_URL = (os.getenv("GROUP_HOURLY_PUSH_BUTTON_1_URL") or "").strip()
GROUP_HOURLY_PUSH_BUTTON_2_TEXT = (os.getenv("GROUP_HOURLY_PUSH_BUTTON_2_TEXT") or "").strip()
GROUP_HOURLY_PUSH_BUTTON_2_URL = (os.getenv("GROUP_HOURLY_PUSH_BUTTON_2_URL") or "").strip()
GROUP_HOURLY_PUSH_BUTTON_3_TEXT = (os.getenv("GROUP_HOURLY_PUSH_BUTTON_3_TEXT") or "").strip()
GROUP_HOURLY_PUSH_BUTTON_3_URL = (os.getenv("GROUP_HOURLY_PUSH_BUTTON_3_URL") or "").strip()

SMTP_HOST = (os.getenv("SMTP_HOST") or "").strip()
SMTP_PORT = env_int("SMTP_PORT", 587)
SMTP_USERNAME = (os.getenv("SMTP_USERNAME") or "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD") or ""
SMTP_FROM = (os.getenv("SMTP_FROM") or SMTP_USERNAME).strip()
SMTP_FROM_NAME = (os.getenv("SMTP_FROM_NAME") or "V2Board Bot").strip()
SMTP_USE_TLS = env_bool("SMTP_USE_TLS", True)
SMTP_USE_SSL = env_bool("SMTP_USE_SSL", False)

if CHECKIN_MIN > CHECKIN_MAX:
    CHECKIN_MIN, CHECKIN_MAX = CHECKIN_MAX, CHECKIN_MIN

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": env_int("DB_PORT", 3306),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

TBL_USER = f"{TABLE_PREFIX}user"
TBL_PLAN = f"{TABLE_PREFIX}plan"
TBL_ORDER = f"{TABLE_PREFIX}order"
TBL_PAYMENT = f"{TABLE_PREFIX}payment"
TBL_SETTING = f"{TABLE_PREFIX}settings"
TBL_MAIL_LOG = f"{TABLE_PREFIX}mail_log"
TBL_TG_BIND = f"{TABLE_PREFIX}tg_bind"
TBL_BOT_NOTICE_LOG = f"{TABLE_PREFIX}bot_notice_log"
TBL_BOT_TASK_LOG = f"{TABLE_PREFIX}bot_task_log"
TBL_COMMISSION_LOG = f"{TABLE_PREFIX}commission_log"
TBL_INVITE_CODE = f"{TABLE_PREFIX}invite_code"

executor = ThreadPoolExecutor(max_workers=10)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

CYCLE_FIELDS = [
    ("month_price", "月付"),
    ("quarter_price", "季付"),
    ("half_year_price", "半年付"),
    ("year_price", "年付"),
    ("two_year_price", "两年付"),
    ("three_year_price", "三年付"),
    ("onetime_price", "一次性"),
]

ORDER_STATUS = {0: "待支付", 1: "处理中", 2: "已取消", 3: "已支付"}

BOT_COMMANDS = [
    BotCommand("start", "显示主菜单"),
    BotCommand("help", "查看帮助"),
    BotCommand("bind", "发送邮箱验证码"),
    BotCommand("register", "注册新账号"),
    BotCommand("verify", "提交绑定验证码"),
    BotCommand("unbind", "解除绑定"),
    BotCommand("info", "查看账号信息"),
    BotCommand("sub", "查看订阅链接"),
    BotCommand("reset_sub", "重置订阅"),
    BotCommand("shop", "购买套餐"),
    BotCommand("orders", "查看订单"),
    BotCommand("checkin", "每日签到"),
    BotCommand("invite", "查看邀请返利"),
    BotCommand("tasks", "查看任务中心"),
    BotCommand("app", "查看 APP 下载"),
]

BIND_GENERIC_REPLY = (
    f"如果该邮箱可用于绑定，验证码将发送到邮箱，请留意查收。"
    f"\n请在 {BIND_CODE_TTL // 60} 分钟内使用 <code>/verify 123456</code> 完成验证。"
)

MENU_CALLBACKS = {
    "menu_home",
    "menu_bind",
    "menu_register",
    "menu_verify",
    "menu_app",
    "menu_info",
    "menu_sub",
    "menu_shop",
    "menu_orders",
    "menu_invite",
    "menu_tasks",
    "menu_checkin",
    "menu_reset_sub",
    "menu_reset_sub_confirm",
    "menu_unbind",
    "menu_unbind_confirm",
    "menu_help",
    "menu_admin_rank",
    "menu_admin_codes",
    "menu_admin_daily",
    "menu_admin_weekly",
}

DAILY_TASKS = {
    "daily_checkin": {"title": "每日签到", "reward_mb": 0},
    "daily_info": {"title": "查看账号信息", "reward_mb": TASK_INFO_REWARD_MB},
    "daily_shop": {"title": "浏览套餐列表", "reward_mb": TASK_SHOP_REWARD_MB},
    "daily_sub": {"title": "查看订阅链接", "reward_mb": TASK_SUB_REWARD_MB},
    "daily_orders": {"title": "查看订单列表", "reward_mb": TASK_ORDERS_REWARD_MB},
    "daily_invite": {"title": "查看邀请中心", "reward_mb": TASK_INVITE_REWARD_MB},
}

STREAK_TASKS = {
    7: {"task_code": "streak_7", "title": "连续签到 7 天奖励", "reward_mb": TASK_STREAK_7_REWARD_MB},
    14: {"task_code": "streak_14", "title": "连续签到 14 天奖励", "reward_mb": TASK_STREAK_14_REWARD_MB},
    30: {"task_code": "streak_30", "title": "连续签到 30 天奖励", "reward_mb": TASK_STREAK_30_REWARD_MB},
}


def validate_settings() -> None:
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not V2BOARD_DOMAIN:
        missing.append("V2BOARD_DOMAIN")
    if not DB_CONFIG.get("user"):
        missing.append("DB_USERNAME")
    if not DB_CONFIG.get("database"):
        missing.append("DB_DATABASE")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def encode_mail_header(value: str) -> str:
    return str(Header(value, "utf-8"))


def encode_from_header(display_name: str, email: str) -> str:
    return formataddr((str(Header(display_name, "utf-8")), email))


def get_payment_webapp_url(pay_url: str, trade_no: str | None = None) -> str:
    base_url = V2BOARD_DOMAIN.rstrip("/") + "/tg-open-link.html"
    query = {"target": pay_url, "source": "payment"}
    if trade_no:
        query["trade_no"] = trade_no
    return f"{base_url}?{urlencode(query)}"


def get_v2board_project_root() -> str | None:
    if not V2BOARD_CONFIG_PATH:
        return None

    path = Path(V2BOARD_CONFIG_PATH)
    if not path.is_absolute():
        return None

    parts = [part.lower() for part in path.parts]
    if len(parts) >= 3 and path.name == "config.php" and path.parent.name == "cache" and path.parent.parent.name == "bootstrap":
        return str(path.parent.parent.parent)
    if len(parts) >= 2 and path.name == "v2board.php" and path.parent.name == "config":
        return str(path.parent.parent)
    return str(path.parent.parent)


def load_v2board_runtime_config_sync(force_refresh: bool = False) -> dict:
    global _V2BOARD_RUNTIME_CONFIG_CACHE

    now = time.time()
    if (
        not force_refresh
        and _V2BOARD_RUNTIME_CONFIG_CACHE["data"]
        and _V2BOARD_RUNTIME_CONFIG_CACHE["expires_at"] > now
    ):
        return dict(_V2BOARD_RUNTIME_CONFIG_CACHE["data"])

    config_path = Path(V2BOARD_CONFIG_PATH) if V2BOARD_CONFIG_PATH else None
    if not config_path or not config_path.is_file():
        return {}

    escaped_config_path = str(config_path).replace("'", "\\'")
    php_script = f"""
$cfg = include '{escaped_config_path}';
echo json_encode([
    'app_key' => $cfg['app']['key'] ?? null,
    'cache_prefix' => $cfg['cache']['prefix'] ?? null,
    'redis_prefix' => $cfg['database']['redis']['options']['prefix'] ?? null,
    'cache_host' => $cfg['database']['redis']['cache']['host'] ?? null,
    'cache_port' => $cfg['database']['redis']['cache']['port'] ?? null,
    'cache_password' => $cfg['database']['redis']['cache']['password'] ?? null,
    'cache_db' => $cfg['database']['redis']['cache']['database'] ?? null,
    'default_host' => $cfg['database']['redis']['default']['host'] ?? null,
    'default_port' => $cfg['database']['redis']['default']['port'] ?? null,
    'default_password' => $cfg['database']['redis']['default']['password'] ?? null
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
"""

    try:
        result = subprocess.run(
            ["php", "-r", php_script],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout.strip())
            _V2BOARD_RUNTIME_CONFIG_CACHE = {
                "expires_at": now + 300,
                "data": data,
            }
            return dict(data)
        logger.warning("load_v2board_runtime_config_sync php helper failed: %s", result.stderr.strip())
    except Exception:
        logger.exception("load_v2board_runtime_config_sync php helper exception")

    return {}


def resolve_redis_socket_path(socket_path: str) -> str | None:
    if not socket_path:
        return None

    direct_path = Path(socket_path)
    if direct_path.exists():
        return str(direct_path)

    socket_suffix = socket_path.lstrip("/")
    proc_root = Path("/proc")
    with suppress(Exception):
        for proc_entry in proc_root.iterdir():
            if not proc_entry.name.isdigit():
                continue
            candidate = proc_entry / "root" / socket_suffix
            with suppress(Exception):
                if candidate.exists():
                    return str(candidate)
    return None


def base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def jwt_encode_hs256(payload: dict, secret: bytes) -> str:
    header = {"typ": "JWT", "alg": "HS256"}
    header_b64 = base64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(secret, signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{base64url_encode(signature)}"


def is_admin_telegram_id(telegram_id: int | None) -> bool:
    if telegram_id is None:
        return False
    return safe_int(telegram_id) in ADMIN_TELEGRAM_IDS


def build_main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🆕 注册账号", callback_data="menu_register"),
            InlineKeyboardButton("📧 绑定账号", callback_data="menu_bind"),
        ],
        [
            InlineKeyboardButton("🔐 输入验证码", callback_data="menu_verify"),
            InlineKeyboardButton("📱 APP 下载", callback_data="menu_app"),
        ],
        [
            InlineKeyboardButton("👤 账户信息", callback_data="menu_info"),
            InlineKeyboardButton("🔗 订阅链接", callback_data="menu_sub"),
        ],
        [
            InlineKeyboardButton("📦 购买套餐", callback_data="menu_shop"),
            InlineKeyboardButton("🧾 我的订单", callback_data="menu_orders"),
        ],
        [
            InlineKeyboardButton("🎯 邀请返利", callback_data="menu_invite"),
            InlineKeyboardButton("🧩 任务中心", callback_data="menu_tasks"),
        ],
        [
            InlineKeyboardButton("🎁 每日签到", callback_data="menu_checkin"),
            InlineKeyboardButton("🔄 重置订阅", callback_data="menu_reset_sub"),
        ],
        [
            InlineKeyboardButton("🪄 解除绑定", callback_data="menu_unbind"),
            InlineKeyboardButton("❓ 使用帮助", callback_data="menu_help"),
        ],
    ]

    if is_admin:
        rows.extend(
            [
                [
                    InlineKeyboardButton("🏆 AFF 排行榜", callback_data="menu_admin_rank"),
                    InlineKeyboardButton("📊 邀请码统计", callback_data="menu_admin_codes"),
                ],
                [
                    InlineKeyboardButton("📈 今日报表", callback_data="menu_admin_daily"),
                    InlineKeyboardButton("📅 每周报表", callback_data="menu_admin_weekly"),
                ],
            ]
        )

    return InlineKeyboardMarkup(rows)


def build_menu_footer(
    extra_rows: list[list[InlineKeyboardButton]] | None = None,
    include_home: bool = True,
) -> InlineKeyboardMarkup:
    rows = list(extra_rows or [])
    if include_home:
        rows.append([InlineKeyboardButton("🏠 返回主菜单", callback_data="menu_home")])
    return InlineKeyboardMarkup(rows)


def render_main_menu_text(is_admin: bool = False, notice_text: str | None = None) -> str:
    lines = [
        "🚀 <b>欢迎使用智能助手</b>",
        "",
        "这里是你的 Telegram 用户中心，常用操作都可以直接点按钮完成。",
        "",
        "📌 功能入口：",
        "• 注册账号与下载 APP",
        "• 绑定账号与邮箱验证",
        "• 查询账户、订阅、订单",
        "• 套餐购买与支付",
        "• 邀请返利与任务中心",
        "• 每日签到奖励",
    ]

    if is_admin:
        lines.extend(
            [
                "",
                "🛠 管理员入口已开启：",
                "• AFF 排行榜",
                "• 邀请码统计",
                "• 日报 / 周报",
            ]
        )

    if notice_text:
        lines.extend(["", notice_text])

    lines.extend(
        [
            "",
            "提示：需要输入内容时，点按钮后按提示发送即可，不必手动记命令。",
        ]
    )

    return "\n".join(lines)


def mail_is_configured() -> bool:
    return bool(SMTP_HOST and SMTP_PORT and SMTP_FROM)


def safe_int(value) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def mask_email(email: str) -> str:
    email = str(email or "").strip()
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 3:
        masked_local = local[0] + "*" * max(1, len(local) - 1)
    else:
        masked_local = local[:3] + "*" * max(3, len(local) - 5) + local[-2:]
    return f"{masked_local}@{domain}"


def format_money(amount) -> str:
    return f"{safe_int(amount) / 100:.2f} 元"


def format_bytes(size) -> str:
    value = float(safe_int(size))
    labels = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while value >= 1024 and index < len(labels) - 1:
        value /= 1024
        index += 1
    return f"{value:.2f}{labels[index]}"


def format_expire(timestamp) -> str:
    value = safe_int(timestamp)
    if value <= 0:
        return "永久"
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d")


def format_created_at(timestamp) -> str:
    value = safe_int(timestamp)
    if value <= 0:
        return "-"
    return datetime.fromtimestamp(value).strftime("%m-%d %H:%M")


def get_progress_bar(used, total, length: int = 10) -> str:
    total_value = safe_int(total)
    used_value = safe_int(used)
    if total_value <= 0:
        return "░" * length + " (0.0%)"
    ratio = min(max(used_value / total_value, 0.0), 1.0)
    filled = min(length, max(0, int(round(length * ratio))))
    return "█" * filled + "░" * (length - filled) + f" ({ratio * 100:.1f}%)"


def get_plan_cycles(plan: dict):
    cycles = []
    for field_name, label in CYCLE_FIELDS:
        amount = safe_int(plan.get(field_name))
        if amount > 0:
            cycles.append((field_name, label, amount))
    return cycles


def build_bind_pending_key(telegram_id: int) -> str:
    return f"v2bot:bind:pending:{telegram_id}"


def build_bind_cooldown_keys(telegram_id: int, email: str):
    return (
        f"v2bot:bind:cooldown:tg:{telegram_id}",
        f"v2bot:bind:cooldown:email:{email}",
    )


def build_register_cooldown_keys(telegram_id: int, email: str):
    return (
        f"v2bot:register:cooldown:tg:{telegram_id}",
        f"v2bot:register:cooldown:email:{email}",
    )


def build_input_state_key(telegram_id: int) -> str:
    return f"v2bot:input_state:{telegram_id}"


def build_checkin_keys(user_id: int, today: str):
    return (
        f"v2bot:checkin:{user_id}:{today}",
        f"v2bot:last_date:{user_id}",
        f"v2bot:streak:{user_id}",
    )


class DataManager:
    @staticmethod
    def get_db_conn():
        return pymysql.connect(**DB_CONFIG)

    @staticmethod
    async def run_db(func):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, func)

    @classmethod
    async def table_exists(cls, table_name: str) -> bool:
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW TABLES LIKE %s", (table_name,))
                    return cur.fetchone() is not None

        try:
            return await cls.run_db(_query)
        except Exception:
            return False

    @classmethod
    async def column_exists(cls, table_name: str, column_name: str) -> bool:
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
                    return cur.fetchone() is not None

        try:
            return await cls.run_db(_query)
        except Exception:
            return False

    @classmethod
    async def count_table_rows(cls, table_name: str) -> int:
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) AS total FROM {table_name}")
                    row = cur.fetchone() or {}
                    return safe_int(row.get("total"))

        return await cls.run_db(_query)

    @classmethod
    async def ensure_schema(cls) -> None:
        def _create():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {TBL_TG_BIND} (
                            id INT NOT NULL AUTO_INCREMENT,
                            user_id INT NOT NULL,
                            telegram_id BIGINT NOT NULL,
                            email VARCHAR(64) NOT NULL,
                            verified_at INT NOT NULL,
                            created_at INT NOT NULL,
                            updated_at INT NOT NULL,
                            PRIMARY KEY (id),
                            UNIQUE KEY uk_user_id (user_id),
                            UNIQUE KEY uk_telegram_id (telegram_id),
                            UNIQUE KEY uk_email (email)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {TBL_BOT_NOTICE_LOG} (
                            id INT NOT NULL AUTO_INCREMENT,
                            user_id INT NOT NULL,
                            telegram_id BIGINT NOT NULL,
                            notice_type VARCHAR(64) NOT NULL,
                            notice_key VARCHAR(128) NOT NULL,
                            payload TEXT,
                            created_at INT NOT NULL,
                            updated_at INT NOT NULL,
                            PRIMARY KEY (id),
                            UNIQUE KEY uk_notice_type_key (notice_type, notice_key),
                            KEY idx_user_notice (user_id, notice_type)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
                        """
                    )
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {TBL_BOT_TASK_LOG} (
                            id INT NOT NULL AUTO_INCREMENT,
                            user_id INT NOT NULL,
                            task_code VARCHAR(64) NOT NULL,
                            task_scope VARCHAR(64) NOT NULL,
                            reward_bytes BIGINT NOT NULL DEFAULT 0,
                            extra_data TEXT,
                            created_at INT NOT NULL,
                            updated_at INT NOT NULL,
                            PRIMARY KEY (id),
                            UNIQUE KEY uk_user_task_scope (user_id, task_code, task_scope),
                            KEY idx_user_scope (user_id, task_scope)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
                        """
                    )
                    conn.commit()

        await cls.run_db(_create)

    @classmethod
    async def get_user_by_email(cls, email: str):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, email, token, transfer_enable, u, d, plan_id, expired_at, banned
                        FROM {TBL_USER}
                        WHERE email = %s
                        LIMIT 1
                        """,
                        (email,),
                    )
                    return cur.fetchone()

        return await cls.run_db(_query)

    @classmethod
    async def get_user_by_id(cls, user_id: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, email, token, transfer_enable, u, d, plan_id, expired_at, banned
                        FROM {TBL_USER}
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (user_id,),
                    )
                    return cur.fetchone()

        return await cls.run_db(_query)

    @classmethod
    async def get_plan_name(cls, plan_id):
        if not plan_id:
            return "未订阅套餐"

        cache_key = f"v2bot:cache:plan_name:{plan_id}"
        cached = await redis_client.get(cache_key)
        if cached:
            return cached

        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT name FROM {TBL_PLAN} WHERE id = %s LIMIT 1", (plan_id,))
                    row = cur.fetchone()
                    return row["name"] if row else "未知套餐"

        name = await cls.run_db(_query)
        if name:
            await redis_client.set(cache_key, name, ex=3600)
        return name

    @classmethod
    async def get_active_plans(cls):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, name, month_price, quarter_price, half_year_price, year_price,
                               two_year_price, three_year_price, onetime_price
                        FROM {TBL_PLAN}
                        WHERE `show` = 1 AND `renew` = 1
                        ORDER BY sort ASC, id ASC
                        """
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_payment_methods(cls):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT id, name, payment FROM {TBL_PAYMENT} WHERE `enable` = 1 ORDER BY id ASC"
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_pending_order(cls, user_id: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT trade_no, total_amount, status, created_at
                        FROM {TBL_ORDER}
                        WHERE user_id = %s AND status = 0
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (user_id,),
                    )
                    return cur.fetchone()

        return await cls.run_db(_query)

    @classmethod
    async def get_order_by_trade_no(cls, user_id: int, trade_no: str):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT trade_no, total_amount, status, created_at
                        FROM {TBL_ORDER}
                        WHERE user_id = %s AND trade_no = %s
                        LIMIT 1
                        """,
                        (user_id, trade_no),
                    )
                    return cur.fetchone()

        return await cls.run_db(_query)

    @classmethod
    async def create_order(cls, user_id: int, plan_id: int, amount: int, cycle: str) -> str:
        trade_no = "".join(random.choices(string.ascii_lowercase + string.digits, k=20))
        now = int(time.time())

        def _insert():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {TBL_ORDER}
                        (user_id, plan_id, type, period, trade_no, total_amount, status, created_at, updated_at)
                        VALUES (%s, %s, 1, %s, %s, %s, 0, %s, %s)
                        """,
                        (user_id, plan_id, cycle, trade_no, amount, now, now),
                    )
                    conn.commit()
            return trade_no

        return await cls.run_db(_insert)

    @classmethod
    async def cancel_order(cls, trade_no: str, user_id: int) -> None:
        def _update():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {TBL_ORDER} SET status = 2 WHERE trade_no = %s AND user_id = %s AND status = 0",
                        (trade_no, user_id),
                    )
                    conn.commit()

        await cls.run_db(_update)

    @classmethod
    async def get_orders(cls, user_id: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT trade_no, total_amount, status, created_at
                        FROM {TBL_ORDER}
                        WHERE user_id = %s
                        ORDER BY created_at DESC
                        LIMIT 10
                        """,
                        (user_id,),
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def add_traffic(cls, user_id: int, flow_bytes: int) -> None:
        def _update():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {TBL_USER} SET transfer_enable = transfer_enable + %s WHERE id = %s",
                        (flow_bytes, user_id),
                    )
                    conn.commit()

        await cls.run_db(_update)

    @classmethod
    async def get_sub_domain(cls) -> str:
        if not await cls.table_exists(TBL_SETTING):
            return V2BOARD_DOMAIN

        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT `value` FROM {TBL_SETTING} WHERE `name` = 'subscribe_url' LIMIT 1"
                    )
                    return cur.fetchone()

        try:
            row = await cls.run_db(_query)
            if row and row.get("value"):
                domains = [item.strip().rstrip("/") for item in row["value"].split(",") if item.strip()]
                if domains:
                    return random.choice(domains)
        except Exception:
            logger.exception("Failed to load subscribe_url from settings, fallback to V2BOARD_DOMAIN")

        return V2BOARD_DOMAIN

    @classmethod
    async def bootstrap_legacy_bindings(cls) -> int:
        if not await cls.column_exists(TBL_USER, "telegram_id"):
            return 0
        if await cls.count_table_rows(TBL_TG_BIND) > 0:
            return 0

        now = int(time.time())

        def _migrate():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, telegram_id, email
                        FROM {TBL_USER}
                        WHERE telegram_id IS NOT NULL AND telegram_id <> 0
                        """
                    )
                    rows = cur.fetchall()
                    imported = 0
                    for row in rows:
                        cur.execute(
                            f"""
                            INSERT IGNORE INTO {TBL_TG_BIND}
                            (user_id, telegram_id, email, verified_at, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                safe_int(row["id"]),
                                safe_int(row["telegram_id"]),
                                normalize_email(row["email"]),
                                now,
                                now,
                                now,
                            ),
                        )
                        if cur.rowcount > 0:
                            imported += 1
                    conn.commit()
                    return imported

        return await cls.run_db(_migrate)

    @staticmethod
    def generate_auth_data(user_id: int):
        global _AUTH_HELPER_FALLBACK_WARNED
        project_root = get_v2board_project_root()
        if project_root:
            php_script = f"""
require '{project_root}/vendor/autoload.php';
$app = require '{project_root}/bootstrap/app.php';
$kernel = $app->make(Illuminate\\Contracts\\Console\\Kernel::class);
$kernel->bootstrap();
$user = App\\Models\\User::find({int(user_id)});
if (!$user) {{
    fwrite(STDERR, 'USER_NOT_FOUND');
    exit(1);
}}
$request = Illuminate\\Http\\Request::create('/', 'POST', []);
$request->server->set('REMOTE_ADDR', '127.0.0.1');
$request->headers->set('User-Agent', 'V2BoardBot/2.0');
$authService = new App\\Services\\AuthService($user);
$data = $authService->generateAuthData($request);
echo $data['auth_data'];
"""
            try:
                result = subprocess.run(
                    ["php", "-r", php_script],
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
                if result.returncode == 0 and result.stdout.strip():
                    return result.stdout.strip()
                warn_key = project_root or "default"
                if warn_key not in _AUTH_HELPER_FALLBACK_WARNED:
                    logger.warning(
                        "generate_auth_data php helper unavailable, fallback mode enabled for %s",
                        project_root,
                    )
                    _AUTH_HELPER_FALLBACK_WARNED.add(warn_key)
                logger.debug("generate_auth_data php helper stderr: %s", result.stderr.strip())
            except Exception:
                logger.exception("generate_auth_data php helper exception for user_id=%s", user_id)

        return DataManager.generate_auth_data_fallback(user_id)

    @staticmethod
    def generate_auth_data_fallback(user_id: int):
        try:
            runtime_config = load_v2board_runtime_config_sync()
            app_key = runtime_config.get("app_key")
            cache_prefix = runtime_config.get("cache_prefix") or "v2board_cache"
            redis_prefix = runtime_config.get("redis_prefix") or "v2board_database_"
            cache_db = safe_int(runtime_config.get("cache_db")) or 1
            cache_host = (
                runtime_config.get("cache_host")
                or runtime_config.get("default_host")
                or "127.0.0.1"
            )
            cache_password = (
                runtime_config.get("cache_password")
                if runtime_config.get("cache_password") is not None
                else runtime_config.get("default_password") or ""
            )
            cache_port = safe_int(runtime_config.get("cache_port")) or safe_int(runtime_config.get("default_port")) or 6379
            if not app_key:
                return None

            session_guid = uuid.uuid4().hex
            auth_data = jwt_encode_hs256(
                {"id": int(user_id), "session": session_guid},
                app_key.encode("utf-8"),
            )
            if str(cache_host).startswith("/"):
                resolved_socket = resolve_redis_socket_path(str(cache_host))
                if resolved_socket:
                    redis_client_sync = redis_sync.Redis(
                        unix_socket_path=resolved_socket,
                        db=cache_db,
                        password=cache_password or None,
                        decode_responses=False,
                    )
                else:
                    redis_url = REDIS_URL.rsplit("/", 1)[0] + f"/{cache_db}" if "/" in REDIS_URL else REDIS_URL
                    redis_client_sync = redis_sync.from_url(
                        redis_url,
                        password=cache_password or None,
                        decode_responses=False,
                    )
            else:
                redis_client_sync = redis_sync.Redis(
                    host=cache_host,
                    port=cache_port,
                    db=cache_db,
                    password=cache_password or None,
                    decode_responses=False,
                )
            session_key = f"{redis_prefix}{cache_prefix}:USER_SESSIONS_{int(user_id)}"

            existing = redis_client_sync.get(session_key)
            sessions = phpserialize.loads(existing, decode_strings=True) if existing else {}
            sessions[session_guid] = {
                "ip": "127.0.0.1",
                "login_at": int(time.time()),
                "ua": "V2BoardBot/2.0",
                "auth_data": auth_data,
            }
            redis_client_sync.set(session_key, phpserialize.dumps(sessions, charset="utf-8"))
            return auth_data
        except Exception:
            logger.exception("generate_auth_data_fallback failed for user_id=%s", user_id)
            return None

    @staticmethod
    def get_quick_login_url(auth_data: str, redirect: str):
        try:
            response = requests.post(
                f"{V2BOARD_DOMAIN}/api/v1/user/getQuickLoginUrl",
                headers=api_headers(),
                json={"auth_data": auth_data, "redirect": redirect},
                timeout=15,
            )
            body = parse_api_response(response)
            if response.status_code >= 400:
                raise RuntimeError(body.get("message") or f"HTTP {response.status_code}")
            return body.get("data")
        except Exception:
            logger.exception("get_quick_login_url failed for redirect=%s", redirect)
            return None

    @staticmethod
    def call_checkout_api(trade_no: str, method_id: int, user_id: int):
        auth_data = DataManager.generate_auth_data(user_id)
        if not auth_data:
            return None

        url = f"{V2BOARD_DOMAIN}/api/v1/user/order/checkout"
        payload = {"trade_no": trade_no, "method": method_id, "auth_data": auth_data}
        try:
            response = requests.post(url, headers=api_headers(), json=payload, timeout=15)
            response.raise_for_status()
            body = response.json()
            data = body.get("data")
            if isinstance(data, str):
                return data
            if isinstance(data, dict):
                for key in ("url", "pay_url", "checkout_url"):
                    if data.get(key):
                        return data[key]
            logger.warning("Unexpected checkout response for %s: %s", trade_no, body)
        except Exception:
            logger.exception("Checkout API failed for %s", trade_no)
        return DataManager.get_quick_login_url(auth_data, f"order/{trade_no}")

    @classmethod
    async def get_order_statuses(cls, trade_nos):
        if not trade_nos:
            return []

        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    placeholders = ",".join(["%s"] * len(trade_nos))
                    cur.execute(
                        f"""
                        SELECT trade_no, total_amount, status
                        FROM {TBL_ORDER}
                        WHERE trade_no IN ({placeholders})
                        """,
                        tuple(trade_nos),
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def reset_security_direct(cls, user_id: int) -> str:
        new_token = "".join(random.choices(string.ascii_lowercase + string.digits, k=32))
        new_uuid = str(uuid.uuid4())
        now = int(time.time())

        def _update():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {TBL_USER} SET token = %s, uuid = %s, updated_at = %s WHERE id = %s",
                        (new_token, new_uuid, now, user_id),
                    )
                    conn.commit()

        await cls.run_db(_update)
        return new_token

    @classmethod
    async def get_binding_by_telegram_id(cls, telegram_id: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, user_id, telegram_id, email, verified_at, created_at, updated_at
                        FROM {TBL_TG_BIND}
                        WHERE telegram_id = %s
                        LIMIT 1
                        """,
                        (telegram_id,),
                    )
                    return cur.fetchone()

        return await cls.run_db(_query)

    @classmethod
    async def get_binding_by_email(cls, email: str):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, user_id, telegram_id, email, verified_at, created_at, updated_at
                        FROM {TBL_TG_BIND}
                        WHERE email = %s
                        LIMIT 1
                        """,
                        (email,),
                    )
                    return cur.fetchone()

        return await cls.run_db(_query)

    @classmethod
    async def bind_telegram(cls, user_id: int, telegram_id: int, email: str):
        now = int(time.time())

        def _bind():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT id, user_id, telegram_id, email FROM {TBL_TG_BIND} WHERE email = %s LIMIT 1",
                        (email,),
                    )
                    email_row = cur.fetchone()
                    if email_row and safe_int(email_row["telegram_id"]) != telegram_id:
                        return {"ok": False, "reason": "email_already_bound"}

                    cur.execute(
                        f"SELECT id, user_id, telegram_id, email FROM {TBL_TG_BIND} WHERE telegram_id = %s LIMIT 1",
                        (telegram_id,),
                    )
                    tg_row = cur.fetchone()
                    if tg_row and safe_int(tg_row["user_id"]) != user_id:
                        cur.execute(f"DELETE FROM {TBL_TG_BIND} WHERE telegram_id = %s", (telegram_id,))

                    cur.execute(f"SELECT id FROM {TBL_TG_BIND} WHERE user_id = %s LIMIT 1", (user_id,))
                    user_row = cur.fetchone()
                    if user_row:
                        cur.execute(
                            f"""
                            UPDATE {TBL_TG_BIND}
                            SET telegram_id = %s, email = %s, verified_at = %s, updated_at = %s
                            WHERE user_id = %s
                            """,
                            (telegram_id, email, now, now, user_id),
                        )
                    else:
                        cur.execute(
                            f"""
                            INSERT INTO {TBL_TG_BIND}
                            (user_id, telegram_id, email, verified_at, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (user_id, telegram_id, email, now, now, now),
                        )
                    conn.commit()
                    return {"ok": True}

        return await cls.run_db(_bind)

    @classmethod
    async def unbind_telegram(cls, telegram_id: int) -> None:
        def _delete():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {TBL_TG_BIND} WHERE telegram_id = %s", (telegram_id,))
                    conn.commit()

        await cls.run_db(_delete)

    @classmethod
    async def log_mail(cls, email: str, subject: str, template_name: str, error: str | None = None) -> None:
        now = int(time.time())

        def _insert():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {TBL_MAIL_LOG}
                        (email, subject, template_name, error, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (email, subject, template_name, error, now, now),
                    )
                    conn.commit()

        try:
            await cls.run_db(_insert)
        except Exception:
            logger.warning("Failed to write mail log for %s", email)

    @classmethod
    async def list_bound_users(cls):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            b.user_id,
                            b.telegram_id,
                            b.email,
                            u.transfer_enable,
                            u.u,
                            u.d,
                            u.expired_at,
                            u.remind_expire,
                            u.remind_traffic,
                            u.commission_balance,
                            u.banned
                        FROM {TBL_TG_BIND} b
                        INNER JOIN {TBL_USER} u ON u.id = b.user_id
                        WHERE u.banned = 0
                        """
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_pending_orders_for_bound_users(cls):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            o.user_id,
                            b.telegram_id,
                            b.email,
                            o.trade_no,
                            o.total_amount,
                            o.created_at,
                            u.token
                        FROM {TBL_ORDER} o
                        INNER JOIN {TBL_TG_BIND} b ON b.user_id = o.user_id
                        INNER JOIN {TBL_USER} u ON u.id = o.user_id
                        WHERE o.status = 0 AND u.banned = 0
                        """
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_recent_commission_events(cls, limit: int = 200):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            c.id,
                            c.invite_user_id AS user_id,
                            c.user_id AS invited_user_id,
                            c.trade_no,
                            c.order_amount,
                            c.get_amount,
                            c.created_at,
                            b.telegram_id,
                            b.email
                        FROM {TBL_COMMISSION_LOG} c
                        INNER JOIN {TBL_TG_BIND} b ON b.user_id = c.invite_user_id
                        ORDER BY c.id DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_invite_summary(cls, user_id: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS invited_users
                        FROM {TBL_USER}
                        WHERE invite_user_id = %s
                        """,
                        (user_id,),
                    )
                    invited_users = cur.fetchone() or {"invited_users": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS total_commission_orders,
                            COALESCE(SUM(order_amount), 0) AS total_order_amount,
                            COALESCE(SUM(get_amount), 0) AS total_commission_amount
                        FROM {TBL_COMMISSION_LOG}
                        WHERE invite_user_id = %s
                        """,
                        (user_id,),
                    )
                    commission = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT commission_balance
                        FROM {TBL_USER}
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (user_id,),
                    )
                    user = cur.fetchone() or {"commission_balance": 0}

                    cur.execute(
                        f"""
                        SELECT trade_no, order_amount, get_amount, created_at
                        FROM {TBL_COMMISSION_LOG}
                        WHERE invite_user_id = %s
                        ORDER BY created_at DESC
                        LIMIT 5
                        """,
                        (user_id,),
                    )
                    recent_logs = cur.fetchall()

                    return {
                        "invited_users": safe_int(invited_users.get("invited_users")),
                        "total_commission_orders": safe_int(commission.get("total_commission_orders")),
                        "total_order_amount": safe_int(commission.get("total_order_amount")),
                        "total_commission_amount": safe_int(commission.get("total_commission_amount")),
                        "commission_balance": safe_int(user.get("commission_balance")),
                        "recent_logs": recent_logs,
                    }

        return await cls.run_db(_query)

    @classmethod
    async def get_invite_codes(cls, user_id: int, limit: int = 5):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT code, status, pv, created_at
                        FROM {TBL_INVITE_CODE}
                        WHERE user_id = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (user_id, limit),
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_aff_leaderboard(cls, limit: int = 10):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            u.id AS user_id,
                            u.email,
                            COALESCE(COUNT(DISTINCT iu.id), 0) AS invited_users,
                            COALESCE(COUNT(DISTINCT cl.id), 0) AS commission_orders,
                            COALESCE(SUM(cl.get_amount), 0) AS commission_amount,
                            COALESCE(SUM(cl.order_amount), 0) AS order_amount,
                            COALESCE(u.commission_balance, 0) AS commission_balance
                        FROM {TBL_USER} u
                        LEFT JOIN {TBL_USER} iu ON iu.invite_user_id = u.id
                        LEFT JOIN {TBL_COMMISSION_LOG} cl ON cl.invite_user_id = u.id
                        WHERE u.banned = 0
                        GROUP BY u.id, u.email, u.commission_balance
                        HAVING invited_users > 0 OR commission_amount > 0 OR commission_balance > 0
                        ORDER BY commission_amount DESC, invited_users DESC, commission_orders DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_invite_code_stats(cls, limit: int = 10):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            ic.user_id,
                            u.email,
                            ic.code,
                            ic.status,
                            ic.pv,
                            ic.created_at
                        FROM {TBL_INVITE_CODE} ic
                        INNER JOIN {TBL_USER} u ON u.id = ic.user_id
                        ORDER BY ic.pv DESC, ic.created_at DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    return cur.fetchall()

        return await cls.run_db(_query)

    @classmethod
    async def get_aff_report_summary(cls, start_ts: int, end_ts: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS commission_records,
                            COALESCE(SUM(order_amount), 0) AS total_order_amount,
                            COALESCE(SUM(get_amount), 0) AS total_commission_amount,
                            COALESCE(COUNT(DISTINCT invite_user_id), 0) AS active_promoters
                        FROM {TBL_COMMISSION_LOG}
                        WHERE created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    summary = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT COALESCE(COUNT(*), 0) AS invite_users
                        FROM {TBL_USER}
                        WHERE invite_user_id IS NOT NULL AND invite_user_id <> 0
                          AND created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    invite_users = cur.fetchone() or {"invite_users": 0}

                    cur.execute(
                        f"""
                        SELECT
                            u.email,
                            COALESCE(SUM(cl.get_amount), 0) AS commission_amount,
                            COALESCE(COUNT(*), 0) AS commission_records
                        FROM {TBL_COMMISSION_LOG} cl
                        INNER JOIN {TBL_USER} u ON u.id = cl.invite_user_id
                        WHERE cl.created_at >= %s AND cl.created_at < %s
                        GROUP BY cl.invite_user_id, u.email
                        ORDER BY commission_amount DESC, commission_records DESC
                        LIMIT 5
                        """,
                        (start_ts, end_ts),
                    )
                    top_promoters = cur.fetchall()

                    return {
                        "commission_records": safe_int(summary.get("commission_records")),
                        "total_order_amount": safe_int(summary.get("total_order_amount")),
                        "total_commission_amount": safe_int(summary.get("total_commission_amount")),
                        "active_promoters": safe_int(summary.get("active_promoters")),
                        "invite_users": safe_int(invite_users.get("invite_users")),
                        "top_promoters": top_promoters,
                    }

        return await cls.run_db(_query)

    @classmethod
    async def get_management_report_summary(cls, start_ts: int, end_ts: int):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS registrations
                        FROM {TBL_USER}
                        WHERE created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    registrations = cur.fetchone() or {"registrations": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS paid_orders,
                            COALESCE(COUNT(DISTINCT user_id), 0) AS paid_users,
                            COALESCE(SUM(total_amount), 0) AS revenue,
                            COALESCE(SUM(CASE WHEN type = 1 THEN 1 ELSE 0 END), 0) AS new_orders,
                            COALESCE(SUM(CASE WHEN type = 2 THEN 1 ELSE 0 END), 0) AS renew_orders,
                            COALESCE(SUM(CASE WHEN type = 3 THEN 1 ELSE 0 END), 0) AS upgrade_orders
                        FROM {TBL_ORDER}
                        WHERE status = 3
                          AND COALESCE(NULLIF(paid_at, 0), created_at) >= %s
                          AND COALESCE(NULLIF(paid_at, 0), created_at) < %s
                        """,
                        (start_ts, end_ts),
                    )
                    revenue = cur.fetchone() or {}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS bind_count
                        FROM {TBL_TG_BIND}
                        WHERE verified_at >= %s AND verified_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    binds = cur.fetchone() or {"bind_count": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS mail_sent
                        FROM {TBL_MAIL_LOG}
                        WHERE template_name = 'telegram_bind_code'
                          AND created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    bind_mails = cur.fetchone() or {"mail_sent": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS checkin_count
                        FROM {TBL_BOT_TASK_LOG}
                        WHERE task_code = 'daily_checkin'
                          AND created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    checkins = cur.fetchone() or {"checkin_count": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS expire_notice_count
                        FROM {TBL_BOT_NOTICE_LOG}
                        WHERE notice_type = 'expire_reminder'
                          AND created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    expire_notices = cur.fetchone() or {"expire_notice_count": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS traffic_notice_count
                        FROM {TBL_BOT_NOTICE_LOG}
                        WHERE notice_type = 'traffic_alert'
                          AND created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    traffic_notices = cur.fetchone() or {"traffic_notice_count": 0}

                    cur.execute(
                        f"""
                        SELECT
                            COALESCE(COUNT(*), 0) AS unpaid_notice_count
                        FROM {TBL_BOT_NOTICE_LOG}
                        WHERE notice_type = 'unpaid_recall'
                          AND created_at >= %s AND created_at < %s
                        """,
                        (start_ts, end_ts),
                    )
                    unpaid_notices = cur.fetchone() or {"unpaid_notice_count": 0}

                    return {
                        "registrations": safe_int(registrations.get("registrations")),
                        "paid_orders": safe_int(revenue.get("paid_orders")),
                        "paid_users": safe_int(revenue.get("paid_users")),
                        "revenue": safe_int(revenue.get("revenue")),
                        "new_orders": safe_int(revenue.get("new_orders")),
                        "renew_orders": safe_int(revenue.get("renew_orders")),
                        "upgrade_orders": safe_int(revenue.get("upgrade_orders")),
                        "bind_count": safe_int(binds.get("bind_count")),
                        "bind_mail_count": safe_int(bind_mails.get("mail_sent")),
                        "checkin_count": safe_int(checkins.get("checkin_count")),
                        "expire_notice_count": safe_int(expire_notices.get("expire_notice_count")),
                        "traffic_notice_count": safe_int(traffic_notices.get("traffic_notice_count")),
                        "unpaid_notice_count": safe_int(unpaid_notices.get("unpaid_notice_count")),
                    }

        return await cls.run_db(_query)

    @classmethod
    async def record_notice_if_new(
        cls,
        user_id: int,
        telegram_id: int,
        notice_type: str,
        notice_key: str,
        payload: dict | None = None,
    ) -> bool:
        now = int(time.time())
        payload_text = json.dumps(payload, ensure_ascii=False) if payload is not None else None

        def _insert():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT IGNORE INTO {TBL_BOT_NOTICE_LOG}
                        (user_id, telegram_id, notice_type, notice_key, payload, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (user_id, telegram_id, notice_type, notice_key, payload_text, now, now),
                    )
                    conn.commit()
                    return cur.rowcount > 0

        return await cls.run_db(_insert)

    @classmethod
    async def get_last_notice_time(cls, user_id: int, notice_type: str, notice_key_prefix: str | None = None):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    if notice_key_prefix:
                        cur.execute(
                            f"""
                            SELECT MAX(created_at) AS last_created_at
                            FROM {TBL_BOT_NOTICE_LOG}
                            WHERE user_id = %s AND notice_type = %s AND notice_key LIKE %s
                            """,
                            (user_id, notice_type, f"{notice_key_prefix}%"),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT MAX(created_at) AS last_created_at
                            FROM {TBL_BOT_NOTICE_LOG}
                            WHERE user_id = %s AND notice_type = %s
                            """,
                            (user_id, notice_type),
                        )
                    row = cur.fetchone() or {}
                    return row.get("last_created_at")

        return await cls.run_db(_query)

    @classmethod
    async def award_task_reward(
        cls,
        user_id: int,
        task_code: str,
        task_scope: str,
        reward_bytes: int = 0,
        extra_data: dict | None = None,
    ) -> bool:
        now = int(time.time())
        extra_text = json.dumps(extra_data, ensure_ascii=False) if extra_data is not None else None

        def _award():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT IGNORE INTO {TBL_BOT_TASK_LOG}
                        (user_id, task_code, task_scope, reward_bytes, extra_data, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (user_id, task_code, task_scope, reward_bytes, extra_text, now, now),
                    )
                    inserted = cur.rowcount > 0
                    if inserted and reward_bytes > 0:
                        cur.execute(
                            f"UPDATE {TBL_USER} SET transfer_enable = transfer_enable + %s WHERE id = %s",
                            (reward_bytes, user_id),
                        )
                    conn.commit()
                    return inserted

        return await cls.run_db(_award)

    @classmethod
    async def get_task_logs(cls, user_id: int, scopes: list[str] | None = None):
        def _query():
            with cls.get_db_conn() as conn:
                with conn.cursor() as cur:
                    if scopes:
                        placeholders = ",".join(["%s"] * len(scopes))
                        cur.execute(
                            f"""
                            SELECT task_code, task_scope, reward_bytes, extra_data, created_at
                            FROM {TBL_BOT_TASK_LOG}
                            WHERE user_id = %s AND task_scope IN ({placeholders})
                            ORDER BY created_at DESC
                            """,
                            tuple([user_id] + scopes),
                        )
                    else:
                        cur.execute(
                            f"""
                            SELECT task_code, task_scope, reward_bytes, extra_data, created_at
                            FROM {TBL_BOT_TASK_LOG}
                            WHERE user_id = %s
                            ORDER BY created_at DESC
                            LIMIT 100
                            """,
                            (user_id,),
                        )
                    return cur.fetchall()

        return await cls.run_db(_query)


async def send_bind_code_email(email: str, code: str) -> None:
    if not mail_is_configured():
        raise RuntimeError("SMTP is not configured")

    site_name = SITE_NAME
    display_from_name = SMTP_FROM_NAME or site_name
    subject = f"【{site_name}】Telegram 账号绑定验证码"
    text_body = (
        f"{site_name}\n\n"
        "您好：\n\n"
        "您正在进行 Telegram 账号绑定操作，请使用以下验证码完成验证。\n\n"
        f"验证码：{code}\n"
        f"有效时间：{BIND_CODE_TTL // 60} 分钟\n\n"
        "安全提示：\n"
        "1. 请勿将验证码透露给任何人。\n"
        "2. 工作人员不会向您索取验证码。\n"
        "3. 如果这不是您的操作，请直接忽略本邮件。\n"
    )
    html_body = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{escape(subject)}</title>
      </head>
      <body style="margin:0;padding:0;background:#eef3fb;font-family:'PingFang SC','Microsoft YaHei',Arial,sans-serif;color:#172033;">
        <div style="max-width:680px;margin:0 auto;padding:32px 18px;">
          <div style="background:#ffffff;border:1px solid #dbe4f0;border-radius:24px;overflow:hidden;box-shadow:0 18px 50px rgba(15,23,42,.10);">
            <div style="padding:34px 36px;background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 55%,#2563eb 100%);color:#ffffff;">
              <div style="font-size:13px;letter-spacing:.12em;opacity:.72;">TELEGRAM 账号绑定验证</div>
              <div style="font-size:30px;font-weight:800;margin-top:14px;line-height:1.25;">账号绑定验证码</div>
              <div style="font-size:16px;opacity:.92;margin-top:10px;line-height:1.8;">
                您正在为 Telegram 机器人绑定账号，请使用下方验证码完成验证。
              </div>
            </div>
            <div style="padding:34px 36px 28px;line-height:1.85;">
              <p style="margin:0 0 14px;font-size:16px;">您好，</p>
              <p style="margin:0;color:#475569;font-size:15px;">
                为了确认本次操作由您本人发起，请在 Telegram 中输入以下 6 位验证码：
              </p>
              <div style="margin:28px 0;padding:24px;border-radius:20px;background:linear-gradient(180deg,#f8fbff 0%,#edf4ff 100%);border:1px solid #bfdbfe;text-align:center;">
                <div style="font-size:12px;color:#2563eb;letter-spacing:.16em;">验证码</div>
                <div style="font-size:40px;font-weight:800;letter-spacing:.34em;color:#0f172a;margin-top:14px;">{escape(code)}</div>
              </div>
              <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;padding:18px 20px;">
                <div style="font-size:14px;color:#334155;margin-bottom:6px;"><strong>验证码有效时间：</strong>{BIND_CODE_TTL // 60} 分钟</div>
                <div style="font-size:14px;color:#334155;"><strong>适用场景：</strong>Telegram 账号绑定验证</div>
              </div>
              <div style="margin-top:22px;font-size:14px;color:#64748b;">
                <div style="font-weight:700;color:#334155;margin-bottom:8px;">安全提醒</div>
                <div>1. 请勿将验证码透露给任何人。</div>
                <div>2. 平台工作人员不会以任何理由向您索取验证码。</div>
                <div>3. 如果这不是您的操作，请直接忽略本邮件。</div>
              </div>
            </div>
            <div style="padding:18px 36px 24px;background:#f8fafc;border-top:1px solid #e5e7eb;color:#64748b;font-size:13px;line-height:1.7;">
              本邮件由系统自动发送，请勿直接回复。<br>
              如需帮助，请通过官方渠道联系平台客服。
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    message = EmailMessage()
    message["Subject"] = encode_mail_header(subject)
    message["From"] = encode_from_header(display_from_name, SMTP_FROM)
    message["To"] = email
    message.set_content(text_body, charset="utf-8")
    message.add_alternative(html_body, subtype="html", charset="utf-8")

    await send_email_message(message, email, subject, "telegram_bind_code")


async def send_email_message(message: EmailMessage, email: str, subject: str, template_name: str) -> None:
    try:
        if SMTP_USE_SSL:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
                if SMTP_USERNAME:
                    smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
                smtp.ehlo()
                if SMTP_USE_TLS:
                    smtp.starttls()
                    smtp.ehlo()
                if SMTP_USERNAME:
                    smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
                smtp.send_message(message)
        await DataManager.log_mail(email, subject, template_name, None)
    except Exception as exc:
        await DataManager.log_mail(email, subject, template_name, str(exc))
        raise


def api_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "V2BoardBot/2.0",
    }


def parse_api_response(response: requests.Response):
    try:
        return response.json()
    except Exception:
        return {"message": response.text}


async def fetch_guest_config():
    cache_key = "v2bot:cache:guest_config"
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            await redis_client.delete(cache_key)

    def _request():
        url = f"{V2BOARD_DOMAIN}/api/v1/guest/comm/config"
        response = requests.get(url, headers=api_headers(), timeout=15)
        response.raise_for_status()
        body = parse_api_response(response)
        return body.get("data") or {}

    config_data = await DataManager.run_db(_request)
    await redis_client.set(cache_key, json.dumps(config_data, ensure_ascii=False), ex=300)
    return config_data


async def fetch_site_download_config():
    cache_key = "v2bot:cache:site_download_config"
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            await redis_client.delete(cache_key)

    def _request():
        config_data = {}
        patterns = {
            "windows_version": r"'windows_version'\s*=>\s*'([^']*)'",
            "windows_download_url": r"'windows_download_url'\s*=>\s*'([^']*)'",
            "macos_version": r"'macos_version'\s*=>\s*'([^']*)'",
            "macos_download_url": r"'macos_download_url'\s*=>\s*'([^']*)'",
            "android_version": r"'android_version'\s*=>\s*'([^']*)'",
            "android_download_url": r"'android_download_url'\s*=>\s*'([^']*)'",
            "telegram_discuss_link": r"'telegram_discuss_link'\s*=>\s*'([^']*)'",
            "app_name": r"'app_name'\s*=>\s*'([^']*)'",
        }

        body = ""
        config_path = Path(V2BOARD_CONFIG_PATH) if V2BOARD_CONFIG_PATH else None
        if config_path and config_path.is_file():
            body = config_path.read_text(encoding="utf-8", errors="ignore")
        else:
            url = f"{V2BOARD_DOMAIN}/api/v1/guest/comm/config"
            response = requests.get(url, headers=api_headers(), timeout=15)
            response.raise_for_status()
            body = response.text

        for key, pattern in patterns.items():
            match = re.search(pattern, body)
            if match:
                config_data[key] = match.group(1).replace("\\/", "/")

        return config_data

    config_data = await DataManager.run_db(_request)
    await redis_client.set(cache_key, json.dumps(config_data, ensure_ascii=False), ex=300)
    return config_data


async def fetch_v2board_runtime_config():
    cache_key = "v2bot:cache:v2board_runtime_config"
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            await redis_client.delete(cache_key)

    config_data = await DataManager.run_db(load_v2board_runtime_config_sync)
    await redis_client.set(cache_key, json.dumps(config_data, ensure_ascii=False), ex=300)
    return config_data


async def send_register_email_verify(email: str):
    def _request():
        url = f"{V2BOARD_DOMAIN}/api/v1/passport/comm/sendEmailVerify"
        response = requests.post(url, headers=api_headers(), json={"email": email}, timeout=15)
        body = parse_api_response(response)
        if response.status_code >= 400:
            raise RuntimeError(body.get("message") or f"HTTP {response.status_code}")
        return body

    return await DataManager.run_db(_request)


async def register_account_via_api(email: str, password: str, email_code: str = "", invite_code: str = ""):
    payload = {
        "email": email,
        "password": password,
        "email_code": email_code,
        "invite_code": invite_code,
    }

    def _request():
        url = f"{V2BOARD_DOMAIN}/api/v1/passport/auth/register"
        response = requests.post(url, headers=api_headers(), json=payload, timeout=20)
        body = parse_api_response(response)
        if response.status_code >= 400:
            raise RuntimeError(body.get("message") or f"HTTP {response.status_code}")
        if body.get("message") and not body.get("data"):
            raise RuntimeError(body.get("message"))
        return body

    return await DataManager.run_db(_request)


async def send_notice_email(
    email: str,
    subject: str,
    headline: str,
    summary: str,
    detail_lines: list[str],
    template_name: str,
) -> None:
    if not mail_is_configured():
        return

    site_name = SITE_NAME
    display_from_name = SMTP_FROM_NAME or site_name
    safe_subject = f"【{site_name}】{subject}"

    text_lines = [site_name, "", headline, "", summary, ""]
    text_lines.extend(detail_lines)
    text_lines.extend(["", "此邮件为系统自动发送，请勿直接回复。"])
    text_body = "\n".join(text_lines)

    details_html = "".join(
        f'<div style="padding:8px 0;border-bottom:1px dashed #dbe4f0;font-size:14px;color:#334155;">{escape(line)}</div>'
        for line in detail_lines
    )
    html_body = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{escape(safe_subject)}</title>
      </head>
      <body style="margin:0;padding:0;background:#eef3fb;font-family:'PingFang SC','Microsoft YaHei',Arial,sans-serif;color:#172033;">
        <div style="max-width:680px;margin:0 auto;padding:32px 18px;">
          <div style="background:#ffffff;border:1px solid #dbe4f0;border-radius:24px;overflow:hidden;box-shadow:0 18px 50px rgba(15,23,42,.10);">
            <div style="padding:34px 36px;background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 55%,#2563eb 100%);color:#ffffff;">
              <div style="font-size:13px;letter-spacing:.12em;opacity:.72;">{escape(site_name)}</div>
              <div style="font-size:30px;font-weight:800;margin-top:14px;line-height:1.25;">{escape(headline)}</div>
              <div style="font-size:16px;opacity:.92;margin-top:10px;line-height:1.8;">{escape(summary)}</div>
            </div>
            <div style="padding:34px 36px 28px;line-height:1.85;">
              <div style="background:#f8fbff;border:1px solid #dbeafe;border-radius:16px;padding:18px 20px;">
                {details_html}
              </div>
              <div style="margin-top:22px;font-size:14px;color:#64748b;">
                请留意 Telegram 私聊通知，以获得更及时的提醒与操作入口。
              </div>
            </div>
            <div style="padding:18px 36px 24px;background:#f8fafc;border-top:1px solid #e5e7eb;color:#64748b;font-size:13px;line-height:1.7;">
              本邮件由系统自动发送，请勿直接回复。<br>
              如需帮助，请通过官方渠道联系平台客服。
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    message = EmailMessage()
    message["Subject"] = encode_mail_header(safe_subject)
    message["From"] = encode_from_header(display_from_name, SMTP_FROM)
    message["To"] = email
    message.set_content(text_body, charset="utf-8")
    message.add_alternative(html_body, subtype="html", charset="utf-8")

    await send_email_message(message, email, safe_subject, template_name)


async def delete_message_later(message, delay_seconds: int) -> None:
    await asyncio.sleep(delay_seconds)
    with suppress(Exception):
        await message.delete()


async def ensure_private_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.effective_chat and update.effective_chat.type == ChatType.PRIVATE:
        return True

    message = update.effective_message
    if not message:
        return False

    bot_user = await context.bot.get_me()
    deep_link = f"https://t.me/{bot_user.username}?start=help" if bot_user.username else None
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("私聊机器人继续", url=deep_link)]]) if deep_link else None

    reply = await message.reply_text(
        "这个功能请在私聊中使用，我已经帮你准备好了入口。",
        reply_markup=keyboard,
    )
    asyncio.create_task(delete_message_later(reply, 10))
    with suppress(Exception):
        await message.delete()
    return False


async def require_bound_user(update: Update):
    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return None, None

    binding = await DataManager.get_binding_by_telegram_id(tg_user.id)
    if not binding:
        await message.reply_text("请先使用 /bind 邮箱 完成邮箱验证绑定。")
        return None, None

    v2_user = await DataManager.get_user_by_id(binding["user_id"])
    if not v2_user:
        await DataManager.unbind_telegram(tg_user.id)
        await message.reply_text("绑定的账号已不存在，旧绑定已清除，请重新绑定。")
        return None, None

    return binding, v2_user


async def set_input_state(telegram_id: int, action: str, extra: dict | None = None) -> None:
    payload = {
        "action": action,
        "extra": extra or {},
        "created_at": int(time.time()),
    }
    await redis_client.set(build_input_state_key(telegram_id), json.dumps(payload), ex=INPUT_STATE_TTL)


async def get_input_state(telegram_id: int):
    cached = await redis_client.get(build_input_state_key(telegram_id))
    if not cached:
        return None
    try:
        return json.loads(cached)
    except json.JSONDecodeError:
        await redis_client.delete(build_input_state_key(telegram_id))
        return None


async def clear_input_state(telegram_id: int) -> None:
    await redis_client.delete(build_input_state_key(telegram_id))


async def show_main_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    edit: bool = False,
    notice_text: str | None = None,
) -> None:
    tg_user = update.effective_user
    is_admin = is_admin_telegram_id(tg_user.id if tg_user else None)
    text = render_main_menu_text(is_admin=is_admin, notice_text=notice_text)
    keyboard = build_main_menu_keyboard(is_admin=is_admin)

    if edit and update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if update.effective_message:
        await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def require_admin_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not await ensure_private_chat(update, context):
        return False

    tg_user = update.effective_user
    message = update.effective_message
    if not tg_user or not message:
        return False

    if not is_admin_telegram_id(tg_user.id):
        await message.reply_text("这个命令仅限管理员使用。")
        return False

    return True


def get_today_scope() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def get_week_scope(now_dt: datetime | None = None) -> str:
    now_dt = now_dt or datetime.now()
    monday = now_dt.date() - timedelta(days=now_dt.weekday())
    return monday.strftime("%Y-%m-%d")


def get_schedule_datetime(target_date) -> datetime:
    base = datetime.combine(target_date, datetime.min.time())
    return base + timedelta(hours=ADMIN_REPORT_HOUR, minutes=ADMIN_REPORT_MINUTE)


def get_schedule_release_datetime(target_date) -> datetime:
    return get_schedule_datetime(target_date) + timedelta(minutes=1)


def get_daily_report_target_date(now_dt: datetime | None = None):
    now_dt = now_dt or datetime.now()
    today = now_dt.date()
    if now_dt >= get_schedule_release_datetime(today):
        return today
    return today - timedelta(days=1)


def get_weekly_report_end_date(now_dt: datetime | None = None):
    now_dt = now_dt or datetime.now()
    today = now_dt.date()
    delta = (today.weekday() - ADMIN_WEEKLY_REPORT_WEEKDAY) % 7
    candidate = today - timedelta(days=delta)
    if now_dt < get_schedule_release_datetime(candidate):
        candidate -= timedelta(days=7)
    return candidate


def get_period_bounds(period: str, completed: bool = False, now_dt: datetime | None = None):
    now_dt = now_dt or datetime.now()

    if period == "weekly":
        if completed:
            end_date = get_weekly_report_end_date(now_dt)
            start_date = end_date - timedelta(days=6)
            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
            return start_dt, end_dt

        start_date = now_dt.date() - timedelta(days=now_dt.weekday())
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = now_dt + timedelta(seconds=1)
        return start_dt, end_dt

    if completed:
        target_date = get_daily_report_target_date(now_dt)
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)
        return start_dt, end_dt

    start_dt = datetime.combine(now_dt.date(), datetime.min.time())
    end_dt = now_dt + timedelta(seconds=1)
    return start_dt, end_dt


def mb_to_bytes(mb: int) -> int:
    return max(0, safe_int(mb)) * 1024 * 1024


def current_scope_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


async def grant_task_reward(
    user_id: int,
    task_code: str,
    task_scope: str,
    reward_mb: int = 0,
    extra_data: dict | None = None,
) -> tuple[bool, int]:
    reward_bytes = mb_to_bytes(reward_mb)
    inserted = await DataManager.award_task_reward(
        user_id=user_id,
        task_code=task_code,
        task_scope=task_scope,
        reward_bytes=reward_bytes,
        extra_data=extra_data,
    )
    return inserted, reward_bytes


async def try_award_daily_task(update: Update, user_id: int, task_code: str) -> None:
    task = DAILY_TASKS.get(task_code)
    if not task or not update.effective_message:
        return

    inserted, reward_bytes = await grant_task_reward(
        user_id=user_id,
        task_code=task_code,
        task_scope=current_scope_date(),
        reward_mb=safe_int(task.get("reward_mb")),
        extra_data={"source": "auto_daily_task"},
    )
    if inserted and reward_bytes > 0:
        await update.effective_message.reply_text(
            f"🎁 已完成今日任务「{task['title']}」，奖励 {format_bytes(reward_bytes)} 已发放。",
            parse_mode=ParseMode.HTML,
        )


async def maybe_broadcast_checkin(
    bot,
    update: Update,
    user_name: str,
    streak: int,
    reward_bytes: int,
    total_reward_bytes: int,
    effect_line_plain: str,
    milestone_messages: list[str],
) -> None:
    if not CHECKIN_BROADCAST_ENABLED or not CHECKIN_BROADCAST_CHAT_ID:
        return

    chat = update.effective_chat
    if not chat:
        return

    is_private = chat.type == ChatType.PRIVATE
    if is_private and not CHECKIN_BROADCAST_PRIVATE_SYNC:
        return
    if not is_private and not CHECKIN_BROADCAST_GROUP_SYNC:
        return
    if not is_private and safe_int(chat.id) == CHECKIN_BROADCAST_CHAT_ID:
        return

    lines = [
        "🎉 <b>签到播报</b>",
        f"👤 用户：{user_name}",
        f"🔥 连续签到：<b>{streak}</b> 天",
        f"✨ 触发效果：{escape(effect_line_plain)}",
        f"🎁 今日获得：<b>{format_bytes(reward_bytes)}</b>",
    ]

    if milestone_messages:
        lines.append(f"🏆 额外里程碑奖励：{'；'.join(milestone_messages)}")

    lines.append(f"🎉 本次总奖励：<b>{format_bytes(total_reward_bytes)}</b>")

    with suppress(Exception):
        await bot.send_message(
            CHECKIN_BROADCAST_CHAT_ID,
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
        )


def get_group_hourly_push_slot(now_dt: datetime | None = None) -> str | None:
    now_dt = now_dt or datetime.now()
    minutes_today = now_dt.hour * 60 + now_dt.minute
    if minutes_today < GROUP_HOURLY_PUSH_ANCHOR_MINUTE:
        return None
    slot_index = (minutes_today - GROUP_HOURLY_PUSH_ANCHOR_MINUTE) // GROUP_HOURLY_PUSH_INTERVAL_MINUTES
    return f"{now_dt.strftime('%Y%m%d')}:{slot_index}"


async def build_group_hourly_push_payload(bot) -> tuple[str, InlineKeyboardMarkup | None]:
    bot_username = getattr(bot, "username", None)
    if not bot_username:
        with suppress(Exception):
            me = await bot.get_me()
            bot_username = me.username

    bot_url = f"https://t.me/{bot_username}?start=menu" if bot_username else ""

    text = GROUP_HOURLY_PUSH_TEXT or (
        f"🌟 <b>{escape(SITE_NAME)} 官方助手</b>\n\n"
        "点击下方按钮即可打开机器人。\n"
        "注册账号、绑定邮箱、购买套餐、查看订阅、APP 下载、每日签到，都可以直接在机器人里完成。"
    )

    custom_buttons = any(
        [
            GROUP_HOURLY_PUSH_BUTTON_1_TEXT,
            GROUP_HOURLY_PUSH_BUTTON_1_URL,
            GROUP_HOURLY_PUSH_BUTTON_2_TEXT,
            GROUP_HOURLY_PUSH_BUTTON_2_URL,
            GROUP_HOURLY_PUSH_BUTTON_3_TEXT,
            GROUP_HOURLY_PUSH_BUTTON_3_URL,
        ]
    )

    if custom_buttons:
        button_specs = [
            (GROUP_HOURLY_PUSH_BUTTON_1_TEXT, GROUP_HOURLY_PUSH_BUTTON_1_URL),
            (GROUP_HOURLY_PUSH_BUTTON_2_TEXT, GROUP_HOURLY_PUSH_BUTTON_2_URL),
            (GROUP_HOURLY_PUSH_BUTTON_3_TEXT, GROUP_HOURLY_PUSH_BUTTON_3_URL),
        ]
    else:
        button_specs = [("🤖 打开机器人", bot_url)]

    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for label, url in button_specs:
        if not label or not url:
            continue
        current_row.append(InlineKeyboardButton(label, url=url))
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)

    return text, InlineKeyboardMarkup(rows) if rows else None


async def render_invite_text(user_id: int) -> str:
    summary = await DataManager.get_invite_summary(user_id)
    invite_codes = await DataManager.get_invite_codes(user_id)

    lines = [
        "🎯 <b>邀请返利中心</b>",
        "",
        f"👥 已邀请用户：{summary['invited_users']}",
        f"💸 累计返利订单：{summary['total_commission_orders']}",
        f"💰 累计返利金额：{format_money(summary['total_commission_amount'])}",
        f"🏦 当前可用返利：{format_money(summary['commission_balance'])}",
        "",
    ]

    if invite_codes:
        lines.append("🔑 最近邀请链接：")
        for code_row in invite_codes:
            code = str(code_row.get("code") or "")
            url = f"{V2BOARD_DOMAIN}/#/register?code={code}"
            lines.append(f"<code>{escape(url)}</code>")
        lines.append("")

    if summary["recent_logs"]:
        lines.append("🧾 最近返利记录：")
        for item in summary["recent_logs"]:
            lines.append(
                f"• {format_created_at(item.get('created_at'))} | 订单 {escape(str(item.get('trade_no') or '-'))} | 返利 {format_money(item.get('get_amount'))}"
            )
    else:
        lines.append("🧾 暂时还没有返利记录。")

    return "\n".join(lines).strip()


async def render_tasks_text(user_id: int) -> str:
    today = current_scope_date()
    logs = await DataManager.get_task_logs(user_id, scopes=[today])
    log_map = {(item["task_code"], item["task_scope"]): item for item in logs}
    today_reward_bytes = sum(safe_int(item.get("reward_bytes")) for item in logs)
    streak_raw = await redis_client.get(f"v2bot:streak:{user_id}")
    streak = safe_int(streak_raw)

    lines = [
        "🧩 <b>任务中心</b>",
        "",
        "📅 今日任务：",
    ]

    for task_code, task in DAILY_TASKS.items():
        done = (task_code, today) in log_map
        status = "已完成" if done else "未完成"
        reward_mb = safe_int(task.get("reward_mb"))
        reward_text = "无额外奖励" if reward_mb <= 0 else f"{reward_mb} MB"
        lines.append(f"• {task['title']}：{status} | 奖励 {reward_text}")

    lines.extend(
        [
            "",
            f"🔥 当前连续签到：{streak} 天",
            "🏁 连签奖励：",
        ]
    )

    for milestone, item in STREAK_TASKS.items():
        achieved = streak >= milestone
        reward_text = "已达成" if achieved else f"还差 {milestone - streak} 天"
        lines.append(f"• {item['title']}：{reward_text} | 奖励 {item['reward_mb']} MB")

    lines.extend(
        [
            "",
            f"🎁 今日任务奖励合计：{format_bytes(today_reward_bytes)}",
            "提示：查看账号、浏览商店和每日签到都会自动记录任务进度。",
        ]
    )

    return "\n".join(lines).strip()


async def render_app_download_text() -> tuple[str, InlineKeyboardMarkup]:
    config_data = await fetch_guest_config()
    download_config = await fetch_site_download_config()
    app_url = str(config_data.get("app_url") or "").strip()
    app_description = str(config_data.get("app_description") or "如需下载客户端，请点击下方按钮。").strip()
    windows_url = str(download_config.get("windows_download_url") or "").strip()
    macos_url = str(download_config.get("macos_download_url") or "").strip()
    android_url = str(download_config.get("android_download_url") or "").strip()
    discuss_url = str(download_config.get("telegram_discuss_link") or "").strip()

    available_platforms = []
    if windows_url:
        available_platforms.append("Windows")
    if macos_url:
        available_platforms.append("macOS")
    if android_url:
        available_platforms.append("Android")
    platform_text = " / ".join(available_platforms) if available_platforms else "请使用官网查看最新客户端"

    text = (
        "📱 <b>APP 下载中心</b>\n\n"
        f"📝 说明：{escape(app_description)}\n"
        f"🌐 官方站点：<code>{escape(app_url or V2BOARD_DOMAIN)}</code>\n"
        f"🧩 可用平台：{escape(platform_text)}"
    )

    extra_rows = []
    if windows_url:
        if macos_url:
            extra_rows.append(
                [
                    InlineKeyboardButton("🪟 Windows 下载", url=windows_url),
                    InlineKeyboardButton("🍎 macOS 下载", url=macos_url),
                ]
            )
            macos_url = ""
        else:
            extra_rows.append([InlineKeyboardButton("🪟 Windows 下载", url=windows_url)])
    if macos_url:
        extra_rows.append([InlineKeyboardButton("🍎 macOS 下载", url=macos_url)])
    if android_url:
        if app_url:
            extra_rows.append(
                [
                    InlineKeyboardButton("🤖 Android 下载", url=android_url),
                    InlineKeyboardButton("🌐 打开官网", url=app_url),
                ]
            )
            app_url = ""
        else:
            extra_rows.append([InlineKeyboardButton("🤖 Android 下载", url=android_url)])
    if app_url:
        extra_rows.append([InlineKeyboardButton("🌐 打开官网", url=app_url)])
    row = []
    if discuss_url:
        row.append(InlineKeyboardButton("💬 Telegram 社群", url=discuss_url))
    row.append(InlineKeyboardButton("🆕 注册账号", callback_data="menu_register"))
    extra_rows.append(row)
    return text, build_menu_footer(extra_rows)


async def render_register_intro() -> tuple[str, InlineKeyboardMarkup]:
    config_data = await fetch_guest_config()
    email_verify = safe_int(config_data.get("is_email_verify")) == 1
    invite_force = safe_int(config_data.get("is_invite_force")) == 1
    suffixes = config_data.get("email_whitelist_suffix") or []
    suffix_text = "、".join(str(item) for item in suffixes[:10]) if suffixes else "不限"

    lines = [
        "🆕 <b>账号注册</b>",
        "",
        "请按步骤完成注册：",
        "1. 输入邮箱",
        "2. 接收并输入邮箱验证码" if email_verify else "2. 设置登录密码",
        "3. 设置登录密码" if email_verify else "",
        "4. 输入邀请码" if invite_force else "4. 如果有邀请码可继续输入，没有可跳过",
        "",
        f"📧 邮箱验证：{'开启' if email_verify else '关闭'}",
        f"🎟 邀请码要求：{'必须填写' if invite_force else '可选'}",
        f"📮 支持邮箱后缀：{escape(suffix_text)}",
        "",
        "点击下方按钮后，直接发送文本输入即可，不需要记命令。",
    ]
    text = "\n".join(line for line in lines if line)
    keyboard = build_menu_footer(
        [[InlineKeyboardButton("🆕 开始注册", callback_data="menu_register_start")]]
    )
    return text, keyboard


async def render_admin_aff_rank_text() -> str:
    rows = await DataManager.get_aff_leaderboard(AFF_RANK_LIMIT)
    lines = [
        "🏆 <b>AFF 排行榜</b>",
        "",
        "╭ 排名依据：累计返利金额",
        "╰ 展示字段：返利 / 邀请 / 订单",
        "",
    ]

    if not rows:
        lines.append("本期暂无可统计的推广数据。")
        return "\n".join(lines)

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for index, row in enumerate(rows, 1):
        title = medals.get(index, f"{index}.")
        email = mask_email(row.get("email"))
        lines.append(
            f"{title} <b>{escape(str(email or '-'))}</b>\n"
            f"   返利：{format_money(row.get('commission_amount'))} ｜ 邀请：{safe_int(row.get('invited_users'))} ｜ 订单：{safe_int(row.get('commission_orders'))}"
        )
        if index != len(rows):
            lines.append("")

    return "\n".join(lines)


async def render_admin_invite_code_stats_text() -> str:
    rows = await DataManager.get_invite_code_stats(INVITE_CODE_STATS_LIMIT)
    lines = ["📊 <b>邀请码使用统计</b>", ""]

    if not rows:
        lines.append("暂时没有邀请码数据。")
        return "\n".join(lines)

    used_count = sum(1 for row in rows if safe_int(row.get("status")) == 1)
    active_count = len(rows) - used_count
    lines.extend(
        [
            f"╭ 已展示：{len(rows)} 条",
            f"├ 可用邀请码：{active_count}",
            f"╰ 已用完邀请码：{used_count}",
            "",
        ]
    )

    for index, row in enumerate(rows, 1):
        status = "已用完" if safe_int(row.get("status")) == 1 else "可用"
        email = mask_email(row.get("email"))
        lines.append(
            f"{index}. <code>{escape(str(row.get('code') or '-'))}</code> ｜ {status} ｜ PV {safe_int(row.get('pv'))}\n"
            f"   账号：{escape(str(email or '-'))} ｜ 创建：{format_created_at(row.get('created_at'))}"
        )
        if index != len(rows):
            lines.append("")

    return "\n".join(lines)


async def render_admin_aff_report_text(period: str, completed: bool = False) -> str:
    start_dt, end_dt = get_period_bounds(period, completed=completed)
    summary = await DataManager.get_aff_report_summary(int(start_dt.timestamp()), int(end_dt.timestamp()))
    management = await DataManager.get_management_report_summary(int(start_dt.timestamp()), int(end_dt.timestamp()))

    if period == "weekly":
        title = f"📈 <b>AFF 周报摘要</b>\n📅 {start_dt.strftime('%Y-%m-%d')} ~ {(end_dt - timedelta(days=1)).strftime('%Y-%m-%d')}"
    else:
        title = f"📈 <b>AFF 日报摘要</b>\n📅 {start_dt.strftime('%Y-%m-%d')}"

    lines = [
        title,
        "",
        "",
        "╭ 核心营运",
        f"├ 注册人数：{management['registrations']}",
        f"├ 付费人数：{management['paid_users']}",
        f"├ 付费订单：{management['paid_orders']}",
        f"├ 总收入：{format_money(management['revenue'])}",
        f"╰ 新购 / 续费 / 升级：{management['new_orders']} / {management['renew_orders']} / {management['upgrade_orders']}",
        "",
        "╭ AFF 表现",
        f"├ 新增被邀请用户：{summary['invite_users']}",
        f"├ 返利记录数：{summary['commission_records']}",
        f"├ 返利总金额：{format_money(summary['total_commission_amount'])}",
        f"├ 关联订单总额：{format_money(summary['total_order_amount'])}",
        f"╰ 活跃推广人数：{summary['active_promoters']}",
        "",
        "╭ 用户行为",
        f"├ 新绑定人数：{management['bind_count']}",
        f"├ 验证码邮件发送数：{management['bind_mail_count']}",
        f"├ 签到人数：{management['checkin_count']}",
        f"╰ 到期 / 流量 / 召回：{management['expire_notice_count']} / {management['traffic_notice_count']} / {management['unpaid_notice_count']}",
        "",
    ]

    if summary["top_promoters"]:
        lines.append("🏅 <b>TOP 推广者</b>")
        for item in summary["top_promoters"]:
            lines.append(
                f"• {escape(mask_email(item.get('email')))} ｜ 返利 {format_money(item.get('commission_amount'))} ｜ 记录 {safe_int(item.get('commission_records'))}"
            )
    else:
        lines.append("🏅 <b>TOP 推广者</b>")
        lines.append("• 本期暂无推广返利数据。")

    return "\n".join(lines)


async def render_shop_menu():
    plans = await DataManager.get_active_plans()
    valid_plans = [plan for plan in plans if get_plan_cycles(plan)]
    if not valid_plans:
        return "当前没有可购买的套餐。", None

    lines = ["🛍 <b>可购买套餐</b>", ""]
    keyboard = []
    for plan in valid_plans:
        raw_name = str(plan.get("name") or "未命名套餐")
        cycles = get_plan_cycles(plan)
        lines.append(f"📦 <b>{escape(raw_name)}</b>")
        lines.append(f"周期: {escape(' / '.join(label for _, label, _ in cycles))}")
        lines.append(f"起价: {format_money(min(amount for _, _, amount in cycles))}")
        lines.append("")
        keyboard.append([InlineKeyboardButton(f"选择 {raw_name[:18]}", callback_data=f"plan:{plan['id']}")])

    return "\n".join(lines).strip(), InlineKeyboardMarkup(keyboard)


async def render_plan_detail(plan_id: int):
    plans = await DataManager.get_active_plans()
    plan = next((item for item in plans if safe_int(item.get("id")) == plan_id), None)
    if not plan:
        return "套餐不存在或已下架。", None

    cycles = get_plan_cycles(plan)
    if not cycles:
        return "这个套餐暂时没有可用价格。", None

    name = str(plan.get("name") or "未命名套餐")
    lines = [f"📦 <b>{escape(name)}</b>", "", "请选择计费周期：", ""]
    keyboard = []
    for cycle_key, label, amount in cycles:
        lines.append(f"• {label}: {format_money(amount)}")
        keyboard.append([InlineKeyboardButton(f"{label} | {format_money(amount)}", callback_data=f"buy:{plan_id}:{cycle_key}")])

    keyboard.append([InlineKeyboardButton("返回套餐列表", callback_data="back_shop")])
    return "\n".join(lines), InlineKeyboardMarkup(keyboard)


async def show_payment_methods(query, trade_no: str, total_amount) -> None:
    methods = await DataManager.get_payment_methods()
    if not methods:
        await query.edit_message_text(
            "当前没有可用的支付方式，请稍后再试。",
            reply_markup=build_menu_footer(),
        )
        return

    keyboard = []
    for method in methods:
        method_name = str(method.get("name") or "支付方式")
        keyboard.append([InlineKeyboardButton(method_name, callback_data=f"pay:{trade_no}:{method['id']}")])
    keyboard.append(
        [
            InlineKeyboardButton("取消订单", callback_data=f"cancel:{trade_no}"),
            InlineKeyboardButton("返回订单", callback_data="menu_orders"),
        ]
    )

    await query.edit_message_text(
        (
            "💳 <b>选择支付方式</b>\n\n"
            f"订单号：<code>{escape(trade_no)}</code>\n"
            f"订单金额：{format_money(total_amount)}\n\n"
            "请选择要使用的支付渠道。\n"
            "点击后会直接跳转到收银台或支付页，无需再次登录网站。\n"
            "支付完成后，机器人会自动通知你。"
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(keyboard),
    )


def get_days_until_expire(expired_at) -> int | None:
    timestamp = safe_int(expired_at)
    if timestamp <= 0:
        return None
    return (datetime.fromtimestamp(timestamp).date() - datetime.now().date()).days


def get_triggered_traffic_threshold(used: int, total: int) -> int | None:
    if total <= 0:
        return None
    usage_percent = used / total * 100
    matched = [threshold for threshold in TRAFFIC_ALERT_THRESHOLDS if usage_percent >= threshold]
    return max(matched) if matched else None


def should_email_expire_notice(days_left: int) -> bool:
    return days_left in EMAIL_NOTIFY_EXPIRE_DAYS


def should_email_traffic_notice(threshold: int) -> bool:
    return threshold in EMAIL_NOTIFY_TRAFFIC_THRESHOLDS


def should_email_unpaid_notice(threshold: int) -> bool:
    return threshold in EMAIL_NOTIFY_UNPAID_MINUTES


def should_email_commission_notice(get_amount: int, order_amount: int) -> bool:
    return safe_int(get_amount) >= COMMISSION_EMAIL_MIN_AMOUNT or safe_int(order_amount) >= COMMISSION_EMAIL_MIN_ORDER_AMOUNT


def in_quiet_hours(now_dt: datetime | None = None) -> bool:
    now_dt = now_dt or datetime.now()
    start = QUIET_HOURS_START % 24
    end = QUIET_HOURS_END % 24
    hour = now_dt.hour

    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def can_bypass_quiet_hours(notice_type: str, payload: dict | None = None) -> bool:
    payload = payload or {}
    if notice_type == "expire_reminder":
        return safe_int(payload.get("days_left")) <= 0
    if notice_type == "traffic_alert":
        return safe_int(payload.get("threshold")) >= 95
    if notice_type == "unpaid_recall":
        return safe_int(payload.get("threshold")) >= 1440
    return notice_type == "commission_notice"


def get_notice_cooldown_minutes(notice_type: str) -> int:
    mapping = {
        "expire_reminder": EXPIRE_REMINDER_COOLDOWN_MINUTES,
        "traffic_alert": TRAFFIC_ALERT_COOLDOWN_MINUTES,
        "unpaid_recall": UNPAID_RECALL_COOLDOWN_MINUTES,
        "commission_notice": COMMISSION_NOTICE_COOLDOWN_MINUTES,
    }
    return max(0, mapping.get(notice_type, 0))


async def process_expire_reminders(bot) -> None:
    users = await DataManager.list_bound_users()
    for item in users:
        if safe_int(item.get("remind_expire")) != 1:
            continue
        days_left = get_days_until_expire(item.get("expired_at"))
        if days_left is None or days_left not in EXPIRE_REMIND_DAYS:
            continue

        notice_key = f"{item['user_id']}:{days_left}:{safe_int(item.get('expired_at'))}"
        payload = {"days_left": days_left, "email": item.get("email")}
        cooldown_minutes = get_notice_cooldown_minutes("expire_reminder")
        last_notice_at = await DataManager.get_last_notice_time(
            safe_int(item["user_id"]),
            "expire_reminder",
            f"{item['user_id']}:{days_left}:",
        )
        if cooldown_minutes > 0 and last_notice_at:
            if int(time.time()) - safe_int(last_notice_at) < cooldown_minutes * 60:
                continue

        if in_quiet_hours() and not can_bypass_quiet_hours("expire_reminder", payload):
            continue

        if await DataManager.get_last_notice_time(
            safe_int(item["user_id"]),
            "expire_reminder",
            notice_key,
        ):
            continue

        if days_left == 0:
            title = "⏰ <b>套餐今天到期</b>"
            desc = "你的服务将在今天到期，建议尽快续费以避免中断。"
        else:
            title = f"⏰ <b>套餐将在 {days_left} 天后到期</b>"
            desc = "建议提前续费，避免服务到期影响使用。"

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("立即续费", callback_data="back_shop")]]
        )
        sent = False
        with suppress(Exception):
            await bot.send_message(
                safe_int(item["telegram_id"]),
                (
                    f"{title}\n"
                    f"📧 账号：{escape(str(item.get('email') or ''))}\n"
                    f"📅 到期日：{format_expire(item.get('expired_at'))}\n"
                    f"{desc}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            sent = True
        if sent:
            await DataManager.record_notice_if_new(
                user_id=safe_int(item["user_id"]),
                telegram_id=safe_int(item["telegram_id"]),
                notice_type="expire_reminder",
                notice_key=notice_key,
                payload=payload,
            )

        if should_email_expire_notice(days_left) and mail_is_configured():
            email_notice_key = f"{notice_key}:email"
            if not await DataManager.get_last_notice_time(
                safe_int(item["user_id"]),
                "expire_reminder_email",
                email_notice_key,
            ):
                with suppress(Exception):
                    await send_notice_email(
                        email=str(item.get("email") or ""),
                        subject="套餐到期提醒",
                        headline="套餐到期提醒",
                        summary=desc,
                        detail_lines=[
                            f"账号：{str(item.get('email') or '')}",
                            f"到期日：{format_expire(item.get('expired_at'))}",
                            f"剩余天数：{days_left}",
                        ],
                        template_name="expire_reminder_email",
                    )
                    await DataManager.record_notice_if_new(
                        user_id=safe_int(item["user_id"]),
                        telegram_id=safe_int(item["telegram_id"]),
                        notice_type="expire_reminder_email",
                        notice_key=email_notice_key,
                        payload=payload,
                    )


async def process_traffic_alerts(bot) -> None:
    users = await DataManager.list_bound_users()
    for item in users:
        if safe_int(item.get("remind_traffic")) != 1:
            continue

        total = safe_int(item.get("transfer_enable"))
        used = safe_int(item.get("u")) + safe_int(item.get("d"))
        threshold = get_triggered_traffic_threshold(used, total)
        if threshold is None:
            continue

        notice_key = f"{item['user_id']}:{threshold}:{total}:{safe_int(item.get('expired_at'))}"
        payload = {"threshold": threshold, "email": item.get("email")}
        cooldown_minutes = get_notice_cooldown_minutes("traffic_alert")
        last_notice_at = await DataManager.get_last_notice_time(
            safe_int(item["user_id"]),
            "traffic_alert",
            f"{item['user_id']}:{threshold}:",
        )
        if cooldown_minutes > 0 and last_notice_at:
            if int(time.time()) - safe_int(last_notice_at) < cooldown_minutes * 60:
                continue

        if in_quiet_hours() and not can_bypass_quiet_hours("traffic_alert", payload):
            continue

        if await DataManager.get_last_notice_time(
            safe_int(item["user_id"]),
            "traffic_alert",
            notice_key,
        ):
            continue

        remain = max(total - used, 0)
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("查看套餐", callback_data="back_shop")],
                [InlineKeyboardButton("查看任务中心", callback_data="show_tasks")],
            ]
        )
        sent = False
        with suppress(Exception):
            await bot.send_message(
                safe_int(item["telegram_id"]),
                (
                    f"📉 <b>流量使用已达到 {threshold}%</b>\n"
                    f"📧 账号：{escape(str(item.get('email') or ''))}\n"
                    f"🌊 已用流量：{format_bytes(used)}\n"
                    f"🧰 总流量：{format_bytes(total)}\n"
                    f"🪫 剩余流量：{format_bytes(remain)}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            sent = True
        if sent:
            await DataManager.record_notice_if_new(
                user_id=safe_int(item["user_id"]),
                telegram_id=safe_int(item["telegram_id"]),
                notice_type="traffic_alert",
                notice_key=notice_key,
                payload=payload,
            )

        if should_email_traffic_notice(threshold) and mail_is_configured():
            email_notice_key = f"{notice_key}:email"
            if not await DataManager.get_last_notice_time(
                safe_int(item["user_id"]),
                "traffic_alert_email",
                email_notice_key,
            ):
                with suppress(Exception):
                    await send_notice_email(
                        email=str(item.get("email") or ""),
                        subject="流量不足提醒",
                        headline=f"流量使用已达到 {threshold}%",
                        summary="你的流量使用已经接近上限，建议尽快续费或升级套餐。",
                        detail_lines=[
                            f"账号：{str(item.get('email') or '')}",
                            f"已用流量：{format_bytes(used)}",
                            f"总流量：{format_bytes(total)}",
                            f"剩余流量：{format_bytes(remain)}",
                        ],
                        template_name="traffic_alert_email",
                    )
                    await DataManager.record_notice_if_new(
                        user_id=safe_int(item["user_id"]),
                        telegram_id=safe_int(item["telegram_id"]),
                        notice_type="traffic_alert_email",
                        notice_key=email_notice_key,
                        payload=payload,
                    )


async def process_unpaid_recalls(bot) -> None:
    now = int(time.time())
    orders = await DataManager.get_pending_orders_for_bound_users()
    for order in orders:
        age_minutes = max(0, (now - safe_int(order.get("created_at"))) // 60)
        for threshold in UNPAID_RECALL_MINUTES:
            if age_minutes < threshold:
                continue

            notice_key = f"{order['trade_no']}:{threshold}"
            payload = {"trade_no": order.get("trade_no"), "threshold": threshold}
            cooldown_minutes = get_notice_cooldown_minutes("unpaid_recall")
            last_notice_at = await DataManager.get_last_notice_time(
                safe_int(order["user_id"]),
                "unpaid_recall",
                f"{order['trade_no']}:{threshold}",
            )
            if cooldown_minutes > 0 and last_notice_at:
                if int(time.time()) - safe_int(last_notice_at) < cooldown_minutes * 60:
                    continue

            if in_quiet_hours() and not can_bypass_quiet_hours("unpaid_recall", payload):
                continue

            if await DataManager.get_last_notice_time(
                safe_int(order["user_id"]),
                "unpaid_recall",
                notice_key,
            ):
                continue

            keyboard = build_menu_footer(
                [
                    [InlineKeyboardButton("继续支付", callback_data=f"repay:{order['trade_no']}")],
                    [InlineKeyboardButton("查看订单", callback_data="menu_orders")],
                ]
            )
            sent = False
            with suppress(Exception):
                await bot.send_message(
                    safe_int(order["telegram_id"]),
                    (
                        "🧾 <b>你有一笔待支付订单</b>\n\n"
                        f"订单号：<code>{escape(str(order.get('trade_no') or ''))}</code>\n"
                        f"订单金额：{format_money(order.get('total_amount'))}\n"
                        f"已等待：{age_minutes} 分钟\n\n"
                        "如果你仍需要这笔订单，可直接继续支付。\n"
                        "支付完成后，机器人会自动通知你。"
                    ),
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
                sent = True
            if sent:
                await DataManager.record_notice_if_new(
                    user_id=safe_int(order["user_id"]),
                    telegram_id=safe_int(order["telegram_id"]),
                    notice_type="unpaid_recall",
                    notice_key=notice_key,
                    payload=payload,
                )

            if should_email_unpaid_notice(threshold) and mail_is_configured():
                email_notice_key = f"{notice_key}:email"
                if not await DataManager.get_last_notice_time(
                    safe_int(order["user_id"]),
                    "unpaid_recall_email",
                    email_notice_key,
                ):
                    with suppress(Exception):
                        await send_notice_email(
                            email=str(order.get("email") or ""),
                            subject="待支付订单提醒",
                            headline="你有一笔待支付订单",
                            summary="如果你仍需要该订单，请尽快完成支付，以免错过当前价格或活动。",
                            detail_lines=[
                                f"账号：{str(order.get('email') or '')}",
                                f"订单号：{str(order.get('trade_no') or '')}",
                                f"订单金额：{format_money(order.get('total_amount'))}",
                                f"等待时长：{age_minutes} 分钟",
                            ],
                            template_name="unpaid_recall_email",
                        )
                        await DataManager.record_notice_if_new(
                            user_id=safe_int(order["user_id"]),
                            telegram_id=safe_int(order["telegram_id"]),
                            notice_type="unpaid_recall_email",
                            notice_key=email_notice_key,
                            payload=payload,
                        )


async def process_commission_notifications(bot) -> None:
    logs = await DataManager.get_recent_commission_events()
    for log_item in reversed(logs):
        payload = {"trade_no": log_item.get("trade_no"), "get_amount": log_item.get("get_amount")}
        cooldown_minutes = get_notice_cooldown_minutes("commission_notice")
        last_notice_at = await DataManager.get_last_notice_time(
            safe_int(log_item["user_id"]),
            "commission_notice",
            str(log_item.get("id")),
        )
        if cooldown_minutes > 0 and last_notice_at:
            if int(time.time()) - safe_int(last_notice_at) < cooldown_minutes * 60:
                continue

        if in_quiet_hours() and not can_bypass_quiet_hours("commission_notice", payload):
            continue

        notice_key = str(log_item.get("id"))
        if await DataManager.get_last_notice_time(
            safe_int(log_item["user_id"]),
            "commission_notice",
            notice_key,
        ):
            continue

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("查看返利中心", callback_data="show_invite")]]
        )
        sent = False
        with suppress(Exception):
            await bot.send_message(
                safe_int(log_item["telegram_id"]),
                (
                    "💸 <b>邀请返利到账通知</b>\n"
                    f"订单号：<code>{escape(str(log_item.get('trade_no') or ''))}</code>\n"
                    f"订单金额：{format_money(log_item.get('order_amount'))}\n"
                    f"返利金额：{format_money(log_item.get('get_amount'))}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            sent = True
        if sent:
            await DataManager.record_notice_if_new(
                user_id=safe_int(log_item["user_id"]),
                telegram_id=safe_int(log_item["telegram_id"]),
                notice_type="commission_notice",
                notice_key=notice_key,
                payload=payload,
            )

        if should_email_commission_notice(log_item.get("get_amount"), log_item.get("order_amount")) and mail_is_configured():
            email_notice_key = f"{notice_key}:email"
            if not await DataManager.get_last_notice_time(
                safe_int(log_item["user_id"]),
                "commission_notice_email",
                email_notice_key,
            ):
                with suppress(Exception):
                    await send_notice_email(
                        email=str(log_item.get("email") or ""),
                        subject="邀请返利到账提醒",
                        headline="你有一笔新的邀请返利到账",
                        summary="本次返利金额达到重要通知门槛，系统已额外通过邮件提醒你查收。",
                        detail_lines=[
                            f"账号：{str(log_item.get('email') or '')}",
                            f"订单号：{str(log_item.get('trade_no') or '')}",
                            f"订单金额：{format_money(log_item.get('order_amount'))}",
                            f"返利金额：{format_money(log_item.get('get_amount'))}",
                        ],
                        template_name="commission_notice_email",
                    )
                    await DataManager.record_notice_if_new(
                        user_id=safe_int(log_item["user_id"]),
                        telegram_id=safe_int(log_item["telegram_id"]),
                        notice_type="commission_notice_email",
                        notice_key=email_notice_key,
                        payload=payload,
                    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return
    await show_main_menu(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start(update, context)


async def app_download(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    text, keyboard = await render_app_download_text()
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    tg_user = update.effective_user
    if not tg_user or not update.effective_message:
        return

    username = f"@{tg_user.username}" if tg_user.username else "未设置"
    await update.effective_message.reply_text(
        (
            "🆔 <b>你的 Telegram 信息</b>\n"
            f"ID：<code>{tg_user.id}</code>\n"
            f"用户名：{escape(username)}"
        ),
        parse_mode=ParseMode.HTML,
    )


async def bind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return

    if not mail_is_configured():
        await message.reply_text("当前机器人还没有配置 SMTP，暂时无法发送邮箱验证码。")
        return

    if not context.args:
        await set_input_state(tg_user.id, "bind_email")
        await message.reply_text(
            "📧 请输入你要绑定的邮箱地址。\n示例：<code>name@example.com</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_menu_footer(),
        )
        return

    email = normalize_email(context.args[0])
    tg_cooldown_key, email_cooldown_key = build_bind_cooldown_keys(tg_user.id, email)
    if await redis_client.get(tg_cooldown_key) or await redis_client.get(email_cooldown_key):
        await message.reply_text(
            "⏳ 验证码刚刚发送过，请稍候再试。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("🔐 输入验证码", callback_data="menu_verify")]]
            ),
        )
        return

    user = await DataManager.get_user_by_email(email)
    current_binding = await DataManager.get_binding_by_telegram_id(tg_user.id)
    email_binding = await DataManager.get_binding_by_email(email)
    await redis_client.set(tg_cooldown_key, 1, ex=BIND_SEND_COOLDOWN_SECONDS)
    await redis_client.set(email_cooldown_key, 1, ex=BIND_SEND_COOLDOWN_SECONDS)

    can_send_code = bool(user) and safe_int(user.get("banned")) != 1
    if email_binding and safe_int(email_binding.get("telegram_id")) not in (0, safe_int(tg_user.id)):
        can_send_code = False

    if can_send_code:
        code = "".join(random.choices(string.digits, k=6))
        payload = {
            "email": email,
            "user_id": safe_int(user["id"]),
            "code": code,
            "attempts": 0,
            "created_at": int(time.time()),
        }
        await redis_client.set(build_bind_pending_key(tg_user.id), json.dumps(payload), ex=BIND_CODE_TTL)

        try:
            await send_bind_code_email(email, code)
        except Exception:
            logger.exception("Failed to send bind code to %s", email)
            await redis_client.delete(build_bind_pending_key(tg_user.id))
            await message.reply_text("验证码发送失败，请稍后再试或检查 SMTP 配置。")
            return
    else:
        await redis_client.delete(build_bind_pending_key(tg_user.id))

    await message.reply_text(
        BIND_GENERIC_REPLY,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(
            [[InlineKeyboardButton("🔐 输入验证码", callback_data="menu_verify")]]
        ),
    )


async def register(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return

    if not context.args:
        text, keyboard = await render_register_intro()
        await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    text, keyboard = await render_register_intro()
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return

    if not context.args:
        await set_input_state(tg_user.id, "verify_code")
        await message.reply_text(
            "🔐 请输入邮箱收到的 6 位验证码。\n示例：<code>123456</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_menu_footer(),
        )
        return

    cached = await redis_client.get(build_bind_pending_key(tg_user.id))
    if not cached:
        await message.reply_text(
            "当前没有待验证的绑定请求，请先绑定邮箱。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("📧 绑定账号", callback_data="menu_bind")]]
            ),
        )
        return

    try:
        payload = json.loads(cached)
    except json.JSONDecodeError:
        await redis_client.delete(build_bind_pending_key(tg_user.id))
        await message.reply_text(
            "绑定请求已失效，请重新发起绑定。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("📧 重新绑定", callback_data="menu_bind")]]
            ),
        )
        return

    code = str(context.args[0]).strip()
    if code != str(payload.get("code", "")).strip():
        payload["attempts"] = safe_int(payload.get("attempts")) + 1
        if payload["attempts"] >= BIND_MAX_ATTEMPTS:
            await redis_client.delete(build_bind_pending_key(tg_user.id))
            await message.reply_text(
                "验证码输入次数过多，请重新获取新的验证码。",
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("📧 重新获取验证码", callback_data="menu_bind")]]
                ),
            )
            return
        ttl = await redis_client.ttl(build_bind_pending_key(tg_user.id))
        await redis_client.set(
            build_bind_pending_key(tg_user.id),
            json.dumps(payload),
            ex=max(1, ttl if ttl > 0 else BIND_CODE_TTL),
        )
        await message.reply_text(
            "验证码错误，请检查后再试。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("🔐 重新输入验证码", callback_data="menu_verify")]]
            ),
        )
        return

    email = normalize_email(payload.get("email"))
    user_id = safe_int(payload.get("user_id"))
    user = await DataManager.get_user_by_id(user_id)
    if not user or normalize_email(user.get("email")) != email:
        await redis_client.delete(build_bind_pending_key(tg_user.id))
        await message.reply_text(
            "账号状态已变化，请重新发起绑定。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("📧 重新绑定", callback_data="menu_bind")]]
            ),
        )
        return

    result = await DataManager.bind_telegram(user_id, tg_user.id, email)
    await redis_client.delete(build_bind_pending_key(tg_user.id))
    if not result.get("ok"):
        if result.get("reason") == "email_already_bound":
            await message.reply_text(
                "这个邮箱暂时无法绑定到当前 Telegram。",
                reply_markup=build_menu_footer(),
            )
        else:
            await message.reply_text(
                "绑定失败，请稍后再试。",
                reply_markup=build_menu_footer(),
            )
        return

    await show_main_menu(
        update,
        context,
        notice_text=f"✅ 绑定成功：<code>{escape(email)}</code>\n你现在可以直接使用下方按钮继续操作。",
    )


async def unbind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    binding, _ = await require_bound_user(update)
    if not binding:
        return

    await DataManager.unbind_telegram(update.effective_user.id)
    await show_main_menu(
        update,
        context,
        notice_text=f"🪄 已解除绑定：<code>{escape(binding['email'])}</code>",
    )


async def info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    binding, user = await require_bound_user(update)
    if not user:
        return

    used_traffic = safe_int(user.get("u")) + safe_int(user.get("d"))
    total_traffic = safe_int(user.get("transfer_enable"))
    remain_traffic = max(total_traffic - used_traffic, 0)
    plan_name = await DataManager.get_plan_name(user.get("plan_id"))

    text = (
        "╭ <b>账户信息</b>\n"
        f"├ 邮箱：<code>{escape(binding['email'])}</code>\n"
        f"├ 套餐：{escape(plan_name)}\n"
        f"├ 到期日：{format_expire(user.get('expired_at'))}\n"
        f"├ 已用流量：{format_bytes(used_traffic)}\n"
        f"├ 总流量：{format_bytes(total_traffic)}\n"
        f"├ 剩余流量：{format_bytes(remain_traffic)}\n"
        "╰ 使用概览：\n"
        f"{get_progress_bar(used_traffic, total_traffic)}"
    )
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(
            [
                [
                    InlineKeyboardButton("🔗 订阅链接", callback_data="menu_sub"),
                    InlineKeyboardButton("🧩 任务中心", callback_data="menu_tasks"),
                ]
            ]
        ),
    )
    await try_award_daily_task(update, user["id"], "daily_info")


async def sub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    _, user = await require_bound_user(update)
    if not user:
        return

    token = str(user.get("token") or "")
    if not token:
        await update.effective_message.reply_text("当前账号没有可用的订阅 token。")
        return

    sub_domain = await DataManager.get_sub_domain()
    url = f"{sub_domain}/api/v1/client/subscribe?token={token}"
    await update.effective_message.reply_text(
        f"╭ <b>订阅链接</b>\n├ 请复制下方链接导入客户端\n╰ <code>{escape(url)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(
            [[InlineKeyboardButton("🔄 重置订阅", callback_data="menu_reset_sub")]]
        ),
    )
    await try_award_daily_task(update, user["id"], "daily_sub")


async def reset_sub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    _, user = await require_bound_user(update)
    if not user:
        return

    progress = await update.effective_message.reply_text("正在重置订阅链接，请稍候...")
    try:
        new_token = await DataManager.reset_security_direct(user["id"])
        sub_domain = await DataManager.get_sub_domain()
        new_url = f"{sub_domain}/api/v1/client/subscribe?token={new_token}"
        await progress.edit_text(
            (
                "✅ <b>订阅已重置</b>\n\n"
                "请使用新的订阅地址更新客户端：\n"
                f"<code>{escape(new_url)}</code>\n\n"
                "旧链接已经失效，请尽快完成替换。"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("🏠 返回主菜单", callback_data="menu_home")]],
                include_home=False,
            ),
        )
    except Exception:
        logger.exception("Failed to reset subscription")
        await progress.edit_text(
            "重置失败，请稍后再试。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("🏠 返回主菜单", callback_data="menu_home")]],
                include_home=False,
            ),
        )


async def shop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    _, user = await require_bound_user(update)
    if not user:
        return

    text, keyboard = await render_shop_menu()
    keyboard_rows = list(keyboard.inline_keyboard if keyboard else [])
    keyboard_rows.append([InlineKeyboardButton("🏠 返回主菜单", callback_data="menu_home")])
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
    )
    await try_award_daily_task(update, user["id"], "daily_shop")


async def orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    _, user = await require_bound_user(update)
    if not user:
        return

    items = await DataManager.get_orders(user["id"])
    if not items:
        await update.effective_message.reply_text("暂时没有订单记录。", reply_markup=build_menu_footer())
        return

    lines = [
        "🧾 <b>最近订单</b>",
        "",
        "这里会显示你最近的订单状态。",
        "如果有待支付订单，可直接继续支付或取消。",
        "",
    ]
    keyboard = []
    pending_order = None

    for order in items:
        trade_no = str(order.get("trade_no") or "")
        status = ORDER_STATUS.get(safe_int(order.get("status")), "未知状态")
        lines.append(f"<code>{escape(trade_no)}</code>")
        lines.append(f"💰 {format_money(order.get('total_amount'))} | {status}")
        lines.append(f"🕒 {format_created_at(order.get('created_at'))}")
        lines.append("")
        if safe_int(order.get("status")) == 0 and pending_order is None:
            pending_order = order

    if pending_order:
        trade_no = str(pending_order.get("trade_no") or "")
        keyboard.append([InlineKeyboardButton("继续支付", callback_data=f"repay:{trade_no}")])
        keyboard.append([InlineKeyboardButton("取消待支付订单", callback_data=f"cancel:{trade_no}")])

    await update.effective_message.reply_text(
        "\n".join(lines).strip(),
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(keyboard),
    )
    await try_award_daily_task(update, user["id"], "daily_orders")


async def invite(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    _, user = await require_bound_user(update)
    if not user:
        return

    text = await render_invite_text(user["id"])
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(
            [[InlineKeyboardButton("📦 去购买套餐", callback_data="menu_shop")]]
        ),
    )
    await try_award_daily_task(update, user["id"], "daily_invite")


async def tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_private_chat(update, context):
        return

    _, user = await require_bound_user(update)
    if not user:
        return

    text = await render_tasks_text(user["id"])
    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_footer(
            [[InlineKeyboardButton("🎁 去签到", callback_data="menu_checkin")]]
        ),
    )


async def admin_aff_rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_user(update, context):
        return

    text = await render_admin_aff_rank_text()
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=build_menu_footer())


async def admin_invite_codes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_user(update, context):
        return

    text = await render_admin_invite_code_stats_text()
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=build_menu_footer())


async def admin_aff_daily(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_user(update, context):
        return

    text = await render_admin_aff_report_text("daily", completed=False)
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=build_menu_footer())


async def admin_aff_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_admin_user(update, context):
        return

    text = await render_admin_aff_report_text("weekly", completed=False)
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=build_menu_footer())


async def checkin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    binding, user = await require_bound_user(update)
    if not user:
        return

    message = update.effective_message
    if not message:
        return

    today = datetime.now().strftime("%Y-%m-%d")
    checkin_key, last_date_key, streak_key = build_checkin_keys(user["id"], today)

    if await redis_client.get(checkin_key):
        reply = await message.reply_text(
            "📅 <b>今天已经签到过了</b>\n明天再来继续领取吧。",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(delete_message_later(reply, 5))
        return

    progress = await message.reply_text("🎲 正在为你抽取今日流量奖励...")
    last_date = await redis_client.get(last_date_key)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    streak = 1
    if last_date == yesterday:
        streak = safe_int(await redis_client.get(streak_key)) + 1

    multiplier = 1.0
    reason = "日常签到"
    critical = False
    if streak % 21 == 0:
        multiplier = 4.0
        reason = "连续签到 21 天四倍奖励"
    elif streak % 14 == 0:
        multiplier = 3.0
        reason = "连续签到 14 天三倍奖励"
    elif streak % 7 == 0:
        multiplier = 2.0
        reason = "连续签到 7 天双倍奖励"

    if random.random() < CRIT_RATE:
        multiplier = max(multiplier, CRIT_MULT)
        reason += " + 幸运暴击"
        critical = True

    base_mb = random.randint(CHECKIN_MIN, CHECKIN_MAX)
    reward_bytes = int(base_mb * multiplier * 1024 * 1024)
    total_reward_bytes = reward_bytes
    milestone_messages = []

    await DataManager.add_traffic(user["id"], reward_bytes)
    await redis_client.set(checkin_key, 1, ex=86400)
    await redis_client.set(last_date_key, today)
    await redis_client.set(streak_key, streak)

    await grant_task_reward(
        user_id=user["id"],
        task_code="daily_checkin",
        task_scope=today,
        reward_mb=0,
        extra_data={"streak": streak},
    )

    for milestone, item in STREAK_TASKS.items():
        if streak >= milestone:
            inserted, bonus_bytes = await grant_task_reward(
                user_id=user["id"],
                task_code=item["task_code"],
                task_scope=f"milestone:{milestone}",
                reward_mb=item["reward_mb"],
                extra_data={"streak": streak, "milestone": milestone},
            )
            if inserted and bonus_bytes > 0:
                total_reward_bytes += bonus_bytes
                milestone_messages.append(f"{item['title']} +{format_bytes(bonus_bytes)}")

    refreshed_user = await DataManager.get_user_by_id(user["id"])
    plan_name = await DataManager.get_plan_name(refreshed_user.get("plan_id"))
    used_traffic = safe_int(refreshed_user.get("u")) + safe_int(refreshed_user.get("d"))
    total_traffic = safe_int(refreshed_user.get("transfer_enable"))
    title = "🎰 <b>欧皇附体！</b>" if critical else "🎉 <b>签到成功！</b>"
    is_private = bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)
    user_name = escape(update.effective_user.first_name or "Telegram 用户")
    effect_line = f"✨ 触发效果：{escape(reason)}"
    milestone_line = f"🏆 额外里程碑奖励：{'；'.join(milestone_messages)}" if milestone_messages else ""

    if is_private:
        text = (
            f"{title}\n"
            f"👤 用户：{user_name}\n"
            f"📧 账号：{escape(binding['email'])}\n"
            f"🔥 连续签到：<b>{streak}</b> 天\n"
            f"{effect_line}\n\n"
            f"📦 当前套餐：{escape(plan_name)}\n"
            f"📅 到期日：{format_expire(refreshed_user.get('expired_at'))}\n"
            f"🎁 本次签到奖励：<b>{format_bytes(reward_bytes)}</b>\n"
            + (f"{milestone_line}\n" if milestone_line else "")
            + f"🎉 本次总奖励：<b>{format_bytes(total_reward_bytes)}</b>\n"
            f"🌊 当前总流量：{format_bytes(total_traffic)}\n"
            f"📈 已用流量：{format_bytes(used_traffic)}\n"
            f"{get_progress_bar(used_traffic, total_traffic)}"
        )
    else:
        text = (
            f"{title}\n"
            f"👤 用户：{user_name}\n"
            f"🔥 连续签到：<b>{streak}</b> 天\n"
            f"{effect_line}\n"
            f"🎁 今日获得：<b>{format_bytes(reward_bytes)}</b>\n"
            + (f"{milestone_line}\n" if milestone_line else "")
            + f"🎉 本次总奖励：<b>{format_bytes(total_reward_bytes)}</b>\n\n"
            "📩 详细账号信息请私聊机器人查看"
        )

    await progress.edit_text(text, parse_mode=ParseMode.HTML)
    await maybe_broadcast_checkin(
        context.bot,
        update,
        user_name=user_name,
        streak=streak,
        reward_bytes=reward_bytes,
        total_reward_bytes=total_reward_bytes,
        effect_line_plain=reason,
        milestone_messages=milestone_messages,
    )


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return

    await query.answer()
    parts = query.data.split(":")
    action = parts[0]

    if action == "menu_home":
        tg_user = update.effective_user
        if tg_user:
            await clear_input_state(tg_user.id)
        await show_main_menu(update, context, edit=True)
        return

    if action == "menu_bind":
        tg_user = update.effective_user
        if tg_user:
            await set_input_state(tg_user.id, "bind_email")
        await query.edit_message_text(
            "📧 请输入你要绑定的邮箱地址。\n示例：<code>name@example.com</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("返回主菜单", callback_data="menu_home")]]),
        )
        return

    if action == "menu_register":
        text, keyboard = await render_register_intro()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if action == "menu_register_start":
        tg_user = update.effective_user
        if tg_user:
            await set_input_state(tg_user.id, "register_email")
        await query.edit_message_text(
            "🆕 请输入你要注册的邮箱地址。\n示例：<code>name@example.com</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_menu_footer(),
        )
        return

    if action == "menu_register_verify":
        tg_user = update.effective_user
        if tg_user:
            await set_input_state(tg_user.id, "register_email_code")
        await query.edit_message_text(
            "🔐 请输入注册时收到的邮箱验证码。\n示例：<code>123456</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=build_menu_footer(),
        )
        return

    if action == "menu_verify":
        tg_user = update.effective_user
        if tg_user:
            await set_input_state(tg_user.id, "verify_code")
        await query.edit_message_text(
            "🔐 请输入邮箱收到的 6 位验证码。\n示例：<code>123456</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("返回主菜单", callback_data="menu_home")]]),
        )
        return

    if action == "menu_app":
        text, keyboard = await render_app_download_text()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if action == "menu_info":
        await info(update, context)
        return

    if action == "menu_sub":
        await sub(update, context)
        return

    if action == "menu_shop":
        text, keyboard = await render_shop_menu()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if action == "menu_orders":
        await orders(update, context)
        return

    if action == "menu_invite":
        await invite(update, context)
        return

    if action == "menu_tasks":
        await tasks(update, context)
        return

    if action == "menu_checkin":
        await checkin(update, context)
        return

    if action == "menu_help":
        await show_main_menu(update, context, edit=True)
        return

    if action == "menu_reset_sub":
        await query.edit_message_text(
            "确认要重置订阅链接吗？重置后旧链接会失效。",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("确认重置", callback_data="menu_reset_sub_confirm")],
                    [InlineKeyboardButton("返回主菜单", callback_data="menu_home")],
                ]
            ),
        )
        return

    if action == "menu_reset_sub_confirm":
        await reset_sub(update, context)
        return

    if action == "menu_unbind":
        await query.edit_message_text(
            "确认要解除当前 Telegram 与账号的绑定吗？",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("确认解除绑定", callback_data="menu_unbind_confirm")],
                    [InlineKeyboardButton("返回主菜单", callback_data="menu_home")],
                ]
            ),
        )
        return

    if action == "menu_unbind_confirm":
        await unbind(update, context)
        return

    if action == "menu_admin_rank":
        await admin_aff_rank(update, context)
        return

    if action == "menu_admin_codes":
        await admin_invite_codes(update, context)
        return

    if action == "menu_admin_daily":
        await admin_aff_daily(update, context)
        return

    if action == "menu_admin_weekly":
        await admin_aff_weekly(update, context)
        return

    if action == "back_shop":
        text, keyboard = await render_shop_menu()
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if action == "show_invite":
        _, user = await require_bound_user(update)
        if not user:
            return
        text = await render_invite_text(user["id"])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        return

    if action == "show_tasks":
        _, user = await require_bound_user(update)
        if not user:
            return
        text = await render_tasks_text(user["id"])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
        return

    if action == "plan" and len(parts) == 2:
        text, keyboard = await render_plan_detail(safe_int(parts[1]))
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return

    if action == "buy" and len(parts) == 3:
        _, user = await require_bound_user(update)
        if not user:
            return

        plan_id = safe_int(parts[1])
        cycle = parts[2]
        pending = await DataManager.get_pending_order(user["id"])
        if pending:
            await show_payment_methods(query, str(pending["trade_no"]), pending["total_amount"])
            await query.answer("你有未支付订单，请先完成或取消它。", show_alert=True)
            return

        plans = await DataManager.get_active_plans()
        plan = next((item for item in plans if safe_int(item.get("id")) == plan_id), None)
        if not plan:
            await query.edit_message_text("套餐不存在或已下架。")
            return

        cycle_map = {field: amount for field, _, amount in get_plan_cycles(plan)}
        amount = cycle_map.get(cycle)
        if not amount:
            await query.edit_message_text("这个计费周期不可用，请重新选择。")
            return

        trade_no = await DataManager.create_order(user["id"], plan_id, amount, cycle)
        await show_payment_methods(query, trade_no, amount)
        return

    if action == "repay" and len(parts) == 2:
        _, user = await require_bound_user(update)
        if not user:
            return

        order = await DataManager.get_order_by_trade_no(user["id"], parts[1])
        if not order or safe_int(order.get("status")) != 0:
            await query.edit_message_text("这个订单当前不需要支付。")
            return

        await show_payment_methods(query, str(order["trade_no"]), order["total_amount"])
        return

    if action == "pay" and len(parts) == 3:
        _, user = await require_bound_user(update)
        if not user:
            return

        trade_no = parts[1]
        method_id = safe_int(parts[2])
        order = await DataManager.get_order_by_trade_no(user["id"], trade_no)
        if not order or safe_int(order.get("status")) != 0:
            await query.edit_message_text("订单状态已变化，请重新查看订单列表。")
            return

        loop = asyncio.get_running_loop()
        pay_url = await loop.run_in_executor(
            executor,
            DataManager.call_checkout_api,
            trade_no,
            method_id,
            safe_int(user.get("id")),
        )
        if not pay_url:
            pay_url = f"{V2BOARD_DOMAIN}/#/order/{trade_no}"

        await redis_client.sadd("v2bot:pending_orders", trade_no)
        await redis_client.set(f"v2bot:order_owner:{trade_no}", update.effective_user.id, ex=7200)

        payment_webapp_url = get_payment_webapp_url(pay_url, trade_no)
        keyboard = build_menu_footer(
            [
                [InlineKeyboardButton("立即支付", web_app=WebAppInfo(url=payment_webapp_url))],
                [InlineKeyboardButton("查看订单", callback_data="menu_orders")],
            ]
        )
        await query.edit_message_text(
            (
                "💰 <b>支付链接已生成</b>\n\n"
                f"订单号：<code>{escape(trade_no)}</code>\n"
                f"订单金额：{format_money(order.get('total_amount'))}\n\n"
                "点击下方按钮后，会先打开支付中转页，再自动跳到系统浏览器中的收银台。\n"
                "这样可以避开 Telegram 内建浏览器对支付宝/微信拉起的兼容问题。\n"
                "整个过程中无需再次登录网站。\n"
                "支付完成后，机器人会自动通知你。"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        return

    if action == "cancel" and len(parts) == 2:
        _, user = await require_bound_user(update)
        if not user:
            return

        trade_no = parts[1]
        await DataManager.cancel_order(trade_no, user["id"])
        await redis_client.srem("v2bot:pending_orders", trade_no)
        await redis_client.delete(f"v2bot:order_owner:{trade_no}")
        await query.edit_message_text("订单已取消。")
        return

    await query.answer("未知操作。", show_alert=True)


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_message:
        await update.effective_message.reply_text("未识别的命令，请使用 /help 查看可用功能。")


async def guided_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or update.effective_chat.type != ChatType.PRIVATE:
        return

    message = update.effective_message
    tg_user = update.effective_user
    if not message or not tg_user:
        return

    state = await get_input_state(tg_user.id)
    if not state:
        await message.reply_text(
            "请点击菜单按钮继续操作，或发送 /start 打开主菜单。",
            reply_markup=build_main_menu_keyboard(is_admin=is_admin_telegram_id(tg_user.id)),
        )
        return

    action = state.get("action")
    text = (message.text or "").strip()

    if action == "bind_email":
        await clear_input_state(tg_user.id)
        context.args = [text]
        await bind(update, context)
        if await redis_client.get(build_bind_pending_key(tg_user.id)):
            await set_input_state(tg_user.id, "verify_code")
        return

    if action == "verify_code":
        context.args = [text]
        await verify(update, context)
        if not await redis_client.get(build_bind_pending_key(tg_user.id)):
            await clear_input_state(tg_user.id)
        return

    if action == "register_email":
        email = normalize_email(text)
        tg_key, email_key = build_register_cooldown_keys(tg_user.id, email)
        if await redis_client.get(tg_key) or await redis_client.get(email_key):
            await message.reply_text(
                "验证码刚刚发送过，请稍候再试。",
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🔐 输入注册验证码", callback_data="menu_register_verify")]]
                ),
            )
            return

        config_data = await fetch_guest_config()
        suffixes = config_data.get("email_whitelist_suffix") or []
        if suffixes and "@" in email:
            suffix = email.split("@", 1)[1].lower()
            if suffix not in [str(item).lower() for item in suffixes]:
                await message.reply_text(
                    "当前邮箱后缀不在允许范围内，请更换邮箱后再试。",
                    reply_markup=build_menu_footer(
                        [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                    ),
                )
                return

        await redis_client.set(tg_key, 1, ex=REGISTER_SEND_COOLDOWN_SECONDS)
        await redis_client.set(email_key, 1, ex=REGISTER_SEND_COOLDOWN_SECONDS)

        if safe_int(config_data.get("is_email_verify")) == 1:
            try:
                await send_register_email_verify(email)
            except Exception as exc:
                logger.exception("Failed to send register email verify to %s", email)
                await message.reply_text(
                    f"验证码发送失败：{escape(str(exc))}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_menu_footer(
                        [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                    ),
                )
                return

            await set_input_state(
                tg_user.id,
                "register_email_code",
                {"email": email, "config": config_data},
            )
            await message.reply_text(
                (
                    f"📮 验证码已发送到 <code>{escape(email)}</code>\n"
                    "请直接发送 6 位邮箱验证码。"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🔐 输入注册验证码", callback_data="menu_register_verify")]]
                ),
            )
        else:
            await set_input_state(
                tg_user.id,
                "register_password",
                {"email": email, "email_code": "", "config": config_data},
            )
            await message.reply_text(
                "🔑 请输入你要设置的登录密码，建议至少 8 位。",
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                ),
            )
        return

    if action == "register_email_code":
        extra = state.get("extra") or {}
        await set_input_state(
            tg_user.id,
            "register_password",
            {"email": extra.get("email"), "email_code": text, "config": extra.get("config", {})},
        )
        await message.reply_text(
            "🔑 请输入你要设置的登录密码，建议至少 8 位。",
            reply_markup=build_menu_footer(
                [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
            ),
        )
        return

    if action == "register_password":
        extra = state.get("extra") or {}
        if len(text) < 8:
            await message.reply_text(
                "密码长度至少需要 8 位，请重新输入。",
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                ),
            )
            return

        config_data = extra.get("config", {})
        if safe_int(config_data.get("is_invite_force")) == 1:
            await set_input_state(
                tg_user.id,
                "register_invite_code",
                {
                    "email": extra.get("email"),
                    "email_code": extra.get("email_code", ""),
                    "password": text,
                    "config": config_data,
                },
            )
            await message.reply_text(
                "🎟 请输入邀请码。",
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                ),
            )
            return

        try:
            await register_account_via_api(
                email=extra.get("email", ""),
                password=text,
                email_code=extra.get("email_code", ""),
                invite_code="",
            )
        except Exception as exc:
            await clear_input_state(tg_user.id)
            await message.reply_text(
                f"注册失败：{escape(str(exc))}",
                parse_mode=ParseMode.HTML,
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                ),
            )
            return

        await clear_input_state(tg_user.id)
        user = await DataManager.get_user_by_email(extra.get("email", ""))
        if user:
            await DataManager.bind_telegram(user["id"], tg_user.id, extra.get("email", ""))

        await show_main_menu(
            update,
            context,
            notice_text=(
                f"✅ 注册成功：<code>{escape(extra.get('email', ''))}</code>\n"
                "账号已自动绑定到当前 Telegram，可直接开始使用。"
            ),
        )
        return

    if action == "register_invite_code":
        extra = state.get("extra") or {}
        invite_code = text.strip()
        try:
            await register_account_via_api(
                email=extra.get("email", ""),
                password=extra.get("password", ""),
                email_code=extra.get("email_code", ""),
                invite_code=invite_code,
            )
        except Exception as exc:
            await clear_input_state(tg_user.id)
            await message.reply_text(
                f"注册失败：{escape(str(exc))}",
                parse_mode=ParseMode.HTML,
                reply_markup=build_menu_footer(
                    [[InlineKeyboardButton("🆕 重新开始注册", callback_data="menu_register_start")]]
                ),
            )
            return

        await clear_input_state(tg_user.id)
        user = await DataManager.get_user_by_email(extra.get("email", ""))
        if user:
            await DataManager.bind_telegram(user["id"], tg_user.id, extra.get("email", ""))

        await show_main_menu(
            update,
            context,
            notice_text=(
                f"✅ 注册成功：<code>{escape(extra.get('email', ''))}</code>\n"
                "账号已自动绑定到当前 Telegram，可直接开始使用。"
            ),
        )
        return


async def payment_monitor(bot) -> None:
    while True:
        try:
            pending = await redis_client.smembers("v2bot:pending_orders")
            if pending:
                orders = await DataManager.get_order_statuses(list(pending))
                for order in orders:
                    trade_no = str(order.get("trade_no") or "")
                    status = safe_int(order.get("status"))
                    if status == 3:
                        tg_id = await redis_client.get(f"v2bot:order_owner:{trade_no}")
                        if tg_id:
                            with suppress(Exception):
                                await bot.send_message(
                                    safe_int(tg_id),
                                    (
                                        "✅ <b>订单已支付</b>\n\n"
                                        f"订单号：<code>{escape(trade_no)}</code>\n"
                                        f"支付金额：{format_money(order.get('total_amount'))}\n\n"
                                        "款项已到账，你现在可以继续使用服务。\n"
                                        "如需查看订阅、订单或返回首页，可直接使用下方入口。"
                                    ),
                                    parse_mode=ParseMode.HTML,
                                    reply_markup=build_menu_footer(
                                        [
                                            [InlineKeyboardButton("订阅链接", callback_data="menu_sub")],
                                            [InlineKeyboardButton("我的订单", callback_data="menu_orders")],
                                        ]
                                    ),
                                )
                        await redis_client.srem("v2bot:pending_orders", trade_no)
                        await redis_client.delete(f"v2bot:order_owner:{trade_no}")
                    elif status == 2:
                        await redis_client.srem("v2bot:pending_orders", trade_no)
                        await redis_client.delete(f"v2bot:order_owner:{trade_no}")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Payment monitor loop failed")

        await asyncio.sleep(PAYMENT_POLL_INTERVAL)


async def retention_monitor(bot) -> None:
    while True:
        try:
            await process_expire_reminders(bot)
            await process_traffic_alerts(bot)
            await process_unpaid_recalls(bot)
            await process_commission_notifications(bot)
            await process_group_hourly_push(bot)
            await process_admin_aff_reports(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Retention monitor loop failed")

        await asyncio.sleep(RETENTION_POLL_INTERVAL)


async def process_admin_aff_reports(bot) -> None:
    if not ADMIN_TELEGRAM_IDS:
        return

    now_dt = datetime.now()
    daily_target_date = get_daily_report_target_date(now_dt)
    weekly_target_end_date = get_weekly_report_end_date(now_dt)
    daily_key = daily_target_date.strftime("%Y-%m-%d")
    weekly_key = weekly_target_end_date.strftime("%Y-%m-%d")
    daily_text = None
    weekly_text = None

    for admin_id in ADMIN_TELEGRAM_IDS:
        daily_notice_key = f"{admin_id}:{daily_key}"
        if not await DataManager.get_last_notice_time(0, "admin_aff_daily_report", daily_notice_key):
            if daily_text is None:
                daily_text = await render_admin_aff_report_text("daily", completed=True)
            sent = False
            with suppress(Exception):
                await bot.send_message(admin_id, daily_text, parse_mode=ParseMode.HTML)
                sent = True
            if sent:
                await DataManager.record_notice_if_new(
                    user_id=0,
                    telegram_id=admin_id,
                    notice_type="admin_aff_daily_report",
                    notice_key=daily_notice_key,
                    payload={"date": daily_key},
                )

        weekly_notice_key = f"{admin_id}:{weekly_key}"
        if not await DataManager.get_last_notice_time(0, "admin_aff_weekly_report", weekly_notice_key):
            if weekly_text is None:
                weekly_text = await render_admin_aff_report_text("weekly", completed=True)
            sent = False
            with suppress(Exception):
                await bot.send_message(admin_id, weekly_text, parse_mode=ParseMode.HTML)
                sent = True
            if sent:
                await DataManager.record_notice_if_new(
                    user_id=0,
                    telegram_id=admin_id,
                    notice_type="admin_aff_weekly_report",
                    notice_key=weekly_notice_key,
                    payload={"week_scope": weekly_key},
                )


async def process_group_hourly_push(bot) -> None:
    if not GROUP_HOURLY_PUSH_ENABLED or not GROUP_HOURLY_PUSH_CHAT_ID:
        return

    slot_key = get_group_hourly_push_slot()
    if not slot_key:
        return

    notice_key = f"{GROUP_HOURLY_PUSH_CHAT_ID}:{slot_key}"
    if await DataManager.get_last_notice_time(0, "group_hourly_push", notice_key):
        return

    text, keyboard = await build_group_hourly_push_payload(bot)
    last_message_key = f"v2bot:group_hourly_push:last_message:{GROUP_HOURLY_PUSH_CHAT_ID}"
    previous_message_id = safe_int(await redis_client.get(last_message_key))
    sent = False
    sent_message = None
    with suppress(Exception):
        sent_message = await bot.send_message(
            GROUP_HOURLY_PUSH_CHAT_ID,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        sent = True

    if sent:
        if previous_message_id > 0 and sent_message and previous_message_id != safe_int(sent_message.message_id):
            with suppress(Exception):
                await bot.delete_message(GROUP_HOURLY_PUSH_CHAT_ID, previous_message_id)
        if sent_message:
            await redis_client.set(last_message_key, sent_message.message_id)
        await DataManager.record_notice_if_new(
            user_id=0,
            telegram_id=GROUP_HOURLY_PUSH_CHAT_ID,
            notice_type="group_hourly_push",
            notice_key=notice_key,
            payload={"slot": slot_key},
        )


async def close_redis() -> None:
    close_method = getattr(redis_client, "aclose", None)
    if close_method is not None:
        await close_method()
        return

    legacy_close = getattr(redis_client, "close", None)
    if legacy_close is not None:
        result = legacy_close()
        if asyncio.iscoroutine(result):
            await result


async def main() -> None:
    validate_settings()
    await DataManager.ensure_schema()
    imported = await DataManager.bootstrap_legacy_bindings()
    if imported:
        logger.info("Imported %s legacy telegram bindings from v2_user.telegram_id", imported)

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("app", app_download))
    app.add_handler(CommandHandler("bind", bind))
    app.add_handler(CommandHandler("verify", verify))
    app.add_handler(CommandHandler("unbind", unbind))
    app.add_handler(CommandHandler("info", info))
    app.add_handler(CommandHandler("sub", sub))
    app.add_handler(CommandHandler("reset_sub", reset_sub))
    app.add_handler(CommandHandler("shop", shop))
    app.add_handler(CommandHandler("orders", orders))
    app.add_handler(CommandHandler("invite", invite))
    app.add_handler(CommandHandler("tasks", tasks))
    app.add_handler(CommandHandler("checkin", checkin))
    app.add_handler(CommandHandler("admin_aff_rank", admin_aff_rank))
    app.add_handler(CommandHandler("admin_invite_codes", admin_invite_codes))
    app.add_handler(CommandHandler("admin_aff_daily", admin_aff_daily))
    app.add_handler(CommandHandler("admin_aff_weekly", admin_aff_weekly))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.Regex(r"^(签到|簽到)$"), checkin))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, guided_input_handler))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    await app.initialize()
    await app.bot.set_my_commands(BOT_COMMANDS)
    await app.start()

    if app.updater is None:
        raise RuntimeError("Telegram updater is unavailable")

    await app.updater.start_polling(drop_pending_updates=True)
    monitor_task = asyncio.create_task(payment_monitor(app.bot))
    retention_task = asyncio.create_task(retention_monitor(app.bot))
    logger.info("Bot started successfully")

    try:
        await asyncio.Event().wait()
    finally:
        monitor_task.cancel()
        retention_task.cancel()
        with suppress(asyncio.CancelledError):
            await monitor_task
        with suppress(asyncio.CancelledError):
            await retention_task

        with suppress(Exception):
            await app.updater.stop()
        with suppress(Exception):
            await app.stop()
        with suppress(Exception):
            await app.shutdown()

        await close_redis()
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    asyncio.run(main())
