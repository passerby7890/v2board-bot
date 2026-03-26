#!/bin/bash

set -euo pipefail

WORK_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-v2bot}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
PLAIN='\033[0m'

print_info() {
    echo -e "${YELLOW}$1${PLAIN}"
}

print_ok() {
    echo -e "${GREEN}$1${PLAIN}"
}

print_error() {
    echo -e "${RED}$1${PLAIN}"
}

ensure_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        print_error "请使用 root 运行此脚本。"
        exit 1
    fi
}

install_system_packages() {
    print_info ">>> 安装系统依赖"

    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -y
        apt-get install -y python3 python3-pip python3-venv redis-server
        systemctl enable --now redis-server
    elif command -v yum >/dev/null 2>&1; then
        yum -y install python3 python3-pip redis
        systemctl enable --now redis
    else
        print_error "不支持的系统，请手动安装 Python 3、pip 和 Redis。"
        exit 1
    fi
}

setup_virtualenv() {
    print_info ">>> 建立虚拟环境并安装依赖"
    cd "${WORK_DIR}"

    if [[ ! -d "venv" ]]; then
        python3 -m venv venv
    fi

    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r requirements.txt
}

ensure_env_file() {
    cd "${WORK_DIR}"

    if [[ -f ".env" ]]; then
        return
    fi

    print_info ">>> 未检测到 .env，准备生成配置文件"

    read -r -p "Bot Token: " input_token
    read -r -p "网站名称 [Duty123 用户中心]: " input_site_name
    read -r -p "V2Board 域名 (示例 https://example.com): " input_domain
    read -r -p "数据库主机 [127.0.0.1]: " input_db_host
    read -r -p "数据库端口 [3306]: " input_db_port
    read -r -p "数据库名称: " input_db_name
    read -r -p "数据库账号: " input_db_user
    read -r -p "数据库密码: " input_db_pass
    read -r -p "Redis 地址 [redis://localhost:6379/0]: " input_redis_url
    read -r -p "SMTP 主机: " input_smtp_host
    read -r -p "SMTP 端口 [465]: " input_smtp_port
    read -r -p "SMTP 用户名: " input_smtp_user
    read -r -p "SMTP 密码: " input_smtp_pass
    read -r -p "发件地址: " input_smtp_from
    read -r -p "发件显示名称 [跟网站名称一致]: " input_smtp_from_name

    input_site_name="${input_site_name:-Duty123 用户中心}"
    input_db_host="${input_db_host:-127.0.0.1}"
    input_db_port="${input_db_port:-3306}"
    input_redis_url="${input_redis_url:-redis://localhost:6379/0}"
    input_smtp_port="${input_smtp_port:-465}"
    input_domain="${input_domain%/}"
    input_smtp_from_name="${input_smtp_from_name:-$input_site_name}"

    cat > .env <<EOF
# --------------------------------------------------
# Telegram Bot 基本设置
# BOT_TOKEN: Telegram BotFather 提供的机器人 Token
# SITE_NAME: 对外显示的网站名称，会用于邮件标题与模板
# V2BOARD_DOMAIN: V2Board 主站网址，不要带最后的 /
# --------------------------------------------------
BOT_TOKEN=${input_token}
SITE_NAME=${input_site_name}
V2BOARD_DOMAIN=${input_domain}

# --------------------------------------------------
# MySQL 数据库设置
# DB_HOST: 数据库主机
# DB_PORT: 数据库端口，MySQL 默认 3306
# DB_DATABASE: V2Board 使用的数据库名称
# DB_USERNAME: 数据库用户名
# DB_PASSWORD: 数据库密码
# DB_TABLE_PREFIX: V2Board 数据表前缀，常见为 v2_
# --------------------------------------------------
DB_HOST=${input_db_host}
DB_PORT=${input_db_port}
DB_DATABASE=${input_db_name}
DB_USERNAME=${input_db_user}
DB_PASSWORD=${input_db_pass}
DB_TABLE_PREFIX=v2_

# --------------------------------------------------
# Redis 设置
# REDIS_URL: Redis 连接字符串，用来存绑定验证码、签到状态、订单轮询数据
# --------------------------------------------------
REDIS_URL=${input_redis_url}

# --------------------------------------------------
# 签到与提醒设置
# CHECKIN_MIN: 每日签到最小奖励，单位 MB
# CHECKIN_MAX: 每日签到最大奖励，单位 MB
# CRIT_RATE: 暴击概率，0.1 代表 10%
# CRIT_MULT: 暴击倍数
# PAYMENT_POLL_INTERVAL: 订单轮询间隔，单位秒
# RETENTION_POLL_INTERVAL: 粘性提醒巡检间隔，单位秒
# EXPIRE_REMIND_DAYS: 到期提醒天数，逗号分隔
# TRAFFIC_ALERT_THRESHOLDS: 流量提醒百分比阈值，逗号分隔
# UNPAID_RECALL_MINUTES: 未付款召回提醒分钟数，逗号分隔
# EMAIL_NOTIFY_EXPIRE_DAYS: 哪些到期提醒要额外发送 Email，逗号分隔
# EMAIL_NOTIFY_TRAFFIC_THRESHOLDS: 哪些流量提醒要额外发送 Email，逗号分隔
# EMAIL_NOTIFY_UNPAID_MINUTES: 哪些未付款召回要额外发送 Email，逗号分隔
# QUIET_HOURS_START: 静默时段开始小时，24 小时制
# QUIET_HOURS_END: 静默时段结束小时，24 小时制
# EXPIRE_REMINDER_COOLDOWN_MINUTES: 到期提醒冷却时间，单位分钟
# TRAFFIC_ALERT_COOLDOWN_MINUTES: 流量提醒冷却时间，单位分钟
# UNPAID_RECALL_COOLDOWN_MINUTES: 未付款召回冷却时间，单位分钟
# COMMISSION_NOTICE_COOLDOWN_MINUTES: 返利通知冷却时间，单位分钟
# COMMISSION_EMAIL_MIN_AMOUNT: 返利达到多少金额时额外发送 Email，单位分
# COMMISSION_EMAIL_MIN_ORDER_AMOUNT: 关联订单达到多少金额时额外发送 Email，单位分
# ADMIN_TELEGRAM_IDS: 管理员 Telegram ID 列表，逗号分隔
# ADMIN_REPORT_HOUR: 管理员日报/周报推送小时
# ADMIN_REPORT_MINUTE: 管理员日报/周报推送分钟
# ADMIN_WEEKLY_REPORT_WEEKDAY: 管理员周报推送星期，0=周一
# AFF_RANK_LIMIT: AFF 排行榜显示数量
# INVITE_CODE_STATS_LIMIT: 邀请码统计显示数量
# --------------------------------------------------
CHECKIN_MIN=100
CHECKIN_MAX=250
CRIT_RATE=0.1
CRIT_MULT=1.5
PAYMENT_POLL_INTERVAL=15
RETENTION_POLL_INTERVAL=300
EXPIRE_REMIND_DAYS=7,3,1,0
TRAFFIC_ALERT_THRESHOLDS=70,85,95
UNPAID_RECALL_MINUTES=10,60,1440
EMAIL_NOTIFY_EXPIRE_DAYS=1,0
EMAIL_NOTIFY_TRAFFIC_THRESHOLDS=95
EMAIL_NOTIFY_UNPAID_MINUTES=1440
QUIET_HOURS_START=0
QUIET_HOURS_END=8
EXPIRE_REMINDER_COOLDOWN_MINUTES=720
TRAFFIC_ALERT_COOLDOWN_MINUTES=1440
UNPAID_RECALL_COOLDOWN_MINUTES=180
COMMISSION_NOTICE_COOLDOWN_MINUTES=0
COMMISSION_EMAIL_MIN_AMOUNT=1000
COMMISSION_EMAIL_MIN_ORDER_AMOUNT=5000
ADMIN_TELEGRAM_IDS=
ADMIN_REPORT_HOUR=23
ADMIN_REPORT_MINUTE=59
ADMIN_WEEKLY_REPORT_WEEKDAY=0
AFF_RANK_LIMIT=10
INVITE_CODE_STATS_LIMIT=10

# --------------------------------------------------
# Telegram 绑定验证设置
# BIND_CODE_TTL: 验证码有效时间，单位秒
# BIND_MAX_ATTEMPTS: 验证码最多可输错次数
# BIND_SEND_COOLDOWN_SECONDS: 同一 Telegram / 邮箱再次发送验证码的冷却时间，单位秒
# --------------------------------------------------
BIND_CODE_TTL=300
BIND_MAX_ATTEMPTS=5
BIND_SEND_COOLDOWN_SECONDS=60

# --------------------------------------------------
# 任务中心奖励设置
# TASK_INFO_REWARD_MB: 每日查看账号信息奖励，单位 MB
# TASK_SHOP_REWARD_MB: 每日浏览套餐列表奖励，单位 MB
# TASK_SUB_REWARD_MB: 每日查看订阅链接奖励，单位 MB
# TASK_ORDERS_REWARD_MB: 每日查看订单列表奖励，单位 MB
# TASK_INVITE_REWARD_MB: 每日查看邀请中心奖励，单位 MB
# TASK_STREAK_7_REWARD_MB: 连签 7 天额外奖励，单位 MB
# TASK_STREAK_14_REWARD_MB: 连签 14 天额外奖励，单位 MB
# TASK_STREAK_30_REWARD_MB: 连签 30 天额外奖励，单位 MB
# CHECKIN_BROADCAST_ENABLED: 是否开启签到成功群内同步播报，1=开启，0=关闭
# CHECKIN_BROADCAST_CHAT_ID: 签到播报目标群组 chat_id
# CHECKIN_BROADCAST_PRIVATE_SYNC: 私聊签到是否同步到群内，1=开启，0=关闭
# CHECKIN_BROADCAST_GROUP_SYNC: 群内签到是否允许同步到指定群，1=开启，0=关闭
# --------------------------------------------------
TASK_INFO_REWARD_MB=20
TASK_SHOP_REWARD_MB=20
TASK_SUB_REWARD_MB=20
TASK_ORDERS_REWARD_MB=20
TASK_INVITE_REWARD_MB=20
TASK_STREAK_7_REWARD_MB=128
TASK_STREAK_14_REWARD_MB=256
TASK_STREAK_30_REWARD_MB=512
CHECKIN_BROADCAST_ENABLED=1
CHECKIN_BROADCAST_CHAT_ID=
CHECKIN_BROADCAST_PRIVATE_SYNC=1
CHECKIN_BROADCAST_GROUP_SYNC=1
GROUP_HOURLY_PUSH_ENABLED=0
GROUP_HOURLY_PUSH_CHAT_ID=
GROUP_HOURLY_PUSH_INTERVAL_MINUTES=60
GROUP_HOURLY_PUSH_ANCHOR_MINUTE=0
GROUP_HOURLY_PUSH_TEXT=
GROUP_HOURLY_PUSH_BUTTON_1_TEXT=
GROUP_HOURLY_PUSH_BUTTON_1_URL=
GROUP_HOURLY_PUSH_BUTTON_2_TEXT=
GROUP_HOURLY_PUSH_BUTTON_2_URL=
GROUP_HOURLY_PUSH_BUTTON_3_TEXT=
GROUP_HOURLY_PUSH_BUTTON_3_URL=

# --------------------------------------------------
# SMTP 邮件设置
# SMTP_HOST: SMTP 服务器地址
# SMTP_PORT: SMTP 端口，SSL 常见 465，TLS 常见 587
# SMTP_USERNAME: SMTP 登录账号
# SMTP_PASSWORD: SMTP 登录密码
# SMTP_FROM: 实际发件地址
# SMTP_FROM_NAME: 邮件发件人显示名称
# SMTP_USE_TLS: 是否使用 STARTTLS，1=开启，0=关闭
# SMTP_USE_SSL: 是否使用 SSL，1=开启，0=关闭
# --------------------------------------------------
SMTP_HOST=${input_smtp_host}
SMTP_PORT=${input_smtp_port}
SMTP_USERNAME=${input_smtp_user}
SMTP_PASSWORD=${input_smtp_pass}
SMTP_FROM=${input_smtp_from}
SMTP_FROM_NAME=${input_smtp_from_name}
SMTP_USE_TLS=0
SMTP_USE_SSL=1
EOF

    print_ok ">>> .env 已创建"
}

create_service() {
    print_info ">>> 创建 systemd 服务"

    cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=V2Board Telegram Bot
After=network.target redis.service redis-server.service

[Service]
Type=simple
WorkingDirectory=${WORK_DIR}
ExecStart=${WORK_DIR}/venv/bin/python ${WORK_DIR}/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable --now "${SERVICE_NAME}"
    print_ok ">>> 服务已启动"
}

case "${1:-}" in
    install)
        ensure_root
        install_system_packages
        ensure_env_file
        setup_virtualenv
        create_service
        print_ok "安装完成"
        ;;
    start)
        ensure_root
        systemctl start "${SERVICE_NAME}"
        ;;
    stop)
        ensure_root
        systemctl stop "${SERVICE_NAME}"
        ;;
    restart)
        ensure_root
        systemctl restart "${SERVICE_NAME}"
        ;;
    status)
        ensure_root
        systemctl status "${SERVICE_NAME}" --no-pager
        ;;
    logs)
        ensure_root
        journalctl -u "${SERVICE_NAME}" -f
        ;;
    uninstall)
        ensure_root
        systemctl stop "${SERVICE_NAME}" >/dev/null 2>&1 || true
        systemctl disable "${SERVICE_NAME}" >/dev/null 2>&1 || true
        rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
        systemctl daemon-reload
        print_ok "systemd 服务已删除，项目文件仍保留在 ${WORK_DIR}"
        ;;
    *)
        echo "用法: $0 {install|start|stop|restart|status|logs|uninstall}"
        exit 1
        ;;
esac
