#!/bin/bash

# =================配置区域=================
# 项目安装目录
WORK_DIR="/root/v2bot"
# 文件的下载基准地址 (Raw URL)
BASE_URL="https://raw.githubusercontent.com/passerby7890/v2board-bot/main"
# =========================================

# 定义颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
PLAIN='\033[0m'

echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN}    V2Board Bot 安装脚本 (数据保护版)        ${PLAIN}"
echo -e "${GREEN}    模式：智能询问 | 强制Wget | 虚拟环境       ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"

# 检查 Root 权限
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}错误：请使用 root 用户运行此脚本！${PLAIN}"
   exit 1
fi

# ==========================================================
# 0. 智能询问：检测已存在的安装
# ==========================================================
if [ -d "$WORK_DIR" ]; then
    echo -e "${YELLOW}检测到安装目录 $WORK_DIR 已存在。${PLAIN}"
    echo -e "${YELLOW}请选择操作：${PLAIN}"
    echo -e "  ${GREEN}1.${PLAIN} 仅更新代码 (保留 bot_data.db 和 .env 配置) [默认]"
    echo -e "  ${RED}2.${PLAIN} 全新安装 (删除所有数据，包括配置和数据库！)"
    read -p "请输入数字 [1-2]: " install_mode
    
    if [[ "$install_mode" == "2" ]]; then
        echo -e "${RED}警告：正在删除旧目录...${PLAIN}"
        rm -rf "$WORK_DIR"
        # 停止旧服务防止占用文件
        systemctl stop v2bot 2>/dev/null
        echo -e "${GREEN}旧数据已清除，准备全新安装。${PLAIN}"
    else
        echo -e "${GREEN}==> 将执行更新模式，数据文件将被保留。<==${PLAIN}"
    fi
fi
# ==========================================================

# 1. 强制检查并安装 Wget
echo -e "${YELLOW}[1/5] 正在强制检查并安装 Wget...${PLAIN}"
if [[ -f /etc/redhat-release ]]; then
    yum -y install wget
elif [[ -f /etc/debian_version ]]; then
    apt-get update -y >/dev/null 2>&1
    apt-get -y install wget
else
    yum -y install wget || apt-get -y install wget
fi

if ! command -v wget &> /dev/null; then
    echo -e "${RED}错误：Wget 安装失败！请手动安装后重试。${PLAIN}"
    exit 1
fi

# 2. 准备系统环境 (Python)
echo -e "${YELLOW}[2/5] 正在安装 Python 环境...${PLAIN}"
if [[ -f /etc/redhat-release ]]; then
    yum -y install python3 python3-pip
elif [[ -f /etc/debian_version ]]; then
    apt-get -y install python3 python3-pip python3-venv
fi

# 创建工作目录 (如果刚才选了删除，这里会重新创建；如果是更新，这里静默跳过)
mkdir -p $WORK_DIR
cd $WORK_DIR

# 3. 下载核心文件
echo -e "${YELLOW}[3/5] 正在同步最新代码...${PLAIN}"

# 备份一下 bot_data.db 防止 wget 出现极端意外（虽然 wget 默认是覆盖同名文件，不是删除）
if [ -f "bot_data.db" ]; then
    cp bot_data.db bot_data.db.bak
fi

# 强制下载 bot.py (覆盖旧代码)
wget -O bot.py "${BASE_URL}/bot.py"
if [ ! -s "bot.py" ]; then
    echo -e "${RED}错误：bot.py 下载失败！请检查网络。${PLAIN}"
    exit 1
else
    echo -e "${GREEN}bot.py 更新成功。${PLAIN}"
fi

# 尝试下载 requirements.txt
wget -O requirements.txt "${BASE_URL}/requirements.txt" 2>/dev/null
if [ ! -s "requirements.txt" ]; then
    rm -f requirements.txt
fi

# 4. 配置 Python 虚拟环境
echo -e "${YELLOW}[4/5] 配置 Python 虚拟环境...${PLAIN}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# 激活环境并安装依赖
echo -e "正在检查依赖库..."
./venv/bin/pip install --upgrade pip >/dev/null 2>&1

if [ -f "requirements.txt" ]; then
    ./venv/bin/pip install -r requirements.txt
else
    ./venv/bin/pip install python-telegram-bot pymysql python-dotenv
fi

# 5. 配置引导 (.env)
echo -e "${YELLOW}[5/5] 检查配置文件...${PLAIN}"

if [ -f ".env" ]; then
    echo -e "${GREEN}检测到 .env 配置文件，保留原有配置。${PLAIN}"
else
    echo -e "${GREEN}>>> 未检测到配置，开始初始化 <<<${PLAIN}"
    
    read -p "请输入 Telegram Bot Token: " input_token
    read -p "请输入 数据库地址 (默认 127.0.0.1): " input_db_host
    input_db_host=${input_db_host:-127.0.0.1}
    read -p "请输入 数据库名: " input_db_name
    read -p "请输入 数据库用户名: " input_db_user
    read -p "请输入 数据库密码: " input_db_pass
    read -p "请输入 用户表名 (默认 v2_user): " input_table_user
    input_table_user=${input_table_user:-v2_user}

    cat > .env <<EOF
BOT_TOKEN=$input_token
DB_HOST=$input_db_host
DB_PORT=3306
DB_DATABASE=$input_db_name
DB_USERNAME=$input_db_user
DB_PASSWORD=$input_db_pass
TABLE_USER=$input_table_user
TABLE_PLAN=v2_plan
REWARD_MIN=100
REWARD_MAX=200
EOF
fi

# 6. 设置 Systemd
echo -e "${YELLOW}设置服务守护...${PLAIN}"

cat > /etc/systemd/system/v2bot.service <<EOF
[Unit]
Description=V2Board Telegram Bot
After=network.target mysql.service

[Service]
Type=simple
User=root
WorkingDirectory=$WORK_DIR
ExecStart=$WORK_DIR/venv/bin/python3 $WORK_DIR/bot.py
Restart=always
RestartSec=10
Environment="PATH=$WORK_DIR/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable v2bot
systemctl restart v2bot

# 7. 完成
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN} ✅ 执行完毕！服务已重启！ ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "常用指令："
echo -e "查看日志: journalctl -u v2bot -f"
echo -e "重启服务: systemctl restart v2bot"
