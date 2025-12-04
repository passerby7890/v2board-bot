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
echo -e "${GREEN}    V2Board Bot 安装脚本 (强制更新版)        ${PLAIN}"
echo -e "${GREEN}    模式：Wget 强制下载 | 虚拟环境 | Systemd   ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"

# 检查 Root 权限
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}错误：请使用 root 用户运行此脚本！${PLAIN}"
   exit 1
fi

# 1. 准备系统环境
echo -e "${YELLOW}[1/5] 正在检查并安装系统依赖...${PLAIN}"
if [ -f /etc/debian_version ]; then
    apt update && apt install -y python3 python3-pip python3-venv wget
elif [ -f /etc/redhat-release ]; then
    yum install -y python3 python3-pip wget
fi

# 创建工作目录
mkdir -p $WORK_DIR
cd $WORK_DIR

# 2. 强制下载核心文件 (这里是修改的重点)
echo -e "${YELLOW}[2/5] 正在强制下载最新代码...${PLAIN}"

# 强制下载 bot.py
wget -O bot.py "${BASE_URL}/bot.py"
if [ ! -s "bot.py" ]; then
    echo -e "${RED}错误：bot.py 下载失败或文件为空！请检查网络或 GitHub 地址。${PLAIN}"
    exit 1
else
    echo -e "${GREEN}bot.py 下载成功。${PLAIN}"
fi

# 强制下载 requirements.txt (如果仓库里没有这个文件，这一步会报 404，但不影响后续运行，我会做个判断)
wget -O requirements.txt "${BASE_URL}/requirements.txt" 2>/dev/null
if [ ! -s "requirements.txt" ]; then
    echo -e "${YELLOW}提示：线上未找到 requirements.txt，将使用内置依赖列表。${PLAIN}"
    rm -f requirements.txt
fi


# 3. 配置 Python 虚拟环境
echo -e "${YELLOW}[3/5] 正在配置 Python 虚拟环境 (venv)...${PLAIN}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}虚拟环境创建成功。${PLAIN}"
fi

# 激活环境并安装依赖
echo -e "正在安装依赖库..."
./venv/bin/pip install --upgrade pip

if [ -f "requirements.txt" ]; then
    ./venv/bin/pip install -r requirements.txt
else
    # 如果没有下载到 requirements.txt，则手动安装你需要的库
    ./venv/bin/pip install python-telegram-bot pymysql python-dotenv
fi


# 4. 配置引导 (.env)
echo -e "${YELLOW}[4/5] 检查配置文件...${PLAIN}"

if [ -f ".env" ]; then
    echo -e "${GREEN}检测到 .env 文件已存在，保留原有配置。${PLAIN}"
else
    echo -e "${GREEN}>>> 新环境，请输入配置信息 <<<${PLAIN}"
    
    read -p "请输入 Telegram Bot Token: " input_token
    read -p "请输入 数据库地址 (默认 127.0.0.1): " input_db_host
    input_db_host=${input_db_host:-127.0.0.1}
    read -p "请输入 数据库名: " input_db_name
    read -p "请输入 数据库用户名: " input_db_user
    read -p "请输入 数据库密码: " input_db_pass
    read -p "请输入 用户表名 (默认 v2_user): " input_table_user
    input_table_user=${input_table_user:-v2_user}

    echo "正在生成 .env 文件..."
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

# 5. 设置 Systemd 守护进程
echo -e "${YELLOW}[5/5] 设置开机自启服务...${PLAIN}"

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

# 6. 完成
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN} ✅ 更新/安装完成！服务已重启！ ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "常用指令："
echo -e "查看日志: journalctl -u v2bot -f"
echo -e "重启服务: systemctl restart v2bot"
