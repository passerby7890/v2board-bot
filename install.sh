#!/bin/bash

# =================配置区域=================
# 项目安装目录
WORK_DIR="/root/v2bot"
# GitHub 仓库地址
REPO_URL="https://github.com/passerby7890/v2board-bot.git"
# =========================================

# 定义颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
PLAIN='\033[0m'

echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN}    V2Board/XBoard 签到机器人 一键安装脚本    ${PLAIN}"
echo -e "${GREEN}    模式：Python 虚拟环境 | 自动拉取 | Systemd    ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"

# 检查 Root 权限
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}错误：请使用 root 用户运行此脚本！${PLAIN}"
   exit 1
fi

# 1. 准备系统环境
echo -e "${YELLOW}[1/5] 正在检查并安装系统依赖...${PLAIN}"
if [ -f /etc/debian_version ]; then
    apt update && apt install -y python3 python3-pip python3-venv git wget
elif [ -f /etc/redhat-release ]; then
    yum install -y python3 python3-pip git wget
    # CentOS 可能需要单独处理 venv，视版本而定，通常包含在 python3 中
fi

# 2. 拉取/更新代码
echo -e "${YELLOW}[2/5] 正在从 GitHub 拉取代码...${PLAIN}"
if [ -d "$WORK_DIR" ]; then
    echo -e "${GREEN}检测到目录已存在，正在备份配置文件并更新代码...${PLAIN}"
    cd $WORK_DIR
    # 备份 .env
    [ -f ".env" ] && cp .env .env.bak
    
    # 强制拉取最新代码
    git fetch --all
    git reset --hard origin/main
    git pull
    
    # 还原 .env
    [ -f ".env.bak" ] && mv .env.bak .env
else
    echo -e "${GREEN}目录不存在，正在克隆仓库...${PLAIN}"
    git clone $REPO_URL $WORK_DIR
    cd $WORK_DIR
fi

# 3. 配置 Python 虚拟环境 (关键修复步骤)
echo -e "${YELLOW}[3/5] 正在配置 Python 虚拟环境 (venv)...${PLAIN}"
# 创建虚拟环境文件夹 venv
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}虚拟环境创建成功。${PLAIN}"
fi

# 激活虚拟环境并安装依赖
echo -e "正在安装 Python 依赖 (requirements.txt)..."
./venv/bin/pip install --upgrade pip
if [ -f "requirements.txt" ]; then
    ./venv/bin/pip install -r requirements.txt
else
    echo -e "${RED}警告：未找到 requirements.txt，尝试手动安装基础依赖...${PLAIN}"
    ./venv/bin/pip install python-telegram-bot pymysql python-dotenv
fi

# 4. 配置引导 (.env)
echo -e "${YELLOW}[4/5] 检查配置文件...${PLAIN}"

if [ -f ".env" ]; then
    echo -e "${GREEN}检测到 .env 文件已存在，跳过配置。${PLAIN}"
    echo -e "如果需要修改，请手动编辑: nano $WORK_DIR/.env"
else
    echo -e "${GREEN}>>> 请根据提示输入配置信息 <<<${PLAIN}"
    
    read -p "请输入 Telegram Bot Token: " input_token
    
    read -p "请输入 数据库地址 (默认 127.0.0.1): " input_db_host
    input_db_host=${input_db_host:-127.0.0.1}
    
    read -p "请输入 数据库名 (例如 0n21_com): " input_db_name
    
    read -p "请输入 数据库用户名 (例如 0n21_com): " input_db_user
    
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
echo -e "${YELLOW}[4/5] 设置开机自启服务...${PLAIN}"

# 注意：ExecStart 指向的是 venv 里的 python，而不是系统的 python
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
echo -e "${GREEN} ✅ 安装完成！服务已重启！ ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "常用指令："
echo -e "查看状态: systemctl status v2bot"
echo -e "查看日志: journalctl -u v2bot -f"
echo -e "重启服务: systemctl restart v2bot"
echo -e "修改配置: nano $WORK_DIR/.env"
