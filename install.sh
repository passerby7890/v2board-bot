#!/bin/bash

# 定義顏色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
PLAIN='\033[0m'

echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN}    V2Board/XBoard 簽到機器人 一鍵安裝腳本    ${PLAIN}"
echo -e "${GREEN}    模式：MySQL 直連 | 隱私保護 | 自動守護    ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"

# 檢查 Root 權限
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}錯誤：請使用 root 用戶運行此腳本！${PLAIN}"
   exit 1
fi

# 1. 準備環境
echo -e "${YELLOW}[1/5] 正在安裝系統依賴...${PLAIN}"
if [ -f /etc/debian_version ]; then
    apt update && apt install -y python3 python3-pip python3-venv git wget
elif [ -f /etc/redhat-release ]; then
    yum install -y python3 python3-pip git wget
fi

# 2. 下載代碼 (這裡假設你會上傳到 GitHub，現在先用本地創建的方式演示)
# 實際使用時，請取消註釋下面這行，並替換你的倉庫地址
# git clone https://github.com/你的用戶名/你的倉庫.git /root/v2bot
# cd /root/v2bot

# --- 模擬下載文件 (如果你還沒上傳 GitHub，腳本會自動創建文件) ---
WORK_DIR="/root/v2bot"
mkdir -p $WORK_DIR
cd $WORK_DIR

# (這裡腳本會自動檢查目錄下是否有 bot.py，如果沒有則提示用戶)
if [ ! -f "bot.py" ]; then
    echo -e "${YELLOW}檢測到目錄為空，正在下載最新代碼... (請替換真實 GitHub 地址)${PLAIN}"
    # wget https://raw.githubusercontent.com/你的用戶名/倉庫/main/bot.py
    # wget https://raw.githubusercontent.com/你的用戶名/倉庫/main/requirements.txt
fi
# -----------------------------------------------------------

# 3. 安裝 Python 依賴
echo -e "${YELLOW}[2/5] 正在安裝 Python 依賴庫...${PLAIN}"
pip3 install python-telegram-bot pymysql python-dotenv --break-system-packages

# 4. 配置引導
echo -e "${YELLOW}[3/5] 開始配置參數...${PLAIN}"

if [ -f ".env" ]; then
    echo -e "${GREEN}檢測到 .env 文件已存在，跳過配置。${PLAIN}"
else
    read -p "請輸入 Telegram Bot Token: " input_token
    read -p "請輸入 數據庫密碼 (DB_PASSWORD): " input_db_pass
    read -p "請輸入 數據庫表名 (默認 v2_user, 回車保持默認): " input_table
    input_table=${input_table:-v2_user}
    
    echo "正在生成 .env 文件..."
    cat > .env <<EOF
BOT_TOKEN=$input_token
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=v2board
DB_USERNAME=v2board
DB_PASSWORD=$input_db_pass
TABLE_USER=$input_table
TABLE_PLAN=v2_plan
REWARD_MIN=100
REWARD_MAX=500
EOF
fi

# 5. 設置 Systemd 守護進程
echo -e "${YELLOW}[4/5] 設置開機自啟...${PLAIN}"

cat > /etc/systemd/system/v2bot.service <<EOF
[Unit]
Description=V2Board Telegram Bot
After=network.target mysql.service

[Service]
Type=simple
User=root
WorkingDirectory=$WORK_DIR
ExecStart=/usr/bin/python3 $WORK_DIR/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable v2bot
systemctl restart v2bot

# 6. 完成
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "${GREEN} ✅ 安裝完成！機器人已啟動！ ${PLAIN}"
echo -e "${GREEN}=============================================${PLAIN}"
echo -e "你可以使用以下指令管理："
echo -e "查看狀態: systemctl status v2bot"
echo -e "查看日誌: journalctl -u v2bot -f"
echo -e "重啟服務: systemctl restart v2bot"
echo -e "修改配置: nano $WORK_DIR/.env"
