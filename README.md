V2Board/XBoard Telegram 簽到機器人 (SQL 直連版)

這是一個通過直接連接 MySQL 數據庫實現流量簽到的 Telegram 機器人。
相比 API 方式，它更穩定、兼容性更強（支持 XBoard 等魔改版），並且具備完善的隱私保護功能。

✨ 功能特點

🚀 SQL 直連：無視 API 路徑變化，直接操作數據庫。

🔒 隱私保護：群組內綁定自動撤回郵箱，支持引導私聊綁定。

🎲 趣味簽到：支持連續簽到倍率獎勵 (7天/14天/21天) 及隨機暴擊。

🔧 高度可配：支持自定義數據庫表名、獎勵範圍、套餐限制。

🛠 安裝方法

一鍵腳本 (推薦)

wget -O install.sh [https://raw.githubusercontent.com/passerby7890/v2board-bot/main/install.sh](https://raw.githubusercontent.com/passerby7890/v2board-bot/main/install.sh) && chmod +x install.sh && ./install.sh



手動安裝

克隆倉庫

安裝依賴 pip3 install -r requirements.txt

複製 .env.example 為 .env 並填寫配置

運行 python3 bot.py

⚙️ 配置說明 (.env)

BOT_TOKEN: Telegram 機器人 Token

DB_HOST: 數據庫地址 (通常 127.0.0.1)

DB_PASSWORD: 數據庫密碼 (查看網站根目錄 .env)

TABLE_USER: 用戶表名 (默認 v2_user，XBoard 可改為 xb_user)
