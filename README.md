# V2Board Telegram Bot

基于 `V2Board / XBoard` 数据库与接口构建的 Telegram 用户中心机器人。

这个项目的目标是让用户尽量在 Telegram 内完成常见操作，而不是反复回到网站后台处理。

## 功能概览

当前版本已支持：

- 邮箱绑定与邮箱验证码验证
- 新用户注册
- 账户信息查询
- 订阅链接查看与重置
- 套餐浏览、下单、支付
- 订单列表与继续支付
- 邀请返利中心
- 任务中心
- 每日签到
- APP 下载中心
- 到期提醒、流量提醒、未支付召回
- AFF 排行榜、邀请码统计、日报、周报
- 群内签到播报
- 群内定时推播

## 交互方式

当前交互原则：

- 按钮式主菜单为主
- 指令作为兼容入口
- 需要输入内容时，采用引导式输入

### 群内允许的内容

群内默认只保留公开互动：

- `签到`
- `簽到`
- `/checkin`
- 群内定时推播

群内不会显示用户隐私信息，例如：

- 邮箱
- 套餐名称
- 到期日
- 总流量 / 已用流量 / 剩余流量
- 订阅链接
- 订单明细
- 返利明细

### 私聊允许的内容

所有涉及用户隐私或账户数据的功能都应在私聊中使用：

- 绑定
- 验证码验证
- 注册
- 查询账户
- 查看订阅
- 购买套餐
- 查看订单
- 邀请返利
- 任务中心

### 管理员功能

以下功能只允许 `.env` 中 `ADMIN_TELEGRAM_IDS` 配置的管理员使用：

- `/admin_aff_rank`
- `/admin_invite_codes`
- `/admin_aff_daily`
- `/admin_aff_weekly`

即使命令被公开展示，非管理员执行时也会被拒绝。

## 主要命令

建议公开给 BotFather 的命令：

```text
start - 打开功能菜单
register - 注册新账号
bind - 绑定账号邮箱
verify - 输入邮箱验证码
info - 查看账户信息
sub - 查看订阅链接
app - 打开下载中心
shop - 购买套餐服务
orders - 查看订单状态
invite - 查看邀请返利
tasks - 查看任务中心
checkin - 每日签到领奖
reset_sub - 重置订阅链接
unbind - 解除账号绑定
help - 查看使用说明
admin_aff_rank - 查看AFF排行榜
admin_invite_codes - 查看邀请码统计
admin_aff_daily - 查看今日AFF报表
admin_aff_weekly - 查看每周AFF报表
```

## 数据来源

### 直接来自 V2Board 数据库

主要读取：

- `v2_user`
- `v2_order`
- `v2_plan`
- `v2_invite_code`
- `v2_commission_log`

### Bot 自建表

机器人会自动创建：

- `v2_tg_bind`
- `v2_bot_notice_log`
- `v2_bot_task_log`

## APP 下载中心

下载中心会读取：

- `V2BOARD_CONFIG_PATH` 指向的配置文件
- V2Board 公开配置接口

优先获取这些信息：

- `app_url`
- `app_description`
- `windows_download_url`
- `macos_download_url`
- `android_download_url`
- `telegram_discuss_link`

## 支付说明

支付流程支持：

- TG 内下单
- TG 内点击支付
- 外部浏览器完成支付
- 支付成功后 bot 自动通知

为兼容支付宝 / 微信等支付场景，项目使用了一个静态中转页：

- `tg-open-link.html`

这个页面用于：

- 从 Telegram 内部打开支付中转页
- 再由中转页调用 Telegram WebApp 的 `openLink()` 打开系统浏览器
- 避免 Telegram 内建浏览器影响支付跳转

## 安装与部署

### 1. 准备项目

建议上传到 GitHub 的文件：

- `bot.py`
- `install.sh`
- `requirements.txt`
- `README.md`
- `.env.example`
- `tg-open-link.html`

不建议上传的文件：

- `.env`
- 任意数据库导出文件
- 任意包含真实账号、密码、Token、SMTP、数据库信息的文件
- `venv/`
- `__pycache__/`

### 2. 准备环境变量

复制 `.env.example` 为 `.env`，然后填写真实配置。

### 3. 安装 bot

在 Linux 服务器上执行：

```bash
bash install.sh install
```

常用命令：

```bash
bash install.sh start
bash install.sh stop
bash install.sh restart
bash install.sh status
bash install.sh logs
```

### 4. 部署支付中转页

`install.sh` 当前会完成：

- Python venv 初始化
- 依赖安装
- systemd 服务创建
- bot 启动

但 **不会自动把 `tg-open-link.html` 复制到 V2Board 网站的 `public` 目录**。

你需要手动把它放到站点公开目录，例如：

```bash
cp tg-open-link.html <v2board_root>/public/tg-open-link.html
```

部署完成后，应能通过以下地址访问：

```text
https://你的域名/tg-open-link.html
```

### 5. 修改 `.env` 后是否需要重启

需要。

当前大部分配置在 `bot.py` 启动时读取，因此修改 `.env` 后建议执行：

```bash
systemctl restart v2bot
```

## 群播与定时推播

### 签到群播

相关参数：

- `CHECKIN_BROADCAST_ENABLED`
- `CHECKIN_BROADCAST_CHAT_ID`
- `CHECKIN_BROADCAST_PRIVATE_SYNC`
- `CHECKIN_BROADCAST_GROUP_SYNC`

### 群内定时推播

相关参数：

- `GROUP_HOURLY_PUSH_ENABLED`
- `GROUP_HOURLY_PUSH_CHAT_ID`
- `GROUP_HOURLY_PUSH_INTERVAL_MINUTES`
- `GROUP_HOURLY_PUSH_ANCHOR_MINUTE`
- `GROUP_HOURLY_PUSH_TEXT`
- `GROUP_HOURLY_PUSH_BUTTON_1_TEXT`
- `GROUP_HOURLY_PUSH_BUTTON_1_URL`
- `GROUP_HOURLY_PUSH_BUTTON_2_TEXT`
- `GROUP_HOURLY_PUSH_BUTTON_2_URL`
- `GROUP_HOURLY_PUSH_BUTTON_3_TEXT`
- `GROUP_HOURLY_PUSH_BUTTON_3_URL`

当前逻辑支持：

- 默认每小时一次
- 只保留最新一条群推
- 默认可只放一个“打开机器人”按钮

## 开发与维护说明

- 奖励流量的方式是增加 `transfer_enable`
- 不是减少 `u`
- 不是减少 `d`
- 不是回滚历史已消耗流量

- 重要提醒默认以 Telegram 为主
- 必要时补发 Email

- 若需公开仓库，建议额外配置 `.gitignore`

推荐示例：

```gitignore
.env
venv/
__pycache__/
*.sql
*.deploy.env
```

## 当前状态

当前版本已具备：

- 完整用户绑定与注册能力
- 套餐购买与支付能力
- 账户与订阅查询能力
- 签到、任务、提醒、返利、报表能力
- 适合继续迭代的基础架构

后续如需扩展，可继续在此基础上追加：

- 更多群运营玩法
- 更多管理报表维度
- 更完整的活动系统
