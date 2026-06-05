# Pi-Yuanbao 🤖💬

通过**腾讯元宝官方 Bot API**，将元宝作为聊天渠道远程控制 **Pi Agent**。

## ✨ 功能

- 🔌 **官方 Bot API** — 使用元宝官方 WebSocket + Protobuf 协议，无需油猴脚本
- 🔐 **自动认证** — sign-token 自动获取和刷新
- 💓 **心跳保活** — WebSocket 心跳 + Reply 心跳（处理中状态）
- 🔄 **自动重连** — 断线指数退避重连
- 🛠️ **自定义工具** — `send_to_yuanbao` 工具 + `/yuanbao` 命令

## 📁 项目结构

```
pi-yuanbao/
├── src/
│   ├── index.ts              # Pi 扩展主入口
│   ├── yuanbao-client.ts     # 元宝 WebSocket 客户端（认证、心跳、消息收发）
│   ├── yuanbao-proto.ts      # Protobuf 协议编解码（纯实现，无第三方依赖）
│   ├── yuanbao-sign.ts       # Sign Token 管理器
│   └── types.ts              # 类型定义
├── reference/                # hermes-agent 参考实现（仅供开发参考）
├── package.json
└── README.md
```

## 🚀 快速开始

### 1. 获取元宝 Bot 凭证

在腾讯元宝开放平台申请 Bot，获取：
- **App Key** (也叫 `app_id`)
- **App Secret**

### 2. 设置环境变量

```bash
export YUANBAO_APP_ID="your_app_key"
export YUANBAO_APP_SECRET="your_app_secret"

# 可选
export YUANBAO_BOT_ID="your_bot_id"        # 通常由 sign-token API 自动返回
export YUANBAO_WS_URL="wss://..."          # 自定义 WebSocket 地址
export YUANBAO_API_DOMAIN="https://..."    # 自定义 API 域名
export YUANBAO_ROUTE_ENV="ci-677"          # 路由环境
```

### 3. 安装扩展

```bash
cd pi-yuanbao
npm install

# 方式一：自动发现
cp -r src/ ~/.pi/agent/extensions/yuanbao/

# 方式二：符号链接
ln -s $(pwd)/src ~/.pi/agent/extensions/yuanbao

# 方式三：手动加载
pi -e ./src/index.ts
```

### 4. 启动

```bash
# Pi 会自动加载扩展
pi

# 或手动指定（也可通过 CLI 标志配置）
pi --yuanbao-app-id=YOUR_KEY --yuanbao-app-secret=YOUR_SECRET
```

### 5. 使用

在元宝中给 Bot 发消息，Pi 会自动接收并处理，响应会回传到元宝。

## 📖 使用命令

在 Pi 中使用以下命令管理元宝连接：

| 命令 | 说明 |
|------|------|
| `/yuanbao start` | 启动元宝 Bot 连接 |
| `/yuanbao stop` | 断开连接 |
| `/yuanbao status` | 查看连接状态 |
| `/yuanbao config` | 查看当前配置 |
| `/yuanbao help` | 显示帮助 |

## 🔧 配置方式

### 环境变量

| 变量名 | 说明 | 必需 |
|--------|------|------|
| `YUANBAO_APP_ID` | 元宝 App Key | ✅ |
| `YUANBAO_APP_SECRET` | 元宝 App Secret | ✅ |
| `YUANBAO_BOT_ID` | Bot ID | ❌ (自动获取) |
| `YUANBAO_WS_URL` | WebSocket 网关地址 | ❌ |
| `YUANBAO_API_DOMAIN` | API 域名 | ❌ |
| `YUANBAO_ROUTE_ENV` | 路由环境 | ❌ |

### CLI 标志

```bash
pi --yuanbao-app-id=KEY --yuanbao-app-secret=SECRET
pi --yuanbao-ws-url=wss://custom.example.com
pi --yuanbao-route-env=ci-677
```

## 🏗️ 架构

```
┌─────────────────────┐    WebSocket (Protobuf)    ┌──────────────────┐
│   腾讯元宝 Bot 网关  │ ◄────────────────────────► │  Pi Extension    │
│   bot-wss.yuanbao   │    AUTH_BIND / Heartbeat   │  (yuanbao-client)│
└─────────────────────┘                            └────────┬─────────┘
                                                            │
                                                            │ sendUserMessage()
                                                            │ on("turn_end")
                                                            ▼
                                                   ┌──────────────────┐
                                                   │  Pi Agent Core   │
                                                   │  (LLM + Tools)   │
                                                   └──────────────────┘
```

### 协议流程

1. **认证**: `sign-token` API 获取 token → WebSocket `AUTH_BIND` → 收到 `BIND_ACK`
2. **心跳**: 每 30 秒发送 `ping`，收到 `pong` 确认
3. **收消息**: `InboundMessagePush` → 解码 protobuf → 提取文本 → `sendUserMessage()`
4. **发消息**: `turn_end` 事件 → 编码 `SendC2CMessageReq`/`SendGroupMessageReq` → WebSocket 发送
5. **Reply 心跳**: 处理中每 2 秒发送 `RUNNING` 状态，完成后发送 `FINISH`

### 消息类型支持

| 消息类型 | Protobuf 类型 | 支持状态 |
|----------|---------------|----------|
| 文本 | `TIMTextElem` | ✅ |
| 图片 | `TIMImageElem` | 🔜 |
| 文件 | `TIMFileElem` | 🔜 |
| 语音 | `TIMSoundElem` | 🔜 |

## 🏛️ 协议实现

协议编解码完全参考 [hermes-agent](https://github.com/nousresearch/hermes-agent) 的 Python 实现，纯 TypeScript 移植，不依赖第三方 protobuf 库。

核心概念：
- **ConnMsg**: WebSocket 帧层，包含 Head（控制信息）+ Data（业务 payload）
- **Head**: `cmd_type`（请求/响应/推送/ACK）, `cmd`（命令字）, `seq_no`, `msg_id`, `module`
- **BizMsg**: 业务层，如 `InboundMessagePush`, `SendC2CMessageReq` 等
- **Wire Format**: 标准 protobuf varint + length-delimited 编码

## 🛠️ 开发

### 调试

```bash
# Pi 扩展日志
# 搜索 [Pi-Yuanbao] 或 [YuanbaoClient] 前缀
```

### 参考代码

`reference/` 目录包含 hermes-agent 的原始 Python 实现，仅供开发参考：

- `yuanbao_proto.py` — 协议编解码
- `yuanbao_hermes.py` — 平台适配器
- `yuanbao_media.py` — 媒体文件处理
- `yuanbao_sticker.py` — 表情包处理

## ⚠️ 注意事项

1. **安全性** — App Secret 请妥善保管，不要提交到版本控制
2. **Token 过期** — sign-token 有有效期，扩展会自动刷新
3. **消息限制** — 单条消息最大 4000 字符（元宝限制）
4. **网络环境** — 需要能访问 `bot-wss.yuanbao.tencent.com` 和 `bot.yuanbao.tencent.com`
5. **Token 消耗** — Pi 使用 LLM API，注意控制调用频率

## 📄 License

MIT
