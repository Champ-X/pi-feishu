# Pi-Yuanbao

[![npm version](https://img.shields.io/npm/v/pi-yuanbao.svg)](https://www.npmjs.com/package/pi-yuanbao)
[![license](https://img.shields.io/npm/l/pi-yuanbao.svg)](https://www.npmjs.com/package/pi-yuanbao)

通过腾讯元宝官方 Bot API，将元宝作为聊天渠道远程控制 Pi Agent。

用手机上的元宝 App 给 Bot 发消息，Pi 自动接收并处理，响应实时回传到手机。

## 功能

- 使用元宝官方 WebSocket + Protobuf 协议，稳定可靠
- sign-token 自动获取和缓存刷新
- WebSocket 心跳 + Reply 心跳（处理中状态推送）
- 断线指数退避重连，自动刷新 token
- 每轮 Turn 实时推送到手机，不漏回复
- 图片、文件自动下载后转发给 Pi 处理
- 支持多用户同时与 Bot 对话
- 注册 `send_to_yuanbao` 工具 + `/yuanbao` 命令

## 安装

```bash
pi install npm:pi-yuanbao
```

## 配置

### 获取凭证

在腾讯元宝开放平台申请 Bot，获取 App Key 和 App Secret。

### 配置方式

支持三种配置方式，优先级从高到低：

**1. CLI 标志**

```bash
pi --yuanbao-app-id=YOUR_KEY --yuanbao-app-secret=YOUR_SECRET
```

**2. 环境变量**

```bash
export YUANBAO_APP_ID="your_app_key"
export YUANBAO_APP_SECRET="your_app_secret"
```

**3. Pi settings.json（推荐）**

在 `.pi/settings.json`（项目级）或 `~/.pi/agent/settings.json`（全局）中添加：

```json
{
  "yuanbao": {
    "appId": "your_app_key",
    "appSecret": "your_app_secret",
    "botId": "optional_bot_id",
    "wsUrl": "optional_ws_url",
    "apiDomain": "optional_api_domain",
    "routeEnv": "optional_route_env"
  }
}
```

字段名支持 camelCase（`appId`）和 snake_case（`app_id`）两种写法。项目级配置覆盖全局配置。

### 配置项说明

| 字段 | 环境变量 | CLI 标志 | 必需 |
|------|----------|----------|------|
| App Key | `YUANBAO_APP_ID` | `--yuanbao-app-id` | 是 |
| App Secret | `YUANBAO_APP_SECRET` | `--yuanbao-app-secret` | 是 |
| Bot ID | `YUANBAO_BOT_ID` | `--yuanbao-bot-id` | 否，自动获取 |
| WebSocket 地址 | `YUANBAO_WS_URL` | `--yuanbao-ws-url` | 否 |
| API 域名 | `YUANBAO_API_DOMAIN` | `--yuanbao-api-domain` | 否 |
| 路由环境 | `YUANBAO_ROUTE_ENV` | `--yuanbao-route-env` | 否 |

### 启动

```bash
pi
```

Pi 启动时会自动加载 pi-yuanbao 扩展并连接元宝 Bot。在元宝 App 中给 Bot 发消息即可开始使用。

## 管理命令

在 Pi 中使用 `/yuanbao` 命令管理连接：

| 命令 | 说明 |
|------|------|
| `/yuanbao start` | 启动元宝 Bot 连接 |
| `/yuanbao stop` | 断开连接 |
| `/yuanbao status` | 查看连接状态 |
| `/yuanbao config` | 查看当前配置 |
| `/yuanbao help` | 显示帮助 |

## 架构

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

1. 认证: `sign-token` API → WebSocket `AUTH_BIND` → `BIND_ACK`
2. 心跳: 每 30s 发送 `ping`，收到 `pong` 确认
3. 收消息: `InboundMessagePush` → 解码 (JSON/Protobuf 双格式) → 提取文本 → `sendUserMessage()`
4. 发消息: `turn_end` 事件 → 分块 → 编码 `SendC2CMessageReq` → WebSocket 发送
5. Reply 心跳: 处理中每 2s 发送 `RUNNING` 状态，完成后发送 `FINISH`

### 消息类型支持

| 消息类型 | 协议类型 | 状态 |
|----------|----------|------|
| 文本 | `TIMTextElem` | 支持 |
| 图片 | `TIMImageElem` | 支持 |
| 文件 | `TIMFileElem` | 支持 |
| 表情 | `TIMFaceElem` | 支持 |
| 语音 | `TIMSoundElem` | 支持 |
| 自定义 | `TIMCustomElem` | 支持 |

## 协议实现

协议编解码参考 [hermes-agent](https://github.com/nousresearch/hermes-agent) 的 Python 实现，纯 TypeScript 手写 Protobuf wire format，零第三方依赖。

## 项目结构

```
pi-yuanbao/
├── src/
│   ├── index.ts              # Pi 扩展主入口
│   ├── yuanbao-client.ts     # 元宝 WebSocket 客户端
│   ├── yuanbao-proto.ts      # Protobuf 协议编解码
│   ├── yuanbao-sign.ts       # Sign Token 管理器
│   └── types.ts              # 类型定义
├── package.json
└── README.md
```

## 开发

```bash
npm install
npm run typecheck
npm run test
pi -e ./src/index.ts --yuanbao-app-id=KEY --yuanbao-app-secret=SECRET
```

## 注意事项

1. App Secret 请妥善保管，不要提交到版本控制
2. sign-token 有有效期，扩展会自动刷新
3. 单条消息最大 4000 字符（元宝限制），长消息自动分块
4. 需要能访问 `bot-wss.yuanbao.tencent.com` 和 `bot.yuanbao.tencent.com`

## License

MIT
