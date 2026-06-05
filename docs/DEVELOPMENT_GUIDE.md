# Pi-Yuanbao 插件开发指南

## 📌 项目概述

**目标**：开发一个 Pi 扩展（Extension），通过腾讯元宝官方 Bot API 将元宝作为聊天渠道，实现远程控制 Pi。

**核心架构**：
- 使用元宝官方 WebSocket Bot API（`wss://bot-wss.yuanbao.tencent.com`）
- 协议基于 Protobuf（`trpc.yuanbao.*`），纯 TypeScript 实现编解码
- 认证通过 `sign-token` API（HMAC-SHA256 签名）
- 参考实现：[hermes-agent](https://github.com/nousresearch/hermes-agent)

## 🏗️ 架构设计

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

## 📁 项目结构

```
pi-yuanbao/
├── src/
│   ├── index.ts              # Pi 扩展主入口
│   ├── yuanbao-client.ts     # WebSocket 客户端（认证、心跳、消息收发）
│   ├── yuanbao-proto.ts      # Protobuf 协议编解码
│   ├── yuanbao-sign.ts       # Sign Token 管理器
│   └── types.ts              # 类型定义
├── reference/                # hermes-agent 参考实现
│   ├── yuanbao_proto.py      # Python 协议实现
│   ├── yuanbao_hermes.py     # Python 平台适配器
│   ├── yuanbao_media.py      # 媒体文件处理
│   └── yuanbao_sticker.py    # 表情包处理
├── docs/
│   └── DEVELOPMENT_GUIDE.md  # 本文档
├── package.json
└── README.md
```

## 🔑 认证流程

### 1. Sign Token 获取

```
POST {api_domain}/api/v5/robotLogic/sign-token
Content-Type: application/json

{
  "app_key": "YOUR_APP_KEY",
  "nonce": "随机字符串",
  "signature": "HMAC-SHA256(app_secret, nonce + timestamp + app_key + app_secret)",
  "timestamp": "2024-01-01T00:00:00+08:00"
}

Response:
{
  "code": 0,
  "data": {
    "token": "auth_token",
    "bot_id": "bot_id",
    "duration": 3600,
    "source": "bot"
  }
}
```

### 2. WebSocket AUTH_BIND

```
ConnMsg {
  head: {
    cmd_type: Request(0),
    cmd: "auth-bind",
    seq_no: 递增序列号,
    msg_id: UUID,
    module: "conn_access"
  },
  data: AuthBindReq {
    biz_id: "ybBot",
    auth_info: {
      uid: bot_id,
      source: "bot",
      token: sign_token
    },
    device_info: {
      instance_id: 17,
      app_operation_system: "linux/darwin/win32"
    }
  }
}
```

### 3. BIND_ACK 响应

```
ConnMsg {
  head: {
    cmd_type: Response(1),
    cmd: "auth-bind"
  },
  data: AuthBindRsp {
    code: 0,           // 0=成功
    message: "",
    connect_id: "xxx"  // 连接 ID
  }
}
```

## 📨 消息协议

### ConnMsg 层

```
message ConnMsg {
  Head  head = 1;   // 控制信息
  bytes data = 2;   // 业务 payload
}

message Head {
  uint32 cmd_type = 1;  // 0=Request, 1=Response, 2=Push, 3=PushAck
  string cmd      = 2;  // 命令字
  uint32 seq_no   = 3;  // 序列号
  string msg_id   = 4;  // 消息 ID
  string module   = 5;  // 模块名
  bool   need_ack = 6;  // 是否需要 ACK
  int32  status   = 10; // 状态码
}
```

### 入站消息 (InboundMessagePush)

```
InboundMessagePush {
  callback_command = 1;   // "C2C.CallbackAfterSendMsg" 或 "Group.CallbackAfterSendMsg"
  from_account     = 2;   // 发送者账号
  to_account       = 3;   // 接收者账号
  sender_nickname  = 4;   // 发送者昵称
  group_id         = 5;   // 群 ID
  group_code       = 6;   // 群号
  group_name       = 7;   // 群名
  msg_seq          = 8;   // 消息序列号
  msg_random       = 9;   // 随机数
  msg_time         = 10;  // 消息时间（秒）
  msg_key          = 11;  // 消息 key
  msg_id           = 12;  // 消息 ID
  msg_body         = 13;  // 消息体（repeated MsgBodyElement）
  cloud_custom_data = 14; // 云自定义数据
  bot_owner_id     = 16;  // Bot 所有者 ID
  claw_msg_type    = 18;  // 消息类型
}
```

### 出站消息 (SendC2CMessageReq)

```
SendC2CMessageReq {
  msg_id      = 1;  // 消息 ID
  to_account  = 2;  // 接收者账号
  from_account = 3; // 发送者账号（Bot）
  msg_random  = 4;  // 随机数
  msg_body    = 5;  // 消息体（repeated MsgBodyElement）
  group_code  = 6;  // 来源群号（群转私聊场景）
}
```

### MsgBodyElement

```
MsgBodyElement {
  msg_type    = 1;  // "TIMTextElem", "TIMImageElem", etc.
  msg_content = 2;  // MsgContent
}

MsgContent {
  text          = 1;  // 文本内容
  uuid          = 2;  // UUID
  image_format  = 3;  // 图片格式
  data          = 4;  // 数据
  desc          = 5;  // 描述
  ext           = 6;  // 扩展
  url           = 10; // URL
  file_size     = 11; // 文件大小
  file_name     = 12; // 文件名
}
```

## 💓 心跳机制

### WebSocket 心跳

每 30 秒发送 `ping` 命令，等待 `pong` 响应：

```
ConnMsg {
  head: { cmd_type: Request, cmd: "ping", module: "conn_access" }
  data: (空)
}
```

连续 2 次未收到 pong 则触发重连。

### Reply 心跳

处理用户消息时，每 2 秒发送 `RUNNING` 状态，完成后发送 `FINISH`：

```
// 私聊
SendPrivateHeartbeatReq {
  from_account = bot_id;
  to_account   = user_id;
  heartbeat    = 1; // RUNNING
}

// 群聊
SendGroupHeartbeatReq {
  from_account = bot_id;
  group_code   = group_code;
  send_time    = timestamp_ms;
  heartbeat    = 1; // RUNNING
}
```

状态常量：
- `WS_HEARTBEAT_RUNNING = 1` — 处理中
- `WS_HEARTBEAT_FINISH = 2` — 处理完成

## 📚 Pi Extension API

| 方法 | 用途 |
|------|------|
| `pi.sendUserMessage(content)` | 发送用户消息给 Pi |
| `pi.on("turn_end", handler)` | 监听 Pi 响应完成 |
| `pi.on("message_update", handler)` | 监听流式消息更新 |
| `pi.registerCommand(name, opts)` | 注册自定义命令 |
| `pi.registerTool(tool)` | 注册自定义工具 |
| `pi.registerFlag(name, opts)` | 注册 CLI 标志 |
| `pi.getFlag(name)` | 获取 CLI 标志值 |

## 🔧 开发环境

### 依赖

```bash
npm install ws
npm install -D typescript @types/ws
```

### 调试

```bash
# 搜索日志前缀
[Pi-Yuanbao]     # 扩展主逻辑
[YuanbaoClient]  # WebSocket 客户端
[SignManager]    # Token 管理
```

### 测试连接

```bash
# 设置环境变量
export YUANBAO_APP_ID="your_key"
export YUANBAO_APP_SECRET="your_secret"

# 启动
pi -e ./src/index.ts

# 在 Pi 中查看状态
/yuanbao status
```

## ⚠️ 注意事项

1. **安全性** — App Secret 不要提交到版本控制，使用环境变量
2. **Token 过期** — sign-token 有效期通常为 1 小时，扩展会自动刷新
3. **消息大小** — 元宝单条消息限制约 4000 字符
4. **Protobuf** — 协议编解码是手写的 varint/wire-format 实现，不依赖 google.protobuf
5. **参考实现** — hermes-agent 的 Python 实现在 `reference/` 目录，仅供开发参考
