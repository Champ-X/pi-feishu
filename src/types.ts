/**
 * Pi-Yuanbao 类型定义
 *
 * 使用元宝官方 Bot API（WebSocket + Protobuf）
 */

/** 元宝客户端配置 */
export interface YuanbaoConfig {
  /** 腾讯元宝 App Key (也叫 app_id) */
  appKey: string;
  /** 腾讯元宝 App Secret */
  appSecret: string;
  /** Bot ID（可选，sign-token API 会返回） */
  botId?: string;
  /** WebSocket 网关地址 */
  wsUrl?: string;
  /** API 域名 */
  apiDomain?: string;
  /** 路由环境（可选，如 "ci-677"） */
  routeEnv?: string;
}

/** 桥接服务配置 */
export interface BridgeConfig extends YuanbaoConfig {
  /** 是否自动重连 */
  autoReconnect: boolean;
  /** 最大消息队列长度 */
  maxQueueSize: number;
}

/** 桥接服务状态 */
export type BridgeStatus = "disconnected" | "connecting" | "connected" | "error";

/** 从元宝收到的处理后消息 */
export interface YuanbaoInMessage {
  type: "user_message";
  content: string;
  /** 元宝消息 ID */
  msgId: string;
  /** 发送者账号 */
  fromAccount: string;
  /** 发送者昵称 */
  senderNickname: string;
  /** 聊天 ID（dm:xxx 或 group:xxx） */
  chatId: string;
  /** 聊天类型 */
  chatType: "dm" | "group";
  /** 群聊时的群号 */
  groupCode?: string;
  /** 群聊时的群名 */
  groupName?: string;
  /** 时间戳 */
  timestamp: number;
}

/** 发送到元宝的消息 */
export interface YuanbaoOutMessage {
  chatId: string;
  content: string;
  /** 引用回复的消息 ID */
  replyToMsgId?: string;
}
