/**
 * yuanbao-client.ts - 元宝 WebSocket 客户端
 *
 * 连接元宝 Bot 网关，处理认证、心跳、消息收发。
 * 移植自 hermes-agent 的 ConnectionManager + YuanbaoAdapter。
 */

import WebSocket from "ws";
import { randomUUID } from "node:crypto";
import {
  CMD_TYPE,
  CMD,
  INSTANCE_ID,
  WS_HEARTBEAT_RUNNING,
  WS_HEARTBEAT_FINISH,
  encodeAuthBind,
  encodePing,
  encodePushAck,
  decodeConnMsg,
  decodeAuthBindRsp,
  decodeInboundPush,
  encodeSendC2CMessage,
  encodeSendGroupMessage,
  encodeSendPrivateHeartbeat,
  encodeSendGroupHeartbeat,
  type ConnHead,
  type InboundPush,
  type MsgBodyElement,
} from "./yuanbao-proto.js";
import { getSignToken, forceRefreshToken, clearTokenCache } from "./yuanbao-sign.js";

// ============================================================
// 常量
// ============================================================

const DEFAULT_WS_URL = "wss://bot-wss.yuanbao.tencent.com/wss/connection";
const DEFAULT_API_DOMAIN = "https://bot.yuanbao.tencent.com";

const HEARTBEAT_INTERVAL_MS = 30_000;
const CONNECT_TIMEOUT_MS = 15_000;
const AUTH_TIMEOUT_MS = 10_000;
const MAX_RECONNECT_ATTEMPTS = 100;
const HEARTBEAT_TIMEOUT_THRESHOLD = 2;
const REPLY_HEARTBEAT_INTERVAL_MS = 2000;
const REPLY_HEARTBEAT_TIMEOUT_MS = 30_000;

/** 不重连的关闭码 */
const NO_RECONNECT_CODES = new Set([4012, 4013, 4014, 4018, 4019, 4021]);

/** 认证失败码（需要重新签名） */
const AUTH_FAILED_CODES = new Set([4001, 4002, 4003]);

/** 认证可重试码 */
const AUTH_RETRYABLE_CODES = new Set([4010, 4011, 4099]);

// ============================================================
// 类型
// ============================================================

export type ClientStatus = "disconnected" | "connecting" | "authenticating" | "connected" | "error";

export interface YuanbaoClientConfig {
  appKey: string;
  appSecret: string;
  botId?: string;
  wsUrl?: string;
  apiDomain?: string;
  routeEnv?: string;
}

/** 统一的入站消息解码结果（兼容 JSON 和 Protobuf） */
interface MsgBodyLike {
  msgType: string;
  msgContent: Record<string, any>;
}

interface InboundPushLike {
  callbackCommand: string;
  fromAccount: string;
  toAccount: string;
  senderNickname: string;
  groupCode: string;
  groupName: string;
  msgId: string;
  msgBody: MsgBodyLike[];
  msgTime?: number;
  senderNickname2?: string;
}

/** 从元宝收到的文本消息 */
export interface IncomingMessage {
  msgId: string;
  fromAccount: string;
  senderNickname: string;
  chatId: string;       // "dm:xxx" 或 "group:xxx"
  chatType: "dm" | "group";
  groupCode?: string;
  groupName?: string;
  text: string;
  /** 引用回复的消息 ID */
  replyToMsgId?: string;
  timestamp: number;
}

/** 消息回调 */
export type MessageHandler = (msg: IncomingMessage) => void;
export type StatusHandler = (status: ClientStatus) => void;

// ============================================================
// YuanbaoClient
// ============================================================

export class YuanbaoClient {
  private config: Required<YuanbaoClientConfig>;
  private ws: WebSocket | null = null;
  private status: ClientStatus = "disconnected";
  private botId: string;
  private tokenSource: string = "bot";  // 从 sign-token API 获取，用于 AUTH_BIND
  private connectId: string | null = null;

  // 心跳
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private consecutiveHbTimeouts = 0;

  // 重连
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private shouldReconnect = true;

  // Reply 心跳（正在处理消息时发送）
  private replyHeartbeatTimers: Map<string, ReturnType<typeof setInterval>> = new Map();

  // 消息去重
  private dedupSet: Set<string> = new Set();
  private dedupCleanupTimer: ReturnType<typeof setInterval> | null = null;

  // 回调
  private messageHandlers: MessageHandler[] = [];
  private statusHandlers: StatusHandler[] = [];

  // 待确认的请求
  private pendingAcks: Map<string, { resolve: (data: any) => void; reject: (err: Error) => void }> = new Map();

  constructor(config: YuanbaoClientConfig) {
    this.config = {
      appKey: config.appKey,
      appSecret: config.appSecret,
      botId: config.botId || "",
      wsUrl: config.wsUrl || DEFAULT_WS_URL,
      apiDomain: config.apiDomain || DEFAULT_API_DOMAIN,
      routeEnv: config.routeEnv || "",
    };
    this.botId = this.config.botId;
  }

  // ─── 事件注册 ───

  onMessage(handler: MessageHandler): void {
    this.messageHandlers.push(handler);
  }

  onStatusChange(handler: StatusHandler): void {
    this.statusHandlers.push(handler);
  }

  // ─── 生命周期 ───

  /** 连接到元宝网关 */
  async connect(): Promise<boolean> {
    if (this.status === "connected" || this.status === "connecting") {
      console.debug("[YuanbaoClient] Already connected/connecting");
      return true;
    }

    if (!this.config.appKey || !this.config.appSecret) {
      console.error("[YuanbaoClient] appKey and appSecret are required");
      this.setStatus("error");
      return false;
    }

    this.shouldReconnect = true;
    this.setStatus("connecting");

    try {
      // Step 1: 获取 sign token
      const tokenData = await getSignToken(
        this.config.appKey,
        this.config.appSecret,
        this.config.apiDomain,
        this.config.routeEnv,
      );

      if (tokenData.botId) {
        this.botId = tokenData.botId;
      }

      // 保存 source，AUTH_BIND 必须使用与 token 一致的 source
      if (tokenData.source) {
        this.tokenSource = tokenData.source;
      }
      console.debug(`[YuanbaoClient] Token source: ${this.tokenSource}, bot_id: ${this.botId}`);

      // Step 2: 建立 WebSocket 连接
      console.debug(`[YuanbaoClient] Connecting to ${this.config.wsUrl}...`);
      this.setStatus("authenticating");

      this.ws = new WebSocket(this.config.wsUrl);
      this.ws.binaryType = "arraybuffer";

      return new Promise<boolean>((resolve) => {
        const connectTimeout = setTimeout(() => {
          console.error("[YuanbaoClient] Connection timed out");
          this.cleanup();
          this.setStatus("error");
          resolve(false);
        }, CONNECT_TIMEOUT_MS);

        this.ws!.onopen = async () => {
          console.debug("[YuanbaoClient] WebSocket connected, sending AUTH_BIND...");
          try {
            const authed = await this.authenticate(tokenData.token);
            clearTimeout(connectTimeout);
            if (authed) {
              this.reconnectAttempts = 0;
              this.setStatus("connected");
              this.startHeartbeat();
              this.startDedupCleanup();
              resolve(true);
            } else {
              this.cleanup();
              this.setStatus("error");
              resolve(false);
            }
          } catch (err) {
            clearTimeout(connectTimeout);
            console.error("[YuanbaoClient] Auth failed:", err);
            this.cleanup();
            this.setStatus("error");
            resolve(false);
          }
        };

        this.ws!.onmessage = (event) => {
          const data = event.data;
          if (data instanceof ArrayBuffer) {
            this.handleFrame(new Uint8Array(data));
          } else if (Buffer.isBuffer(data)) {
            this.handleFrame(new Uint8Array(data));
          }
        };

        this.ws!.onclose = (event) => {
          console.warn(`[YuanbaoClient] WebSocket closed: code=${event.code} reason=${event.reason}`);
          this.stopHeartbeat();
          if (this.shouldReconnect && !NO_RECONNECT_CODES.has(event.code)) {
            this.scheduleReconnect();
          } else {
            this.setStatus("disconnected");
          }
        };

        this.ws!.onerror = (err) => {
          console.error("[YuanbaoClient] WebSocket error:", err);
        };
      });
    } catch (err) {
      console.error("[YuanbaoClient] Connection failed:", err);
      this.setStatus("error");
      return false;
    }
  }

  /** 断开连接 */
  disconnect(): void {
    this.shouldReconnect = false;
    this.stopHeartbeat();
    this.stopDedupCleanup();
    this.clearReconnectTimer();
    this.clearAllReplyHeartbeats();
    this.failAllPendingAcks("Client disconnected");
    clearTokenCache();
    this.cleanup();
    this.setStatus("disconnected");
  }

  /** 获取当前状态 */
  getStatus(): ClientStatus {
    return this.status;
  }

  /** 获取 bot ID */
  getBotId(): string {
    return this.botId;
  }

  // ─── 发送消息 ───

  /** 发送文本消息（自动判断私聊/群聊） */
  sendMessage(chatId: string, text: string, replyToMsgId?: string): void {
    const msgBody: MsgBodyElement[] = [
      { msgType: "TIMTextElem", msgContent: { text } },
    ];

    let frame: Uint8Array;
    if (chatId.startsWith("group:")) {
      const groupCode = chatId.replace("group:", "");
      frame = encodeSendGroupMessage(
        groupCode,
        msgBody,
        this.botId,
        "",
        replyToMsgId || "",
      );
    } else {
      const toAccount = chatId.replace("dm:", "");
      frame = encodeSendC2CMessage(
        toAccount,
        msgBody,
        this.botId,
      );
    }

    this.sendFrame(frame);
  }

  /** 开始发送 reply 心跳（处理中） */
  startReplyHeartbeat(chatId: string): void {
    // 避免重复启动
    if (this.replyHeartbeatTimers.has(chatId)) return;

    const sendHeartbeat = () => {
      try {
        let frame: Uint8Array;
        if (chatId.startsWith("group:")) {
          const groupCode = chatId.replace("group:", "");
          frame = encodeSendGroupHeartbeat(this.botId, groupCode, WS_HEARTBEAT_RUNNING);
        } else {
          const toAccount = chatId.replace("dm:", "");
          frame = encodeSendPrivateHeartbeat(this.botId, toAccount, WS_HEARTBEAT_RUNNING);
        }
        this.sendFrame(frame);
      } catch (err) {
        console.error("[YuanbaoClient] Reply heartbeat error:", err);
      }
    };

    // 立即发送一次
    sendHeartbeat();

    // 定时发送
    const timer = setInterval(sendHeartbeat, REPLY_HEARTBEAT_INTERVAL_MS);
    this.replyHeartbeatTimers.set(chatId, timer);

    // 超时自动停止
    setTimeout(() => {
      this.stopReplyHeartbeat(chatId, true);
    }, REPLY_HEARTBEAT_TIMEOUT_MS);
  }

  /** 停止 reply 心跳 */
  stopReplyHeartbeat(chatId: string, sendFinish = true): void {
    const timer = this.replyHeartbeatTimers.get(chatId);
    if (timer) {
      clearInterval(timer);
      this.replyHeartbeatTimers.delete(chatId);
    }

    if (sendFinish) {
      try {
        let frame: Uint8Array;
        if (chatId.startsWith("group:")) {
          const groupCode = chatId.replace("group:", "");
          frame = encodeSendGroupHeartbeat(this.botId, groupCode, WS_HEARTBEAT_FINISH);
        } else {
          const toAccount = chatId.replace("dm:", "");
          frame = encodeSendPrivateHeartbeat(this.botId, toAccount, WS_HEARTBEAT_FINISH);
        }
        this.sendFrame(frame);
      } catch (err) {
        console.error("[YuanbaoClient] Reply heartbeat finish error:", err);
      }
    }
  }

  // ─── 内部方法 ───

  /** 发送 AUTH_BIND 并等待 BIND_ACK */
  private async authenticate(token: string): Promise<boolean> {
    if (!this.ws) return false;

    const msgId = randomUUID();
    const authBytes = encodeAuthBind(
      "ybBot",
      this.botId,
      this.tokenSource,  // 使用 sign-token API 返回的 source
      token,
      msgId,
      "",
      process.platform,
      "",
      this.config.routeEnv,
    );

    this.ws.send(authBytes);
    console.debug(`[YuanbaoClient] AUTH_BIND sent (msg_id=${msgId} uid=${this.botId})`);

    return new Promise<boolean>((resolve) => {
      const timeout = setTimeout(() => {
        console.error("[YuanbaoClient] AUTH_BIND timeout");
        resolve(false);
      }, AUTH_TIMEOUT_MS);

      const onAuthMessage = (event: WebSocket.MessageEvent) => {
        const data = event.data;
        let bytes: Uint8Array;
        if (data instanceof ArrayBuffer) {
          bytes = new Uint8Array(data);
        } else if (Buffer.isBuffer(data)) {
          bytes = new Uint8Array(data);
        } else {
          return;
        }

        try {
          const msg = decodeConnMsg(bytes);
          const head = msg.head;

          if (head.cmdType === CMD_TYPE.Response && head.cmd === CMD.AuthBind) {
            clearTimeout(timeout);
            this.ws?.removeEventListener("message", onAuthMessage);

            const rsp = decodeAuthBindRsp(msg.data);
            if (rsp.code !== 0) {
              console.error(`[YuanbaoClient] AuthBindRsp error: code=${rsp.code} message=${rsp.message}`);

              // 认证失败，需要重新签名
              if (AUTH_FAILED_CODES.has(rsp.code)) {
                forceRefreshToken(
                  this.config.appKey,
                  this.config.appSecret,
                  this.config.apiDomain,
                  this.config.routeEnv,
                ).catch(() => {});
              }

              resolve(false);
              return;
            }

            this.connectId = rsp.connectId;
            console.debug(`[YuanbaoClient] BIND_ACK received: connectId=${rsp.connectId}`);
            resolve(true);
          }
        } catch {
          // 忽略解析错误
        }
      };

      this.ws!.addEventListener("message", onAuthMessage);
    });
  }

  /** 处理收到的 WebSocket 帧 */
  private handleFrame(raw: Uint8Array): void {
    let msg;
    try {
      msg = decodeConnMsg(raw);
    } catch (err) {
      console.debug("[YuanbaoClient] Failed to decode frame:", err);
      return;
    }

    const head = msg.head;
    const cmdType = head.cmdType;
    const cmd = head.cmd;
    const msgId = head.msgId;
    const data = msg.data;

    // 心跳响应
    if (cmdType === CMD_TYPE.Response && cmd === CMD.Ping) {
      // PONG 静默处理，不打印日志
      this.resolvePendingAck(msgId, null);
      this.consecutiveHbTimeouts = 0;
      return;
    }

    // 心跳 ACK 和消息发送 ACK（静默丢弃）
    if (cmdType === CMD_TYPE.Response && (
      cmd === "send_group_heartbeat" ||
      cmd === "send_private_heartbeat" ||
      cmd === "send_c2c_message" ||
      cmd === "send_group_message"
    )) {
      return;
    }

    // 业务响应
    if (cmdType === CMD_TYPE.Response) {
      const pending = this.pendingAcks.get(msgId);
      if (pending) {
        this.pendingAcks.delete(msgId);
        pending.resolve({ head, data });
      } else if (cmd !== CMD.AuthBind) {
        // auth-bind 响应由 authenticate() 的独立监听器处理，这里静默跳过
        console.debug(`[YuanbaoClient] Unmatched Response: cmd=${cmd} msg_id=${msgId}`);
      }
      return;
    }

    // Push 消息
    if (cmdType === CMD_TYPE.Push) {
      // 发送 ACK
      if (head.needAck) {
        const ack = encodePushAck(head);
        this.sendFrame(ack);
      }

      // 处理入站消息
      // 注意：服务器实际发送的 cmd 名是 "inbound_message"，
      // 但我们也兼容 "InboundMessagePush" 以防变化
      if (cmd === "inbound_message" || cmd === "InboundMessagePush") {
        this.handleInboundPush(data);
      } else if (data && data.length > 0) {
        // 其他带 data 的 push 也尝试解码（兼容未来变更）
        console.debug(`[YuanbaoClient] Attempting to decode unknown push: cmd=${cmd}`);
        this.handleInboundPush(data);
      } else {
        console.debug(`[YuanbaoClient] Unhandled push: cmd=${cmd}`);
      }
      return;
    }

    // Kickout
    if (cmd === CMD.Kickout) {
      console.warn("[YuanbaoClient] Kicked out by server");
      this.shouldReconnect = false;
      this.disconnect();
      return;
    }
  }

  /**
   * 解码入站消息推送数据。
   * 先尝试 JSON 格式（服务器可能发送 JSON），失败则回退到 Protobuf。
   * 对齐 Python 参考实现 DecodeMiddleware._decode_single 的逻辑。
   */
  private decodePush(data: Uint8Array): InboundPushLike | null {
    // Step 1: 尝试 JSON 解析
    try {
      const text = new TextDecoder().decode(data);
      const json = JSON.parse(text);
      if (typeof json === "object" && json !== null) {
        const push = this.parseJsonPush(json);
        if (push) {
          return push;
        }
      }
    } catch {
      // 不是 JSON，继续尝试 protobuf
    }

    // Step 2: 尝试 Protobuf 解析
    const push = decodeInboundPush(data);
    if (push) {
      return push;
    }

    return null;
  }

  /**
   * 解析 JSON 格式的入站消息推送。
   * 兼容 snake_case 和 PascalCase（腾讯 IM 回调格式）。
   */
  private parseJsonPush(raw: Record<string, any>): InboundPushLike | null {
    const fromAccount =
      raw["from_account"] || raw["From_Account"] || "";
    const groupCode =
      raw["group_code"] || raw["GroupId"] || raw["group_id"] || "";
    const msgBodyRaw = raw["msg_body"] || raw["MsgBody"] || [];
    const msgBody = this.convertJsonMsgBody(msgBodyRaw);

    // 如果关键字段都缺失，返回 null
    if (!fromAccount && !msgBody.length && !raw["callback_command"]) {
      return null;
    }

    return {
      callbackCommand: raw["callback_command"] || "",
      fromAccount: fromAccount,
      toAccount: raw["to_account"] || raw["To_Account"] || "",
      senderNickname: raw["sender_nickname"] || raw["nick_name"] || "",
      groupCode: groupCode,
      groupName: raw["group_name"] || "",
      msgId: raw["msg_id"] || raw["msg_key"] || raw["MsgKey"] || "",
      msgBody,
      msgTime: raw["msg_time"] || raw["MsgTime"] || 0,
      senderNickname2: raw["sender_nickname"] || "",
    };
  }

  /** 将 JSON msg_body 数组标准化为统一格式 */
  private convertJsonMsgBody(rawBody: any[]): MsgBodyLike[] {
    const result: MsgBodyLike[] = [];
    for (const item of rawBody || []) {
      if (typeof item !== "object" || item === null) continue;
      const msgType = item["msg_type"] || item["MsgType"] || "";
      let msgContent = item["msg_content"] || item["MsgContent"] || {};
      if (typeof msgContent === "string") {
        try { msgContent = JSON.parse(msgContent); } catch { msgContent = { text: msgContent }; }
      }
      result.push({ msgType, msgContent: msgContent || {} });
    }
    return result;
  }

  /** 从 TIMImageElem 的 msgContent 中提取图片 URL */
  private extractImageUrl(c: Record<string, any>): string {
    // 优先从 image_info_array 中取（JSON 格式）
    const infoArray = c.image_info_array || c["ImageInfoArray"];
    if (Array.isArray(infoArray) && infoArray.length > 0) {
      // type=1 是原图，type=2 是大图，type=3 是缩略图
      const original = infoArray.find((info: any) => info.type === 1) || infoArray[0];
      if (original?.url) return original.url;
    }
    // 回退：直接 url 字段（Protobuf 格式）
    if (c.url) return c.url;
    return "";
  }

  /** 处理入站消息推送（支持 JSON 和 Protobuf 两种格式） */
  private handleInboundPush(data: Uint8Array): void {
    const push = this.decodePush(data);
    if (!push) {
      console.warn("[YuanbaoClient] Failed to decode InboundMessagePush (tried JSON and Protobuf)");
      return;
    }

    // 去重
    if (push.msgId && this.dedupSet.has(push.msgId)) {
      console.debug(`[YuanbaoClient] Duplicate message: ${push.msgId}`);
      return;
    }
    if (push.msgId) this.dedupSet.add(push.msgId);

    // 忽略自己发的消息
    if (push.fromAccount === this.botId) {
      return;
    }

    // 仅在 debug 时输出详细信息
    console.debug(
      `[YuanbaoClient] Inbound: from=${push.fromAccount.substring(0, 8)}... group=${push.groupCode} ` +
      `msg_id=${push.msgId.substring(0, 16)}... types=${push.msgBody.map((b) => b.msgType).join(",")}`
    );

    // 提取文本（支持图片、文件、表情等多种消息类型）
    const textParts: string[] = [];
    for (const body of push.msgBody) {
      const c = body.msgContent;
      if (!c) continue;

      switch (body.msgType) {
        case "TIMTextElem":
          if (c.text) textParts.push(c.text);
          break;
        case "TIMImageElem": {
          // 图片 URL 在 image_info_array[0].url 中，或直接在 url 字段
          const url = this.extractImageUrl(c as Record<string, any>);
          textParts.push(url ? `[图片] ${url}` : "[图片]");
          break;
        }
        case "TIMFileElem": {
          // 文件：file_name + url（注意 JSON 格式用 file_name 而非 fileName）
          const fileName = (c as any).file_name || c.fileName || "";
          const fileUrl = c.url || "";
          if (fileName && fileUrl) {
            textParts.push(`[文件: ${fileName}] ${fileUrl}`);
          } else if (fileName) {
            textParts.push(`[文件: ${fileName}]`);
          } else {
            textParts.push("[文件]");
          }
          break;
        }
        case "TIMFaceElem":
          textParts.push("[表情]");
          break;
        case "TIMSoundElem":
          textParts.push("[语音]");
          break;
        case "TIMCustomElem":
          if (c.data) {
            try {
              const data = typeof c.data === "string" ? JSON.parse(c.data) : c.data;
              if (data?.text) textParts.push(data.text);
            } catch {
              if (typeof c.data === "string") textParts.push(c.data);
            }
          }
          break;
        default:
          if (c.desc) textParts.push(c.desc);
          else if (c.text) textParts.push(c.text);
          break;
      }
    }
    const text = textParts.join("").trim();
    if (!text) {
      console.debug("[YuanbaoClient] No extractable content in message");
      return;
    }

    // 构建 IncomingMessage
    const isGroup = !!push.groupCode;
    const chatId = isGroup ? `group:${push.groupCode}` : `dm:${push.fromAccount}`;

    const incomingMsg: IncomingMessage = {
      msgId: push.msgId,
      fromAccount: push.fromAccount,
      senderNickname: push.senderNickname,
      chatId,
      chatType: isGroup ? "group" : "dm",
      groupCode: push.groupCode || undefined,
      groupName: push.groupName || undefined,
      text,
      timestamp: push.msgTime ? push.msgTime * 1000 : Date.now(),
    };

    // 触发回调
    for (const handler of this.messageHandlers) {
      try {
        handler(incomingMsg);
      } catch (err) {
        console.error("[YuanbaoClient] Message handler error:", err);
      }
    }
  }

  // ─── 心跳 ───

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.consecutiveHbTimeouts = 0;

    this.heartbeatTimer = setInterval(async () => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

      try {
        const msgId = randomUUID();
        const pingBytes = encodePing(msgId);
        this.ws.send(pingBytes);
        // PING 不打印日志，避免刷屏

        // 等待 PONG（通过 pendingAcks）
        const pongPromise = new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => {
            this.pendingAcks.delete(msgId);
            reject(new Error("PONG timeout"));
          }, 10_000);
          this.pendingAcks.set(msgId, {
            resolve: () => { clearTimeout(timeout); resolve(); },
            reject: (err) => { clearTimeout(timeout); reject(err); },
          });
        });

        await pongPromise;
        this.consecutiveHbTimeouts = 0;
      } catch {
        this.consecutiveHbTimeouts++;
        console.warn(
          `[YuanbaoClient] PONG timeout (${this.consecutiveHbTimeouts}/${HEARTBEAT_TIMEOUT_THRESHOLD})`
        );
        if (this.consecutiveHbTimeouts >= HEARTBEAT_TIMEOUT_THRESHOLD) {
          console.warn("[YuanbaoClient] Heartbeat threshold exceeded, reconnecting...");
          this.scheduleReconnect();
        }
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  // ─── Reply 心跳管理 ───

  private clearAllReplyHeartbeats(): void {
    for (const [chatId, timer] of this.replyHeartbeatTimers) {
      clearInterval(timer);
    }
    this.replyHeartbeatTimers.clear();
  }

  // ─── 去重清理 ───

  private startDedupCleanup(): void {
    this.stopDedupCleanup();
    this.dedupCleanupTimer = setInterval(() => {
      this.dedupSet.clear();
    }, 5 * 60 * 1000); // 每 5 分钟清理
  }

  private stopDedupCleanup(): void {
    if (this.dedupCleanupTimer) {
      clearInterval(this.dedupCleanupTimer);
      this.dedupCleanupTimer = null;
    }
  }

  // ─── 重连 ───

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    if (this.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
      console.error("[YuanbaoClient] Max reconnect attempts reached");
      this.setStatus("error");
      return;
    }

    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 60_000);
    this.reconnectAttempts++;
    console.debug(`[YuanbaoClient] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})...`);

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      // 重连时强制刷新 token，避免使用过期的缓存 token
      forceRefreshToken(
        this.config.appKey,
        this.config.appSecret,
        this.config.apiDomain,
        this.config.routeEnv,
      ).catch(() => {});
      this.cleanup();
      await this.connect();
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  // ─── 工具方法 ───

  private sendFrame(data: Uint8Array): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
    } else {
      console.warn("[YuanbaoClient] Cannot send: WebSocket not open");
    }
  }

  private resolvePendingAck(msgId: string, data: any): void {
    const pending = this.pendingAcks.get(msgId);
    if (pending) {
      this.pendingAcks.delete(msgId);
      pending.resolve(data);
    }
  }

  private failAllPendingAcks(reason: string): void {
    for (const [msgId, pending] of this.pendingAcks) {
      pending.reject(new Error(reason));
    }
    this.pendingAcks.clear();
  }

  private cleanup(): void {
    this.stopHeartbeat();
    this.stopDedupCleanup();
    if (this.ws) {
      this.ws.removeAllListeners();
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
    this.connectId = null;
  }

  private setStatus(status: ClientStatus): void {
    if (this.status !== status) {
      this.status = status;
      console.debug(`[YuanbaoClient] Status: ${status}`);
      for (const handler of this.statusHandlers) {
        try {
          handler(status);
        } catch (err) {
          console.error("[YuanbaoClient] Status handler error:", err);
        }
      }
    }
  }
}
