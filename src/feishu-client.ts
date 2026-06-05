/**
 * feishu-client.ts — 飞书 WebSocket 客户端
 *
 * 基于 @larksuiteoapi/node-sdk 的 Client + WSClient + EventDispatcher 封装。
 * 负责：WebSocket 长连接管理、事件接收、消息发送、消息去重。
 *
 * 参考：openclaw-lark 项目的 lark-client.ts 和 monitor.ts
 */

import * as Lark from "@larksuiteoapi/node-sdk";
import type { FeishuConfig, BridgeStatus } from "./types.js";

// ─── 日志 ─────────────────────────────────────────────

const DEBUG = false;
function _log(...args: unknown[]): void {
  if (DEBUG) console.log("[FeishuClient]", ...args);
}
function _warn(...args: unknown[]): void {
  console.warn("[FeishuClient]", ...args);
}

// ─── 常量 ─────────────────────────────────────────────

/** 消息去重 TTL（12 小时） */
const DEDUP_TTL_MS = 12 * 60 * 60 * 1000;
/** 去重最大条目 */
const DEDUP_MAX_ENTRIES = 5000;
/** 去重定期清理间隔（5 分钟） */
const DEDUP_SWEEP_INTERVAL = 5 * 60 * 1000;
/** 消息过期判定（30 分钟，用于丢弃 WS 重连后的积压消息） */
const MESSAGE_EXPIRY_MS = 30 * 60 * 60 * 1000;

// ─── 飞书事件类型 ───────────────────────────────────────

/** SDK 传入的 im.message.receive_v1 事件数据结构 */
interface FeishuMessageEvent {
  sender: {
    sender_id: {
      open_id?: string;
      user_id?: string;
      union_id?: string;
    };
    sender_type?: string;
    tenant_key?: string;
  };
  message: {
    message_id: string;
    root_id?: string;
    parent_id?: string;
    create_time?: string;
    update_time?: string;
    chat_id: string;
    thread_id?: string;
    chat_type: "p2p" | "group";
    message_type: string;
    content: string; // JSON 字符串
    mentions?: Array<{
      key: string;
      id: { open_id?: string; user_id?: string; union_id?: string };
      name: string;
      tenant_key?: string;
    }>;
    user_agent?: string;
  };
}

// ─── FeishuClient 类 ───────────────────────────────────

export class FeishuClient {
  private client: Lark.Client;
  private wsClient: Lark.WSClient | null = null;
  private abortController: AbortController | null = null;
  private status: BridgeStatus = "disconnected";

  // 消息去重
  private dedupMap: Map<string, number> = new Map();
  private dedupSweepTimer: ReturnType<typeof setInterval> | null = null;

  // Bot 身份（连接后探测）
  private botOpenId: string = "";

  // 回调
  private onMessageCallback:
    | ((chatId: string, msgId: string, text: string, chatType: "p2p" | "group") => void)
    | null = null;
  private onStatusChangeCallback: ((status: BridgeStatus) => void) | null = null;

  constructor(private config: FeishuConfig) {
    const domain = config.domain === "lark" ? Lark.Domain.Lark : Lark.Domain.Feishu;

    this.client = new Lark.Client({
      appId: config.appId,
      appSecret: config.appSecret,
      appType: Lark.AppType.SelfBuild,
      domain,
    });
  }

  // ─── 公开 API ───────────────────────────────────────

  /** 连接飞书 WebSocket 长连接 */
  async connect(): Promise<void> {
    if (this.status === "connected" || this.status === "connecting") {
      _log("Already connected or connecting, skip");
      return;
    }

    this.setStatus("connecting");
    _log("Connecting to Feishu WebSocket...");

    try {
      // 创建 AbortController
      this.abortController = new AbortController();

      // 创建事件分发器
      const dispatcher = new Lark.EventDispatcher({
        encryptKey: this.config.encryptKey ?? "",
        verificationToken: this.config.verificationToken ?? "",
      });

      // 注册事件处理器
      dispatcher.register({
        "im.message.receive_v1": (data: any) => {
          this.handleInboundMessage(data);
        },
        "im.message.message_read_v1": async () => {},
        "im.chat.member.bot.added_v1": async () => {},
        "im.chat.member.bot.deleted_v1": async () => {},
        "im.chat.access_event.bot_p2p_chat_entered_v1": async () => {},
      });

      // 关闭旧的 WSClient
      if (this.wsClient) {
        try {
          (this.wsClient as any).close({ force: true });
        } catch {
          // ignore
        }
      }

      // 创建 WSClient
      const domain = this.config.domain === "lark" ? Lark.Domain.Lark : Lark.Domain.Feishu;
      this.wsClient = new Lark.WSClient({
        appId: this.config.appId,
        appSecret: this.config.appSecret,
        domain,
        loggerLevel: Lark.LoggerLevel.info,
      });

      // Monkey-patch card events
      this.patchCardEvents();

      // 启动去重清理
      this.startDedupSweep();

      // 启动 WebSocket（返回 Promise，AbortSignal 触发时 resolve）
      await this.wsClient.start({
        eventDispatcher: dispatcher,
      });

      this.setStatus("connected");
      _log("Feishu WebSocket connected");
    } catch (err) {
      _warn("Connect failed:", err);
      this.setStatus("error");
      throw err;
    }
  }

  /** 断开连接 */
  disconnect(): void {
    _log("Disconnecting...");

    // Abort WSClient
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }

    // Close WSClient
    if (this.wsClient) {
      try {
        (this.wsClient as any).close({ force: true });
      } catch {
        // ignore
      }
      this.wsClient = null;
    }

    // 停止去重清理
    if (this.dedupSweepTimer) {
      clearInterval(this.dedupSweepTimer);
      this.dedupSweepTimer = null;
    }

    this.setStatus("disconnected");
  }

  /** 获取当前状态 */
  getStatus(): BridgeStatus {
    return this.status;
  }

  /** 发送消息到飞书（回复模式优先） */
  async sendMessage(chatId: string, text: string, replyToMsgId?: string): Promise<void> {
    const content = this.buildPostContent(text);

    try {
      if (replyToMsgId) {
        // 回复消息
        await this.client.im.message.reply({
          path: { message_id: replyToMsgId },
          data: {
            content,
            msg_type: "post",
          },
        });
      } else {
        // 创建新消息
        await this.client.im.message.create({
          params: { receive_id_type: "chat_id" },
          data: {
            receive_id: chatId,
            content,
            msg_type: "post",
          },
        });
      }
      _log(`Message sent to ${chatId}${replyToMsgId ? ` (reply to ${replyToMsgId})` : ""}`);
    } catch (err: any) {
      // 回复失败时降级为创建新消息（消息可能被撤回）
      if (replyToMsgId && err?.code === 230011) {
        _warn("Reply failed (message withdrawn), falling back to create");
        await this.client.im.message.create({
          params: { receive_id_type: "chat_id" },
          data: {
            receive_id: chatId,
            content,
            msg_type: "post",
          },
        });
      } else {
        throw err;
      }
    }
  }

  /** 注册消息回调 */
  setOnMessage(cb: (chatId: string, msgId: string, text: string, chatType: "p2p" | "group") => void): void {
    this.onMessageCallback = cb;
  }

  /** 注册状态变更回调 */
  setOnStatusChange(cb: (status: BridgeStatus) => void): void {
    this.onStatusChangeCallback = cb;
  }

  // ─── 内部方法 ───────────────────────────────────────

  /** 更新状态并通知 */
  private setStatus(status: BridgeStatus): void {
    this.status = status;
    this.onStatusChangeCallback?.(status);
  }

  /** 处理入站消息事件 */
  private handleInboundMessage(data: FeishuMessageEvent): void {
    try {
      const msg = data.message;
      const sender = data.sender;

      // 消息过期检查
      if (msg.create_time && this.isMessageExpired(msg.create_time)) {
        _log(`Skipping expired message ${msg.message_id}`);
        return;
      }

      // 去重检查
      if (!this.tryRecordDedup(msg.message_id)) {
        _log(`Skipping duplicate message ${msg.message_id}`);
        return;
      }

      // 过滤 bot 发送的消息
      const senderType = sender.sender_type;
      if (senderType === "bot" || senderType === "app") {
        _log(`Skipping bot/app message from ${sender.sender_id.open_id}`);
        return;
      }

      // 提取 chat 信息
      const chatId = msg.chat_id;
      const chatType = msg.chat_type;
      const messageId = msg.message_id;

      // 解析消息内容
      const text = this.parseContent(msg.content, msg.message_type, msg.mentions);
      if (!text || text.trim().length === 0) {
        _log(`Skipping empty message ${messageId}`);
        return;
      }

      _log(`Inbound: chatId=${chatId}, type=${chatType}, msgId=${messageId}, text=${text.substring(0, 50)}...`);

      // 调用回调
      this.onMessageCallback?.(chatId, messageId, text, chatType);
    } catch (err) {
      _warn("Error handling inbound message:", err);
    }
  }

  /**
   * 解析飞书消息内容为纯文本
   *
   * 飞书 content 是 JSON 字符串，结构因 message_type 不同而异：
   * - text: {"text": "消息内容 @_user_1"}
   * - post: {"zh_cn": {"title": "...", "content": [[{tag, text}, ...]]}}
   * - image: {"image_key": "img_xxx"}
   * - file: {"file_key": "xxx", "file_name": "report.pdf"}
   */
  private parseContent(
    rawContent: string,
    messageType: string,
    mentions?: FeishuMessageEvent["message"]["mentions"],
  ): string {
    let parsed: any;
    try {
      parsed = JSON.parse(rawContent);
    } catch {
      return rawContent;
    }

    let text: string;

    switch (messageType) {
      case "text":
        text = parsed?.text ?? "";
        break;

      case "post": {
        // post 类型：遍历所有 locale 和 content rows 提取文本
        const parts: string[] = [];
        const locale = parsed?.zh_cn ?? parsed?.en_us ?? parsed?.ja_jp;
        if (locale?.title) parts.push(locale.title);
        if (Array.isArray(locale?.content)) {
          for (const row of locale.content) {
            if (Array.isArray(row)) {
              for (const elem of row) {
                if (elem?.tag === "text" && elem.text) parts.push(elem.text);
                else if (elem?.tag === "a" && elem.text) parts.push(elem.text);
                else if (elem?.tag === "md" && elem.text) parts.push(elem.text);
              }
            }
          }
        }
        text = parts.join("");
        break;
      }

      case "image":
        text = `[图片: ${parsed?.image_key ?? "unknown"}]`;
        break;

      case "file":
        text = `[文件: ${parsed?.file_name ?? parsed?.file_key ?? "unknown"}]`;
        break;

      case "audio":
        text = `[语音消息]`;
        break;

      case "video":
        text = `[视频]`;
        break;

      case "sticker":
        text = `[表情]`;
        break;

      case "interactive":
        text = `[卡片消息]`;
        break;

      case "share_chat":
        text = `[群分享]`;
        break;

      case "merge_forward":
        text = `[合并转发消息]`;
        break;

      default:
        text = typeof parsed === "string" ? parsed : JSON.stringify(parsed);
    }

    // 移除 @Bot mention 占位符
    if (mentions && text) {
      text = this.stripMentionPlaceholders(text, mentions);
    }

    return text.trim();
  }

  /** 移除消息中的 mention 占位符（如 @_user_1） */
  private stripMentionPlaceholders(
    text: string,
    mentions: FeishuMessageEvent["message"]["mentions"],
  ): string {
    let result = text;
    for (const m of mentions ?? []) {
      // 只移除 @Bot 的 mention
      if (this.botOpenId && m.id.open_id === this.botOpenId) {
        result = result.replace(new RegExp(escapeRegExp(m.key) + "\\s*", "g"), "");
      }
    }
    return result;
  }

  /** 构建 post 格式的消息内容（支持 markdown） */
  private buildPostContent(text: string): string {
    return JSON.stringify({
      zh_cn: {
        content: [[{ tag: "md", text }]],
      },
    });
  }

  /** Monkey-patch WSClient 以支持 card.action.trigger 事件路由 */
  private patchCardEvents(): void {
    if (!this.wsClient) return;
    const wsClientAny = this.wsClient as any;
    const origHandleEventData = wsClientAny.handleEventData?.bind(wsClientAny);
    if (!origHandleEventData) return;

    wsClientAny.handleEventData = (data: any) => {
      const msgType = data?.headers?.find?.((h: any) => h?.key === "type")?.value;
      if (msgType === "card") {
        const patchedData = {
          ...data,
          headers: data.headers.map((h: any) =>
            h.key === "type" ? { ...h, value: "event" } : h,
          ),
        };
        return origHandleEventData(patchedData);
      }
      return origHandleEventData(data);
    };
  }

  // ─── 去重 ───────────────────────────────────────────

  /** 记录消息 ID，返回 true 表示新消息，false 表示重复 */
  private tryRecordDedup(msgId: string): boolean {
    const now = Date.now();
    const existing = this.dedupMap.get(msgId);
    if (existing !== undefined) {
      if (now - existing < DEDUP_TTL_MS) {
        return false; // 重复
      }
      // 过期重复，重新记录
      this.dedupMap.delete(msgId);
    }

    // 容量限制
    if (this.dedupMap.size >= DEDUP_MAX_ENTRIES) {
      const firstKey = this.dedupMap.keys().next().value;
      if (firstKey !== undefined) this.dedupMap.delete(firstKey);
    }

    this.dedupMap.set(msgId, now);
    return true;
  }

  /** 定期清理过期去重条目 */
  private startDedupSweep(): void {
    if (this.dedupSweepTimer) clearInterval(this.dedupSweepTimer);
    this.dedupSweepTimer = setInterval(() => {
      const now = Date.now();
      for (const [key, ts] of this.dedupMap) {
        if (now - ts >= DEDUP_TTL_MS) {
          this.dedupMap.delete(key);
        } else {
          break; // Map 按插入顺序，后续条目更新
        }
      }
    }, DEDUP_SWEEP_INTERVAL);
    // 不阻止进程退出
    if (this.dedupSweepTimer.unref) this.dedupSweepTimer.unref();
  }

  /** 检查消息是否过期 */
  private isMessageExpired(createTimeStr: string): boolean {
    const createTime = parseInt(createTimeStr, 10);
    if (isNaN(createTime)) return false;
    return Date.now() - createTime > MESSAGE_EXPIRY_MS;
  }
}

// ─── 工具函数 ─────────────────────────────────────────

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
