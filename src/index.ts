/**
 * Pi-Yuanbao 扩展主入口
 *
 * 使用元宝官方 Bot API 将腾讯元宝作为聊天渠道控制 Pi。
 *
 * 功能：
 * 1. 通过元宝官方 WebSocket Bot API 连接元宝
 * 2. 接收元宝消息 → 转发为 Pi 用户消息
 * 3. 监听 Pi 响应 → 回传给元宝
 * 4. 注册 /yuanbao 命令管理连接状态
 *
 * 配置（通过环境变量或 CLI 标志）：
 *   YUANBAO_APP_ID      - 元宝 App Key
 *   YUANBAO_APP_SECRET   - 元宝 App Secret
 *   YUANBAO_BOT_ID       - Bot ID（可选）
 *   YUANBAO_WS_URL       - WebSocket 地址（可选）
 *   YUANBAO_API_DOMAIN   - API 域名（可选）
 *   YUANBAO_ROUTE_ENV    - 路由环境（可选）
 */

import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionCommandContext,
  AgentEndEvent,
} from "@earendil-works/pi-coding-agent";
import type { Static } from "typebox";
import { YuanbaoClient } from "./yuanbao-client.js";
import type { IncomingMessage } from "./yuanbao-client.js";
import type { YuanbaoConfig } from "./types.js";

// ─── 常量 ───

/** 元宝单条消息最大字符数 */
const MAX_TEXT_CHUNK = 4000;

// ─── 默认配置 ───

function loadConfig(): YuanbaoConfig {
  return {
    appKey: process.env.YUANBAO_APP_ID || "",
    appSecret: process.env.YUANBAO_APP_SECRET || "",
    botId: process.env.YUANBAO_BOT_ID || "",
    wsUrl: process.env.YUANBAO_WS_URL || "",
    apiDomain: process.env.YUANBAO_API_DOMAIN || "",
    routeEnv: process.env.YUANBAO_ROUTE_ENV || "",
  };
}

// ─── 扩展入口 ───

export default function (pi: ExtensionAPI) {
  let client: YuanbaoClient | null = null;
  let config: YuanbaoConfig = loadConfig();
  let ctxRef: ExtensionContext | null = null;

  /**
   * 消息队列：chatId → 等待中的 Pi turn 信息。
   * 支持多个用户同时与 Bot 对话，每个 chatId 有独立的回复目标。
   */
  interface PendingTurn {
    chatId: string;
    msgId: string;
  }
  const pendingTurns: Map<string, PendingTurn> = new Map();

  // ─── 注册 CLI 标志 ───

  pi.registerFlag("yuanbao-app-id", {
    description: "元宝 App Key (也叫 app_id)",
    type: "string",
    default: "",
  });

  pi.registerFlag("yuanbao-app-secret", {
    description: "元宝 App Secret",
    type: "string",
    default: "",
  });

  pi.registerFlag("yuanbao-bot-id", {
    description: "元宝 Bot ID（可选，sign-token API 会返回）",
    type: "string",
    default: "",
  });

  pi.registerFlag("yuanbao-ws-url", {
    description: "元宝 WebSocket 网关地址",
    type: "string",
    default: "",
  });

  pi.registerFlag("yuanbao-api-domain", {
    description: "元宝 API 域名",
    type: "string",
    default: "",
  });

  pi.registerFlag("yuanbao-route-env", {
    description: "路由环境（如 ci-677）",
    type: "string",
    default: "",
  });

  // ─── 启动元宝客户端 ───

  async function startYuanbaoClient(): Promise<void> {
    if (client) {
      client.disconnect();
      client = null;
    }

    // 从 CLI 标志读取配置（覆盖环境变量）
    const flagOverrides: Record<string, string> = {};
    for (const [key, flag] of [
      ["appKey", "yuanbao-app-id"],
      ["appSecret", "yuanbao-app-secret"],
      ["botId", "yuanbao-bot-id"],
      ["wsUrl", "yuanbao-ws-url"],
      ["apiDomain", "yuanbao-api-domain"],
      ["routeEnv", "yuanbao-route-env"],
    ] as const) {
      const val = pi.getFlag(flag);
      if (val) flagOverrides[key] = String(val);
    }

    // 合并配置
    config = {
      ...config,
      ...Object.fromEntries(
        Object.entries(flagOverrides).filter(([, v]) => v)
      ),
    };

    // 检查必要配置
    if (!config.appKey || !config.appSecret) {
      if (ctxRef?.hasUI) {
        ctxRef.ui.notify("元宝连接失败：缺少 appKey/appSecret", "error");
      }
      return;
    }

    client = new YuanbaoClient(config);

    // 注册消息处理：元宝消息 → Pi 用户消息
    client.onMessage((msg) => {
      handleYuanbaoMessage(msg);
    });

    // 注册状态变化处理
    client.onStatusChange((status) => {
      updateStatus(ctxRef, status);
    });

    try {
      const connected = await client.connect();
      if (!connected && ctxRef?.hasUI) {
        ctxRef.ui.notify("元宝 Bot 连接失败", "error");
      }
    } catch (err) {
      if (ctxRef?.hasUI) {
        ctxRef.ui.notify(`元宝连接错误: ${err}`, "error");
      }
    }
  }

  // ─── 处理元宝消息 → 转发给 Pi ───

  function handleYuanbaoMessage(msg: IncomingMessage): void {
    const content = msg.text?.trim();
    if (!content) return;

    flashStatus(`元宝: 📩 ${content.substring(0, 20)}${content.length > 20 ? "..." : ""}`);

    // 记录该 chatId 对应的消息，用于后续回复
    pendingTurns.set(msg.chatId, {
      chatId: msg.chatId,
      msgId: msg.msgId,
    });

    // 开始发送 reply heartbeat（告诉元宝正在处理中）
    client?.startReplyHeartbeat(msg.chatId);

    // 发送用户消息给 Pi
    pi.sendUserMessage(content);

    // 更新状态
    updateStatus(ctxRef, client?.getStatus() ?? "disconnected");
  }

  // ─── 监听 Pi 响应 → 回传给元宝 ───

  // Agent 循环结束（一次用户消息可能触发多轮 turn：思考→工具→再思考→最终回复）
  // 必须在 agent_end 时发送，否则只会发送第一轮的部分思考内容
  pi.on("agent_end", (event: AgentEndEvent) => {
    if (!client || client.getStatus() !== "connected") return;

    const chatId = findActiveChatId();
    if (!chatId) return;

    const pending = pendingTurns.get(chatId);
    const replyToMsgId = pending?.msgId;

    // 从所有消息中提取最后一条 assistant 消息的文本
    const lastAssistantText = extractLastAssistantText(event.messages);
    if (!lastAssistantText) {
      // 没有文本内容，停止 heartbeat 并清理
      client.stopReplyHeartbeat(chatId);
      pendingTurns.delete(chatId);
      return;
    }

    // 停止 reply heartbeat
    client.stopReplyHeartbeat(chatId);

    // 清除该 chat 的 pending turn
    pendingTurns.delete(chatId);

    // 分块发送（元宝单条消息限制 4000 字符）
    const chunks = chunkText(lastAssistantText, MAX_TEXT_CHUNK);
    for (const chunk of chunks) {
      client.sendMessage(chatId, chunk, replyToMsgId || undefined);
    }
    flashStatus(`元宝: ✅ 已回复 (${lastAssistantText.length}字)`);
  });

  /**
   * 找到当前应该回复的 chatId。
   * 策略：返回 pendingTurns 中最后一个（最近收到的消息来源）。
   */
  function findActiveChatId(): string | null {
    // pendingTurns 是按插入顺序的，最后一个是最新的
    let lastKey: string | null = null;
    for (const key of pendingTurns.keys()) {
      lastKey = key;
    }
    return lastKey;
  }

  // ─── 注册 /yuanbao 命令 ───

  pi.registerCommand("yuanbao", {
    description: "管理元宝 Bot 连接 (start/stop/status/config/help)",
    handler: async (args: string, ctx: ExtensionCommandContext) => {
      const action = args.trim().toLowerCase() || "status";

      switch (action) {
        case "start":
          await startYuanbaoClient();
          ctx.ui.notify("元宝客户端已启动", "info");
          break;

        case "stop":
          if (client) {
            client.disconnect();
            client = null;
          }
          ctx.ui.notify("元宝客户端已停止", "info");
          break;

        case "status": {
          const status = client?.getStatus() ?? "未启动";
          const botId = client?.getBotId() ?? "无";
          ctx.ui.notify(
            `元宝 Bot 状态: ${status}\n` +
            `Bot ID: ${botId}\n` +
            `App Key: ${config.appKey ? "****" + config.appKey.slice(-4) : "未设置"}\n` +
            `API Domain: ${config.apiDomain || "默认"}`,
            "info"
          );
          break;
        }

        case "config":
          ctx.ui.notify(
            `当前配置:\n` +
            `App Key: ${config.appKey ? "****" + config.appKey.slice(-4) : "未设置"}\n` +
            `App Secret: ${config.appSecret ? "****" : "未设置"}\n` +
            `Bot ID: ${config.botId || "自动获取"}\n` +
            `WS URL: ${config.wsUrl || "默认"}\n` +
            `API Domain: ${config.apiDomain || "默认"}\n` +
            `Route Env: ${config.routeEnv || "无"}`,
            "info"
          );
          break;

        case "help":
          ctx.ui.notify(
            `/yuanbao 命令用法:\n` +
            `  /yuanbao start   - 启动元宝 Bot 连接\n` +
            `  /yuanbao stop    - 断开元宝 Bot 连接\n` +
            `  /yuanbao status  - 查看连接状态\n` +
            `  /yuanbao config  - 查看当前配置\n` +
            `  /yuanbao help    - 显示帮助\n\n` +
            `配置方式:\n` +
            `  环境变量: YUANBAO_APP_ID, YUANBAO_APP_SECRET\n` +
            `  CLI 标志: --yuanbao-app-id, --yuanbao-app-secret`,
            "info"
          );
          break;

        default:
          ctx.ui.notify(`未知命令: ${action}，使用 /yuanbao help 查看帮助`, "warning");
      }
    },
  });

  // ─── 注册自定义工具：发送消息到元宝 ───

  // 工具参数 schema（纯 JSON Schema，兼容 TypeBox）
  const SendToYuanbaoParams = {
    type: "object" as const,
    properties: {
      message: { type: "string" as const, description: "要发送的消息内容" },
      chat_id: { type: "string" as const, description: "目标聊天 ID（如 dm:xxx 或 group:xxx），留空则发送到最近活跃的聊天" },
    },
    required: ["message"],
  };

  pi.registerTool({
    name: "send_to_yuanbao",
    label: "发送到元宝",
    description: "发送消息到元宝聊天界面。当用户要求通过元宝发送消息时使用。",
    parameters: SendToYuanbaoParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendToYuanbaoParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const message = params.message as string;
      const chatId = (params.chat_id as string) || findActiveChatId();

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [{ type: "text" as const, text: "错误: 元宝 Bot 未连接。请先运行 /yuanbao start 启动连接。" }],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [{ type: "text" as const, text: "错误: 没有活跃的元宝聊天。请先在元宝中发送一条消息。" }],
          details: {} as Record<string, unknown>,
        };
      }

      client.sendMessage(chatId, message);
      return {
        content: [{ type: "text" as const, text: `已发送到元宝 [${chatId}]: ${message}` }],
        details: { sent: true, chatId, message } as Record<string, unknown>,
      };
    },
  });

  // ─── 会话启动时自动连接 ───

  pi.on("session_start", async (_event, ctx) => {
    ctxRef = ctx;

    // 设置状态栏
    updateStatus(ctx, "disconnected");

    // 自动连接
    try {
      await startYuanbaoClient();
    } catch (err) {
      if (ctx.hasUI) {
        ctx.ui.notify(`元宝连接失败: ${err}`, "error");
      }
    }
  });

  // ─── 会话关闭时清理 ───

  pi.on("session_shutdown", async () => {
    if (client) {
      client.disconnect();
      client = null;
    }
    pendingTurns.clear();
  });

  // ─── 辅助函数 ───

  /** 从消息中提取文本内容 */
  function extractTextFromMessage(message: any): string | null {
    if (!message?.content) return null;

    const parts: string[] = [];
    for (const block of message.content) {
      if (block.type === "text" && block.text) {
        parts.push(block.text);
      }
    }
    return parts.length > 0 ? parts.join("\n") : null;
  }

  /** 从 agent_end 的所有消息中提取最后一条 assistant 消息的文本 */
  function extractLastAssistantText(messages: any[]): string | null {
    if (!messages || !Array.isArray(messages)) return null;

    // 从后往前找最后一条 assistant 消息
    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i];
      if (msg?.role === "assistant") {
        const text = extractTextFromMessage(msg);
        if (text) return text;
      }
    }
    return null;
  }

  /** 状态栏瞬态消息定时器 */
  let statusTimer: ReturnType<typeof setTimeout> | null = null;

  /** 更新状态栏显示 */
  function updateStatus(ctx: ExtensionContext | null, status: string): void {
    if (!ctx?.hasUI) return;

    const statusMap: Record<string, string> = {
      connecting: "元宝: 连接中",
      authenticating: "元宝: 认证中",
      connected: "元宝: 已连接",
      disconnected: "元宝: 未连接",
      error: "元宝: 错误",
    };

    const text = statusMap[status] ?? `元宝: ${status}`;
    ctx.ui.setStatus("yuanbao", text);
  }

  /**
   * 在状态栏显示瞬态消息（如"收到消息"、"已回复"）。
   * 5 秒后自动恢复为连接状态。
   */
  function flashStatus(message: string): void {
    if (!ctxRef?.hasUI) return;

    if (statusTimer) clearTimeout(statusTimer);
    ctxRef.ui.setStatus("yuanbao", message);

    statusTimer = setTimeout(() => {
      statusTimer = null;
      // 恢复为连接状态
      if (client && client.getStatus() === "connected") {
        ctxRef?.ui.setStatus("yuanbao", "元宝: 已连接");
      }
    }, 5000);
  }

  /**
   * 将长文本分块，避免超过元宝单条消息限制。
   * 优先在换行符处分割，保持段落完整性。
   */
  function chunkText(text: string, maxLen: number): string[] {
    if (text.length <= maxLen) return [text];

    const chunks: string[] = [];
    let remaining = text;

    while (remaining.length > 0) {
      if (remaining.length <= maxLen) {
        chunks.push(remaining);
        break;
      }

      // 尝试在换行符处分割
      let splitPos = remaining.lastIndexOf("\n", maxLen);
      if (splitPos <= 0) {
        // 没有合适的换行符，在空格处分割
        splitPos = remaining.lastIndexOf(" ", maxLen);
      }
      if (splitPos <= 0) {
        // 没有合适的空格，强制分割（不超过 maxLen）
        chunks.push(remaining.substring(0, maxLen));
        remaining = remaining.substring(maxLen);
      } else {
        // 在分割符处分割（包含分割符）
        chunks.push(remaining.substring(0, splitPos + 1));
        remaining = remaining.substring(splitPos + 1);
      }
    }

    return chunks;
  }
}
