/**
 * Pi-Feishu 扩展主入口
 *
 * 使用飞书官方 Bot API（WebSocket 长连接）将飞书作为聊天渠道控制 Pi。
 *
 * 功能：
 * 1. 通过飞书官方 Node.js SDK 连接飞书 WebSocket 长连接
 * 2. 接收飞书消息 → 转发为 Pi 用户消息
 * 3. 监听 Pi 响应 → 回传给飞书（回复/新消息/交互卡片）
 * 4. 媒体收发：下载图片/文件 → 上传到 Pi，Pi 生成的图片/文件 → 上传到飞书
 * 5. Reaction 输入指示：处理中显示 Typing，失败显示 CrossMark
 * 6. 流式卡片：长响应使用交互卡片实时更新
 * 7. 注册 /feishu 命令管理连接状态
 *
 * 配置优先级（从高到低）：
 *   1. CLI 标志: --feishu-app-id, --feishu-app-secret 等
 *   2. 环境变量: FEISHU_APP_ID, FEISHU_APP_SECRET 等
 *   3. Pi settings.json 中的 feishu 字段
 *
 * settings.json 配置示例：
 *   {
 *     "feishu": {
 *       "appId": "cli_xxx",
 *       "appSecret": "xxx",
 *       "domain": "feishu",
 *       "encryptKey": "",
 *       "verificationToken": ""
 *     }
 *   }
 */

import type {
  ExtensionAPI,
  ExtensionContext,
  ExtensionCommandContext,
  TurnEndEvent,
  AgentEndEvent,
} from "@earendil-works/pi-coding-agent";
import type { Static } from "typebox";
import { readFileSync, existsSync, readdirSync, unlinkSync, statSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { FeishuClient } from "./feishu-client.js";
import type { InboundResource } from "./feishu-client.js";
import type { FeishuConfig } from "./types.js";

// ─── 常量 ─────────────────────────────────────────────

/** 飞书 post 消息单条最大字符数（约 4000） */
const MAX_TEXT_CHUNK = 4000;

/** 使用流式卡片的消息长度阈值（超过此值用卡片，否则普通文本） */
const STREAMING_CARD_THRESHOLD = 800;

/** 流式卡片更新间隔（毫秒） */
const CARD_UPDATE_INTERVAL = 2000;

// ─── 从 Pi settings.json 读取 feishu 配置段 ──────────────

/**
 * 从 JSON 文件中读取 feishu 配置段。
 * 文件不存在或解析失败时静默返回空对象。
 */
function readFeishuFromSettingsFile(filePath: string): Record<string, string> {
  try {
    if (!existsSync(filePath)) return {};
    const raw = readFileSync(filePath, "utf-8");
    const json = JSON.parse(raw);
    const fs = json?.feishu;
    if (!fs || typeof fs !== "object") return {};
    return {
      appId: fs.appId ?? fs.app_id ?? "",
      appSecret: fs.appSecret ?? fs.app_secret ?? "",
      domain: fs.domain ?? "",
      encryptKey: fs.encryptKey ?? fs.encrypt_key ?? "",
      verificationToken: fs.verificationToken ?? fs.verification_token ?? "",
    };
  } catch {
    return {};
  }
}

/** 合并配置：项目 settings > 全局 settings > 环境变量 */
function loadConfig(): FeishuConfig {
  const globalSettings = readFeishuFromSettingsFile(
    join(homedir(), ".pi", "agent", "settings.json"),
  );
  const projectSettings = readFeishuFromSettingsFile(
    join(process.cwd(), ".pi", "settings.json"),
  );
  const s: Record<string, string> = { ...globalSettings, ...projectSettings };

  const domain = (process.env.FEISHU_DOMAIN || s.domain || "feishu") as "feishu" | "lark";

  return {
    appId: process.env.FEISHU_APP_ID || s.appId || "",
    appSecret: process.env.FEISHU_APP_SECRET || s.appSecret || "",
    domain,
    encryptKey: process.env.FEISHU_ENCRYPT_KEY || s.encryptKey || undefined,
    verificationToken: process.env.FEISHU_VERIFICATION_TOKEN || s.verificationToken || undefined,
  };
}

// ─── 扩展入口 ───────────────────────────────────────────

export default function (pi: ExtensionAPI) {
  let client: FeishuClient | null = null;
  let config: FeishuConfig = loadConfig();
  let ctxRef: ExtensionContext | null = null;

  /**
   * 消息队列：chatId → 等待中的 Pi turn 信息。
   * 支持多个用户同时与 Bot 对话，每个 chatId 有独立的回复目标。
   */
  interface PendingTurn {
    chatId: string;
    msgId: string;
    /** 流式卡片消息 ID（用于实时更新） */
    cardMsgId: string | null;
    /** 累积的响应文本（用于流式卡片） */
    accumulatedText: string;
    /** 最后一次卡片更新时间 */
    lastCardUpdate: number;
  }
  const pendingTurns: Map<string, PendingTurn> = new Map();

  // ─── 注册 CLI 标志 ────────────────────────────────────

  pi.registerFlag("feishu-app-id", {
    description: "飞书 App ID",
    type: "string",
    default: "",
  });

  pi.registerFlag("feishu-app-secret", {
    description: "飞书 App Secret",
    type: "string",
    default: "",
  });

  pi.registerFlag("feishu-domain", {
    description: "飞书域名 (feishu 或 lark)",
    type: "string",
    default: "",
  });

  pi.registerFlag("feishu-encrypt-key", {
    description: "飞书事件加密密钥（可选）",
    type: "string",
    default: "",
  });

  pi.registerFlag("feishu-verification-token", {
    description: "飞书事件验证令牌（可选）",
    type: "string",
    default: "",
  });

  // ─── 启动飞书客户端 ──────────────────────────────────

  async function startFeishuClient(): Promise<void> {
    if (client) {
      client.disconnect();
      client = null;
    }

    // 从 CLI 标志读取配置（覆盖环境变量）
    const flagMap: Record<string, string> = {
      appId: "feishu-app-id",
      appSecret: "feishu-app-secret",
      domain: "feishu-domain",
      encryptKey: "feishu-encrypt-key",
      verificationToken: "feishu-verification-token",
    };
    const overrides: Partial<FeishuConfig> = {};
    for (const [key, flag] of Object.entries(flagMap)) {
      const val = pi.getFlag(flag);
      if (val) (overrides as any)[key] = String(val);
    }

    config = { ...config, ...overrides };

    // 检查必要配置
    if (!config.appId || !config.appSecret) {
      if (ctxRef?.hasUI) {
        ctxRef.ui.notify("飞书连接失败：缺少 appId/appSecret", "error");
      }
      return;
    }

    client = new FeishuClient(config);

    // 注册消息处理：飞书消息 → Pi 用户消息
    client.setOnMessage((chatId, msgId, text, chatType, resources) => {
      handleFeishuMessage(chatId, msgId, text, chatType, resources);
    });

    // 注册状态变化处理
    client.setOnStatusChange((status) => {
      updateStatus(ctxRef, status);
    });

    try {
      await client.connect();
    } catch (err) {
      if (ctxRef?.hasUI) {
        ctxRef.ui.notify(`飞书连接错误: ${err}`, "error");
      }
    }
  }

  // ─── 处理飞书消息 → 转发给 Pi ────────────────────────

  async function handleFeishuMessage(
    chatId: string,
    msgId: string,
    text: string,
    _chatType: "p2p" | "group",
    resources: InboundResource[],
  ): Promise<void> {
    const content = text.trim();
    if (!content && resources.length === 0) return;

    flashStatus(`飞书: 📩 ${content.substring(0, 20)}${content.length > 20 ? "..." : ""}`);

    // ─── Phase 2: 处理入站媒体资源 ──────────────────────
    let resourceDescription = "";
    for (const res of resources) {
      const localPath = await client!.downloadResource(
        msgId,
        res.fileKey,
        res.type,
        res.fileName,
      );
      if (localPath) {
        resourceDescription += `\n[收到${res.type === "image" ? "图片" : res.type === "audio" ? "语音" : res.type === "video" ? "视频" : "文件"}: ${localPath}]`;
      }
    }

    // 记录该 chatId 对应的消息，用于后续回复
    pendingTurns.set(chatId, {
      chatId,
      msgId,
      cardMsgId: null,
      accumulatedText: "",
      lastCardUpdate: 0,
    });

    // ─── Phase 3: 添加 Typing Reaction ─────────────────
    await client!.startTyping(chatId, msgId);

    // 发送给 Pi（附加资源描述）
    const fullContent = content + (resourceDescription ? "\n" + resourceDescription : "");
    pi.sendUserMessage(fullContent);
  }

  // ─── 监听 Pi 响应 → 回传给飞书 ────────────────────────

  // 每轮 Turn 结束 → 推送到飞书
  pi.on("turn_end", (event: TurnEndEvent) => {
    if (!client || client.getStatus() !== "connected") return;

    const chatId = findActiveChatId();
    if (!chatId) return;

    // 只处理 assistant 消息
    if (event.message?.role !== "assistant") return;

    const textContent = extractTextFromMessage(event.message);
    if (!textContent) return;

    const pending = pendingTurns.get(chatId);
    const replyToMsgId = pending?.msgId;

    // 累积文本
    if (pending) {
      pending.accumulatedText += (pending.accumulatedText ? "\n\n" : "") + textContent;
    }

    // ─── Phase 3: 流式卡片 vs 普通文本 ──────────────────
    const accumulated = pending?.accumulatedText ?? textContent;

    if (accumulated.length > STREAMING_CARD_THRESHOLD && pending) {
      // 长响应：使用流式卡片
      handleStreamingCard(chatId, pending, accumulated);
    } else {
      // 短响应：直接发文本
      const chunks = chunkText(textContent, MAX_TEXT_CHUNK);
      for (const chunk of chunks) {
        client.sendMessage(chatId, chunk, replyToMsgId || undefined);
      }
    }

    flashStatus(`飞书: 📤 推送中 (${textContent.length}字)`);
  });

  // Agent 循环结束 → 最终化并发送完整响应
  pi.on("agent_end", (_event: AgentEndEvent) => {
    if (!client || client.getStatus() !== "connected") return;

    const chatId = findActiveChatId();
    if (!chatId) return;

    const pending = pendingTurns.get(chatId);

    // ─── Phase 3: 最终化流式卡片 ────────────────────────
    if (pending?.cardMsgId && pending.accumulatedText) {
      const finalCard = FeishuClient.buildFinalCard(pending.accumulatedText);
      client.updateCard(pending.cardMsgId, finalCard).catch(() => {});
    }

    // ─── Phase 3: 移除 Typing Reaction ─────────────────
    client.stopTyping(chatId, true).catch(() => {});

    // 清除该 chat 的 pending turn
    pendingTurns.delete(chatId);
    flashStatus("飞书: ✅ 完成");
  });

  // ─── 流式卡片处理 ──────────────────────────────────────

  /**
   * 处理流式卡片更新。
   * 首次创建卡片，后续实时更新内容。
   */
  function handleStreamingCard(
    chatId: string,
    pending: PendingTurn,
    text: string,
  ): void {
    if (!client) return;

    const now = Date.now();

    // 首次创建卡片
    if (!pending.cardMsgId) {
      const card = FeishuClient.buildStreamingCard(text, "🤔 思考中...");
      const replyTo = pending.msgId || undefined;

      client.sendCard(chatId, card, replyTo).then((cardMsgId) => {
        if (cardMsgId) {
          pending.cardMsgId = cardMsgId;
          pending.lastCardUpdate = Date.now();
        }
      }).catch(() => {});

      return;
    }

    // 节流：避免过于频繁更新卡片
    if (now - pending.lastCardUpdate < CARD_UPDATE_INTERVAL) return;

    // 更新现有卡片
    const card = FeishuClient.buildStreamingCard(text, "⏳ 生成中...");
    client.updateCard(pending.cardMsgId, card).then(() => {
      pending.lastCardUpdate = Date.now();
    }).catch(() => {
      // 更新失败，可能消息被撤回，清除 cardMsgId
      pending.cardMsgId = null;
    });
  }

  /**
   * 找到当前应该回复的 chatId。
   * 策略：返回 pendingTurns 中最后一个（最近收到的消息来源）。
   */
  function findActiveChatId(): string | null {
    let lastKey: string | null = null;
    for (const key of pendingTurns.keys()) {
      lastKey = key;
    }
    return lastKey;
  }

  // ─── 注册 /feishu 命令 ────────────────────────────────

  pi.registerCommand("feishu", {
    description: "管理飞书 Bot 连接 (start/stop/status/config/help)",
    handler: async (args: string, ctx: ExtensionCommandContext) => {
      const action = args.trim().toLowerCase() || "status";

      switch (action) {
        case "start":
          await startFeishuClient();
          ctx.ui.notify("飞书客户端已启动", "info");
          break;

        case "stop":
          if (client) {
            client.disconnect();
            client = null;
          }
          ctx.ui.notify("飞书客户端已停止", "info");
          break;

        case "status": {
          const status = client?.getStatus() ?? "未启动";
          ctx.ui.notify(
            `飞书 Bot 状态: ${status}\n` +
              `App ID: ${config.appId ? "****" + config.appId.slice(-4) : "未设置"}\n` +
              `Domain: ${config.domain || "feishu"}`,
            "info",
          );
          break;
        }

        case "config":
          ctx.ui.notify(
            `当前配置:\n` +
              `App ID: ${config.appId ? "****" + config.appId.slice(-4) : "未设置"}\n` +
              `App Secret: ${config.appSecret ? "****" : "未设置"}\n` +
              `Domain: ${config.domain || "feishu"}\n` +
              `Encrypt Key: ${config.encryptKey ? "已设置" : "未设置"}\n` +
              `Verification Token: ${config.verificationToken ? "已设置" : "未设置"}`,
            "info",
          );
          break;

        case "help":
          ctx.ui.notify(
            `/feishu 命令用法:\n` +
              `  /feishu start   - 启动飞书 Bot 连接\n` +
              `  /feishu stop    - 断开飞书 Bot 连接\n` +
              `  /feishu status  - 查看连接状态\n` +
              `  /feishu config  - 查看当前配置\n` +
              `  /feishu help    - 显示帮助\n\n` +
              `配置优先级（从高到低）:\n` +
              `  1. CLI 标志: --feishu-app-id, --feishu-app-secret\n` +
              `  2. 环境变量: FEISHU_APP_ID, FEISHU_APP_SECRET\n` +
              `  3. settings.json 中的 feishu 字段`,
            "info",
          );
          break;

        default:
          ctx.ui.notify(`未知命令: ${action}，使用 /feishu help 查看帮助`, "warning");
      }
    },
  });

  // ─── 注册自定义工具：发送消息到飞书 ────────────────────

  const SendToFeishuParams = {
    type: "object" as const,
    properties: {
      message: { type: "string" as const, description: "要发送的消息内容" },
      chat_id: {
        type: "string" as const,
        description: "目标聊天 ID（飞书 chat_id），留空则发送到最近活跃的聊天",
      },
    },
    required: ["message"],
  };

  pi.registerTool({
    name: "send_to_feishu",
    label: "发送到飞书",
    description: "发送消息到飞书聊天界面。当用户要求通过飞书发送消息时使用。",
    parameters: SendToFeishuParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendToFeishuParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const message = params.message as string;
      const chatId = (params.chat_id as string) || findActiveChatId();

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [
            { type: "text" as const, text: "错误: 飞书 Bot 未连接。请先运行 /feishu start 启动连接。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [
            { type: "text" as const, text: "错误: 没有活跃的飞书聊天。请先在飞书中发送一条消息。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      await client.sendMessage(chatId, message);
      return {
        content: [{ type: "text" as const, text: `已发送到飞书 [${chatId}]: ${message}` }],
        details: { sent: true, chatId, message } as Record<string, unknown>,
      };
    },
  });

  // ─── 注册自定义工具：发送图片到飞书 ────────────────────

  const SendImageToFeishuParams = {
    type: "object" as const,
    properties: {
      file_path: { type: "string" as const, description: "本地图片文件路径" },
      chat_id: {
        type: "string" as const,
        description: "目标聊天 ID，留空则发送到最近活跃的聊天",
      },
    },
    required: ["file_path"],
  };

  pi.registerTool({
    name: "send_image_to_feishu",
    label: "发送图片到飞书",
    description: "将本地图片文件上传到飞书并发送。当需要发送图片到飞书聊天时使用。",
    parameters: SendImageToFeishuParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendImageToFeishuParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const filePath = params.file_path as string;
      const chatId = (params.chat_id as string) || findActiveChatId();

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [
            { type: "text" as const, text: "错误: 飞书 Bot 未连接。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [
            { type: "text" as const, text: "错误: 没有活跃的飞书聊天。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      const imageKey = await client.uploadImage(filePath);
      if (!imageKey) {
        return {
          content: [{ type: "text" as const, text: "错误: 图片上传失败。" }],
          details: {} as Record<string, unknown>,
        };
      }

      await client.sendImage(chatId, imageKey);
      return {
        content: [{ type: "text" as const, text: `图片已发送到飞书 [${chatId}]: ${filePath}` }],
        details: { sent: true, chatId, filePath, imageKey } as Record<string, unknown>,
      };
    },
  });

  // ─── 注册自定义工具：发送文件到飞书 ────────────────────

  const SendFileToFeishuParams = {
    type: "object" as const,
    properties: {
      file_path: { type: "string" as const, description: "本地文件路径" },
      file_name: { type: "string" as const, description: "文件名" },
      chat_id: {
        type: "string" as const,
        description: "目标聊天 ID，留空则发送到最近活跃的聊天",
      },
    },
    required: ["file_path", "file_name"],
  };

  pi.registerTool({
    name: "send_file_to_feishu",
    label: "发送文件到飞书",
    description: "将本地文件上传到飞书并发送。当需要发送文件到飞书聊天时使用。",
    parameters: SendFileToFeishuParams,
    async execute(
      _toolCallId: string,
      params: Static<typeof SendFileToFeishuParams>,
      _signal: AbortSignal | undefined,
      _onUpdate: any,
      _ctx: ExtensionContext,
    ) {
      const filePath = params.file_path as string;
      const fileName = params.file_name as string;
      const chatId = (params.chat_id as string) || findActiveChatId();

      if (!client || client.getStatus() !== "connected") {
        return {
          content: [
            { type: "text" as const, text: "错误: 飞书 Bot 未连接。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      if (!chatId) {
        return {
          content: [
            { type: "text" as const, text: "错误: 没有活跃的飞书聊天。" },
          ],
          details: {} as Record<string, unknown>,
        };
      }

      const fileKey = await client.uploadFile(filePath, fileName);
      if (!fileKey) {
        return {
          content: [{ type: "text" as const, text: "错误: 文件上传失败。" }],
          details: {} as Record<string, unknown>,
        };
      }

      await client.sendFile(chatId, fileKey);
      return {
        content: [{ type: "text" as const, text: `文件已发送到飞书 [${chatId}]: ${fileName}` }],
        details: { sent: true, chatId, filePath, fileName, fileKey } as Record<string, unknown>,
      };
    },
  });

  // ─── 会话启动时自动连接 ────────────────────────────────

  pi.on("session_start", async (_event, ctx) => {
    ctxRef = ctx;
    updateStatus(ctx, "disconnected");

    // 自动连接
    try {
      await startFeishuClient();
    } catch (err) {
      if (ctx.hasUI) {
        ctx.ui.notify(`飞书连接失败: ${err}`, "error");
      }
    }
  });

  // ─── 会话关闭时清理 ────────────────────────────────────

  pi.on("session_shutdown", async () => {
    if (client) {
      client.disconnect();
      client = null;
    }
    pendingTurns.clear();
  });

  // ─── 辅助函数 ────────────────────────────────────────

  /** 从 Pi 消息中提取文本内容 */
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

  /** 状态栏瞬态消息定时器 */
  let statusTimer: ReturnType<typeof setTimeout> | null = null;
  /** 当前状态栏显示的文本 */
  let currentStatusText: string = "";

  /** 更新状态栏显示（连接状态变化时调用） */
  function updateStatus(ctx: ExtensionContext | null, status: string): void {
    if (!ctx?.hasUI) return;

    // 取消待恢复的 flash 定时器
    if (statusTimer) {
      clearTimeout(statusTimer);
      statusTimer = null;
    }

    const statusMap: Record<string, string> = {
      connecting: "飞书: 连接中",
      connected: "飞书: 已连接",
      disconnected: "飞书: 未连接",
      error: "飞书: 错误",
    };

    const text = statusMap[status] ?? `飞书: ${status}`;
    if (currentStatusText === text) return;
    currentStatusText = text;
    ctx.ui.setStatus("feishu", text);
  }

  /**
   * 在状态栏显示瞬态消息。
   * 3 秒后自动恢复为连接状态。
   */
  function flashStatus(message: string): void {
    if (!ctxRef?.hasUI) return;
    if (statusTimer) clearTimeout(statusTimer);

    if (currentStatusText === message) return;
    currentStatusText = message;
    ctxRef.ui.setStatus("feishu", message);

    statusTimer = setTimeout(() => {
      statusTimer = null;
      if (client && client.getStatus() === "connected") {
        const text = "飞书: 已连接";
        if (currentStatusText !== text) {
          currentStatusText = text;
          ctxRef?.ui.setStatus("feishu", text);
        }
      }
    }, 3000);
  }

  /**
   * 将长文本分块，避免超过飞书单条消息限制。
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
        // 强制分割
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
