/**
 * yuanbao-sign.ts - 元宝 Sign Token 管理器
 *
 * 负责获取和缓存 WebSocket 认证所需的 sign-token。
 * 移植自 hermes-agent 的 SignManager。
 */

import { createHmac, randomBytes } from "node:crypto";

// ============================================================
// 类型
// ============================================================

export interface TokenData {
  token: string;
  botId: string;
  duration: number;
  product: string;
  source: string;
}

interface CachedToken extends TokenData {
  expireTs: number;
}

// ============================================================
// SignManager
// ============================================================

const TOKEN_PATH = "/api/v5/robotLogic/sign-token";
const RETRYABLE_CODE = 10099;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;
const CACHE_REFRESH_MARGIN_S = 60;
const HTTP_TIMEOUT_MS = 10_000;

// 日志静默：console.log 在 Pi 中会显示到聊天区域
const _log = (..._args: any[]) => {};

let _cache: CachedToken | null = null;
let _refreshPromise: Promise<TokenData> | null = null;

/** 计算 HMAC-SHA256 签名 */
export function computeSignature(nonce: string, timestamp: string, appKey: string, appSecret: string): string {
  const plain = nonce + timestamp + appKey + appSecret;
  return createHmac("sha256", appSecret).update(plain).digest("hex");
}

/** 构建北京时间 ISO-8601 时间戳 */
export function buildTimestamp(): string {
  const now = new Date();
  // 转换为北京时间 (UTC+8)
  const offset = 8 * 60; // 8 hours in minutes
  const local = new Date(now.getTime() + offset * 60 * 1000);
  const iso = local.toISOString().replace(/\.\d{3}Z$/, "+08:00");
  // 确保格式为 2006-01-02T15:04:05+08:00
  return iso.replace("Z", "+08:00");
}

/** 检查缓存是否有效 */
function isCacheValid(entry: CachedToken): boolean {
  return entry.expireTs - Date.now() / 1000 > CACHE_REFRESH_MARGIN_S;
}

/** HTTP 请求获取 sign-token */
async function fetchToken(
  appKey: string,
  appSecret: string,
  apiDomain: string,
  routeEnv = "",
): Promise<TokenData> {
  const url = `${apiDomain.replace(/\/+$/, "")}${TOKEN_PATH}`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const nonce = randomBytes(16).toString("hex");
    const timestamp = buildTimestamp();
    const signature = computeSignature(nonce, timestamp, appKey, appSecret);

    const payload = {
      app_key: appKey,
      nonce,
      signature,
      timestamp,
    };

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-AppVersion": "pi-yuanbao/2.0.0",
      "X-OperationSystem": process.platform,
      "X-Instance-Id": "17",
      "X-Bot-Version": "pi-yuanbao/2.0.0",
    };
    if (routeEnv) headers["X-Route-Env"] = routeEnv;

    if (attempt > 0) {
      _log(`[SignManager] Retry ${attempt}/${MAX_RETRIES}...`);
      await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
    }

    _log(`[SignManager] Fetching sign token from ${url}`);

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      clearTimeout(timeout);

      if (!response.ok) {
        throw new Error(`Sign token API returned ${response.status}: ${await response.text()}`);
      }

      const result = await response.json() as any;
      const code = result.code;

      if (code === 0) {
        const data = result.data;
        if (!data || typeof data !== "object") {
          throw new Error(`Sign token response missing 'data' field: ${JSON.stringify(result)}`);
        }
        _log(`[SignManager] Sign token success: bot_id=${data.bot_id}`);
        return {
          token: data.token || "",
          botId: data.bot_id || "",
          duration: data.duration || 0,
          product: data.product || "",
          source: data.source || "",
        };
      }

      if (code === RETRYABLE_CODE && attempt < MAX_RETRIES) {
        _log(`[SignManager] Retryable error: code=${code}, retrying...`);
        continue;
      }

      const msg = result.msg || "";
      throw new Error(`Sign token error: code=${code}, msg=${msg}`);
    } catch (err: any) {
      clearTimeout(timeout);
      if (err.name === "AbortError") {
        throw new Error("Sign token request timed out");
      }
      if (attempt >= MAX_RETRIES) throw err;
      _log(`[SignManager] Request failed: ${err.message}, retrying...`);
    }
  }

  throw new Error("Sign token failed: max retries exceeded");
}

/**
 * 获取 sign-token（带缓存）
 */
export async function getSignToken(
  appKey: string,
  appSecret: string,
  apiDomain: string,
  routeEnv = "",
): Promise<TokenData> {
  // 缓存命中
  if (_cache && isCacheValid(_cache)) {
    const remain = Math.floor(_cache.expireTs - Date.now() / 1000);
    _log(`[SignManager] Using cached token (${remain}s remaining)`);
    return { ..._cache };
  }

  // 防止并发重复请求
  if (_refreshPromise) {
    return _refreshPromise;
  }

  _refreshPromise = (async () => {
    try {
      const data = await fetchToken(appKey, appSecret, apiDomain, routeEnv);
      const duration = data.duration || 3600;
      _cache = {
        ...data,
        expireTs: Date.now() / 1000 + duration,
      };
      return data;
    } finally {
      _refreshPromise = null;
    }
  })();

  return _refreshPromise;
}

/**
 * 强制刷新 token
 */
export async function forceRefreshToken(
  appKey: string,
  appSecret: string,
  apiDomain: string,
  routeEnv = "",
): Promise<TokenData> {
  _log(`[SignManager] Force refreshing token...`);
  _cache = null;
  _refreshPromise = null;
  return getSignToken(appKey, appSecret, apiDomain, routeEnv);
}

/** 清除缓存（断开连接时调用） */
export function clearTokenCache(): void {
  _cache = null;
  _refreshPromise = null;
}
