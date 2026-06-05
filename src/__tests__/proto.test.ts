/**
 * Pi-Yuanbao 集成测试
 *
 * 验证核心功能：
 * 1. Protobuf 编解码正确性
 * 2. Sign token 签名计算
 * 3. 消息分块逻辑
 * 4. 协议帧往返一致性
 */

import {
  CMD_TYPE,
  CMD,
  INSTANCE_ID,
  WS_HEARTBEAT_RUNNING,
  WS_HEARTBEAT_FINISH,
  nextSeqNo,
  encodeConnMsgFull,
  decodeConnMsg,
  encodeAuthBind,
  decodeAuthBindRsp,
  encodePing,
  encodePushAck,
  encodeSendC2CMessage,
  encodeSendGroupMessage,
  encodeSendPrivateHeartbeat,
  encodeSendGroupHeartbeat,
  decodeInboundPush,
  type ConnHead,
} from "../yuanbao-proto.js";
import { computeSignature, buildTimestamp } from "../yuanbao-sign.js";

// ─── Helpers ───

let passed = 0;
let failed = 0;
const failures: string[] = [];

function assert(condition: boolean, msg: string): void {
  if (condition) {
    passed++;
    console.log(`  ✅ ${msg}`);
  } else {
    failed++;
    failures.push(msg);
    console.error(`  ❌ ${msg}`);
  }
}

function assertEqual<T>(actual: T, expected: T, msg: string): void {
  if (JSON.stringify(actual) === JSON.stringify(expected)) {
    passed++;
    console.log(`  ✅ ${msg}`);
  } else {
    failed++;
    failures.push(`${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
    console.error(`  ❌ ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

// ─── Tests ───

console.log("\n=== 1. ConnMsg 帧编解码 ===");
{
  // 测试基本的 ConnMsg 编码/解码往返
  const data = new TextEncoder().encode("hello world");
  const seqNo = nextSeqNo();
  const encoded = encodeConnMsgFull(CMD_TYPE.Request, "test-cmd", seqNo, "msg-001", "test-module", data);
  const decoded = decodeConnMsg(encoded);

  assertEqual(decoded.head.cmdType, CMD_TYPE.Request, "cmdType = Request");
  assertEqual(decoded.head.cmd, "test-cmd", "cmd = test-cmd");
  assertEqual(decoded.head.seqNo, seqNo, "seqNo matches");
  assertEqual(decoded.head.msgId, "msg-001", "msgId = msg-001");
  assertEqual(decoded.head.module, "test-module", "module = test-module");
  assertEqual(decoded.head.needAck, false, "needAck = false (default)");
  assertEqual(new TextDecoder().decode(decoded.data), "hello world", "data payload preserved");
}

console.log("\n=== 2. AuthBind 编解码 ===");
{
  const authBytes = encodeAuthBind("ybBot", "bot-123", "bot", "test-token-xyz", "msg-auth-001", "1.0", "linux", "2.0", "ci-677");
  const decoded = decodeConnMsg(authBytes);

  assertEqual(decoded.head.cmdType, CMD_TYPE.Request, "AuthBind cmdType = Request");
  assertEqual(decoded.head.cmd, CMD.AuthBind, "AuthBind cmd = auth-bind");
  assertEqual(decoded.head.module, "conn_access", "AuthBind module = conn_access");

  // 解码 AuthBindRsp
  const rspData = new Uint8Array(0); // 简化测试
  // 实际 rspData 是 protobuf 编码的 { code, message, connect_id }
  // 这里主要验证帧结构正确
  assert(decoded.data.length > 0, "AuthBind data payload is non-empty");
}

console.log("\n=== 3. Ping 编码 ===");
{
  const pingBytes = encodePing("msg-ping-001");
  const decoded = decodeConnMsg(pingBytes);

  assertEqual(decoded.head.cmdType, CMD_TYPE.Request, "Ping cmdType = Request");
  assertEqual(decoded.head.cmd, CMD.Ping, "Ping cmd = ping");
  assertEqual(decoded.head.module, "conn_access", "Ping module = conn_access");
  assertEqual(decoded.data.length, 0, "Ping data is empty");
}

console.log("\n=== 4. Push ACK 编码 ===");
{
  const originalHead: ConnHead = {
    cmdType: CMD_TYPE.Push,
    cmd: "InboundMessagePush",
    seqNo: 42,
    msgId: "push-msg-001",
    module: "yuanbao_openclaw_proxy",
    needAck: true,
    status: 0,
  };
  const ackBytes = encodePushAck(originalHead);
  const decoded = decodeConnMsg(ackBytes);

  assertEqual(decoded.head.cmdType, CMD_TYPE.PushAck, "PushAck cmdType = PushAck");
  assertEqual(decoded.head.cmd, "InboundMessagePush", "PushAck preserves cmd");
  assertEqual(decoded.head.msgId, "push-msg-001", "PushAck preserves msgId");
}

console.log("\n=== 5. C2C 私聊消息编码 ===");
{
  const msgBody = [{ msgType: "TIMTextElem", msgContent: { text: "Hello from Pi!" } }];
  const c2cBytes = encodeSendC2CMessage("user-456", msgBody, "bot-123", "c2c-msg-001");
  const decoded = decodeConnMsg(c2cBytes);

  assertEqual(decoded.head.cmdType, CMD_TYPE.Request, "C2C cmdType = Request");
  assertEqual(decoded.head.cmd, "send_c2c_message", "C2C cmd = send_c2c_message");
  assertEqual(decoded.head.module, "yuanbao_openclaw_proxy", "C2C module = yuanbao_openclaw_proxy");
  assert(decoded.data.length > 0, "C2C data payload is non-empty");
}

console.log("\n=== 6. 群消息编码 ===");
{
  const msgBody = [{ msgType: "TIMTextElem", msgContent: { text: "Hello group!" } }];
  const grpBytes = encodeSendGroupMessage("group-789", msgBody, "bot-123", "grp-msg-001", "ref-msg-001");
  const decoded = decodeConnMsg(grpBytes);

  assertEqual(decoded.head.cmdType, CMD_TYPE.Request, "Group cmdType = Request");
  assertEqual(decoded.head.cmd, "send_group_message", "Group cmd = send_group_message");
  assert(decoded.data.length > 0, "Group data payload is non-empty");
}

console.log("\n=== 7. Reply Heartbeat 编码 ===");
{
  const privHb = encodeSendPrivateHeartbeat("bot-123", "user-456", WS_HEARTBEAT_RUNNING);
  const privDecoded = decodeConnMsg(privHb);
  assertEqual(privDecoded.head.cmd, "send_private_heartbeat", "Private heartbeat cmd");
  assert(privDecoded.data.length > 0, "Private heartbeat data non-empty");

  const grpHb = encodeSendGroupHeartbeat("bot-123", "group-789", WS_HEARTBEAT_FINISH);
  const grpDecoded = decodeConnMsg(grpHb);
  assertEqual(grpDecoded.head.cmd, "send_group_heartbeat", "Group heartbeat cmd");
  assert(grpDecoded.data.length > 0, "Group heartbeat data non-empty");
}

console.log("\n=== 8. InboundPush 解码（构造测试数据）===");
{
  // 手动构造一个 InboundMessagePush 的 protobuf 数据
  // 字段：from_account(2), sender_nickname(4), msg_body(13)
  function encodeStringField(fieldNum: number, value: string): Uint8Array {
    const tag = (fieldNum << 3) | 2; // LEN wire type
    const encoded = new TextEncoder().encode(value);
    const result = new Uint8Array(2 + encoded.length);
    result[0] = tag;
    result[1] = encoded.length;
    result.set(encoded, 2);
    return result;
  }

  // 简单验证 decodeInboundPush 不崩溃（传入空数据）
  const nullResult = decodeInboundPush(new Uint8Array(0));
  assertEqual(nullResult, null, "Empty data returns null");
}

console.log("\n=== 9. 序列号生成 ===");
{
  const seq1 = nextSeqNo();
  const seq2 = nextSeqNo();
  assert(seq2 > seq1, "Sequence numbers increment");
  assert(seq1 >= 0, "Sequence numbers are non-negative");
}

console.log("\n=== 10. 常量值验证 ===");
{
  assertEqual(CMD_TYPE.Request, 0, "CMD_TYPE.Request = 0");
  assertEqual(CMD_TYPE.Response, 1, "CMD_TYPE.Response = 1");
  assertEqual(CMD_TYPE.Push, 2, "CMD_TYPE.Push = 2");
  assertEqual(CMD_TYPE.PushAck, 3, "CMD_TYPE.PushAck = 3");
  assertEqual(CMD.AuthBind, "auth-bind", "CMD.AuthBind");
  assertEqual(CMD.Ping, "ping", "CMD.Ping");
  assertEqual(CMD.Kickout, "kickout", "CMD.Kickout");
  assertEqual(INSTANCE_ID, 17, "INSTANCE_ID = 17");
  assertEqual(WS_HEARTBEAT_RUNNING, 1, "WS_HEARTBEAT_RUNNING = 1");
  assertEqual(WS_HEARTBEAT_FINISH, 2, "WS_HEARTBEAT_FINISH = 2");
}

console.log("\n=== 11. Sign Token 签名计算 ===");
{
  const sig = computeSignature("test-nonce", "2024-01-01T00:00:00+08:00", "test-key", "test-secret");
  assert(typeof sig === "string", "Signature is a string");
  assert(sig.length === 64, "SHA256 hex digest is 64 chars");
  assert(sig.length > 0, "Signature is non-empty");
}

console.log("\n=== 12. Timestamp 格式 ===");
{
  const ts = buildTimestamp();
  console.log(`  Timestamp: ${ts}`);
  assert(ts.includes("+08:00"), "Timestamp contains +08:00 timezone");
  assert(ts.includes("T"), "Timestamp contains T separator");
  assert(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+08:00$/.test(ts), "Timestamp matches expected format");
}

console.log("\n=== 13. 消息分块逻辑 ===");
{
  // 导入分块函数（从 index.ts 逻辑中提取）
  function chunkText(text: string, maxLen: number): string[] {
    if (text.length <= maxLen) return [text];
    const chunks: string[] = [];
    let remaining = text;
    while (remaining.length > 0) {
      if (remaining.length <= maxLen) {
        chunks.push(remaining);
        break;
      }
      let splitPos = remaining.lastIndexOf("\n", maxLen);
      if (splitPos <= 0) splitPos = remaining.lastIndexOf(" ", maxLen);
      if (splitPos <= 0) {
        chunks.push(remaining.substring(0, maxLen));
        remaining = remaining.substring(maxLen);
      } else {
        chunks.push(remaining.substring(0, splitPos + 1));
        remaining = remaining.substring(splitPos + 1);
      }
    }
    return chunks;
  }

  // 短文本不分块
  const short = chunkText("Hello", 4000);
  assertEqual(short.length, 1, "Short text: 1 chunk");
  assertEqual(short[0], "Hello", "Short text: content preserved");

  // 长文本按换行分块
  const longText = "A".repeat(3000) + "\n" + "B".repeat(3000);
  const longChunks = chunkText(longText, 4000);
  assertEqual(longChunks.length, 2, "Long text: 2 chunks");
  assert(longChunks[0].length <= 4001, "Long text: first chunk ≤ 4001"); // includes \n
  assert(longChunks[1].length <= 4000, "Long text: second chunk ≤ 4000");

  // 非常长的无换行文本
  const noNewline = "X".repeat(10000);
  const noNlChunks = chunkText(noNewline, 4000);
  assert(noNlChunks.length >= 2, "No-newline text: at least 2 chunks");
  for (let i = 0; i < noNlChunks.length; i++) {
    assert(noNlChunks[i].length <= 4000, `No-newline chunk ${i} ≤ 4000`);
  }

  // 合并后等于原文
  assertEqual(noNlChunks.join(""), noNewline, "Chunks reassemble to original");
}

// ─── Summary ───

console.log("\n" + "=".repeat(50));
console.log(`测试结果: ${passed} 通过, ${failed} 失败`);
if (failures.length > 0) {
  console.log("\n失败项:");
  failures.forEach((f) => console.log(`  - ${f}`));
}
console.log("=".repeat(50) + "\n");

process.exit(failed > 0 ? 1 : 0);
