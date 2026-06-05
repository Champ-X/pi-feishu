/**
 * yuanbao-proto.ts - Yuanbao WebSocket 协议编解码
 *
 * 协议层级：
 *   WebSocket frame
 *     └── ConnMsg (protobuf: trpc.yuanbao.conn_common.ConnMsg)
 *           ├── head: Head  (cmd_type, cmd, seq_no, msg_id, module, ...)
 *           └── data: bytes  (业务 payload)
 *                 └── InboundMessagePush / SendC2CMessageReq / ...
 *
 * 移植自 hermes-agent 的 yuanbao_proto.py，纯 TypeScript 实现，
 * 不依赖第三方 protobuf 库。
 */

// ============================================================
// 常量
// ============================================================

/** cmd_type 枚举 */
export const CMD_TYPE = {
  Request: 0,   // 上行请求
  Response: 1,  // 上行请求的回包
  Push: 2,      // 下行推送
  PushAck: 3,   // 下行推送的回包（ACK）
} as const;

/** 内置命令字 */
export const CMD = {
  AuthBind: "auth-bind",
  Ping: "ping",
  Kickout: "kickout",
  UpdateMeta: "update-meta",
} as const;

/** 内置模块名 */
export const MODULE = {
  ConnAccess: "conn_access",
} as const;

/** biz 层服务名 */
const BIZ_PKG = "yuanbao_openclaw_proxy";

/** openclaw instance_id（固定值 17） */
export const INSTANCE_ID = 17;

/** Reply Heartbeat 状态常量 */
export const WS_HEARTBEAT_RUNNING = 1;
export const WS_HEARTBEAT_FINISH = 2;

// ============================================================
// 序列号生成
// ============================================================

let _seqCounter = 0;
const SEQ_MAX = 2 ** 32 - 1;

export function nextSeqNo(): number {
  const val = _seqCounter;
  _seqCounter = (_seqCounter + 1) & SEQ_MAX;
  return val;
}

// ============================================================
// Protobuf wire-format 基础工具
// ============================================================

const WT_VARINT = 0;
const WT_64BIT = 1;
const WT_LEN = 2;
const WT_32BIT = 5;

function encodeVarint(value: number): Uint8Array {
  if (value < 0) value = value & 0xFFFFFFFFFFFFFFFF;
  const out: number[] = [];
  while (true) {
    const bits = value & 0x7F;
    value >>>= 7;  // 使用无符号右移
    if (value) {
      out.push(bits | 0x80);
    } else {
      out.push(bits);
      break;
    }
  }
  return new Uint8Array(out);
}

function decodeVarint(data: Uint8Array, pos: number): [number, number] {
  let result = 0;
  let shift = 0;
  while (pos < data.length) {
    const b = data[pos++];
    result |= (b & 0x7F) << shift;
    shift += 7;
    if (!(b & 0x80)) break;
    if (shift >= 64) throw new Error("varint too long");
  }
  return [result, pos];
}

function encodeField(fieldNumber: number, wireType: number, value: Uint8Array): Uint8Array {
  const tag = (fieldNumber << 3) | wireType;
  return concat(encodeVarint(tag), value);
}

function encodeString(s: string): Uint8Array {
  const encoded = new TextEncoder().encode(s);
  return concat(encodeVarint(encoded.length), encoded);
}

function encodeBytes(b: Uint8Array): Uint8Array {
  return concat(encodeVarint(b.length), b);
}

function encodeMessage(b: Uint8Array): Uint8Array {
  return concat(encodeVarint(b.length), b);
}

function concat(...arrays: Uint8Array[]): Uint8Array {
  const totalLen = arrays.reduce((sum, a) => sum + a.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

/** 解析 protobuf message 的所有字段 */
function parseFields(data: Uint8Array): Array<[number, number, number | Uint8Array]> {
  const fields: Array<[number, number, number | Uint8Array]> = [];
  let pos = 0;
  while (pos < data.length) {
    let tag: number;
    [tag, pos] = decodeVarint(data, pos);
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;
    if (wireType === WT_VARINT) {
      let val: number;
      [val, pos] = decodeVarint(data, pos);
      fields.push([fieldNumber, wireType, val]);
    } else if (wireType === WT_LEN) {
      let length: number;
      [length, pos] = decodeVarint(data, pos);
      const val = data.slice(pos, pos + length);
      pos += length;
      fields.push([fieldNumber, wireType, val]);
    } else if (wireType === WT_64BIT) {
      const val = data.slice(pos, pos + 8);
      pos += 8;
      fields.push([fieldNumber, wireType, val]);
    } else if (wireType === WT_32BIT) {
      const val = data.slice(pos, pos + 4);
      pos += 4;
      fields.push([fieldNumber, wireType, val]);
    } else {
      throw new Error(`unknown wire type ${wireType} at pos ${pos - 1}`);
    }
  }
  return fields;
}

/** 将 fields 列表转为 {fieldNumber: [value, ...]} 字典 */
type FieldDict = Map<number, Array<[number, number | Uint8Array]>>;

function fieldsToDict(fields: Array<[number, number, number | Uint8Array]>): FieldDict {
  const d: FieldDict = new Map();
  for (const [fn, wt, val] of fields) {
    if (!d.has(fn)) d.set(fn, []);
    d.get(fn)!.push([wt, val]);
  }
  return d;
}

function getString(fdict: FieldDict, fn: number, defaultVal = ""): string {
  const entries = fdict.get(fn);
  if (!entries?.length) return defaultVal;
  const [wt, val] = entries[0];
  if (wt === WT_LEN && val instanceof Uint8Array) {
    return new TextDecoder().decode(val);
  }
  return defaultVal;
}

function getVarint(fdict: FieldDict, fn: number, defaultVal = 0): number {
  const entries = fdict.get(fn);
  if (!entries?.length) return defaultVal;
  const [wt, val] = entries[0];
  if (wt === WT_VARINT && typeof val === "number") return val;
  return defaultVal;
}

function getBytes(fdict: FieldDict, fn: number): Uint8Array {
  const entries = fdict.get(fn);
  if (!entries?.length) return new Uint8Array(0);
  const [wt, val] = entries[0];
  if (wt === WT_LEN && val instanceof Uint8Array) return val;
  return new Uint8Array(0);
}

function getRepeatedBytes(fdict: FieldDict, fn: number): Uint8Array[] {
  const entries = fdict.get(fn) || [];
  return entries
    .filter(([wt]) => wt === WT_LEN)
    .map(([, val]) => val as Uint8Array);
}

// ============================================================
// ConnMsg 层编解码
// ============================================================

/** ConnMsg.Head */
export interface ConnHead {
  cmdType: number;
  cmd: string;
  seqNo: number;
  msgId: string;
  module: string;
  needAck: boolean;
  status: number;
}

/** ConnMsg 解码结果 */
export interface ConnMsg {
  msgType: number;
  seqNo: number;
  data: Uint8Array;
  head: ConnHead;
}

function encodeHead(
  cmdType: number,
  cmd: string,
  seqNo: number,
  msgId: string,
  module: string,
  needAck = false,
  status = 0,
): Uint8Array {
  let buf: Uint8Array = new Uint8Array(0);
  if (cmdType !== 0) buf = concat(buf, encodeField(1, WT_VARINT, encodeVarint(cmdType)));
  if (cmd) buf = concat(buf, encodeField(2, WT_LEN, encodeString(cmd)));
  if (seqNo !== 0) buf = concat(buf, encodeField(3, WT_VARINT, encodeVarint(seqNo)));
  if (msgId) buf = concat(buf, encodeField(4, WT_LEN, encodeString(msgId)));
  if (module) buf = concat(buf, encodeField(5, WT_LEN, encodeString(module)));
  if (needAck) buf = concat(buf, encodeField(6, WT_VARINT, encodeVarint(1)));
  if (status !== 0) buf = concat(buf, encodeField(10, WT_VARINT, encodeVarint(status)));
  return buf;
}

function decodeHead(data: Uint8Array): ConnHead {
  const fdict = fieldsToDict(parseFields(data));
  return {
    cmdType: getVarint(fdict, 1, 0),
    cmd: getString(fdict, 2, ""),
    seqNo: getVarint(fdict, 3, 0),
    msgId: getString(fdict, 4, ""),
    module: getString(fdict, 5, ""),
    needAck: !!getVarint(fdict, 6, 0),
    status: getVarint(fdict, 10, 0),
  };
}

/** 编码 ConnMsg */
export function encodeConnMsg(msgType: number, seqNo: number, data: Uint8Array): Uint8Array {
  const headBytes = encodeHead(msgType, "", seqNo, "", "");
  let buf = encodeField(1, WT_LEN, encodeMessage(headBytes));
  if (data.length) buf = concat(buf, encodeField(2, WT_LEN, encodeBytes(data)));
  return buf;
}

/** 解码 ConnMsg */
export function decodeConnMsg(data: Uint8Array): ConnMsg {
  const fdict = fieldsToDict(parseFields(data));
  const headBytes = getBytes(fdict, 1);
  const payload = getBytes(fdict, 2);
  const head = headBytes.length ? decodeHead(headBytes) : {
    cmdType: 0, cmd: "", seqNo: 0, msgId: "", module: "", needAck: false, status: 0,
  };
  return {
    msgType: head.cmdType,
    seqNo: head.seqNo,
    data: payload,
    head,
  };
}

/** 编码完整的 ConnMsg */
export function encodeConnMsgFull(
  cmdType: number,
  cmd: string,
  seqNo: number,
  msgId: string,
  module: string,
  data: Uint8Array,
  needAck = false,
): Uint8Array {
  const headBytes = encodeHead(cmdType, cmd, seqNo, msgId, module, needAck);
  let buf = encodeField(1, WT_LEN, encodeMessage(headBytes));
  if (data.length) buf = concat(buf, encodeField(2, WT_LEN, encodeBytes(data)));
  return buf;
}

// ============================================================
// 业务消息编解码
// ============================================================

/** MsgContent */
interface MsgContent {
  text?: string;
  uuid?: string;
  data?: string;
  desc?: string;
  ext?: string;
  url?: string;
  fileName?: string;
  imageFormat?: number;
  fileSize?: number;
  index?: number;
}

/** MsgBodyElement */
export interface MsgBodyElement {
  msgType: string;  // e.g. "TIMTextElem"
  msgContent: MsgContent;
}

function encodeMsgContent(content: MsgContent): Uint8Array {
  let buf: Uint8Array = new Uint8Array(0);
  const strFields: [number, string][] = [
    [1, content.text || ""],
    [2, content.uuid || ""],
    [4, content.data || ""],
    [5, content.desc || ""],
    [6, content.ext || ""],
    [10, content.url || ""],
    [12, content.fileName || ""],
  ];
  for (const [fn, val] of strFields) {
    if (val) buf = concat(buf, encodeField(fn, WT_LEN, encodeString(val)));
  }
  const varintFields: [number, number][] = [
    [3, content.imageFormat || 0],
    [9, content.index || 0],
    [11, content.fileSize || 0],
  ];
  for (const [fn, val] of varintFields) {
    if (val) buf = concat(buf, encodeField(fn, WT_VARINT, encodeVarint(val)));
  }
  return buf;
}

function decodeMsgContent(data: Uint8Array): MsgContent {
  const fdict = fieldsToDict(parseFields(data));
  const content: MsgContent = {};
  const strMap: [number, keyof MsgContent][] = [
    [1, "text"], [2, "uuid"], [4, "data"], [5, "desc"],
    [6, "ext"], [10, "url"], [12, "fileName"],
  ];
  for (const [fn, key] of strMap) {
    const val = getString(fdict, fn);
    if (val) (content as any)[key] = val;
  }
  const intMap: [number, keyof MsgContent][] = [
    [3, "imageFormat"], [9, "index"], [11, "fileSize"],
  ];
  for (const [fn, key] of intMap) {
    const val = getVarint(fdict, fn);
    if (val) (content as any)[key] = val;
  }
  return content;
}

function encodeMsgBodyElement(element: MsgBodyElement): Uint8Array {
  let buf: Uint8Array = new Uint8Array(0);
  if (element.msgType) {
    buf = concat(buf, encodeField(1, WT_LEN, encodeString(element.msgType)));
  }
  if (element.msgContent) {
    const contentBytes = encodeMsgContent(element.msgContent);
    buf = concat(buf, encodeField(2, WT_LEN, encodeMessage(contentBytes)));
  }
  return buf;
}

function decodeMsgBodyElement(data: Uint8Array): MsgBodyElement {
  const fdict = fieldsToDict(parseFields(data));
  const msgType = getString(fdict, 1, "");
  const contentBytes = getBytes(fdict, 2);
  const msgContent = contentBytes.length ? decodeMsgContent(contentBytes) : {};
  return { msgType, msgContent };
}

// ============================================================
// AuthBind 编码
// ============================================================

export function encodeAuthBind(
  bizId: string,
  uid: string,
  source: string,
  token: string,
  msgId: string,
  appVersion = "",
  operationSystem = "",
  botVersion = "",
  routeEnv = "",
): Uint8Array {
  // AuthInfo
  const authBuf = concat(
    encodeField(1, WT_LEN, encodeString(uid)),
    encodeField(2, WT_LEN, encodeString(source)),
    encodeField(3, WT_LEN, encodeString(token)),
  );
  // DeviceInfo
  let devBuf: Uint8Array = new Uint8Array(0);
  if (appVersion) devBuf = concat(devBuf, encodeField(1, WT_LEN, encodeString(appVersion)));
  if (operationSystem) devBuf = concat(devBuf, encodeField(2, WT_LEN, encodeString(operationSystem)));
  devBuf = concat(devBuf, encodeField(10, WT_LEN, encodeString(String(INSTANCE_ID))));
  if (botVersion) devBuf = concat(devBuf, encodeField(24, WT_LEN, encodeString(botVersion)));

  let reqBuf: Uint8Array = concat(
    encodeField(1, WT_LEN, encodeString(bizId)),
    encodeField(2, WT_LEN, encodeMessage(authBuf)),
    encodeField(3, WT_LEN, encodeMessage(devBuf)),
  );
  if (routeEnv) reqBuf = concat(reqBuf, encodeField(5, WT_LEN, encodeString(routeEnv)));

  return encodeConnMsgFull(
    CMD_TYPE.Request,
    CMD.AuthBind,
    nextSeqNo(),
    msgId,
    MODULE.ConnAccess,
    reqBuf,
  );
}

/** 解码 AuthBindRsp */
export function decodeAuthBindRsp(data: Uint8Array): { code: number; message: string; connectId: string } {
  const fdict = fieldsToDict(parseFields(data));
  return {
    code: getVarint(fdict, 1, 0),
    message: getString(fdict, 2, ""),
    connectId: getString(fdict, 3, ""),
  };
}

// ============================================================
// Ping 编码
// ============================================================

export function encodePing(msgId: string): Uint8Array {
  return encodeConnMsgFull(
    CMD_TYPE.Request,
    CMD.Ping,
    nextSeqNo(),
    msgId,
    MODULE.ConnAccess,
    new Uint8Array(0),
  );
}

// ============================================================
// Push ACK 编码
// ============================================================

export function encodePushAck(originalHead: ConnHead): Uint8Array {
  return encodeConnMsgFull(
    CMD_TYPE.PushAck,
    originalHead.cmd,
    nextSeqNo(),
    originalHead.msgId,
    originalHead.module,
    new Uint8Array(0),
  );
}

// ============================================================
// 入站消息解析
// ============================================================

/** 入站消息推送 */
export interface InboundPush {
  callbackCommand: string;
  fromAccount: string;
  toAccount: string;
  senderNickname: string;
  groupId: string;
  groupCode: string;
  groupName: string;
  msgSeq: number;
  msgRandom: number;
  msgTime: number;
  msgKey: string;
  msgId: string;
  msgBody: MsgBodyElement[];
  cloudCustomData: string;
  botOwnerId: string;
  clawMsgType: number;
  privateFromGroupCode: string;
  traceId: string;
}

/** 解码入站消息推送 */
export function decodeInboundPush(data: Uint8Array): InboundPush | null {
  if (!data || data.length === 0) return null;
  try {
    const fdict = fieldsToDict(parseFields(data));

    const msgBody: MsgBodyElement[] = [];
    for (const elBytes of getRepeatedBytes(fdict, 13)) {
      msgBody.push(decodeMsgBodyElement(elBytes));
    }

    const logExtBytes = getBytes(fdict, 20);
    let traceId = "";
    if (logExtBytes.length) {
      const ldict = fieldsToDict(parseFields(logExtBytes));
      traceId = getString(ldict, 1, "");
    }

    return {
      callbackCommand: getString(fdict, 1, ""),
      fromAccount: getString(fdict, 2, ""),
      toAccount: getString(fdict, 3, ""),
      senderNickname: getString(fdict, 4, ""),
      groupId: getString(fdict, 5, ""),
      groupCode: getString(fdict, 6, ""),
      groupName: getString(fdict, 7, ""),
      msgSeq: getVarint(fdict, 8),
      msgRandom: getVarint(fdict, 9),
      msgTime: getVarint(fdict, 10),
      msgKey: getString(fdict, 11, ""),
      msgId: getString(fdict, 12, ""),
      msgBody,
      cloudCustomData: getString(fdict, 14, ""),
      botOwnerId: getString(fdict, 16, ""),
      clawMsgType: getVarint(fdict, 18),
      privateFromGroupCode: getString(fdict, 19, ""),
      traceId,
    };
  } catch {
    return null;
  }
}

// ============================================================
// 出站消息编码
// ============================================================

/** 编码 C2C 私聊消息发送请求 */
export function encodeSendC2CMessage(
  toAccount: string,
  msgBody: MsgBodyElement[],
  fromAccount: string,
  msgId = "",
  msgRandom = 0,
  groupCode = "",
  traceId = "",
): Uint8Array {
  let bizBuf: Uint8Array = new Uint8Array(0);
  if (msgId) bizBuf = concat(bizBuf, encodeField(1, WT_LEN, encodeString(msgId)));
  bizBuf = concat(bizBuf, encodeField(2, WT_LEN, encodeString(toAccount)));
  if (fromAccount) bizBuf = concat(bizBuf, encodeField(3, WT_LEN, encodeString(fromAccount)));
  if (msgRandom) bizBuf = concat(bizBuf, encodeField(4, WT_VARINT, encodeVarint(msgRandom)));
  for (const el of msgBody) {
    const elBytes = encodeMsgBodyElement(el);
    bizBuf = concat(bizBuf, encodeField(5, WT_LEN, encodeMessage(elBytes)));
  }
  if (groupCode) bizBuf = concat(bizBuf, encodeField(6, WT_LEN, encodeString(groupCode)));
  if (traceId) {
    const logBytes = encodeField(1, WT_LEN, encodeString(traceId));
    bizBuf = concat(bizBuf, encodeField(8, WT_LEN, encodeMessage(logBytes)));
  }

  const reqId = msgId || `c2c_${nextSeqNo()}`;
  return encodeConnMsgFull(
    CMD_TYPE.Request,
    "send_c2c_message",
    nextSeqNo(),
    reqId,
    BIZ_PKG,
    bizBuf,
  );
}

/** 编码群消息发送请求 */
export function encodeSendGroupMessage(
  groupCode: string,
  msgBody: MsgBodyElement[],
  fromAccount: string,
  msgId = "",
  refMsgId = "",
  traceId = "",
): Uint8Array {
  let bizBuf: Uint8Array = new Uint8Array(0);
  if (msgId) bizBuf = concat(bizBuf, encodeField(1, WT_LEN, encodeString(msgId)));
  bizBuf = concat(bizBuf, encodeField(2, WT_LEN, encodeString(groupCode)));
  if (fromAccount) bizBuf = concat(bizBuf, encodeField(3, WT_LEN, encodeString(fromAccount)));
  for (const el of msgBody) {
    const elBytes = encodeMsgBodyElement(el);
    bizBuf = concat(bizBuf, encodeField(6, WT_LEN, encodeMessage(elBytes)));
  }
  if (refMsgId) bizBuf = concat(bizBuf, encodeField(7, WT_LEN, encodeString(refMsgId)));
  if (traceId) {
    const logBytes = encodeField(1, WT_LEN, encodeString(traceId));
    bizBuf = concat(bizBuf, encodeField(9, WT_LEN, encodeMessage(logBytes)));
  }

  const reqId = msgId || `grp_${nextSeqNo()}`;
  return encodeConnMsgFull(
    CMD_TYPE.Request,
    "send_group_message",
    nextSeqNo(),
    reqId,
    BIZ_PKG,
    bizBuf,
  );
}

// ============================================================
// Reply Heartbeat 编码
// ============================================================

/** 编码私聊心跳 */
export function encodeSendPrivateHeartbeat(
  fromAccount: string,
  toAccount: string,
  heartbeat: number = WS_HEARTBEAT_RUNNING,
): Uint8Array {
  const buf = concat(
    encodeField(1, WT_LEN, encodeString(fromAccount)),
    encodeField(2, WT_LEN, encodeString(toAccount)),
    encodeField(3, WT_VARINT, encodeVarint(heartbeat)),
  );
  const reqId = `hb_priv_${nextSeqNo()}`;
  return encodeConnMsgFull(
    CMD_TYPE.Request,
    "send_private_heartbeat",
    nextSeqNo(),
    reqId,
    BIZ_PKG,
    buf,
  );
}

/** 编码群聊心跳 */
export function encodeSendGroupHeartbeat(
  fromAccount: string,
  groupCode: string,
  heartbeat: number = WS_HEARTBEAT_RUNNING,
): Uint8Array {
  const ts = Date.now();
  const buf = concat(
    encodeField(1, WT_LEN, encodeString(fromAccount)),
    encodeField(2, WT_LEN, encodeString("")),
    encodeField(3, WT_LEN, encodeString(groupCode)),
    encodeField(4, WT_VARINT, encodeVarint(ts)),
    encodeField(5, WT_VARINT, encodeVarint(heartbeat)),
  );
  const reqId = `hb_grp_${nextSeqNo()}`;
  return encodeConnMsgFull(
    CMD_TYPE.Request,
    "send_group_heartbeat",
    nextSeqNo(),
    reqId,
    BIZ_PKG,
    buf,
  );
}
