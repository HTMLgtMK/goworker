import { SocketAcpAgentTransport, type AcpAgentTransportConnection } from '@htmlgtmk/acp-ui';

// daemon session/list 里的一条会话摘要（对齐 Go 侧 protocol.SessionInfo）。
export type AcpSessionListEntry = {
  sessionId: string;
  cwd?: string;
  title?: string;
  updatedAt?: string;
};

export type FetchAcpSessionListOptions = {
  /** 整条短连接的截止时间；超时抛错并断开。 */
  timeoutMs?: number;
};

const defaultTimeoutMs = 5_000;
const initializeRequestId = 1;
const listRequestId = 2;

/**
 * sidebar 的轻量会话清单连接：一条短连接只做 initialize + session/list，用完即断。
 *
 * 为什么手写最小 NDJSON JSON-RPC 而不用 SDK 的 AcpAgentProcess.listAllSessions：
 * 后者以 `agentCapabilities.sessionCapabilities.list` 为门控，GOWORKER daemon 只声明
 * `loadSession`（能力协商照实上报），过不了该门控，会被 SDK 拒发 session/list。
 * 传输层仍复用 SDK 的 SocketAcpAgentTransport（unix socket + web streams），
 * 这里只补 JSON-RPC 请求/响应关联与 framing。
 */
export async function fetchAcpSessionList(
  socketPath: string,
  options: FetchAcpSessionListOptions = {},
): Promise<AcpSessionListEntry[]> {
  const transport = new SocketAcpAgentTransport({ socketPath });
  const connection = await transport.connect({ agentName: 'goworker-session-list' });
  try {
    return await withTimeout(listSessions(connection), options.timeoutMs ?? defaultTimeoutMs);
  } finally {
    connection.dispose();
  }
}

// 窄化 session/list 响应体：保留 sessionId 非空字符串的条目，其余字段按字符串取。
// 导出给单元测试直接覆盖 wire 形状。
export function narrowSessionListEntries(result: unknown): AcpSessionListEntry[] {
  if (typeof result !== 'object' || result === null) return [];
  const sessions = (result as { sessions?: unknown }).sessions;
  if (!Array.isArray(sessions)) return [];
  const out: AcpSessionListEntry[] = [];
  for (const item of sessions) {
    if (typeof item !== 'object' || item === null) continue;
    const source = item as Record<string, unknown>;
    if (typeof source['sessionId'] !== 'string' || source['sessionId'].length === 0) continue;
    const entry: AcpSessionListEntry = { sessionId: source['sessionId'] };
    for (const key of ['cwd', 'title', 'updatedAt'] as const) {
      const value = source[key];
      if (typeof value === 'string' && value.length > 0) entry[key] = value;
    }
    out.push(entry);
  }
  return out;
}

async function listSessions(connection: AcpAgentTransportConnection): Promise<AcpSessionListEntry[]> {
  const writer = connection.toAgent.getWriter();
  const reader = connection.fromAgent.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = '';

  const send = async (id: number, method: string, params: unknown): Promise<void> => {
    await writer.write(encoder.encode(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`));
  };

  // 按 id 取响应：逐行读，跳过 daemon 的 notification 与其他响应。
  const awaitResponse = async (id: number): Promise<Record<string, unknown>> => {
    for (;;) {
      const newline = buffer.indexOf('\n');
      if (newline === -1) {
        const chunk = await reader.read();
        if (chunk.done) throw new Error('ACP daemon closed the session list connection before responding');
        // { stream: true } 保留多字节字符跨 chunk 的拆分状态
        buffer += decoder.decode(chunk.value, { stream: true });
        continue;
      }
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      if (line.length === 0) continue;
      let message: unknown;
      try {
        message = JSON.parse(line);
      } catch {
        continue; // 非 JSON 行（对端噪音）忽略，与 daemon 侧 jsonrpc.dispatch 同策略
      }
      if (isResponseFor(message, id)) return message;
    }
  };

  try {
    await send(initializeRequestId, 'initialize', { protocolVersion: 1 });
    throwIfRpcError('initialize', await awaitResponse(initializeRequestId));
    await send(listRequestId, 'session/list', {});
    const list = await awaitResponse(listRequestId);
    throwIfRpcError('session/list', list);
    return narrowSessionListEntries(list['result']);
  } finally {
    writer.releaseLock();
    await reader.cancel().catch(() => undefined);
  }
}

function isResponseFor(message: unknown, id: number): message is Record<string, unknown> {
  if (typeof message !== 'object' || message === null) return false;
  const record = message as Record<string, unknown>;
  return record['method'] === undefined && record['id'] === id;
}

function throwIfRpcError(method: string, response: Record<string, unknown>): void {
  const error = response['error'];
  if (typeof error !== 'object' || error === null) return;
  const record = error as Record<string, unknown>;
  const code = typeof record['code'] === 'number' ? record['code'] : 'unknown';
  const message = typeof record['message'] === 'string' ? record['message'] : 'unknown error';
  throw new Error(`${method} failed (rpc ${code}): ${message}`);
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}
