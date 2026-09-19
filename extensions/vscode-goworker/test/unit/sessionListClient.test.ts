import assert from 'node:assert/strict';
import * as net from 'node:net';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fetchAcpSessionList, narrowSessionListEntries } from '../../src/acpUi/sessionListClient';

// 轻量 session/list 短连接回归：本地 NDJSON JSON-RPC mock daemon 走真实 unix socket，
// 覆盖 initialize → session/list → 断开全链路与各类失败路径。

type WireMessage = {
  jsonrpc?: string;
  id?: number | string;
  method?: string;
  params?: unknown;
  result?: unknown;
  error?: { code: number; message: string };
};

type ListDaemon = {
  socketPath: string;
  stats: { connected: number; disconnected: number };
  received: WireMessage[];
  close(): Promise<void>;
};

function send(socket: net.Socket, message: WireMessage): void {
  socket.write(`${JSON.stringify(message)}\n`);
}

function startListDaemon(
  handleMessage: (message: WireMessage, socket: net.Socket, daemon: ListDaemon) => void,
): Promise<ListDaemon> {
  const dir = mkdtempSync(join(tmpdir(), 'goworker-list-'));
  const socketPath = join(dir, 'vscode.sock');
  const daemon: ListDaemon = {
    socketPath,
    stats: { connected: 0, disconnected: 0 },
    received: [],
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  };
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    daemon.stats.connected += 1;
    sockets.add(socket);
    socket.on('close', () => {
      daemon.stats.disconnected += 1;
      sockets.delete(socket);
    });
    let buffer = '';
    socket.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      let newline = buffer.indexOf('\n');
      while (newline !== -1) {
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (line.length > 0) {
          const message = JSON.parse(line) as WireMessage;
          daemon.received.push(message);
          handleMessage(message, socket, daemon);
        }
        newline = buffer.indexOf('\n');
      }
    });
  });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => resolve(daemon));
  });
}

// 标准 daemon：initialize 正常应答，session/list 交给用例自定义。
function standardDaemon(
  onSessionList: (message: WireMessage, socket: net.Socket, daemon: ListDaemon) => void,
): (message: WireMessage, socket: net.Socket, daemon: ListDaemon) => void {
  return (message, socket, daemon) => {
    if (message.method === 'initialize') {
      send(socket, {
        jsonrpc: '2.0',
        id: message.id,
        result: { protocolVersion: 1, agentCapabilities: { loadSession: true } },
      });
      return;
    }
    onSessionList(message, socket, daemon);
  };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('fetchAcpSessionList runs initialize + session/list over one short connection', async () => {
  const daemon = await startListDaemon(
    standardDaemon((message, socket) => {
      // 先插一条 notification（如 session/update）：客户端必须跳过它继续等响应。
      send(socket, { jsonrpc: '2.0', method: 'session/update', params: { sessionId: 'noise' } });
      send(socket, {
        jsonrpc: '2.0',
        id: message.id,
        result: {
          sessions: [
            { sessionId: 'current-1', cwd: '', title: 'fix the bug', updatedAt: '2026-09-16T10:00:00Z', isCurrent: true },
            { sessionId: 'archive-9', title: 'older chat' },
          ],
        },
      });
    }),
  );
  try {
    const sessions = await fetchAcpSessionList(daemon.socketPath);

    assert.deepEqual(
      daemon.received.map((message) => message.method),
      ['initialize', 'session/list'],
    );
    assert.equal(daemon.received[0]?.params !== undefined, true); // initialize 带 params
    assert.deepEqual(sessions, [
      // daemon 的 cwd 恒为空串（jsonl 不落 cwd），窄化时丢弃空字段；当前会话保留 isCurrent: true。
      { sessionId: 'current-1', title: 'fix the bug', updatedAt: '2026-09-16T10:00:00Z', isCurrent: true },
      // 归档会话按 omitempty 不带 isCurrent，窄化后同样缺省。
      { sessionId: 'archive-9', title: 'older chat' },
    ]);

    // 用完即断：短连接由客户端主动关闭。
    await delay(50);
    assert.equal(daemon.stats.connected, 1);
    assert.equal(daemon.stats.disconnected, 1);
  } finally {
    await daemon.close();
  }
});

test('fetchAcpSessionList surfaces session/list RPC errors', async () => {
  const daemon = await startListDaemon(
    standardDaemon((message, socket) => {
      send(socket, {
        jsonrpc: '2.0',
        id: message.id,
        error: { code: -32000, message: 'dispatch: session list not supported' },
      });
    }),
  );
  try {
    await assert.rejects(fetchAcpSessionList(daemon.socketPath), (error: Error) => {
      assert.match(error.message, /rpc -32000/);
      assert.match(error.message, /session list not supported/);
      return true;
    });
  } finally {
    await daemon.close();
  }
});

test('fetchAcpSessionList rejects when the daemon hangs up before answering', async () => {
  const daemon = await startListDaemon(
    standardDaemon((_message, socket) => {
      socket.destroy();
    }),
  );
  try {
    await assert.rejects(fetchAcpSessionList(daemon.socketPath), /closed the session list connection/);
  } finally {
    await daemon.close();
  }
});

test('fetchAcpSessionList times out when the daemon never answers', async () => {
  const daemon = await startListDaemon(() => undefined); // 收到请求也不回
  try {
    await assert.rejects(fetchAcpSessionList(daemon.socketPath, { timeoutMs: 150 }), /timed out after 150ms/);
  } finally {
    await daemon.close();
  }
});

test('fetchAcpSessionList rejects when the daemon socket does not exist', async () => {
  const missing = join(mkdtempSync(join(tmpdir(), 'goworker-missing-')), 'nope.sock');
  await assert.rejects(fetchAcpSessionList(missing), /Failed to connect to ACP daemon socket/);
});

// isCurrent 必须保留完整布尔值：false 是「这条是归档」的有效信息，抹成 undefined
// 就与「旧版 wire 不表达该字段」混为一谈，上层只能靠位置猜。
test('narrowSessionListEntries preserves isCurrent booleans (both true and false)', () => {
  assert.deepEqual(
    narrowSessionListEntries({
      sessions: [
        { sessionId: 'live', isCurrent: true },
        { sessionId: 'off', isCurrent: false },
        { sessionId: 'str', isCurrent: 'true' },
        { sessionId: 'absent' },
      ],
    }),
    [
      { sessionId: 'live', isCurrent: true },
      { sessionId: 'off', isCurrent: false },
      { sessionId: 'str' }, // 非布尔一律丢弃，不猜
      { sessionId: 'absent' },
    ],
  );
});

test('narrowSessionListEntries keeps only entries with a usable sessionId', () => {
  assert.deepEqual(
    narrowSessionListEntries({
      sessions: [
        { sessionId: 's1', cwd: '/tmp', title: 't', updatedAt: '2026-09-16T10:00:00Z' },
        { title: 'no id' },
        null,
        { sessionId: '' },
        { sessionId: 's2' },
      ],
    }),
    [
      { sessionId: 's1', cwd: '/tmp', title: 't', updatedAt: '2026-09-16T10:00:00Z' },
      { sessionId: 's2' },
    ],
  );
  assert.deepEqual(narrowSessionListEntries({ sessions: null }), []);
  assert.deepEqual(narrowSessionListEntries({}), []);
  assert.deepEqual(narrowSessionListEntries('garbage'), []);
});
