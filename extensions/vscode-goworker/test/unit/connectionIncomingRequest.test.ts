import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer, type Server } from 'node:net';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ACPConnection } from '../../src/acp/connection';

// 起一个最小 NDJSON-RPC 服务端：连上后主动向客户端发一个请求，把收到的应答
// 通过 resolve 交回测试。
async function withServer(
  sendToClient: (write: (message: unknown) => void) => void,
): Promise<{ socketPath: string; received: Promise<unknown>; close: () => Promise<void> }> {
  const dir = await mkdtemp(join(tmpdir(), 'goworker-acp-'));
  const socketPath = join(dir, 'test.sock');
  let resolveReceived: (value: unknown) => void;
  const received = new Promise<unknown>((resolve) => {
    resolveReceived = resolve;
  });

  const server: Server = createServer((socket) => {
    socket.setEncoding('utf8');
    let buffer = '';
    socket.on('data', (chunk: string) => {
      buffer += chunk;
      for (;;) {
        const newline = buffer.indexOf('\n');
        if (newline < 0) return;
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (line.trim() === '') continue;
        const parsed = JSON.parse(line) as Record<string, unknown>;
        // 只关心客户端对我们那个请求的应答（带 id 且无 method）。
        if ('id' in parsed && !('method' in parsed)) resolveReceived(parsed);
      }
    });
    sendToClient((message) => socket.write(`${JSON.stringify(message)}\n`));
  });

  await new Promise<void>((resolve) => server.listen(socketPath, resolve));
  return {
    socketPath,
    received,
    close: async () => {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(dir, { recursive: true, force: true });
    },
  };
}

// 关键回归：服务端发来的**请求**（带 id 且带 method）以前被当成响应处理、
// 在 pending 表里查不到就静默丢弃，对端于是永远等不到应答 —— daemon 的
// permission 请求就是这样挂死的。
test('answers an incoming request instead of dropping it', async () => {
  const server = await withServer((write) => {
    write({
      jsonrpc: '2.0',
      id: 'server-1',
      method: 'session/request_permission',
      params: { sessionId: 'sess-1' },
    });
  });
  try {
    const connection = new ACPConnection(server.socketPath);
    connection.onRequest('session/request_permission', async () => ({
      outcome: { outcome: 'selected', optionId: 'allow_once' },
    }));
    await connection.connect();

    const response = (await server.received) as Record<string, unknown>;
    assert.equal(response.id, 'server-1');
    assert.deepEqual(response.result, { outcome: { outcome: 'selected', optionId: 'allow_once' } });
    connection.close();
  } finally {
    await server.close();
  }
});

// 没注册处理器时必须显式回 -32601（method not found），而不是静默丢弃：
// daemon 侧靠这个错误码区分"该客户端没这个能力"，据此换下一个订阅者。
test('replies method-not-found when no handler is registered', async () => {
  const server = await withServer((write) => {
    write({ jsonrpc: '2.0', id: 'server-2', method: 'session/request_permission', params: {} });
  });
  try {
    const connection = new ACPConnection(server.socketPath);
    await connection.connect();

    const response = (await server.received) as { error?: { code: number } };
    assert.equal(response.error?.code, -32601);
    connection.close();
  } finally {
    await server.close();
  }
});

// 处理器抛错 → RPC error（-32000），不是把错误当成 result 回过去。
test('turns a handler failure into an RPC error', async () => {
  const server = await withServer((write) => {
    write({ jsonrpc: '2.0', id: 'server-3', method: 'session/request_permission', params: {} });
  });
  try {
    const connection = new ACPConnection(server.socketPath);
    connection.onRequest('session/request_permission', async () => {
      throw new Error('boom');
    });
    await connection.connect();

    const response = (await server.received) as { error?: { code: number; message: string } };
    assert.equal(response.error?.code, -32000);
    assert.match(response.error?.message ?? '', /boom/);
    connection.close();
  } finally {
    await server.close();
  }
});
