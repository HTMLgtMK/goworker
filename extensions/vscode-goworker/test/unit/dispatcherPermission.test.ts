import assert from 'node:assert/strict';
import test from 'node:test';
import {
  DispatcherClient,
  narrowPermissionRequest,
  type TaskPermissionDecision,
  type TaskPermissionRequest,
} from '../../src/acp/dispatcherClient';
import type { ACPConnection } from '../../src/acp/connection';

// 只实现 DispatcherClient 用到的连接面。request/notify/onNotification 给空实现
// （本文件不驱动 prompt），onRequest 捕获处理器以便直接调用。
class FakeConnection {
  handler?: (params: unknown) => Promise<unknown>;

  onRequest(method: string, handler: (params: unknown) => Promise<unknown>): () => void {
    assert.equal(method, 'session/request_permission');
    this.handler = handler;
    return () => {
      this.handler = undefined;
    };
  }

  onNotification(): () => void {
    return () => undefined;
  }

  async request(): Promise<never> {
    throw new Error('not used');
  }

  notify(): void {}
}

function newClient(handler?: (request: TaskPermissionRequest) => Promise<TaskPermissionDecision>) {
  const connection = new FakeConnection();
  const client = new DispatcherClient(
    connection as unknown as ACPConnection,
    '/tmp/repo',
    handler,
  );
  return { connection, client };
}

function validParams() {
  return {
    sessionId: 'sess-1',
    toolCall: { toolCallId: 'call-1', title: 'rm -rf build/' },
    options: [
      { optionId: 'allow_once', name: 'Allow once', kind: 'allow_once' },
      { optionId: 'reject_once', name: 'Reject', kind: 'reject_once' },
    ],
  };
}

// 处理器必须在**构造时**就注册：连接可能重连，且 daemon 的请求可能早于任何 prompt。
test('registers the permission handler on construction', () => {
  const { connection } = newClient(async () => ({ outcome: 'cancelled' }));
  assert.equal(typeof connection.handler, 'function');
});

test('forwards the user selection as an ACP selected outcome', async () => {
  const { connection } = newClient(async () => ({ outcome: 'selected', optionId: 'allow_once' }));

  const result = await connection.handler?.(validParams());

  // 线上形状必须是嵌套的 {"outcome":{"outcome":"selected",...}}：
  // 扁平形状对端 schema 校验不过，会当成取消/拒绝。
  assert.deepEqual(result, { outcome: { outcome: 'selected', optionId: 'allow_once' } });
});

test('maps a dismissed dialog to a cancelled outcome', async () => {
  const { connection } = newClient(async () => ({ outcome: 'cancelled' }));

  const result = await connection.handler?.(validParams());

  assert.deepEqual(result, { outcome: { outcome: 'cancelled' } });
});

// 失败方向必须是拒绝：未接 UI 时不能回 selected。
test('cancels when no handler is wired', async () => {
  const { connection } = newClient(undefined);

  const result = await connection.handler?.(validParams());

  assert.deepEqual(result, { outcome: { outcome: 'cancelled' } });
});

test('cancels when the handler throws', async () => {
  const { connection } = newClient(async () => {
    throw new Error('webview blew up');
  });

  const result = await connection.handler?.(validParams());

  assert.deepEqual(result, { outcome: { outcome: 'cancelled' } });
});

// 空 optionId 不能当成有效选择放行。
test('cancels on an empty option id', async () => {
  const { connection } = newClient(async () => ({ outcome: 'selected', optionId: '' }));

  const result = await connection.handler?.(validParams());

  assert.deepEqual(result, { outcome: { outcome: 'cancelled' } });
});

test('cancels on malformed params instead of guessing', async () => {
  const { connection } = newClient(async () => ({ outcome: 'selected', optionId: 'allow_once' }));

  for (const params of [undefined, {}, { sessionId: 's' }, { ...validParams(), options: [] }]) {
    const result = await connection.handler?.(params);
    assert.deepEqual(result, { outcome: { outcome: 'cancelled' } }, `params=${JSON.stringify(params)}`);
  }
});

test('narrowPermissionRequest keeps only rendered fields', () => {
  const narrowed = narrowPermissionRequest({
    ...validParams(),
    _meta: { whatever: true },
    toolCall: { toolCallId: 'call-1', title: 'rm -rf build/', extra: 'dropped' },
  });

  assert.equal(narrowed?.sessionId, 'sess-1');
  assert.equal(narrowed?.toolCall.title, 'rm -rf build/');
  assert.deepEqual(narrowed?.options, [
    { optionId: 'allow_once', name: 'Allow once', kind: 'allow_once' },
    { optionId: 'reject_once', name: 'Reject', kind: 'reject_once' },
  ]);
});
