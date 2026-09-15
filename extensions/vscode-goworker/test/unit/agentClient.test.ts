import assert from 'node:assert/strict';
import test from 'node:test';
import { AgentClient, type ACPRpcTransport } from '../../src/acp/agentClient';
import {
  isAgentMessageChunk,
  isAgentThoughtChunk,
  isToolCall,
  isToolCallUpdate,
  type ACPUpdate,
} from '../../src/contracts/messages';

interface FakeTransport extends ACPRpcTransport {
  readonly calls: Array<{ method: string; params?: unknown }>;
  readonly notifyCalls: Array<{ method: string; params?: unknown }>;
  readonly listenerCount: number;
  connectCalls: number;
  closeCalls: number;
  resolveLast(method: string, result: unknown): void;
  rejectLast(method: string, error: Error): void;
  emit(method: string, params: unknown): void;
  setConnected(value: boolean): void;
}

// 极简 fake transport：记录 request/notify 调用，允许测试手动 resolve/reject 与注入 server 通知。
// 生产路径仍由真实 ACPConnection 承载（结构化满足 ACPRpcTransport）。
function createFakeTransport(): FakeTransport {
  const calls: Array<{ method: string; params?: unknown }> = [];
  const notifyCalls: Array<{ method: string; params?: unknown }> = [];
  const pending: Array<{ method: string; resolve(value: unknown): void; reject(error: Error): void }> = [];
  const listeners = new Set<(method: string, params: unknown) => void>();
  let connected = false;
  const transport: FakeTransport = {
    calls,
    notifyCalls,
    connectCalls: 0,
    closeCalls: 0,
    get listenerCount(): number {
      return listeners.size;
    },
    get connected(): boolean {
      return connected;
    },
    async connect(): Promise<void> {
      if (connected) return;
      transport.connectCalls += 1;
      connected = true;
    },
    request<T>(method: string, params?: unknown): Promise<T> {
      calls.push({ method, params });
      return new Promise<unknown>((resolve, reject) => {
        pending.push({ method, resolve, reject });
      }) as Promise<T>;
    },
    notify(method: string, params?: unknown): void {
      notifyCalls.push({ method, params });
    },
    onNotification(listener: (method: string, params: unknown) => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    close(): void {
      transport.closeCalls += 1;
      connected = false;
    },
    resolveLast(method: string, result: unknown): void {
      const index = pending.findIndex((entry) => entry.method === method);
      assert.ok(index >= 0, `no pending request for ${method}`);
      const [entry] = pending.splice(index, 1);
      entry.resolve(result);
    },
    rejectLast(method: string, error: Error): void {
      const index = pending.findIndex((entry) => entry.method === method);
      assert.ok(index >= 0, `no pending request for ${method}`);
      const [entry] = pending.splice(index, 1);
      entry.reject(error);
    },
    emit(method: string, params: unknown): void {
      for (const listener of listeners) listener(method, params);
    },
    setConnected(value: boolean): void {
      connected = value;
    },
  };
  return transport;
}

// 让挂起的微任务（promise 链）全部落地。
function tick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function update(kind: string, extra: Partial<ACPUpdate> = {}): ACPUpdate {
  return { sessionUpdate: kind, ...extra };
}

test('prompt sends the prompt text as a session/prompt content block', () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');

  client.prompt('sess-1', 'run the failing tests', () => {}, () => {});

  assert.deepEqual(transport.calls, [
    {
      method: 'session/prompt',
      params: { sessionId: 'sess-1', prompt: [{ type: 'text', text: 'run the failing tests' }] },
    },
  ]);
});

test('newSession connects, initializes once, and sends session/new with cwd', async () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');

  const first = client.newSession();
  await tick();
  transport.resolveLast('initialize', {});
  await tick();
  transport.resolveLast('session/new', { sessionId: 'sess-1' });
  assert.equal(await first, 'sess-1');

  assert.deepEqual(transport.calls.map((call) => call.method), ['initialize', 'session/new']);
  assert.deepEqual(transport.calls[0].params, { protocolVersion: 1, clientCapabilities: {} });
  assert.deepEqual(transport.calls[1].params, { cwd: '/repo', mcpServers: [] });

  // 已初始化的连接复用：下一次 newSession 不再重发 initialize
  const second = client.newSession();
  await tick();
  transport.resolveLast('session/new', { sessionId: 'sess-2' });
  assert.equal(await second, 'sess-2');
  assert.deepEqual(transport.calls.map((call) => call.method), ['initialize', 'session/new', 'session/new']);
});

test('connect initializes once concurrently and re-initializes after a reconnect', async () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');

  const both = Promise.all([client.connect(), client.connect()]);
  await tick();
  transport.resolveLast('initialize', {});
  await both;
  assert.equal(transport.connectCalls, 1);
  assert.equal(transport.calls.filter((call) => call.method === 'initialize').length, 1);

  // 断线重连后必须重新 initialize
  transport.setConnected(false);
  const reconnect = client.connect();
  await tick();
  transport.resolveLast('initialize', {});
  await reconnect;
  assert.equal(transport.connectCalls, 2);
  assert.equal(transport.calls.filter((call) => call.method === 'initialize').length, 2);
});

test('prompt streams only the matching session updates to onUpdate', () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');
  const mine: ACPUpdate[] = [];
  const theirs: ACPUpdate[] = [];

  client.prompt('sess-1', 'hi', (received) => mine.push(received), () => {});
  client.prompt('sess-2', 'hi', (received) => theirs.push(received), () => {});

  transport.emit('session/update', { sessionId: 'sess-1', update: update('agent_message_chunk', { content: { type: 'text', text: 'hello' } }) });
  transport.emit('session/update', { sessionId: 'sess-2', update: update('agent_message_chunk', { content: { type: 'text', text: 'other' } }) });
  transport.emit('session/update', { sessionId: 'sess-1', update: update('agent_thought_chunk', { content: { type: 'text', text: 'thinking' } }) });
  transport.emit('session/update', { sessionId: 'sess-1', update: update('tool_call', { toolCallId: 't1', title: 'Read', status: 'pending' }) });
  transport.emit('session/update', { sessionId: 'sess-1', update: update('tool_call_update', { toolCallId: 't1', title: 'Read', status: 'completed' }) });
  transport.emit('session/update', { sessionId: 'sess-1', update: update('future_update', { feature: true }) });
  transport.emit('other/notification', { sessionId: 'sess-1' });
  transport.emit('session/update', { update: update('agent_message_chunk', { content: { type: 'text', text: 'no session id' } }) });

  assert.deepEqual(mine.map((received) => received.sessionUpdate), [
    'agent_message_chunk', 'agent_thought_chunk', 'tool_call', 'tool_call_update', 'future_update',
  ]);
  assert.deepEqual(theirs.map((received) => received.sessionUpdate), ['agent_message_chunk']);
  assert.equal(mine[2].title, 'Read');
});

test('prompt completion resolves without an error and rejects with the RPC error', async () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');
  const completions: Array<Error | undefined> = [];

  client.prompt('sess-1', 'hi', () => {}, (error) => completions.push(error));
  transport.resolveLast('session/prompt', { stopReason: 'end_turn' });
  await tick();
  assert.equal(completions.length, 1);
  assert.equal(completions[0], undefined);
  assert.equal(transport.listenerCount, 0);

  client.prompt('sess-2', 'hi', () => {}, (error) => completions.push(error));
  transport.rejectLast('session/prompt', new Error('RPC -32603: worker exploded'));
  await tick();
  assert.equal(completions.length, 2);
  assert.match(completions[1]?.message ?? '', /RPC -32603: worker exploded/);
  assert.equal(transport.listenerCount, 0);
});

test('cancel sends a session/cancel notification', () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');

  client.cancel('sess-1');

  assert.deepEqual(transport.notifyCalls, [{ method: 'session/cancel', params: { sessionId: 'sess-1' } }]);
});

test('dispose cancels the session, detaches listeners and swallows late callbacks', async () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');
  const updates: ACPUpdate[] = [];
  const completions: Array<Error | undefined> = [];

  const handle = client.prompt('sess-1', 'hi', (received) => updates.push(received), (error) => completions.push(error));
  handle.dispose();
  handle.dispose(); // 幂等：不重复发 cancel

  assert.deepEqual(transport.notifyCalls, [{ method: 'session/cancel', params: { sessionId: 'sess-1' } }]);

  // dispose 之后到达的 update 与 resolve/reject 全部忽略
  transport.emit('session/update', { sessionId: 'sess-1', update: update('agent_message_chunk', { content: { type: 'text', text: 'late' } }) });
  transport.resolveLast('session/prompt', { stopReason: 'end_turn' });
  await tick();
  assert.deepEqual(updates, []);
  // 注意：deepEqual(x, []) 会把 x 断言收窄为 never[]，此处用 length 判断以便后续继续 push。
  assert.equal(completions.length, 0);

  // 主动取消触发的 reject（RPC -32000 context canceled）不冒泡为失败
  const cancelled = client.prompt('sess-2', 'hi', () => {}, (error) => completions.push(error));
  cancelled.dispose();
  transport.rejectLast('session/prompt', new Error('RPC -32000: context canceled'));
  await tick();
  assert.deepEqual(completions, []);
  assert.deepEqual(transport.notifyCalls, [
    { method: 'session/cancel', params: { sessionId: 'sess-1' } },
    { method: 'session/cancel', params: { sessionId: 'sess-2' } },
  ]);
});

test('disconnect resets initialization state and closes the connection', () => {
  const transport = createFakeTransport();
  const client = new AgentClient(transport, '/repo');

  client.disconnect();

  assert.equal(transport.closeCalls, 1);
  assert.equal(client.connected, false);
});

test('agent stream guards narrow update kinds without guessing fields', () => {
  const message = update('agent_message_chunk', { content: { type: 'text', text: 'hello' } });
  const thought = update('agent_thought_chunk', { content: { type: 'text', text: 'hmm' } });
  const call = update('tool_call', { toolCallId: 't1', title: 'Read', status: 'pending' });
  const callUpdate = update('tool_call_update', { toolCallId: 't1', status: 'completed' });

  assert.equal(isAgentMessageChunk(message), true);
  assert.equal(isAgentMessageChunk(thought), false);
  assert.equal(isAgentThoughtChunk(thought), true);
  assert.equal(isAgentThoughtChunk(message), false);
  assert.equal(isToolCall(call), true);
  assert.equal(isToolCall(callUpdate), false);
  assert.equal(isToolCallUpdate(callUpdate), true);
  assert.equal(isToolCallUpdate(call), false);

  // 缺少 content.text 或未知 kind 时不误判
  assert.equal(isAgentMessageChunk(update('agent_message_chunk')), false);
  assert.equal(isAgentThoughtChunk(update('agent_thought_chunk', { content: { type: 'text' } })), false);
  assert.equal(isAgentMessageChunk(update('future_update')), false);
  assert.equal(isToolCall(update('future_update')), false);

  // 窄化后字段可安全访问
  if (isToolCall(call)) assert.equal(call.title, 'Read');
  if (isAgentMessageChunk(message)) assert.equal(message.content.text, 'hello');
});
