import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  AcpSessionBridge,
  MockAcpNdjsonDaemon,
  type ExtensionToWebviewMessage,
} from '@htmlgtmk/acp-ui';
import { createAcpSessionHostRuntime, createGoworkerAgentSpawnConfig } from '../../src/acpUi/hostRuntime';

// 集成回归（不 import vscode）：用 SDK 的 MockAcpNdjsonDaemon + SocketAcpAgentTransport
// 走通生产壳的 hostRuntime 注入点与 AcpSessionBridge 全链路。
// connect(session/new) → prompt → postToWebview 收到流式 appendAgentText → turnComplete；
// cancel → 回合以 stopReason "cancelled" 收束。

function tick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('bridge full chain over the mock daemon: connect, prompt streams agent text, turn completes', async () => {
  const received: ExtensionToWebviewMessage[] = [];
  const daemon = await MockAcpNdjsonDaemon.start({
    onPrompt: (mock, params) => {
      mock.notifySessionUpdate(params.sessionId, {
        sessionUpdate: 'agent_message_chunk',
        content: { type: 'text', text: 'hello from agent' },
      });
      return 'end_turn';
    },
  });
  try {
    const workspaceRoot = mkdtempSync(join(tmpdir(), 'acp-host-'));
    const runtime = createAcpSessionHostRuntime({ getWorkspaceRoot: () => workspaceRoot });
    const bridge = new AcpSessionBridge(
      createGoworkerAgentSpawnConfig(daemon.socketPath),
      (message) => received.push(message),
      runtime,
    );

    await bridge.connect();

    // connect 全链路：initialize 往返 + session/new（cwd 来自 getWorkspaceRoot）
    assert.equal(daemon.recordedByMethod('initialize').length, 1);
    assert.equal(
      (daemon.recordedByMethod('initialize')[0]?.params as { protocolVersion?: number } | undefined)
        ?.protocolVersion,
      1,
    );
    assert.deepEqual(daemon.recordedByMethod('session/new')[0]?.params, {
      cwd: workspaceRoot,
      mcpServers: [],
    });
    assert.equal(bridge.sessionId, 'mock-session');
    await daemon.waitForClientConnected();

    await bridge.prompt('run the tests');
    await tick(); // 等 NDJSON 流上的 session/update 通知处理落地

    const texts = received.filter((message) => message.type === 'appendAgentText');
    assert.deepEqual(
      texts.map((message) => (message.type === 'appendAgentText' ? message.text : '')),
      ['hello from agent'],
    );
    const completions = received.filter((message) => message.type === 'turnComplete');
    assert.deepEqual(
      completions.map((message) => (message.type === 'turnComplete' ? message.stopReason : '')),
      ['end_turn'],
    );

    bridge.dispose();
    // dispose 必须关掉 socket 连接（不挂起：2s 内未断开则断言失败）
    await Promise.race([daemon.waitForClientDisconnected(), delay(2000)]);
    assert.ok(daemon.clientDisconnectCount >= 1);
  } finally {
    await daemon.close();
  }
});

test('cancel settles the in-flight turn with stopReason "cancelled"', async () => {
  const received: ExtensionToWebviewMessage[] = [];
  let cancelled = false;
  const daemon = await MockAcpNdjsonDaemon.start({
    // 模拟长回合：阻塞 prompt 响应，直到 session/cancel 通知到达
    onPrompt: async (mock, params) => {
      while (!cancelled) await tick();
      mock.notifySessionUpdate(params.sessionId, {
        sessionUpdate: 'agent_message_chunk',
        content: { type: 'text', text: 'partial output' },
      });
      return 'cancelled';
    },
    onCancel: () => {
      cancelled = true;
    },
  });
  try {
    const runtime = createAcpSessionHostRuntime({ getWorkspaceRoot: () => tmpdir() });
    const bridge = new AcpSessionBridge(
      createGoworkerAgentSpawnConfig(daemon.socketPath),
      (message) => received.push(message),
      runtime,
    );
    await bridge.connect();

    const prompting = bridge.prompt('long running turn');
    while (!bridge.isPrompting) await tick();
    assert.deepEqual(daemon.recordedByMethod('session/cancel'), []);

    await bridge.cancel();
    await prompting;
    await tick();

    assert.equal(daemon.recordedByMethod('session/cancel').length, 1);
    assert.deepEqual(
      received
        .filter((message) => message.type === 'turnComplete')
        .map((message) => (message.type === 'turnComplete' ? message.stopReason : '')),
      ['cancelled'],
    );

    bridge.dispose();
  } finally {
    await daemon.close();
  }
});
