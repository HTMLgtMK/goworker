import assert from 'node:assert/strict';
import test from 'node:test';
import {
  lastSessionStorageKey,
  readLastSessionId,
  writeLastSessionId,
  type SessionIdStore,
} from '../../src/acpUi/sessionRecord';

// 「上次会话」记录逻辑：假 memento 驱动，不 import vscode。

class FakeMemento implements SessionIdStore {
  private readonly values = new Map<string, unknown>();

  get(key: string): unknown {
    return this.values.get(key);
  }

  async update(key: string, value: unknown): Promise<void> {
    this.values.set(key, value);
  }
}

test('readLastSessionId starts empty and round-trips a recorded id', () => {
  const store = new FakeMemento();
  assert.equal(readLastSessionId(store), undefined);

  writeLastSessionId(store, 'session-1');
  assert.equal(readLastSessionId(store), 'session-1');
  assert.equal(store.get(lastSessionStorageKey), 'session-1');
});

test('readLastSessionId treats blank records as absent', () => {
  const store = new FakeMemento();
  writeLastSessionId(store, '   ');
  assert.equal(readLastSessionId(store), undefined);
});

test('writeLastSessionId is fire-and-forget and survives a stalled memento', async () => {
  const stalled: SessionIdStore = {
    get: () => undefined,
    update: () => new Promise<void>(() => undefined), // 永不 resolve
  };
  // 不抛错也不挂起：记录失败不影响面板生命周期。
  writeLastSessionId(stalled, 'session-2');
  await new Promise((resolve) => setTimeout(resolve, 10));
});
