// 「上次会话」记录：面板 connect 成功与关闭时回写 bridge.sessionId，供启动重开
// （重连重放）与 sidebar 的 last 标记。存 workspaceState 而非 globalState：
// 会话 id 是 daemon 本地的（current.jsonl head / 归档时间戳），daemon 按项目运行，
// 跨项目记录会指向另一 daemon 的未知会话。

// 结构化最小接口：vscode.Memento 可直接赋给本类型，单元测试用假 memento。
export interface SessionIdStore {
  get(key: string): unknown;
  update(key: string, value: unknown): Thenable<void>;
}

export const lastSessionStorageKey = 'goworker.agentChat.lastSessionId';

export function readLastSessionId(store: SessionIdStore): string | undefined {
  const value = store.get(lastSessionStorageKey);
  return typeof value === 'string' && value.trim().length > 0 ? value : undefined;
}

// fire-and-forget：记录失败不影响面板生命周期。
export function writeLastSessionId(store: SessionIdStore, sessionId: string): void {
  void store.update(lastSessionStorageKey, sessionId);
}
