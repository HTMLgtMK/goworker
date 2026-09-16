import {
  AcpSessionBridge,
  type AcpAgentSpawnConfig,
  type AcpSessionConnectOptions,
  type AcpSessionHostRuntime,
  type PostToWebview,
} from '@htmlgtmk/acp-ui';
import type { WebviewPanel } from 'vscode';

export type PanelAcpChatSessionDeps = {
  config: AcpAgentSpawnConfig;
  host: AcpSessionHostRuntime;
  postToWebview: PostToWebview;
};

// 一个面板一条 bridge 连接（一面板一连接，多面板由 daemon 侧多路复用）。
// 负责 bridge 的生命周期：懒建连、connect 失败清理、resetSession 重建、dispose。
export class PanelAcpChatSession {
  private bridge: AcpSessionBridge | undefined;
  private connectInFlight: Promise<AcpSessionBridge | undefined> | undefined;
  private generation = 0;

  constructor(private readonly deps: PanelAcpChatSessionDeps) {}

  get currentBridge(): AcpSessionBridge | undefined {
    return this.bridge;
  }

  // 首版 connect() 固定 session/new（不传 runtimeSessionId）；options 原样透传给 bridge。
  ensureConnected(options?: AcpSessionConnectOptions): Promise<AcpSessionBridge | undefined> {
    if (this.bridge) return Promise.resolve(this.bridge);
    if (this.connectInFlight) return this.connectInFlight;
    const generation = this.generation;
    const connect = this.connectBridge(generation, options);
    this.connectInFlight = connect;
    return connect.finally(() => {
      if (this.connectInFlight === connect) this.connectInFlight = undefined;
    });
  }

  // resetSession：断开旧 bridge，重建新连接（同一面板复用）。
  async reset(): Promise<AcpSessionBridge | undefined> {
    this.disposeBridge();
    this.deps.postToWebview({ type: 'sessionReset' });
    return this.ensureConnected();
  }

  dispose(): void {
    this.disposeBridge();
  }

  private disposeBridge(): void {
    this.generation += 1;
    this.connectInFlight = undefined;
    const bridge = this.bridge;
    this.bridge = undefined;
    bridge?.dispose();
  }

  private async connectBridge(
    generation: number,
    options?: AcpSessionConnectOptions,
  ): Promise<AcpSessionBridge | undefined> {
    const bridge = new AcpSessionBridge(this.deps.config, this.deps.postToWebview, this.deps.host);
    try {
      await bridge.connect(options);
    } catch (error) {
      bridge.dispose();
      if (generation !== this.generation) return undefined;
      this.deps.postToWebview({
        type: 'error',
        message: `Failed to connect to agent: ${asMessage(error)}`,
      });
      return undefined;
    }
    if (generation !== this.generation) {
      // connect 期间面板已关闭/重置：丢弃这个迟到的连接。
      bridge.dispose();
      return undefined;
    }
    this.bridge = bridge;
    return bridge;
  }
}

// 面板 ↔ bridge 实例映射：支持多面板并存，面板关闭时由 chatPanel 触发 disposeSession。
export class AcpChatSessionRegistry {
  private readonly sessions = new Map<WebviewPanel, PanelAcpChatSession>();

  register(panel: WebviewPanel, session: PanelAcpChatSession): void {
    this.sessions.set(panel, session);
  }

  // 组装并注册一个面板会话（bridge 懒建连，首次 ready 时真正 connect）。
  createSession(panel: WebviewPanel, deps: PanelAcpChatSessionDeps): PanelAcpChatSession {
    const session = new PanelAcpChatSession(deps);
    this.register(panel, session);
    return session;
  }

  sessionFor(panel: WebviewPanel): PanelAcpChatSession | undefined {
    return this.sessions.get(panel);
  }

  disposeSession(panel: WebviewPanel): void {
    this.sessions.get(panel)?.dispose();
    this.sessions.delete(panel);
  }

  disposeAll(): void {
    for (const [panel, session] of this.sessions) {
      session.dispose();
      this.sessions.delete(panel);
    }
  }
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
