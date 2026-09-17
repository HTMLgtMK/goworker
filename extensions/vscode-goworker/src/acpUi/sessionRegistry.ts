import {
  AcpSessionBridge,
  type AcpAgentSpawnConfig,
  type AcpSessionBridgeHooks,
  type AcpSessionConnectOptions,
  type AcpSessionHostRuntime,
  type PostToWebview,
} from '@htmlgtmk/acp-ui';
import type { WebviewPanel } from 'vscode';
import { narrowSessionInfoUpdate, type AcpSessionInfoUpdate } from '../views/chatEntries';

export type PanelAcpChatSessionDeps = {
  config: AcpAgentSpawnConfig;
  host: AcpSessionHostRuntime;
  postToWebview: PostToWebview;
  /**
   * 首次 connect 的选项（runtimeSessionId = 要接续/重放的会话）。connect 尝试开始即
   * 消费：首次建连失败后重试走 session/new，不反复 load 同一个旧会话。
   */
  connectOptions?: AcpSessionConnectOptions;
  /** 建连成功后回写实际 sessionId（load 成功 id 不变；回落 new 是新 id）；面板关闭时再回写终值。 */
  onSessionIdResolved?: (sessionId: string) => void;
  /** daemon session_info_update（title/updatedAt）→ sidebar 清单缓存。 */
  onSessionInfoUpdate?: (update: AcpSessionInfoUpdate) => void;
};

// 一个面板一条 bridge 连接（一面板一连接，多面板由 daemon 侧多路复用）。
// 负责 bridge 的生命周期：懒建连、connect 失败清理、resetSession 重建、dispose。
export class PanelAcpChatSession {
  private bridge: AcpSessionBridge | undefined;
  private connectInFlight: Promise<AcpSessionBridge | undefined> | undefined;
  private generation = 0;
  private pendingConnectOptions: AcpSessionConnectOptions | undefined;
  // 本次 connect 是否走过 session/load（onResumeSession 触发）——用于配对 loading 状态。
  private loadAttempted = false;

  constructor(private readonly deps: PanelAcpChatSessionDeps) {
    this.pendingConnectOptions = deps.connectOptions;
  }

  get currentBridge(): AcpSessionBridge | undefined {
    return this.bridge;
  }

  // 懒建连：首次调用时用 pendingConnectOptions 建连（含 runtimeSessionId → session/load）。
  ensureConnected(): Promise<AcpSessionBridge | undefined> {
    if (this.bridge) return Promise.resolve(this.bridge);
    if (this.connectInFlight) return this.connectInFlight;
    const generation = this.generation;
    const options = this.pendingConnectOptions;
    this.pendingConnectOptions = undefined;
    const connect = this.connectBridge(generation, options);
    this.connectInFlight = connect;
    return connect.finally(() => {
      if (this.connectInFlight === connect) this.connectInFlight = undefined;
    });
  }

  // resetSession：断开旧 bridge，重建新连接（同一面板复用）。语义是「开新会话」，
  // 因此丢弃可能残留的 runtimeSessionId，不再重放旧会话。
  async reset(): Promise<AcpSessionBridge | undefined> {
    this.pendingConnectOptions = undefined;
    this.disposeBridge();
    this.deps.postToWebview({ type: 'sessionReset' });
    return this.ensureConnected();
  }

  dispose(): void {
    // 面板关闭：记录本面板最终会话（未建连成功则不值可记）。
    const sessionId = this.bridge?.sessionId;
    if (sessionId) this.deps.onSessionIdResolved?.(sessionId);
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
    this.loadAttempted = false;
    const bridge = new AcpSessionBridge(
      this.deps.config,
      this.deps.postToWebview,
      this.deps.host,
      this.buildHooks(),
    );
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
    if (this.loadAttempted) {
      // session/load 的重放随 connect 完成而结束（回落 new 时 onLoadSessionFailed 已清过，
      // 这里重复清一次是幂等的）。
      this.deps.postToWebview({ type: 'sessionHistoryLoading', loading: false });
    }
    const sessionId = bridge.sessionId;
    if (sessionId) this.deps.onSessionIdResolved?.(sessionId);
    return bridge;
  }

  // bridge hooks：重放加载态 / 回落反馈 / 会话信息增量。
  // hooks 只在 connect 内部触发，而 connect 由面板 ready（init 已下发）驱动，
  // 因此这些消息一定晚于 init，不会在 webview 引导前发出。
  private buildHooks(): AcpSessionBridgeHooks {
    return {
      onResumeSession: () => {
        this.loadAttempted = true;
        this.deps.postToWebview({ type: 'sessionHistoryLoading', loading: true });
      },
      onLoadSessionFailed: () => {
        this.deps.postToWebview({ type: 'sessionHistoryLoading', loading: false });
        this.deps.postToWebview({
          type: 'commandFeedback',
          message: 'Could not resume that chat; started a new session instead.',
        });
      },
      onSessionInfoUpdate: (update) => {
        const narrowed = narrowSessionInfoUpdate(update);
        if (narrowed !== undefined) this.deps.onSessionInfoUpdate?.(narrowed);
      },
    };
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
