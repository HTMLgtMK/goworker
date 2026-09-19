import type { ACPConnection } from './connection';
import type { ACPUpdate } from '../contracts/messages';

// 极简传输接口：生产环境传入真实 ACPConnection（独立实例、独立 socket 路径、独立 initialize），
// 单元测试注入 fake transport，避免依赖真实 socket。
export interface ACPRpcTransport {
  readonly connected: boolean;
  connect(): Promise<void>;
  request<T>(method: string, params?: unknown): Promise<T>;
  notify(method: string, params?: unknown): void;
  onNotification(listener: (method: string, params: unknown) => void): () => void;
  close(): void;
}

// 编译期保证：真实 ACPConnection 必须持续满足传输接口（AgentClient 直接复用它）。
const _connectionCompatible: ACPConnection extends ACPRpcTransport ? true : never = true;

export interface PromptHandle {
  dispose(): void;
}

export type PromptCompletion = (error?: Error) => void;

interface NewSessionResponse {
  sessionId: string;
}

interface PromptResponse {
  stopReason: string;
}

// session/update 通知信封：{ sessionId, update }，update 的窄化类型见 contracts/messages.ts。
interface SessionUpdateEnvelope {
  sessionId: string;
  update: ACPUpdate;
}

export class AgentClient {
  private initialized = false;
  private initializing?: Promise<void>;

  constructor(private readonly connection: ACPRpcTransport, private readonly cwd: string) {}

  get connected(): boolean {
    return this.connection.connected;
  }

  // 幂等连接 + initialize 去重，风格对齐 DispatcherClient；断线后重连会重新 initialize。
  async connect(): Promise<void> {
    const reconnecting = !this.connection.connected;
    await this.connection.connect();
    if (reconnecting) this.initialized = false;
    if (this.initialized) return;
    if (!this.initializing) {
      const initialization = this.connection.request('initialize', { protocolVersion: 1, clientCapabilities: {} })
        .then(() => {
          this.initialized = true;
        })
        .finally(() => {
          if (this.initializing === initialization) this.initializing = undefined;
        });
      this.initializing = initialization;
    }
    await this.initializing;
  }

  // 每个 ChatRequest 创建一次 session；经 connect() 幂等覆盖断线重连。
  async newSession(): Promise<string> {
    await this.connect();
    const response = await this.connection.request<NewSessionResponse>('session/new', { cwd: this.cwd, mcpServers: [] });
    if (!response.sessionId) throw new Error('Agent returned an empty session ID');
    return response.sessionId;
  }

  cancel(sessionId: string): void {
    this.connection.notify('session/cancel', { sessionId });
  }

  // 发送 session/prompt，并把本 session 的 session/update 过滤后回调 onUpdate。
  // onComplete：正常 resolve 不带参数，真实错误带 Error；dispose 主动取消后到达的
  // update/完成（含 RPC -32000 "context canceled" 的 reject）一律忽略，不冒泡为失败。
  prompt(sessionId: string, text: string, onUpdate: (update: ACPUpdate) => void, onComplete: PromptCompletion): PromptHandle {
    let closed = false;
    const unsubscribe = this.connection.onNotification((method, params) => {
      if (method !== 'session/update' || closed) return;
      if (!isSessionUpdateEnvelope(params) || params.sessionId !== sessionId) return;
      onUpdate(params.update);
    });
    const finish = (error?: Error): void => {
      if (closed) return;
      closed = true;
      unsubscribe();
      onComplete(error);
    };
    void this.connection.request<PromptResponse>('session/prompt', {
      sessionId,
      prompt: [{ type: 'text', text }],
    }).then(
      () => finish(),
      (error: unknown) => finish(asError(error)),
    );
    return {
      dispose: () => {
        if (closed) return;
        closed = true;
        unsubscribe();
        this.cancel(sessionId);
      },
    };
  }

  disconnect(): void {
    this.initialized = false;
    this.initializing = undefined;
    this.connection.close();
  }
}

function isSessionUpdateEnvelope(value: unknown): value is SessionUpdateEnvelope {
  if (typeof value !== 'object' || value === null) return false;
  const envelope = value as Partial<SessionUpdateEnvelope>;
  return typeof envelope.sessionId === 'string'
    && typeof envelope.update === 'object'
    && envelope.update !== null;
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
