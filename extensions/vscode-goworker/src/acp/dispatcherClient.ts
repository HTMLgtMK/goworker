import { ACPConnection } from './connection';
import type {
  ACPUpdate,
  CatalogUpdate,
  TaskDetail,
  TaskDetailUpdate,
  TaskListUpdate,
  TaskSummary,
} from '../contracts/messages';

interface NewSessionResponse {
  sessionId: string;
}

interface PromptResponse {
  stopReason: string;
}

interface SessionUpdate {
  sessionId: string;
  update: ACPUpdate;
}

export interface TaskObserver {
  dispose(): void;
}

// session/request_permission 的请求形状（ACP）。只取渲染需要的字段，
// 其余字段原样忽略——对端可能带 _meta 等扩展。
export interface TaskPermissionRequest {
  sessionId: string;
  toolCall: { toolCallId?: string; title?: string };
  options: Array<{ optionId: string; name?: string; kind?: string }>;
}

// 用户在 task detail 页对权限请求的裁决。形状对齐 ACP 的
// RequestPermissionOutcome（嵌套判别联合），线上由 daemon 的
// PermissionResponse 解析。
export type TaskPermissionDecision =
  | { outcome: 'selected'; optionId: string }
  | { outcome: 'cancelled' };

// 裁决回调：由上层（taskPanel）接 UI，返回用户的决定。
export type TaskPermissionHandler = (request: TaskPermissionRequest) => Promise<TaskPermissionDecision>;

export class DispatcherClient {
  private initialized = false;
  private initializing?: Promise<void>;

  constructor(
    private readonly connection: ACPConnection,
    private readonly cwd: string,
    private readonly onPermission?: TaskPermissionHandler,
  ) {
    // 注册在构造时而非 connect 时：连接可能重连，处理器必须一直挂着。
    this.connection.onRequest('session/request_permission', (params) => this.handlePermission(params));
  }

  get connected(): boolean {
    return this.connection.connected;
  }

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

  async listTasks(): Promise<TaskListUpdate> {
    return this.catalogQuery<TaskListUpdate>('--console/tasks', 'goworker_task_list');
  }

  async getTask(taskId: string): Promise<TaskDetailUpdate> {
    return this.catalogQuery<TaskDetailUpdate>(`--console/task ${taskId}`, 'goworker_task_detail');
  }

  async observeTask(
    taskId: string,
    onUpdate: (update: ACPUpdate) => void,
    onComplete: (error?: Error) => void,
  ): Promise<TaskObserver> {
    await this.connect();
    const sessionId = await this.newSession();
    let disposed = false;
    const unsubscribe = this.connection.onNotification((method, params) => {
      if (method !== 'session/update' || !isSessionUpdate(params) || params.sessionId !== sessionId || disposed) return;
      onUpdate(params.update);
    });
    void this.prompt(sessionId, `--attach ${taskId}`).then(
      () => onComplete(),
      (error: unknown) => onComplete(asError(error)),
    );
    return {
      dispose: () => {
        if (disposed) return;
        disposed = true;
        unsubscribe();
        this.connection.notify('session/cancel', { sessionId });
      },
    };
  }

  disconnect(): void {
    this.initialized = false;
    this.initializing = undefined;
    this.connection.close();
  }

  // 服务端权限请求 → UI 裁决 → ACP 应答。任何无法完成裁决的路径（未接 UI、
  // 载荷畸形、UI 抛错）都回 cancelled，绝不回 selected —— daemon 侧把 cancelled
  // 与"问不到人"一律当拒绝处理，失败方向必须是拒绝。
  private async handlePermission(params: unknown): Promise<unknown> {
    const request = narrowPermissionRequest(params);
    if (!request || !this.onPermission) {
      return { outcome: { outcome: 'cancelled' } };
    }
    try {
      const decision = await this.onPermission(request);
      if (decision.outcome === 'selected' && decision.optionId) {
        return { outcome: { outcome: 'selected', optionId: decision.optionId } };
      }
      return { outcome: { outcome: 'cancelled' } };
    } catch {
      return { outcome: { outcome: 'cancelled' } };
    }
  }

  private async catalogQuery<T extends CatalogUpdate>(prompt: string, expectedType: T['sessionUpdate']): Promise<T> {
    await this.connect();
    const sessionId = await this.newSession();
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const unsubscribe = this.connection.onNotification((method, params) => {
        if (method !== 'session/update' || !isSessionUpdate(params) || params.sessionId !== sessionId) return;
        const catalog = parseCatalogUpdate(params.update, expectedType);
        if (catalog instanceof Error) {
          settle(catalog);
          return;
        }
        settle(undefined, catalog as T);
      });
      const settle = (error?: Error, value?: T) => {
        if (settled) return;
        settled = true;
        unsubscribe();
        this.connection.notify('session/cancel', { sessionId });
        if (error) reject(error);
        else resolve(value as T);
      };
      void this.prompt(sessionId, prompt).then(
        () => settle(new Error(`Dispatcher completed ${expectedType} without a catalog update`)),
        (error: unknown) => settle(asError(error)),
      );
    });
  }

  private async newSession(): Promise<string> {
    const response = await this.connection.request<NewSessionResponse>('session/new', { cwd: this.cwd, mcpServers: [] });
    if (!response.sessionId) throw new Error('Dispatcher returned an empty session ID');
    return response.sessionId;
  }

  private async prompt(sessionId: string, text: string): Promise<PromptResponse> {
    return this.connection.request<PromptResponse>('session/prompt', {
      sessionId,
      prompt: [{ type: 'text', text }],
    });
  }
}

function parseCatalogUpdate(update: ACPUpdate, expectedType: CatalogUpdate['sessionUpdate']): CatalogUpdate | Error {
  if (update.sessionUpdate !== expectedType || update.version !== 1) {
    return new Error(`Unsupported GOWORKER catalog contract ${String(update.version)}`);
  }
  if (expectedType === 'goworker_task_list') {
    if (!Array.isArray(update.tasks) || !update.tasks.every(isTaskSummary)) {
      return new Error('Invalid GOWORKER task list catalog payload');
    }
    return { sessionUpdate: expectedType, version: 1, tasks: update.tasks };
  }
  if (!isTaskDetail(update.task)) return new Error('Invalid GOWORKER task detail catalog payload');
  return { sessionUpdate: expectedType, version: 1, task: update.task };
}

function isTaskSummary(value: unknown): value is TaskSummary {
  if (!isRecord(value)) return false;
  return typeof value.id === 'string'
    && typeof value.source === 'string'
    && (value.kind === 'code' || value.kind === 'general')
    && typeof value.status === 'string'
    && typeof value.worker === 'string'
    && typeof value.prompt === 'string'
    && typeof value.created_at === 'string'
    && typeof value.updated_at === 'string';
}

function isTaskDetail(value: unknown): value is TaskDetail {
  if (!isTaskSummary(value)) return false;
  if (!isRecord(value)) return false;
  return optionalString(value.repo)
    && optionalString(value.base_commit)
    && optionalString(value.worktree)
    && optionalString(value.branch)
    && optionalString(value.worker_session)
    && optionalString(value.error)
    && (value.commits === undefined || (Array.isArray(value.commits) && value.commits.every((commit) => typeof commit === 'string')));
}

function optionalString(value: unknown): boolean {
  return value === undefined || typeof value === 'string';
}

function isSessionUpdate(value: unknown): value is SessionUpdate {
  if (!isRecord(value) || typeof value.sessionId !== 'string' || !isRecord(value.update)) return false;
  return typeof value.update.sessionUpdate === 'string';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

// 窄化服务端发来的 permission 请求：字段不合规就返回 undefined，由调用方
// 回 cancelled（不猜、不补默认值）。
export function narrowPermissionRequest(params: unknown): TaskPermissionRequest | undefined {
  if (!isRecord(params)) return undefined;
  if (typeof params.sessionId !== 'string' || params.sessionId.length === 0) return undefined;
  const toolCall = params.toolCall;
  if (!isRecord(toolCall)) return undefined;
  if (!Array.isArray(params.options) || params.options.length === 0) return undefined;
  const options: TaskPermissionRequest['options'] = [];
  for (const option of params.options) {
    if (!isRecord(option) || typeof option.optionId !== 'string' || option.optionId.length === 0) return undefined;
    options.push({
      optionId: option.optionId,
      name: typeof option.name === 'string' ? option.name : undefined,
      kind: typeof option.kind === 'string' ? option.kind : undefined,
    });
  }
  return {
    sessionId: params.sessionId,
    toolCall: {
      toolCallId: typeof toolCall.toolCallId === 'string' ? toolCall.toolCallId : undefined,
      title: typeof toolCall.title === 'string' ? toolCall.title : undefined,
    },
    options,
  };
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
