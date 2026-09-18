import * as vscode from 'vscode';
import { openAgentChatPanel } from './acpUi/chatPanel';
import { createAcpSessionHostRuntime, createGoworkerAgentSpawnConfig } from './acpUi/hostRuntime';
import { fetchAcpSessionList } from './acpUi/sessionListClient';
import { readLastSessionId, writeLastSessionId } from './acpUi/sessionRecord';
import { AcpChatSessionRegistry } from './acpUi/sessionRegistry';
import { ACPConnection } from './acp/connection';
import { AgentClient } from './acp/agentClient';
import { DispatcherClient } from './acp/dispatcherClient';
import {
  isAgentMessageChunk,
  isAgentThoughtChunk,
  isToolCall,
  isToolCallUpdate,
  type ACPUpdate,
} from './contracts/messages';
import { toChatEntryClick, type AcpSessionInfoUpdate } from './views/chatEntries';
import { ChatsTreeProvider } from './views/chatsTree';
import { TaskTreeProvider } from './views/taskTree';
import { StaticTreeProvider } from './views/staticTree';
import { TaskPanel } from './views/taskPanel';

export function activate(context: vscode.ExtensionContext): void {
  const cwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd();
  const tasks = new TaskTreeProvider();
  const workers = new StaticTreeProvider([{ label: 'Configured workers load with task catalog', icon: 'server-process' }]);
  const runtime = new StaticTreeProvider([{ label: 'Dispatcher: connecting…', icon: 'sync~spin' }]);
  // 权限裁决经 TaskPanel 路由：daemon 的 ask 策略会把 worker 的
  // session/request_permission 转给订阅了该任务的连接，这里接住弹给用户。
  const client = new DispatcherClient(
    new ACPConnection(dispatcherSocketPath()),
    cwd,
    (request) => TaskPanel.handlePermission(request),
  );
  const agent = new AgentClient(new ACPConnection(agentSocketPath()), cwd);
  const chatRegistry = new AcpChatSessionRegistry();
  const chats = new ChatsTreeProvider(listAgentSessions, () => readLastSessionId(context.workspaceState));

  // 原生 Chat participant（follower mode）：只经 ChatResponseStream 输出，
  // 不触碰 webview/DOM/文件系统，也不提供命令执行能力。
  const participant = vscode.chat.createChatParticipant('goworker.chat', (request, _chatContext, response, token) =>
    handleAgentChat(agent, request, response, token),
  );

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('goworker.tasks', tasks),
    vscode.window.registerTreeDataProvider('goworker.workers', workers),
    vscode.window.registerTreeDataProvider('goworker.runtime', runtime),
    vscode.window.registerTreeDataProvider('goworker.chats', chats),
    vscode.commands.registerCommand('goworker.refreshTasks', () => refresh()),
    vscode.commands.registerCommand('goworker.reconnect', () => reconnect()),
    vscode.commands.registerCommand('goworker.openTask', (taskId: string) => TaskPanel.open(context, client, taskId)),
    vscode.commands.registerCommand('goworker.refreshChats', () => void chats.refresh()),
    vscode.commands.registerCommand('goworker.openAgentChat', (options?: { runtimeSessionId?: string }) =>
      openChat({ runtimeSessionId: options?.runtimeSessionId }),
    ),
    vscode.commands.registerCommand('goworker.openChatSession', (raw: unknown) =>
      void openChatSession(raw),
    ),
    vscode.commands.registerCommand('goworker.newChatSession', () => void newChatSession()),
    { dispose: () => client.disconnect() },
    participant,
    { dispose: () => agent.disconnect() },
    { dispose: () => chatRegistry.disposeAll() },
  );

  void reconnect();
  void chats.refresh();

  // 打开聊天面板的统一入口（命令/树点击/启动自动打开共用）：会话记录与
  // session_info_update 的接线在这里闭包，openAgentChat 保持纯参数。
  // 返回面板供调用方继续操作（如「新建会话」要往这个面板里发 /new）。
  function openChat(options: { runtimeSessionId?: string } = {}): vscode.WebviewPanel | undefined {
    return openAgentChat(context, chatRegistry, {
      runtimeSessionId: options.runtimeSessionId,
      onSessionInfoUpdate: (update) => chats.noteSessionInfoUpdate(update),
    });
  }

  // Chats 树顶「新建会话」：确保面板存在，再在其中执行 daemon 的 /new。
  // 语义是 daemon 侧结束当前会话（固化记忆 → 清空 STM → 换新 Session），
  // 与前端 resetSession（仅重建传输连接）不同，详见 PanelAcpChatSession.startNewSession。
  async function newChatSession(): Promise<void> {
    // 不传 runtimeSessionId：新建会话不该先重放旧会话历史。
    const panel = openChat();
    if (!panel) return;
    const session = chatRegistry.sessionFor(panel);
    if (!session) return;
    try {
      await session.startNewSession();
      void chats.refresh(); // 新会话的标题/时间要反映到清单
    } catch (error) {
      void vscode.window.showErrorMessage(`New session failed: ${asError(error).message}`);
    }
  }

  // Chats 树点击：当前会话与归档会话都直接 load 重放（daemon 现支持归档只读重放；
  // 对归档会话发消息会被拒，错误经面板既有 error 通道显示）。保留窄化与空值保护。
  function openChatSession(raw: unknown): void {
    const click = toChatEntryClick(raw);
    if (click === undefined) return;
    openChat({ runtimeSessionId: click.sessionId });
  }

  // 启动默认打开 Agent Chat：异步触发不阻塞激活；无 workspace folder 的窗口不开。
  const autoOpen = vscode.workspace.getConfiguration('goworker').get<boolean>('autoOpenChat', true);
  if (autoOpen && vscode.workspace.workspaceFolders?.length) {
    setTimeout(() => {
      openChat({ runtimeSessionId: readLastSessionId(context.workspaceState) });
    }, 0);
  }

  async function reconnect(): Promise<void> {
    client.disconnect();
    runtime.setItems([{ label: 'Dispatcher: connecting…', icon: 'sync~spin' }]);
    await refresh();
  }

  async function refresh(): Promise<void> {
    try {
      const catalog = await client.listTasks();
      tasks.setTasks(catalog.tasks);
      runtime.setItems([
        { label: 'Dispatcher: connected', description: `${catalog.tasks.length} tasks`, icon: 'pass' },
        { label: 'Socket', description: dispatcherSocketPath(), icon: 'plug' },
      ]);
      workers.setItems(uniqueWorkers(catalog.tasks));
    } catch (error) {
      const message = asError(error).message;
      tasks.setError(message);
      runtime.setItems([
        { label: 'Dispatcher: disconnected', description: message, icon: 'error' },
        { label: 'Reconnect', tooltip: 'Run GOWORKER: Reconnect after starting the daemon.', icon: 'refresh' },
      ]);
    }
  }
}

export function deactivate(): void {}

// 每个 ChatRequest：newSession + prompt，把 ACP update 映射到 ChatResponseStream。
async function handleAgentChat(
  agent: AgentClient,
  request: vscode.ChatRequest,
  response: vscode.ChatResponseStream,
  token: vscode.CancellationToken,
): Promise<vscode.ChatResult | undefined> {
  const text = request.prompt;
  let sessionId: string | undefined;
  let handle: { dispose(): void } | undefined;
  let failure: string | undefined;

  await new Promise<void>((resolve) => {
    let settled = false;
    let cancellation: vscode.Disposable | undefined;
    const finish = (): void => {
      if (settled) return;
      settled = true;
      cancellation?.dispose();
      resolve();
    };
    const cancel = (): void => {
      if (settled) return;
      if (handle) handle.dispose();
      else if (sessionId) agent.cancel(sessionId);
      response.markdown('_GOWORKER agent request cancelled._');
      finish();
    };

    cancellation = token.onCancellationRequested(cancel);
    if (token.isCancellationRequested) {
      cancel();
      return;
    }

    void agent.newSession().then(
      (id) => {
        sessionId = id;
        if (settled) {
          agent.cancel(id);
          return;
        }
        handle = agent.prompt(id, text, (update) => renderAgentUpdate(update, response), (error) => {
          if (error) {
            failure = error.message;
            response.markdown(`GOWORKER agent failed: ${error.message}`);
          }
          finish();
        });
      },
      (error: unknown) => {
        if (settled) return;
        const message = `GOWORKER agent is unavailable: ${asError(error).message}`;
        failure = message;
        response.markdown(message);
        finish();
      },
    );
  });

  return failure ? { errorDetails: { message: failure } } : undefined;
}

function renderAgentUpdate(update: ACPUpdate, response: vscode.ChatResponseStream): void {
  if (isAgentMessageChunk(update)) {
    response.markdown(update.content.text);
  } else if (isAgentThoughtChunk(update)) {
    response.progress(`💭 ${update.content.text}`);
  } else if (isToolCall(update)) {
    response.progress(`⚙ ${update.title ?? update.toolCallId ?? 'tool'} (running)`);
  } else if (isToolCallUpdate(update)) {
    response.progress(`⚙ ${update.title ?? update.toolCallId ?? 'tool'} (${update.status ?? 'unknown'})`);
  }
  // 未知 update 一律忽略，不猜测字段。
}

function dispatcherSocketPath(): string {
  const configured = vscode.workspace.getConfiguration('goworker').get<string>('socketPath', '');
  return resolveSocketPath(configured, 'dispatch/acp.sock');
}

// ACP 聊天面板：复用 agentSocketPath() 解析 daemon socket（goworker.agentSocketPath 配置，
// fallback ~/.config/goworker/frontend/vscode.sock），transport 注入由 hostRuntime 完成。
// runtimeSessionId 有值时首连走 session/load（重连重放），失败由 bridge 回落 session/new。
function openAgentChat(
  context: vscode.ExtensionContext,
  registry: AcpChatSessionRegistry,
  options: {
    runtimeSessionId?: string;
    onSessionInfoUpdate?: (update: AcpSessionInfoUpdate) => void;
  } = {},
): vscode.WebviewPanel | undefined {
  let socketPath: string;
  try {
    socketPath = agentSocketPath();
  } catch (error) {
    void vscode.window.showErrorMessage(asError(error).message);
    return undefined;
  }
  return openAgentChatPanel({
    context,
    registry,
    agentConfig: createGoworkerAgentSpawnConfig(socketPath),
    host: createAcpSessionHostRuntime({
      getWorkspaceRoot: () => vscode.workspace.workspaceFolders?.[0]?.uri.fsPath,
    }),
    runtimeSessionId: options.runtimeSessionId,
    // 会话记录：connect 成功（load 不变 / 回落新 id）与面板关闭时回写 workspaceState。
    onSessionIdResolved: (sessionId) => writeLastSessionId(context.workspaceState, sessionId),
    // daemon session_info_update → Chats 树清单缓存就地更新（接线在 openChat 闭包）。
    onSessionInfoUpdate: options.onSessionInfoUpdate,
  });
}

// Chats 树数据源：每次刷新一条轻量 daemon 连接（initialize + session/list 后即断）。
// socket 配置非法时抛错，由树侧转为错误态。
function listAgentSessions(): Promise<unknown> {
  return fetchAcpSessionList(agentSocketPath());
}

function agentSocketPath(): string {
  const configured = vscode.workspace.getConfiguration('goworker').get<string>('agentSocketPath', '');
  return resolveSocketPath(configured, 'frontend/vscode.sock');
}

// 空配置时按 <home>/.config/goworker/<fallback> 派生；拒绝 URL 形式的配置。
function resolveSocketPath(configured: string, fallback: string): string {
  const value = configured.trim();
  if (value) {
    if (value.includes('://')) throw new Error('GOWORKER socket override must be a Unix socket path, not a URL');
    return value;
  }
  const home = process.env.HOME ?? process.env.USERPROFILE;
  if (!home) throw new Error('Cannot determine home directory for GOWORKER socket');
  return `${home}/.config/goworker/${fallback}`;
}

function uniqueWorkers(tasks: ReadonlyArray<{ worker: string }>): { label: string; icon: string }[] {
  const workers = [...new Set(tasks.map((task) => task.worker).filter(Boolean))];
  return workers.length > 0
    ? workers.map((worker) => ({ label: worker, icon: 'server-process' }))
    : [{ label: 'No workers observed', icon: 'circle-outline' }];
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
