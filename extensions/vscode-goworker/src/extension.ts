import * as vscode from 'vscode';
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
import { TaskTreeProvider } from './views/taskTree';
import { StaticTreeProvider } from './views/staticTree';
import { TaskPanel } from './views/taskPanel';

export function activate(context: vscode.ExtensionContext): void {
  const cwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd();
  const tasks = new TaskTreeProvider();
  const workers = new StaticTreeProvider([{ label: 'Configured workers load with task catalog', icon: 'server-process' }]);
  const runtime = new StaticTreeProvider([{ label: 'Dispatcher: connecting…', icon: 'sync~spin' }]);
  const client = new DispatcherClient(new ACPConnection(dispatcherSocketPath()), cwd);
  const agent = new AgentClient(new ACPConnection(agentSocketPath()), cwd);

  // 原生 Chat participant（follower mode）：只经 ChatResponseStream 输出，
  // 不触碰 webview/DOM/文件系统，也不提供命令执行能力。
  const participant = vscode.chat.createChatParticipant('goworker.chat', (request, _chatContext, response, token) =>
    handleAgentChat(agent, request, response, token),
  );

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('goworker.tasks', tasks),
    vscode.window.registerTreeDataProvider('goworker.workers', workers),
    vscode.window.registerTreeDataProvider('goworker.runtime', runtime),
    vscode.commands.registerCommand('goworker.refreshTasks', () => refresh()),
    vscode.commands.registerCommand('goworker.reconnect', () => reconnect()),
    vscode.commands.registerCommand('goworker.openTask', (taskId: string) => TaskPanel.open(context, client, taskId)),
    { dispose: () => client.disconnect() },
    participant,
    { dispose: () => agent.disconnect() },
  );

  void reconnect();

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
