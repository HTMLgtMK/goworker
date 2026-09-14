import * as vscode from 'vscode';
import { ACPConnection } from './acp/connection';
import { DispatcherClient } from './acp/dispatcherClient';
import { TaskTreeProvider } from './views/taskTree';
import { StaticTreeProvider } from './views/staticTree';
import { TaskPanel } from './views/taskPanel';

export function activate(context: vscode.ExtensionContext): void {
  const tasks = new TaskTreeProvider();
  const workers = new StaticTreeProvider([{ label: 'Configured workers load with task catalog', icon: 'server-process' }]);
  const runtime = new StaticTreeProvider([{ label: 'Dispatcher: connecting…', icon: 'sync~spin' }]);
  const client = new DispatcherClient(new ACPConnection(socketPath()), vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd());

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('goworker.tasks', tasks),
    vscode.window.registerTreeDataProvider('goworker.workers', workers),
    vscode.window.registerTreeDataProvider('goworker.runtime', runtime),
    vscode.commands.registerCommand('goworker.refreshTasks', () => refresh()),
    vscode.commands.registerCommand('goworker.reconnect', () => reconnect()),
    vscode.commands.registerCommand('goworker.openTask', (taskId: string) => TaskPanel.open(context, client, taskId)),
    { dispose: () => client.disconnect() },
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
        { label: 'Socket', description: socketPath(), icon: 'plug' },
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

function socketPath(): string {
  const configured = vscode.workspace.getConfiguration('goworker').get<string>('socketPath', '').trim();
  if (configured) {
    if (configured.includes('://')) throw new Error('goworker.socketPath must be a Unix socket path, not a URL');
    return configured;
  }
  const home = process.env.HOME ?? process.env.USERPROFILE;
  if (!home) throw new Error('Cannot determine home directory for GOWORKER socket');
  return `${home}/.config/goworker/dispatch/acp.sock`;
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
