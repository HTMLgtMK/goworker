import * as vscode from 'vscode';
import type { DispatcherClient, TaskObserver } from '../acp/dispatcherClient';
import type { HostMessage, WebviewMessage } from '../contracts/messages';

export class TaskPanel {
  private static current?: TaskPanel;
  private observer?: TaskObserver;
  private requestVersion = 0;
  private disposed = false;

  static async open(context: vscode.ExtensionContext, client: DispatcherClient, taskId: string): Promise<void> {
    if (TaskPanel.current) {
      TaskPanel.current.panel.reveal(vscode.ViewColumn.Active);
      await TaskPanel.current.show(taskId);
      return;
    }
    const panel = vscode.window.createWebviewPanel('goworker.taskDetail', `GOWORKER · ${taskId}`, vscode.ViewColumn.Active, {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [vscode.Uri.joinPath(context.extensionUri, 'dist')],
    });
    TaskPanel.current = new TaskPanel(context, panel, client);
    await TaskPanel.current.show(taskId);
  }

  private constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly panel: vscode.WebviewPanel,
    private readonly client: DispatcherClient,
  ) {
    panel.webview.html = this.html();
    panel.onDidDispose(() => this.dispose(), undefined, context.subscriptions);
    panel.webview.onDidReceiveMessage((message: WebviewMessage) => this.receive(message), undefined, context.subscriptions);
  }

  private async show(taskId: string): Promise<void> {
    const requestVersion = ++this.requestVersion;
    this.observer?.dispose();
    this.observer = undefined;
    this.panel.title = `GOWORKER · ${taskId}`;
    this.post({ type: 'trace-reset', taskId });
    try {
      const detail = await this.client.getTask(taskId);
      if (!this.isCurrent(requestVersion)) return;
      this.post({ type: 'catalog-detail', payload: detail });
      const observer = await this.client.observeTask(
        taskId,
        (update) => {
          if (this.isCurrent(requestVersion)) this.post({ type: 'trace-update', update });
        },
        (error) => {
          if (!this.isCurrent(requestVersion)) return;
          if (error) this.post({ type: 'trace-error', error: error.message });
          else this.post({ type: 'trace-complete' });
        },
      );
      if (!this.isCurrent(requestVersion)) {
        observer.dispose();
        return;
      }
      this.observer = observer;
    } catch (error) {
      if (this.isCurrent(requestVersion)) this.post({ type: 'trace-error', error: asError(error).message });
    }
  }

  private isCurrent(requestVersion: number): boolean {
    return !this.disposed && requestVersion === this.requestVersion;
  }

  private receive(message: WebviewMessage): void {
    if (message.type === 'refresh-tasks') void vscode.commands.executeCommand('goworker.refreshTasks');
  }

  private post(message: HostMessage): void {
    void this.panel.webview.postMessage(message);
  }

  private dispose(): void {
    this.disposed = true;
    this.requestVersion += 1;
    this.observer?.dispose();
    this.observer = undefined;
    TaskPanel.current = undefined;
  }

  private html(): string {
    const script = this.panel.webview.asWebviewUri(vscode.Uri.joinPath(this.context.extensionUri, 'dist', 'webview.js'));
    const styles = this.panel.webview.asWebviewUri(vscode.Uri.joinPath(this.context.extensionUri, 'dist', 'styles.css'));
    const nonce = randomNonce();
    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${this.panel.webview.cspSource}; script-src 'nonce-${nonce}';">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="${styles}">
  <title>GOWORKER Task Detail</title>
</head>
<body><div id="root"></div><script nonce="${nonce}" src="${script}"></script></body>
</html>`;
  }
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function randomNonce(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  return Array.from({ length: 32 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}
