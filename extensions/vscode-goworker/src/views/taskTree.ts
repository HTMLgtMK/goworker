import * as vscode from 'vscode';
import type { TaskSummary } from '../contracts/messages';

export class TaskTreeProvider implements vscode.TreeDataProvider<TaskItem> {
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this.changed.event;
  private tasks: TaskSummary[] = [];
  private error?: string;

  setTasks(tasks: TaskSummary[]): void {
    this.tasks = tasks;
    this.error = undefined;
    this.changed.fire();
  }

  setError(error: string): void {
    this.error = error;
    this.changed.fire();
  }

  refresh(): void {
    this.changed.fire();
  }

  getTreeItem(element: TaskItem): vscode.TreeItem {
    return element;
  }

  getChildren(): TaskItem[] {
    if (this.error) return [TaskItem.info(`Disconnected: ${this.error}`)];
    if (this.tasks.length === 0) return [TaskItem.info('No tasks')];
    return this.tasks.map((task) => new TaskItem(task));
  }
}

export class TaskItem extends vscode.TreeItem {
  constructor(readonly task?: TaskSummary, label?: string) {
    super(task ? task.prompt : label ?? '', task ? vscode.TreeItemCollapsibleState.None : vscode.TreeItemCollapsibleState.None);
    if (!task) return;
    this.id = task.id;
    this.description = `${task.status} · ${task.worker}`;
    this.tooltip = `${task.id}\n${task.prompt}`;
    this.command = { command: 'goworker.openTask', title: 'Open GOWORKER Task', arguments: [task.id] };
    this.iconPath = new vscode.ThemeIcon(iconForStatus(task.status));
    this.contextValue = 'goworker.task';
  }

  static info(label: string): TaskItem {
    const item = new TaskItem(undefined, label);
    item.iconPath = new vscode.ThemeIcon('info');
    return item;
  }
}

function iconForStatus(status: TaskSummary['status']): string {
  if (status === 'working' || status === 'dispatching' || status === 'merging') return 'sync~spin';
  if (status === 'awaiting_review') return 'beaker';
  if (status === 'done') return 'pass';
  if (status === 'failed' || status === 'cancelled' || status === 'rejected') return 'error';
  return 'circle-outline';
}
