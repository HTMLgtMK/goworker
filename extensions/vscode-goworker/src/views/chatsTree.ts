import * as vscode from 'vscode';
import {
  chatEntryDescription,
  chatEntryLabel,
  toChatEntries,
  type AcpSessionInfoUpdate,
  type ChatEntry,
} from './chatEntries';

// Chats 树（GOWORKER 容器顶部第一棵）：数据源 = 每次刷新开一条轻量 ACP 连接拉
// daemon session/list，展示完整清单（当前会话置顶 + 归档会话按 mtime 倒序，见
// toChatEntries），不做实时监听。树可见首次 getChildren 与树顶刷新按钮触发拉取；
// 失败保留旧清单并置错误条目。
export class ChatsTreeProvider implements vscode.TreeDataProvider<ChatEntryItem> {
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this.changed.event;
  private entries: ChatEntry[] = [];
  private error?: string;
  private loaded = false;
  private loading = false;

  constructor(
    // 返回 session/list 条目数组（sessionListClient 已窄化）；抛错 = daemon 不在。
    private readonly listSessions: () => Promise<unknown>,
    // 「上次会话」id（workspaceState），用于 last 标记。
    private readonly lastSessionId: () => string | undefined,
  ) {}

  // 树顶刷新按钮 / 错误重试。
  async refresh(): Promise<void> {
    if (this.loading) return; // 并发刷新去重：一次拉取覆盖全部
    this.loading = true;
    try {
      const raw = await this.listSessions();
      this.entries = toChatEntries(raw, this.lastSessionId());
      this.error = undefined;
    } catch (error) {
      this.error = asMessage(error);
    } finally {
      this.loading = false;
      this.loaded = true;
      this.changed.fire();
    }
  }

  // daemon session_info_update：就地修补清单缓存（title/updatedAt），不重拉。
  noteSessionInfoUpdate(update: AcpSessionInfoUpdate): void {
    if (update.sessionId === undefined) return;
    let changed = false;
    this.entries = this.entries.map((entry) => {
      if (entry.sessionId !== update.sessionId) return entry;
      changed = true;
      return {
        ...entry,
        ...(update.title !== undefined ? { title: update.title } : {}),
        ...(update.updatedAt !== undefined ? { updatedAt: update.updatedAt } : {}),
      };
    });
    if (changed) this.changed.fire();
  }

  getTreeItem(element: ChatEntryItem): vscode.TreeItem {
    return element;
  }

  getChildren(): ChatEntryItem[] {
    // 树可见即刷新（首次 getChildren 触发一次拉取，之后靠按钮/命令）。
    if (!this.loaded && !this.loading) void this.refresh();
    if (!this.loaded) return [ChatEntryItem.info('Loading chats…', 'sync~spin')];
    const items: ChatEntryItem[] = [];
    if (this.error !== undefined) items.push(ChatEntryItem.error(`Session list unavailable: ${this.error}`));
    items.push(...this.entries.map((entry) => new ChatEntryItem(entry)));
    if (items.length === 0) items.push(ChatEntryItem.info('No chats yet; send a message in Agent Chat.'));
    return items;
  }
}

export class ChatEntryItem extends vscode.TreeItem {
  constructor(readonly entry?: ChatEntry, label?: string) {
    super(entry !== undefined ? chatEntryLabel(entry) : label ?? '', vscode.TreeItemCollapsibleState.None);
    if (entry === undefined) {
      this.iconPath = new vscode.ThemeIcon('info');
      return;
    }
    this.id = entry.sessionId;
    this.description = chatEntryDescription(entry, new Date());
    this.tooltip = [
      entry.title,
      entry.updatedAt,
      entry.sessionId,
      // 归档行克制提示：可回看历史，但 prompt 只读。
      ...(entry.isCurrent ? [] : ['Archived · read-only']),
    ]
      .filter(Boolean)
      .join('\n');
    this.command = {
      command: 'goworker.openChatSession',
      title: 'Open GOWORKER Chat Session',
      arguments: [{ sessionId: entry.sessionId, isCurrent: entry.isCurrent }],
    };
    // 当前会话用主题 codicon（不设文本圆点，TreeItem 无法做圆角背景）；归档行用 history。
    this.iconPath = new vscode.ThemeIcon(entry.isCurrent ? 'comment-draft' : 'history');
    this.contextValue = entry.isCurrent ? 'goworker.chat.current' : 'goworker.chat.archived';
  }

  static info(label: string, icon = 'info'): ChatEntryItem {
    const item = new ChatEntryItem(undefined, label);
    item.iconPath = new vscode.ThemeIcon(icon);
    return item;
  }

  // 错误态：点击条目本身即重试（树顶刷新按钮亦可）。
  static error(label: string): ChatEntryItem {
    const item = new ChatEntryItem(undefined, label);
    item.iconPath = new vscode.ThemeIcon('error');
    item.description = 'Click to retry';
    item.command = { command: 'goworker.refreshChats', title: 'Retry refreshing the chat list' };
    return item;
  }
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
