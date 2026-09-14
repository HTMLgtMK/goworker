import * as vscode from 'vscode';

export class StaticTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this.changed.event;

  constructor(private items: readonly StaticItem[]) {}

  setItems(items: readonly StaticItem[]): void {
    this.items = items;
    this.changed.fire();
  }

  getTreeItem(element: vscode.TreeItem): vscode.TreeItem {
    return element;
  }

  getChildren(): vscode.TreeItem[] {
    return this.items.map((item) => {
      const treeItem = new vscode.TreeItem(item.label);
      treeItem.description = item.description;
      treeItem.iconPath = new vscode.ThemeIcon(item.icon);
      treeItem.tooltip = item.tooltip ?? item.label;
      return treeItem;
    });
  }
}

export interface StaticItem {
  label: string;
  description?: string;
  tooltip?: string;
  icon: string;
}
