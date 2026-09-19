import {
  tryParseWebviewMessage,
  type ExtensionToWebviewMessage,
  type WebviewToExtensionMessage,
  type WorkspacePathOpenTarget,
} from '@htmlgtmk/acp-ui';
import { isAbsolute, join } from 'node:path';
import * as vscode from 'vscode';
import type { PanelAcpChatSession } from './sessionRegistry';

// init 消息除 type 外的完整负载（chatPanel 组装，plumbing 在 ready 时下发）。
export type AcpChatInitPayload = Omit<Extract<ExtensionToWebviewMessage, { type: 'init' }>, 'type'>;

// webview onDidReceiveMessage → tryParseWebviewMessage 校验 → 分发到 bridge / VS Code 动作。
// host → webview 方向全部经 PanelAcpChatSession 的 postToWebview，不在这里发出。
export function wireAcpChatMessagePlumbing(options: {
  panel: vscode.WebviewPanel;
  session: PanelAcpChatSession;
  initPayload: AcpChatInitPayload;
}): vscode.Disposable {
  const { panel, session, initPayload } = options;
  const post = (message: ExtensionToWebviewMessage): void => {
    void panel.webview.postMessage(message);
  };
  let bootstrapSent = false;

  return panel.webview.onDidReceiveMessage((raw: unknown) => {
    const parsed = tryParseWebviewMessage(raw);
    if (parsed === null) return; // 不认识的消息一律丢弃，不猜测字段
    void dispatch(parsed);
  });

  async function dispatch(parsed: WebviewToExtensionMessage): Promise<void> {
    switch (parsed.type) {
      case 'ready': {
        // webview 每次实例化只引导一次；retainContextWhenHidden 下不会重复触发。
        if (bootstrapSent) return;
        bootstrapSent = true;
        post({ type: 'init', ...initPayload });
        await session.ensureConnected();
        return;
      }
      case 'send': {
        const bridge = await session.ensureConnected();
        if (!bridge) return;
        if (bridge.isPrompting) await bridge.cancel();
        await bridge.prompt(parsed.body);
        return;
      }
      case 'cancel': {
        (await session.ensureConnected())?.cancel();
        return;
      }
      case 'setSessionModel': {
        const bridge = session.currentBridge;
        if (!bridge) return;
        try {
          await bridge.setSessionModel(parsed.modelId);
        } catch (error) {
          post({ type: 'error', message: `Model change failed: ${asMessage(error)}` });
        }
        return;
      }
      case 'setSessionConfigOption': {
        const bridge = await session.ensureConnected();
        if (!bridge) return;
        try {
          await bridge.setSessionConfigOption(parsed.configId, parsed.value);
        } catch (error) {
          post({ type: 'error', message: `Config change failed: ${asMessage(error)}` });
        }
        return;
      }
      case 'resetSession': {
        await session.reset();
        return;
      }
      case 'permissionResponse': {
        session.currentBridge?.handlePermissionResponse(parsed);
        return;
      }
      // Cursor 专属消息：不裁不接协议，仅把响应原样路由回 bridge（daemon 不会主动发这些请求）。
      case 'cursorAskQuestionResponse': {
        session.currentBridge?.handleCursorAskQuestionResponse(parsed);
        return;
      }
      case 'cursorCreatePlanResponse': {
        session.currentBridge?.handleCursorCreatePlanResponse(parsed);
        return;
      }
      case 'openNewChat': {
        await vscode.commands.executeCommand('goworker.openAgentChat');
        return;
      }
      case 'openWorkspacePath': {
        await openWorkspacePath(parsed.path, parsed.target);
        return;
      }
      case 'renameSession': {
        post({ type: 'commandFeedback', message: 'Renaming is not supported in the GOWORKER agent chat yet.' });
        return;
      }
      // saveHistory（composer 历史持久化）与 setSessionAgent（单 agent 固定）面板版不做。
      default:
        return;
    }
  }
}

// 紧凑工具路径链接：相对路径按 workspace root 解析；auto 模式下目录进 explorer、文件进编辑器。
async function openWorkspacePath(candidate: string, target: WorkspacePathOpenTarget | undefined): Promise<void> {
  const root = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  const absolute = isAbsolute(candidate) || root === undefined ? candidate : join(root, candidate);
  const uri = vscode.Uri.file(absolute);
  try {
    if (target !== 'file') {
      const stat = await vscode.workspace.fs.stat(uri);
      if (stat.type & vscode.FileType.Directory) {
        await vscode.commands.executeCommand('revealInExplorer', uri);
        return;
      }
    }
    await vscode.window.showTextDocument(uri, { preview: true });
  } catch {
    void vscode.window.showErrorMessage(`Could not open ${candidate}`);
  }
}

function asMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
