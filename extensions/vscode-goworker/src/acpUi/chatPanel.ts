import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import {
  buildAcpUiWebviewHtml,
  resolveAcpUiWebviewAssets,
  type AcpAgentSpawnConfig,
  type AcpSessionHostRuntime,
} from '@htmlgtmk/acp-ui';
import * as vscode from 'vscode';
import { wireAcpChatMessagePlumbing, type AcpChatInitPayload } from './messagePlumbing';
import * as path from 'node:path';
import type { AcpChatSessionRegistry } from './sessionRegistry';

// ACP 聊天面板薄壳：webview 托管 + HTML 组装 + 会话注册；bridge 逻辑全在 SDK。
export function openAgentChatPanel(options: {
  context: vscode.ExtensionContext;
  registry: AcpChatSessionRegistry;
  agentConfig: AcpAgentSpawnConfig;
  host: AcpSessionHostRuntime;
}): vscode.WebviewPanel {
  const { context, registry, agentConfig, host } = options;
  const webviewDir = join(context.extensionPath, 'node_modules', '@htmlgtmk', 'acp-ui', 'webview');

  const panel = vscode.window.createWebviewPanel(
    'goworker.agentChat',
    'Agent Chat',
    { viewColumn: vscode.ViewColumn.Beside, preserveFocus: true },
    {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.file(webviewDir), vscode.Uri.file(join(context.extensionPath, 'media'))],
      // 隐藏再显示不销毁前端实例，bridge 状态与 UI 保持一致（不必处理 webview 重建）。
      retainContextWhenHidden: true,
    },
  );
  panel.iconPath = {
    light: vscode.Uri.file(join(context.extensionPath, 'media', 'goworker.svg')),
    dark: vscode.Uri.file(join(context.extensionPath, 'media', 'goworker.svg')),
  };

  panel.webview.html = buildAgentChatHtml(context.extensionPath, webviewDir, panel.webview);

  const session = registry.createSession(panel, {
    config: agentConfig,
    host,
    postToWebview: (message) => {
      void panel.webview.postMessage(message);
    },
  });

  wireAcpChatMessagePlumbing({ panel, session, initPayload: buildInitPayload(context, agentConfig) });

  // 面板关闭：断开 bridge 连接并清除 registry 映射。
  panel.onDidDispose(() => registry.disposeSession(panel), undefined, context.subscriptions);
  return panel;
}

// 契约 §4：resolveAcpUiWebviewAssets + buildAcpUiWebviewHtml，资产位于 node_modules 的 SDK 包内。
// 资产缺失时 resolveAcpUiWebviewAssets 会 throw（提示先 build webview）。
function buildAgentChatHtml(extensionPath: string, webviewDir: string, webview: vscode.Webview): string {
  const assets = resolveAcpUiWebviewAssets(webviewDir);
  const base = buildAcpUiWebviewHtml({
    scriptUri: webview.asWebviewUri(vscode.Uri.file(assets.scriptPath)).toString(),
    styleUri: webview.asWebviewUri(vscode.Uri.file(assets.stylePath)).toString(),
    cspSource: webview.cspSource,
  });
  // 终端风主题覆盖层：CSP 的 style-src 只放行 cspSource，覆盖样式必须走 webview URI 的 link，不能 inline。
  const themeUri = webview.asWebviewUri(
    vscode.Uri.file(path.join(extensionPath, 'media', 'acp-chat-theme.css')),
  ).toString();
  return base.replace('</head>', `<link rel="stylesheet" href="${themeUri}">\n</head>`);
}

function buildInitPayload(context: vscode.ExtensionContext, agentConfig: AcpAgentSpawnConfig): AcpChatInitPayload {
  const folder = vscode.workspace.workspaceFolders?.[0];
  const homeDir = process.env.HOME ?? process.env.USERPROFILE;
  const pkg = context.extension.packageJSON as { version?: unknown };
  const version = typeof pkg.version === 'string' && pkg.version.length > 0 ? `v${pkg.version}` : undefined;
  return {
    sessionId: randomUUID(),
    title: 'Agent Chat',
    workspaceLabel: folder !== undefined ? formatPathWithTilde(folder.uri.fsPath, homeDir) : undefined,
    homeDir,
    agentVersionLabel: version,
    acpAgentName: agentConfig.name,
    // agent 在面板创建时已固定为 GOWORKER daemon，置灰 agent 切换。
    lockSessionAgent: true,
  };
}

function formatPathWithTilde(path: string, homeDir: string | undefined): string {
  if (homeDir !== undefined && (path === homeDir || path.startsWith(`${homeDir}/`))) {
    return `~${path.slice(homeDir.length)}`;
  }
  return path;
}
