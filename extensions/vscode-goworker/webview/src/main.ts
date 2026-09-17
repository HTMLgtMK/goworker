import type { HostMessage, WebviewMessage } from '../../src/contracts/messages';
import { appendTraceUpdate, completeTrace, initialTraceState, type TraceState } from './state/traceReducer';
import { renderTraceItem } from './components/traceItem';

declare function acquireVsCodeApi(): { postMessage(message: WebviewMessage): void };

const vscode = acquireVsCodeApi();
const rootElement = document.getElementById('root');
if (!(rootElement instanceof HTMLElement)) throw new Error('Missing root element');
const root: HTMLElement = rootElement;

let trace: TraceState = initialTraceState;
let taskId = '';
let detail: Record<string, unknown> | undefined;
let followLive = true;
let pendingEvents = 0;

// 挂起的权限请求：一次只显示一个（daemon 侧同一任务的请求是串行的），
// 队列里等前一个裁决完再显示下一个。
interface PermissionPrompt {
  requestId: string;
  toolTitle: string;
  options: Array<{ optionId: string; name: string }>;
}
const permissionQueue: PermissionPrompt[] = [];
let activePermission: PermissionPrompt | undefined;

window.addEventListener('message', ({ data }: MessageEvent<HostMessage>) => {
  switch (data.type) {
    case 'trace-reset':
      taskId = data.taskId;
      detail = undefined;
      trace = initialTraceState;
      pendingEvents = 0;
      render();
      break;
    case 'catalog-detail':
      detail = data.payload.task as unknown as Record<string, unknown>;
      render();
      break;
    case 'trace-update':
      trace = appendTraceUpdate(trace, data.update);
      pendingEvents += followLive ? 0 : 1;
      render();
      if (followLive) scrollToBottom();
      break;
    case 'trace-complete':
      trace = completeTrace(trace);
      render();
      break;
    case 'trace-error':
      renderError(data.error);
      break;
    case 'permission-request':
      permissionQueue.push({ requestId: data.requestId, toolTitle: data.toolTitle, options: data.options });
      showNextPermission();
      break;
    case 'permission-dismiss':
      // 该请求已失效（daemon 超时判拒/面板重置）：从队列与当前显示中移除。
      dropPermission(data.requestId);
      break;
  }
});

// 显示队首请求；已有请求在显示时不动（避免后来的请求顶掉用户正在看的对话框）。
function showNextPermission(): void {
  if (activePermission || permissionQueue.length === 0) return;
  activePermission = permissionQueue.shift();
  render();
}

function resolvePermission(optionId: string | undefined): void {
  const current = activePermission;
  if (!current) return;
  activePermission = undefined;
  if (optionId === undefined) {
    vscode.postMessage({ type: 'permission-cancel', requestId: current.requestId });
  } else {
    vscode.postMessage({ type: 'permission-response', requestId: current.requestId, optionId });
  }
  showNextPermission();
  render();
}

function dropPermission(requestId: string): void {
  const index = permissionQueue.findIndex((item) => item.requestId === requestId);
  if (index >= 0) {
    permissionQueue.splice(index, 1);
  } else if (activePermission?.requestId === requestId) {
    activePermission = undefined;
    showNextPermission();
  }
  render();
}

root.addEventListener('scroll', () => {
  const nearBottom = root.scrollHeight - root.scrollTop - root.clientHeight < 48;
  if (followLive !== nearBottom) {
    followLive = nearBottom;
    vscode.postMessage({ type: 'follow-live', enabled: followLive });
    render();
  }
});

function render(): void {
  root.replaceChildren();
  root.append(renderHeader(), renderFilters(), renderTrace());
  if (activePermission) root.append(renderPermissionDialog(activePermission));
  if (!followLive && pendingEvents > 0) {
    const button = document.createElement('button');
    button.className = 'new-events';
    button.textContent = `↓ ${pendingEvents} 条新事件`;
    button.onclick = () => {
      followLive = true;
      pendingEvents = 0;
      vscode.postMessage({ type: 'follow-live', enabled: true });
      render();
      scrollToBottom();
    };
    root.append(button);
  }
  root.append(renderComposer());
}

function renderHeader(): HTMLElement {
  const section = document.createElement('section');
  section.className = 'task-header';
  const title = document.createElement('strong');
  title.textContent = taskId || 'Task Detail';
  const meta = document.createElement('span');
  meta.textContent = detail ? [detail.status, detail.kind, detail.worker, detail.branch].filter(Boolean).join(' · ') : 'loading catalog…';
  section.append(title, meta);
  return section;
}

function renderFilters(): HTMLElement {
  const section = document.createElement('section');
  section.className = 'filters';
  section.textContent = `全部 · 消息 · 思考 · 工具 · 未知事件 · Live ${followLive ? '✓' : 'paused'}`;
  return section;
}

function renderTrace(): HTMLElement {
  const section = document.createElement('main');
  section.className = 'trace-list';
  for (const item of trace.items) section.append(renderTraceItem(item));
  if (trace.completed) {
    const completed = document.createElement('p');
    completed.className = 'completed';
    completed.textContent = '── worker trace completed ──';
    section.append(completed);
  }
  return section;
}

// 权限对话框：渲染在 trace 之上、composer 之前。按钮直接来自服务端下发的
// options，不在这里编造选项——UI 只负责把裁决原样回传。
function renderPermissionDialog(prompt: PermissionPrompt): HTMLElement {
  const section = document.createElement('section');
  section.className = 'permission-dialog';

  const heading = document.createElement('strong');
  heading.textContent = 'Permission required';
  const tool = document.createElement('pre');
  tool.className = 'permission-tool';
  tool.textContent = prompt.toolTitle;

  const actions = document.createElement('div');
  actions.className = 'permission-actions';
  for (const option of prompt.options) {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = option.name;
    button.onclick = () => resolvePermission(option.optionId);
    actions.append(button);
  }
  const dismiss = document.createElement('button');
  dismiss.type = 'button';
  dismiss.className = 'permission-dismiss';
  dismiss.textContent = 'Dismiss';
  dismiss.onclick = () => resolvePermission(undefined);
  actions.append(dismiss);

  section.append(heading, tool, actions);
  return section;
}

function renderComposer(): HTMLElement {
  const section = document.createElement('footer');
  section.className = 'composer';
  const input = document.createElement('input');
  input.disabled = true;
  input.placeholder = '当前 Task Detail 为只读观察；reply / resume 尚未开放';
  section.append(input);
  return section;
}

function renderError(error: string): void {
  root.replaceChildren();
  const element = document.createElement('p');
  element.className = 'error';
  element.textContent = error;
  root.append(element);
}

function scrollToBottom(): void {
  requestAnimationFrame(() => root.scrollTo({ top: root.scrollHeight, behavior: 'smooth' }));
}

vscode.postMessage({ type: 'ready' });
