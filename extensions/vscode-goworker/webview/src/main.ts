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
  }
});

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
