import type { TraceItem } from '../state/traceReducer';

export function renderTraceItem(item: TraceItem): HTMLElement {
  const element = document.createElement(item.kind === 'thought' ? 'details' : 'article');
  element.className = `trace-item trace-${item.kind}`;

  if (item.kind === 'thought') {
    const summary = document.createElement('summary');
    summary.textContent = 'agent thought';
    element.append(summary);
    element.append(textBlock(item.text ?? ''));
    return element;
  }

  if (item.kind === 'tool') {
    const header = document.createElement('header');
    header.textContent = `tool · ${item.title ?? 'unknown tool'}${item.status ? ` · ${item.status}` : ''}`;
    element.append(header);
    if (item.uncertain) element.append(note('关联不确定：worker 没有发送 toolCallId。'));
    element.append(rawDetails(item));
    return element;
  }

  if (item.kind === 'unknown') {
    element.append(note(`unknown update · ${String(item.raw?.sessionUpdate ?? 'missing')}`));
    element.append(rawDetails(item));
    return element;
  }

  if (item.kind === 'plan') {
    element.append(label('plan'));
  } else {
    element.append(label('agent message'));
  }
  element.append(textBlock(item.text ?? ''));
  return element;
}

function textBlock(text: string): HTMLElement {
  const content = document.createElement('pre');
  content.className = 'trace-text';
  content.textContent = text;
  return content;
}

function label(text: string): HTMLElement {
  const header = document.createElement('header');
  header.textContent = text;
  return header;
}

function note(text: string): HTMLElement {
  const content = document.createElement('p');
  content.className = 'trace-note';
  content.textContent = text;
  return content;
}

function rawDetails(item: TraceItem): HTMLElement {
  const details = document.createElement('details');
  const summary = document.createElement('summary');
  summary.textContent = 'raw update';
  const raw = document.createElement('pre');
  raw.className = 'trace-raw';
  raw.textContent = JSON.stringify(item.raw, null, 2);
  details.append(summary, raw);
  return details;
}
