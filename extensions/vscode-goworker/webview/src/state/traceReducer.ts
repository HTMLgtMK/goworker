import type { ACPUpdate } from '../../../src/contracts/messages';

export type TraceKind = 'message' | 'thought' | 'tool' | 'plan' | 'unknown';

export interface TraceItem {
  id: string;
  kind: TraceKind;
  text?: string;
  title?: string;
  status?: string;
  toolCallId?: string;
  uncertain?: boolean;
  raw?: ACPUpdate;
}

export interface TraceState {
  items: TraceItem[];
  completed: boolean;
}

export const initialTraceState: TraceState = { items: [], completed: false };

export function appendTraceUpdate(state: TraceState, update: ACPUpdate): TraceState {
  switch (update.sessionUpdate) {
    case 'agent_message_chunk':
      return appendText(state, 'message', update.content?.text ?? '', update);
    case 'agent_thought_chunk':
      return appendText(state, 'thought', update.content?.text ?? '', update);
    case 'tool_call':
      return {
        ...state,
        items: [...state.items, {
          id: nextID(state), kind: 'tool', title: update.title, status: update.status,
          toolCallId: update.toolCallId, raw: update,
        }],
      };
    case 'tool_call_update':
      return updateTool(state, update);
    case 'plan':
      return {
        ...state,
        items: [...state.items, {
          id: nextID(state), kind: 'plan', text: update.content?.text ?? update.title, raw: update,
        }],
      };
    default:
      return {
        ...state,
        items: [...state.items, { id: nextID(state), kind: 'unknown', raw: update }],
      };
  }
}

export function completeTrace(state: TraceState): TraceState {
  return { ...state, completed: true };
}

function appendText(state: TraceState, kind: 'message' | 'thought', text: string, raw: ACPUpdate): TraceState {
  if (text === '') return state;
  const previous = state.items.at(-1);
  if (previous?.kind === kind) {
    return {
      ...state,
      items: [...state.items.slice(0, -1), { ...previous, text: `${previous.text ?? ''}${text}`, raw }],
    };
  }
  return {
    ...state,
    items: [...state.items, { id: nextID(state), kind, text, raw }],
  };
}

function updateTool(state: TraceState, update: ACPUpdate): TraceState {
  const matchIndex = findLastIndex(
    state.items,
    update.toolCallId
      ? (item) => item.kind === 'tool' && item.toolCallId === update.toolCallId
      : (item) => item.kind === 'tool' && item.status !== 'completed',
  );
  if (matchIndex < 0) {
    return {
      ...state,
      items: [...state.items, {
        id: nextID(state), kind: 'tool', title: update.title, status: update.status,
        toolCallId: update.toolCallId, uncertain: !update.toolCallId, raw: update,
      }],
    };
  }
  const matched = state.items[matchIndex];
  const replacement: TraceItem = {
    ...matched,
    title: update.title ?? matched.title,
    status: update.status ?? matched.status,
    toolCallId: update.toolCallId ?? matched.toolCallId,
    uncertain: !update.toolCallId || matched.uncertain === true,
    raw: update,
  };
  return {
    ...state,
    items: state.items.map((item, index) => index === matchIndex ? replacement : item),
  };
}

function findLastIndex(items: readonly TraceItem[], matches: (item: TraceItem) => boolean): number {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (matches(items[index])) return index;
  }
  return -1;
}

function nextID(state: TraceState): string {
  return `trace-${state.items.length + 1}`;
}
