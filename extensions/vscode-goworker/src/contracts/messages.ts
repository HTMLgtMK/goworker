export type TaskStatus =
  | 'queued'
  | 'dispatching'
  | 'working'
  | 'awaiting_review'
  | 'merging'
  | 'done'
  | 'rejected'
  | 'failed'
  | 'cancelled';

export interface TaskSummary {
  id: string;
  source: string;
  kind: 'code' | 'general';
  status: TaskStatus;
  worker: string;
  prompt: string;
  created_at: string;
  updated_at: string;
  expires_at?: string;
}

export interface TaskDetail extends TaskSummary {
  repo?: string;
  base_commit?: string;
  worktree?: string;
  branch?: string;
  worker_session?: string;
  commits?: string[];
  error?: string;
}

export interface TaskListUpdate {
  sessionUpdate: 'goworker_task_list';
  version: 1;
  tasks: TaskSummary[];
}

export interface TaskDetailUpdate {
  sessionUpdate: 'goworker_task_detail';
  version: 1;
  task: TaskDetail;
}

export type CatalogUpdate = TaskListUpdate | TaskDetailUpdate;

export interface ACPContentBlock {
  type: string;
  text?: string;
}

export interface ACPUpdate {
  sessionUpdate: string;
  content?: ACPContentBlock;
  toolCallId?: string;
  title?: string;
  kind?: string;
  status?: string;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Agent（chat participant）流式更新：窄化类型与纯 guard，供 participant 映射与单测共用。
// ---------------------------------------------------------------------------

export interface AgentMessageChunkUpdate extends ACPUpdate {
  sessionUpdate: 'agent_message_chunk';
  content: ACPContentBlock & { text: string };
}

export interface AgentThoughtChunkUpdate extends ACPUpdate {
  sessionUpdate: 'agent_thought_chunk';
  content: ACPContentBlock & { text: string };
}

export interface AgentToolCallUpdate extends ACPUpdate {
  sessionUpdate: 'tool_call';
  toolCallId?: string;
  title?: string;
  status?: string;
}

// 'tool_call_update' 携带最新状态；字段与 'tool_call' 同形。
export interface AgentToolCallStatusUpdate extends ACPUpdate {
  sessionUpdate: 'tool_call_update';
  toolCallId?: string;
  title?: string;
  status?: string;
}

export type AgentStreamUpdate =
  | AgentMessageChunkUpdate
  | AgentThoughtChunkUpdate
  | AgentToolCallUpdate
  | AgentToolCallStatusUpdate;

export function isAgentMessageChunk(update: ACPUpdate): update is AgentMessageChunkUpdate {
  return update.sessionUpdate === 'agent_message_chunk' && typeof update.content?.text === 'string';
}

export function isAgentThoughtChunk(update: ACPUpdate): update is AgentThoughtChunkUpdate {
  return update.sessionUpdate === 'agent_thought_chunk' && typeof update.content?.text === 'string';
}

export function isToolCall(update: ACPUpdate): update is AgentToolCallUpdate {
  return update.sessionUpdate === 'tool_call';
}

export function isToolCallUpdate(update: ACPUpdate): update is AgentToolCallStatusUpdate {
  return update.sessionUpdate === 'tool_call_update';
}

export type HostMessage =
  | { type: 'connection-state'; state: 'connected' | 'connecting' | 'disconnected'; error?: string }
  | { type: 'catalog-list'; payload: TaskListUpdate }
  | { type: 'catalog-detail'; payload: TaskDetailUpdate }
  | { type: 'trace-reset'; taskId: string }
  | { type: 'trace-update'; update: ACPUpdate }
  | { type: 'trace-complete' }
  | { type: 'trace-error'; error: string };

export type WebviewMessage =
  | { type: 'ready' }
  | { type: 'refresh-tasks' }
  | { type: 'open-task'; taskId: string }
  | { type: 'follow-live'; enabled: boolean };
