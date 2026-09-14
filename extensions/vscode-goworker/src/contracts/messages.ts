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
