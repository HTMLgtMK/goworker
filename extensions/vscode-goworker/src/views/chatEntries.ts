// 会话清单的展示层纯函数：不 import vscode，chatsTree 与单元测试共用。
// 输入一律 unknown 防御式窄化——wire 数据（daemon session/list、session_info_update）
// 与命令参数（树点击回传）都不被信任。

// daemon session_info_update（title/updatedAt 增量）的窄化形状。
export type AcpSessionInfoUpdate = {
  sessionId?: string;
  title?: string;
  updatedAt?: string;
};

// 树里的一条会话展示行。
export type ChatEntry = {
  sessionId: string;
  title?: string;
  updatedAt?: string;
  /** daemon 标记的当前活动会话（单活动会话模型下至多一条）；缺失时回退 index 0 约定。 */
  isCurrent: boolean;
  /** 与扩展记录的「上次会话」id 相同。 */
  isLast: boolean;
};

// 树点击命令的回传参数（ChatEntryItem 组装、openChatSession 窄化）。
export type ChatEntryClick = {
  sessionId: string;
  isCurrent: boolean;
};

/**
 * session/list 响应条目 → 展示行：遍历完整清单（当前会话 + 归档会话）生成行。
 * 归档会话现在可查看：load 会重放历史，prompt 会被 daemon 拒绝（只读归档错误，
 * 由面板既有 error 通道显示）。isCurrent 优先用条目自身的 isCurrent === true
 * （daemon 对当前会话置 true、归档按 omitempty 省略），缺失时回退现约定
 * （index 0 为当前，兼容不带该字段的旧 wire 数据）；其余行一律 isCurrent: false。
 * lastSessionId 命中时加 isLast 标记。
 */
export function toChatEntries(raw: unknown, lastSessionId?: string): ChatEntry[] {
  const items = Array.isArray(raw) ? raw : [];
  const last = lastSessionId?.trim() ?? '';
  const out: ChatEntry[] = [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    const sessionId = readString(item, 'sessionId');
    if (!sessionId) continue; // 非法条目跳过；回退判定按 wire 位置，不把归档条目补位成当前会话
    const flagged = readBoolean(item, 'isCurrent');
    out.push({
      sessionId,
      title: readString(item, 'title'),
      updatedAt: readString(item, 'updatedAt'),
      isCurrent: flagged ?? index === 0,
      isLast: sessionId === last,
    });
  }
  return out;
}

// 行标签：title 优先；无 title 用 sessionId 前 12 字符。当前会话不用文本前缀
// 标记（● 前缀已否），改由 description 表达（见 chatEntryDescription）。
export function chatEntryLabel(entry: ChatEntry): string {
  return entry.title && entry.title.trim().length > 0 ? entry.title : entry.sessionId.slice(0, 12);
}

// 行描述：当前会话标 now（活会话标记，替代被否的 ● 前缀）；其余行按 updatedAt
// 相对时间；「上次会话」追加 last 标记。
export function chatEntryDescription(entry: ChatEntry, now: Date): string | undefined {
  const parts = [entry.isCurrent ? 'now' : formatRelativeTime(entry.updatedAt, now), ...(entry.isLast ? ['last'] : [])];
  const joined = parts.filter((part): part is string => part !== undefined).join(' · ');
  return joined.length > 0 ? joined : undefined;
}

// RFC3339 相对时间：just now / Xm ago / Xh ago / Xd ago，更旧退化为 ISO 日期。
// updatedAt 缺失或不可解析返回 undefined。
export function formatRelativeTime(updatedAt: string | undefined, now: Date): string | undefined {
  if (updatedAt === undefined || updatedAt.trim().length === 0) return undefined;
  const at = new Date(updatedAt);
  const timestamp = at.getTime();
  if (Number.isNaN(timestamp)) return undefined;
  const seconds = Math.floor((now.getTime() - timestamp) / 1000);
  if (seconds < 60) return 'just now'; // 未来时间与一分钟内都算刚刚
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d ago`;
  return at.toISOString().slice(0, 10);
}

// 窄化树点击命令参数；形状不符返回 undefined（调用方直接忽略）。
export function toChatEntryClick(raw: unknown): ChatEntryClick | undefined {
  if (typeof raw !== 'object' || raw === null) return undefined;
  const sessionId = readString(raw, 'sessionId');
  if (!sessionId) return undefined;
  const isCurrent = (raw as Record<string, unknown>)['isCurrent'];
  return { sessionId, isCurrent: isCurrent === true || isCurrent === 'true' };
}

// 窄化 daemon session_info_update；没有任何可用字段返回 undefined。
export function narrowSessionInfoUpdate(raw: unknown): AcpSessionInfoUpdate | undefined {
  if (typeof raw !== 'object' || raw === null) return undefined;
  const update: AcpSessionInfoUpdate = {
    sessionId: readString(raw, 'sessionId'),
    title: readString(raw, 'title'),
    updatedAt: readString(raw, 'updatedAt'),
  };
  return update.sessionId !== undefined || update.title !== undefined || update.updatedAt !== undefined
    ? update
    : undefined;
}

function readString(source: unknown, key: string): string | undefined {
  if (typeof source !== 'object' || source === null) return undefined;
  const value = (source as Record<string, unknown>)[key];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

// 严格布尔窄化：非布尔返回 undefined，让调用方走各自的缺省约定。
function readBoolean(source: unknown, key: string): boolean | undefined {
  if (typeof source !== 'object' || source === null) return undefined;
  const value = (source as Record<string, unknown>)[key];
  return typeof value === 'boolean' ? value : undefined;
}
