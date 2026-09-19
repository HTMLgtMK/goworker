import assert from 'node:assert/strict';
import test from 'node:test';
import {
  chatEntryDescription,
  chatEntryLabel,
  formatRelativeTime,
  narrowSessionInfoUpdate,
  toChatEntries,
  toChatEntryClick,
} from '../../src/views/chatEntries';

// chatsTree 的展示层纯函数：清单窄化/当前会话标记/标签/相对时间/命令参数窄化。

const NOW = new Date('2026-09-16T12:00:00Z');

function secondsAgo(seconds: number): string {
  return new Date(NOW.getTime() - seconds * 1000).toISOString();
}

test('toChatEntries keeps the full list and marks the daemon-flagged current session', () => {
  const entries = toChatEntries(
    [
      { sessionId: 'current-1', title: 'fix the bug', updatedAt: secondsAgo(10), isCurrent: true },
      { sessionId: 'archive-1', title: 'older chat', updatedAt: secondsAgo(3600) },
    ],
    'current-1',
  );

  // daemon 现支持归档只读重放：完整清单展示；归档行 isCurrent:false（wire 上按 omitempty 缺省）。
  assert.deepEqual(entries, [
    { sessionId: 'current-1', title: 'fix the bug', updatedAt: secondsAgo(10), isCurrent: true, isLast: true },
    { sessionId: 'archive-1', title: 'older chat', updatedAt: secondsAgo(3600), isCurrent: false, isLast: false },
  ]);

  // 「上次会话」命中归档行同样标 last。
  const withLastArchive = toChatEntries(
    [
      { sessionId: 'current-1', isCurrent: true },
      { sessionId: 'archive-1', title: 'older chat' },
    ],
    'archive-1',
  );
  assert.equal(withLastArchive[1]?.isLast, true);
  assert.equal(withLastArchive[0]?.isLast, false);
});

test('toChatEntries falls back to the index-0 convention when isCurrent is missing', () => {
  // 旧 wire 数据不带 isCurrent：沿用现约定（index 0 为当前会话）。
  assert.deepEqual(toChatEntries([{ sessionId: 'current-1' }, { sessionId: 'archive-1' }]), [
    { sessionId: 'current-1', title: undefined, updatedAt: undefined, isCurrent: true, isLast: false },
    { sessionId: 'archive-1', title: undefined, updatedAt: undefined, isCurrent: false, isLast: false },
  ]);
  // 显式 isCurrent:false 即使在 index 0 也按归档行处理。
  assert.deepEqual(toChatEntries([{ sessionId: 'archived', isCurrent: false }]), [
    { sessionId: 'archived', title: undefined, updatedAt: undefined, isCurrent: false, isLast: false },
  ]);
});

// 回归：/new 之后 store 被归档、head 置空，daemon 不再产出当前会话条目，
// 清单全是显式 isCurrent:false 的归档。逐条回退 `?? index === 0` 会把最新的
// 那条归档标成当前会话，点开即报 read-only（用户实际踩到的 bug）。
test('toChatEntries marks nothing current when the list is all archived', () => {
  const entries = toChatEntries([
    { sessionId: '1789695701804819000', isCurrent: false },
    { sessionId: '1789000000000000000', isCurrent: false },
  ]);

  assert.deepEqual(
    entries.map((entry) => entry.isCurrent),
    [false, false],
  );
});

// 只要清单里有一条带布尔 isCurrent，就整份按它取值 —— 归档的 false 不能被
// 位置约定覆盖掉。
test('toChatEntries trusts the flag for the whole list once any entry carries it', () => {
  const entries = toChatEntries([
    { sessionId: 'archive-newest', isCurrent: false },
    { sessionId: 'live', isCurrent: true },
  ]);

  assert.deepEqual(
    entries.map((entry) => [entry.sessionId, entry.isCurrent]),
    [
      ['archive-newest', false],
      ['live', true],
    ],
  );
});

test('toChatEntries never promotes a later entry to current when index 0 is malformed', () => {
  // index 0 非法（缺 sessionId）时宁缺毋滥：回退判定按 wire 位置，不把归档条目补位成当前会话。
  assert.deepEqual(toChatEntries([{ title: 'no id' }, { sessionId: 'archive-1' }]), [
    { sessionId: 'archive-1', title: undefined, updatedAt: undefined, isCurrent: false, isLast: false },
  ]);
});

test('toChatEntries drops malformed wire data and tolerates non-arrays', () => {
  assert.deepEqual(toChatEntries([null, 'nope', { title: 'no id' }]), []);
  assert.deepEqual(toChatEntries(undefined), []);
  assert.deepEqual(toChatEntries('not an array'), []);
});

test('chatEntryLabel prefers the title and never prefixes a text marker', () => {
  assert.equal(chatEntryLabel({ sessionId: 'abc', title: 'fix the bug', isCurrent: true, isLast: false }), 'fix the bug');
  assert.equal(chatEntryLabel({ sessionId: 'abc', title: 'fix the bug', isCurrent: false, isLast: false }), 'fix the bug');
});

test('chatEntryLabel falls back to the first 12 characters of the session id', () => {
  assert.equal(chatEntryLabel({ sessionId: '0123456789abcdef', isCurrent: false, isLast: false }), '0123456789ab');
  assert.equal(chatEntryLabel({ sessionId: '0123456789abcdef', isCurrent: true, isLast: false }), '0123456789ab');
});

test('formatRelativeTime buckets stay human and fall back to an ISO date', () => {
  assert.equal(formatRelativeTime(undefined, NOW), undefined);
  assert.equal(formatRelativeTime('not a date', NOW), undefined);
  assert.equal(formatRelativeTime(secondsAgo(30), NOW), 'just now');
  assert.equal(formatRelativeTime(secondsAgo(90), NOW), '1m ago');
  assert.equal(formatRelativeTime(secondsAgo(5 * 60), NOW), '5m ago');
  assert.equal(formatRelativeTime(secondsAgo(3 * 3600), NOW), '3h ago');
  assert.equal(formatRelativeTime(secondsAgo(2 * 86400), NOW), '2d ago');
  assert.equal(formatRelativeTime(secondsAgo(10 * 86400), NOW), '2026-09-06');
  // 未来时间（时钟偏差）按刚刚处理。
  assert.equal(formatRelativeTime(new Date(NOW.getTime() + 60000).toISOString(), NOW), 'just now');
});

test('chatEntryDescription marks the current session with now and joins the last marker', () => {
  // 当前会话：description 标 now（替代被否的 ● 文本前缀），不显示相对时间。
  assert.equal(chatEntryDescription({ sessionId: 'a', updatedAt: secondsAgo(10), isCurrent: true, isLast: false }, NOW), 'now');
  assert.equal(chatEntryDescription({ sessionId: 'a', isCurrent: true, isLast: true }, NOW), 'now · last');
  assert.equal(chatEntryDescription({ sessionId: 'a', isCurrent: false, isLast: false }, NOW), undefined);
  assert.equal(
    chatEntryDescription({ sessionId: 'a', updatedAt: secondsAgo(120), isCurrent: false, isLast: false }, NOW),
    '2m ago',
  );
  assert.equal(
    chatEntryDescription({ sessionId: 'a', updatedAt: secondsAgo(120), isCurrent: false, isLast: true }, NOW),
    '2m ago · last',
  );
  assert.equal(chatEntryDescription({ sessionId: 'a', isCurrent: false, isLast: true }, NOW), 'last');
});

test('toChatEntryClick narrows the tree command argument and rejects garbage', () => {
  assert.deepEqual(toChatEntryClick({ sessionId: 's1', isCurrent: true }), { sessionId: 's1', isCurrent: true });
  assert.deepEqual(toChatEntryClick({ sessionId: 's1', isCurrent: 'true' }), { sessionId: 's1', isCurrent: true });
  assert.deepEqual(toChatEntryClick({ sessionId: 's1' }), { sessionId: 's1', isCurrent: false });
  assert.equal(toChatEntryClick({ isCurrent: true }), undefined);
  assert.equal(toChatEntryClick('session'), undefined);
  assert.equal(toChatEntryClick(null), undefined);
});

test('narrowSessionInfoUpdate keeps only usable string fields', () => {
  assert.deepEqual(narrowSessionInfoUpdate({ sessionId: 's1', title: 'renamed' }), {
    sessionId: 's1',
    title: 'renamed',
    updatedAt: undefined,
  });
  assert.deepEqual(narrowSessionInfoUpdate({ updatedAt: '2026-09-16T00:00:00Z' }), {
    sessionId: undefined,
    title: undefined,
    updatedAt: '2026-09-16T00:00:00Z',
  });
  // 全空/垃圾输入返回 undefined，调用方直接忽略。
  assert.equal(narrowSessionInfoUpdate({}), undefined);
  assert.equal(narrowSessionInfoUpdate({ title: 42 }), undefined);
  assert.equal(narrowSessionInfoUpdate('update'), undefined);
  assert.equal(narrowSessionInfoUpdate(undefined), undefined);
});
