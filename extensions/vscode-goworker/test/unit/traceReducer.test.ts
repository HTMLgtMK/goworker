import assert from 'node:assert/strict';
import test from 'node:test';
import { appendTraceUpdate, completeTrace, initialTraceState } from '../../webview/src/state/traceReducer';

test('merges adjacent message chunks without mutating prior state', () => {
  const first = appendTraceUpdate(initialTraceState, {
    sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'hello ' },
  });
  const second = appendTraceUpdate(first, {
    sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'world' },
  });

  assert.equal(first.items.length, 1);
  assert.equal(first.items[0].text, 'hello ');
  assert.equal(second.items.length, 1);
  assert.equal(second.items[0].text, 'hello world');
});

test('updates a tool card by stable toolCallId', () => {
  const called = appendTraceUpdate(initialTraceState, {
    sessionUpdate: 'tool_call', toolCallId: 'tool-1', title: 'Read', status: 'pending',
  });
  const updated = appendTraceUpdate(called, {
    sessionUpdate: 'tool_call_update', toolCallId: 'tool-1', title: 'Read', status: 'completed', rawOutput: 'source',
  });

  assert.equal(updated.items.length, 1);
  assert.equal(updated.items[0].status, 'completed');
  assert.equal(updated.items[0].uncertain, false);
  assert.equal(updated.items[0].raw?.rawOutput, 'source');
});

test('marks fallback tool association as uncertain when worker omits toolCallId', () => {
  const called = appendTraceUpdate(initialTraceState, {
    sessionUpdate: 'tool_call', title: 'Read', status: 'pending',
  });
  const updated = appendTraceUpdate(called, {
    sessionUpdate: 'tool_call_update', title: 'Read', status: 'completed',
  });

  assert.equal(updated.items.length, 1);
  assert.equal(updated.items[0].uncertain, true);
});

test('preserves unknown updates and trace completion', () => {
  const traced = appendTraceUpdate(initialTraceState, { sessionUpdate: 'future_update', feature: true });
  const completed = completeTrace(traced);

  assert.equal(traced.items[0].kind, 'unknown');
  assert.deepEqual(traced.items[0].raw, { sessionUpdate: 'future_update', feature: true });
  assert.equal(completed.completed, true);
  assert.equal(traced.completed, false);
});
