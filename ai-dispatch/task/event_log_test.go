package task

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestEventLog_ReplaysHistoryThenStreamsLiveEvents(t *testing.T) {
	log, err := OpenEventLog(t.TempDir() + "/task-events.jsonl")
	if err != nil {
		t.Fatalf("OpenEventLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })

	first := TaskEvent{TaskID: "task_1", Type: EventUpdate, Update: []byte(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"first"}}`)}
	if err := log.Append(first); err != nil {
		t.Fatalf("Append first: %v", err)
	}

	history, cursor, live, unsubscribe := log.Subscribe("task_1")
	t.Cleanup(unsubscribe)
	if len(history) != 1 || string(history[0].Update) != string(first.Update) {
		t.Fatalf("history = %#v, want first update", history)
	}

	second := TaskEvent{TaskID: "task_1", Type: EventUpdate, Update: []byte(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"second"}}`)}
	if err := log.Append(second); err != nil {
		t.Fatalf("Append second: %v", err)
	}

	select {
	case <-live:
		events, next := log.EventsAfter("task_1", cursor)
		if len(events) != 1 || string(events[0].Update) != string(second.Update) {
			t.Errorf("live events = %#v, want second update", events)
		}
		if next != 2 {
			t.Errorf("next cursor = %d, want 2", next)
		}
	case <-time.After(time.Second):
		t.Fatal("live update not received")
	}
}

func TestEventLog_ReplaysAfterReopenAndSkipsPartialLine(t *testing.T) {
	path := t.TempDir() + "/task-events.jsonl"
	log, err := OpenEventLog(path)
	if err != nil {
		t.Fatalf("OpenEventLog: %v", err)
	}
	event := TaskEvent{TaskID: "task_1", Type: EventUpdate, Update: []byte(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"durable"}}`)}
	if err := log.Append(event); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteString(`{"task_id":"partial"`); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close partial: %v", err)
	}

	log, err = OpenEventLog(path)
	if err != nil {
		t.Fatalf("reopen EventLog: %v", err)
	}
	second := TaskEvent{TaskID: "task_1", Type: EventUpdate, Update: []byte(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"after crash"}}`)}
	if err := log.Append(second); err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close after append: %v", err)
	}

	log, err = OpenEventLog(path)
	if err != nil {
		t.Fatalf("reopen after append: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	history, _, _, unsubscribe := log.Subscribe("task_1")
	t.Cleanup(unsubscribe)
	if len(history) != 2 || string(history[0].Update) != string(event.Update) || string(history[1].Update) != string(second.Update) {
		t.Fatalf("replayed history = %#v, want durable and post-crash events", history)
	}
}

func TestEventLog_SlowSubscriberRecoversEveryEvent(t *testing.T) {
	log, err := OpenEventLog(t.TempDir() + "/task-events.jsonl")
	if err != nil {
		t.Fatalf("OpenEventLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })

	_, cursor, live, unsubscribe := log.Subscribe("task_1")
	t.Cleanup(unsubscribe)
	for i := 0; i < 100; i++ {
		event := TaskEvent{TaskID: "task_1", Type: EventUpdate, Update: []byte(fmt.Sprintf(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"%d"}}`, i))}
		if err := log.Append(event); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	select {
	case <-live:
		events, next := log.EventsAfter("task_1", cursor)
		if len(events) != 100 {
			t.Fatalf("events = %d, want 100", len(events))
		}
		if next != 100 {
			t.Fatalf("next cursor = %d, want 100", next)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber was not notified")
	}
}
