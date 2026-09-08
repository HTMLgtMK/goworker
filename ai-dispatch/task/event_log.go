package task

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type EventType string

const (
	EventUpdate EventType = "update"
	EventStatus EventType = "status"
)

// TaskEvent is one durable worker update or task status transition.
type TaskEvent struct {
	TaskID    string          `json:"task_id"`
	Timestamp time.Time       `json:"timestamp"`
	Type      EventType       `json:"type"`
	Update    json.RawMessage `json:"update,omitempty"`
	From      Status          `json:"from,omitempty"`
	To        Status          `json:"to,omitempty"`
}

// EventLog keeps an append-only, task-scoped output history.
type EventLog struct {
	mu          sync.Mutex
	file        *os.File
	events      map[string][]TaskEvent
	subscribers map[string]map[chan struct{}]struct{}
}

func OpenEventLog(path string) (*EventLog, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("task event log: create directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, fmt.Errorf("task event log: restrict directory: %w", err)
	}

	log := &EventLog{
		events:      make(map[string][]TaskEvent),
		subscribers: make(map[string]map[chan struct{}]struct{}),
	}
	if err := log.replay(path); err != nil {
		return nil, err
	}
	if err := trimPartialRecord(path); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("task event log: open %s: %w", path, err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("task event log: restrict file: %w", err)
	}
	log.file = file
	return log, nil
}

func trimPartialRecord(path string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("task event log: read tail %s: %w", path, err)
	}
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return nil
	}
	lastNewline := bytes.LastIndexByte(data, '\n')
	if err := os.Truncate(path, int64(lastNewline+1)); err != nil {
		return fmt.Errorf("task event log: trim partial record: %w", err)
	}
	return nil
}

func (l *EventLog) replay(path string) error {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("task event log: read %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var event TaskEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil || !validEvent(event) {
			continue
		}
		l.events[event.TaskID] = append(l.events[event.TaskID], cloneEvent(event))
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("task event log: scan %s: %w", path, err)
	}
	return nil
}

func (l *EventLog) Append(event TaskEvent) error {
	if !validEvent(event) {
		return fmt.Errorf("task event log: invalid event")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("task event log: marshal: %w", err)
	}
	if _, err := l.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("task event log: append: %w", err)
	}

	event = cloneEvent(event)
	l.events[event.TaskID] = append(l.events[event.TaskID], event)
	for subscriber := range l.subscribers[event.TaskID] {
		select {
		case subscriber <- struct{}{}:
		default:
		}
	}
	return nil
}

// Subscribe atomically returns task history, its cursor, and a signal stream for
// future events. Call EventsAfter after each signal to receive every event since
// the cursor; a full signal channel never loses durable output.
func (l *EventLog) Subscribe(taskID string) ([]TaskEvent, int, <-chan struct{}, func()) {
	l.mu.Lock()
	history := cloneEvents(l.events[taskID])
	cursor := len(history)
	live := make(chan struct{}, 1)
	if l.subscribers[taskID] == nil {
		l.subscribers[taskID] = make(map[chan struct{}]struct{})
	}
	l.subscribers[taskID][live] = struct{}{}
	l.mu.Unlock()

	var once sync.Once
	return history, cursor, live, func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.subscribers[taskID], live)
			if len(l.subscribers[taskID]) == 0 {
				delete(l.subscribers, taskID)
			}
		})
	}
}

// EventsAfter returns all task events written after cursor and the next cursor.
func (l *EventLog) EventsAfter(taskID string, cursor int) ([]TaskEvent, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	events := l.events[taskID]
	if cursor < 0 {
		cursor = 0
	}
	if cursor > len(events) {
		cursor = len(events)
	}
	return cloneEvents(events[cursor:]), len(events)
}

func (l *EventLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

func validEvent(event TaskEvent) bool {
	if event.TaskID == "" {
		return false
	}
	switch event.Type {
	case EventUpdate:
		return len(event.Update) > 0
	case EventStatus:
		return event.To != ""
	default:
		return false
	}
}

func cloneEvents(events []TaskEvent) []TaskEvent {
	out := make([]TaskEvent, len(events))
	for i, event := range events {
		out[i] = cloneEvent(event)
	}
	return out
}

func cloneEvent(event TaskEvent) TaskEvent {
	event.Update = append(json.RawMessage(nil), event.Update...)
	return event
}
