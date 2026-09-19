package task

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store 以 JSONL 追加快照的方式持久化任务：每行是一条完整任务快照，
// 重放时同 ID 后行覆盖前行；半行写入（崩溃残留）在重放时静默跳过。
type Store struct {
	mu    sync.Mutex
	path  string
	file  *os.File
	tasks map[string]*Task
	order []string // 首次出现顺序，保证 List 稳定
}

// Open 打开（或创建）store，并重放已有记录。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("task: mkdir: %w", err)
	}
	s := &Store{
		path:  path,
		tasks: make(map[string]*Task),
	}
	if err := s.replay(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("task: open %s: %w", path, err)
	}
	s.file = f
	return s, nil
}

func (s *Store) replay() error {
	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("task: open %s: %w", s.path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var t Task
		if err := json.Unmarshal(line, &t); err != nil || t.ID == "" {
			continue // 崩溃残留的半行
		}
		if _, ok := s.tasks[t.ID]; !ok {
			s.order = append(s.order, t.ID)
		}
		s.tasks[t.ID] = &t
	}
	return scanner.Err()
}

// Add 写入新任务；ID 冲突或状态非 queued 报错。
func (s *Store) Add(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		return fmt.Errorf("task: empty id")
	}
	if _, ok := s.tasks[t.ID]; ok {
		return fmt.Errorf("task: duplicate id %s", t.ID)
	}
	if t.Status != StatusQueued {
		return fmt.Errorf("task: new task must be %s, got %s", StatusQueued, t.Status)
	}
	if !ValidKind(t.Kind) {
		return fmt.Errorf("task: invalid kind %q", t.Kind)
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = t.CreatedAt
	}
	return s.appendLocked(t)
}

// Update 追加任务快照；任务必须已存在。
func (s *Store) Update(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[t.ID]; !ok {
		return fmt.Errorf("task: unknown id %s", t.ID)
	}
	t.UpdatedAt = time.Now()
	return s.appendLocked(t)
}

func (s *Store) appendLocked(t *Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("task: marshal: %w", err)
	}
	if _, err := s.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("task: append: %w", err)
	}
	snapshot := *t
	if _, exists := s.tasks[t.ID]; !exists {
		s.order = append(s.order, t.ID)
	}
	s.tasks[t.ID] = &snapshot
	return nil
}

// Get 返回任务副本。
func (s *Store) Get(id string) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

// List 按插入顺序返回全部任务副本。
func (s *Store) List() []Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Task, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, *s.tasks[id])
	}
	return out
}

// Close 落盘关闭。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
