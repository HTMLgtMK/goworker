// Package memory 提供可独立发布引入的 agent 记忆组件：
// MTM（任务档案，Task）与 LTM（事实条目，Fact）双层存储，配 mem0-like 的
// 统一检索入口（Client.Search）与 LLM 固化抽取（Checkpointer）。
//
// 设计取舍：LTM 存的是 LLM 已提炼的短事实，文件名/命令/API 名都是强 token，
// 关键词检索命中率够用，故起步零存储依赖 —— 两个 JSONL 文件 + 内存缓存，
// 写时全量原子重写（tmp + rename）。Store 接口拆成 FactStore/TaskStore，
// 将来想上 embedding/RAG 只换 Retriever 实现，调用方不感知。
//
// 生命周期模型：STM（conversation）是会话内工作记忆，由调用方维护；MTM 的
// Task 是跨会话的任务档案，在"固化检查点"写入；LTM 的 Fact 由检查点抽取
// 或调用方手动写入。检索面（Retriever）抽象成接口，默认 KeywordRetriever。
package memory

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Fact 是一条跨会话长期记忆（LTM）。ID 跨更新保持稳定，供 update/delete 引用。
type Fact struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`          // 1-3 句事实陈述，检索命中的主体
	Topic     string    `json:"topic,omitempty"`  // LLM 打的短标签，检索可命中
	Source    string    `json:"source,omitempty"` // "checkpoint:<id>" 自动抽取 | "user" 手动添加
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Task 是一条跨会话的中期记忆（MTM）：一个语义任务，可横跨多个 run/query。
// 只有固化检查点才写入，新会话开场被注入提醒。
type Task struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`  // "修 config 解析 bug"
	Status     string    `json:"status"` // open | closed
	Cwd        string    `json:"cwd,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	Runs       []string  `json:"runs,omitempty"` // 属于该 task 的 runID，留痕
	Summary    string    `json:"summary"`        // 跨检查点累积摘要
	KeyFiles   []string  `json:"key_files,omitempty"`
	Commands   []string  `json:"commands,omitempty"`
	NextSteps  []string  `json:"next_steps,omitempty"`
	TokenUsage int       `json:"token_usage"`
}

// Store 是 memory 的读写面，MemoryMiddleware 与 /memory、/task 命令的注入依赖。
type Store interface {
	FactStore
	TaskStore
	Close() error
}

// FactStore 是 LTM 事实条目的读写接口。换 embedding 后端时只改这里的实现。
type FactStore interface {
	AddFact(f *Fact) error
	UpdateFact(f *Fact) error // 按 ID 替换，保留 CreatedAt；ID 不存在时忽略
	DeleteFact(id string) error
	SearchFacts(query string, topK int) ([]Fact, error)
	ListFacts(limit int) ([]Fact, error) // 最近优先
}

// TaskStore 是 MTM 任务档案的读写接口。
type TaskStore interface {
	UpsertTask(t *Task) error            // ID 空=新建；ID 有=更新（保留 CreatedAt、追加 Runs）
	OpenTasks(limit int) ([]Task, error) // 最近的 open task，最近优先
	ListTasks(limit int) ([]Task, error) // 所有 task（含 closed），最近优先
	CloseTask(id string) error           // 不存在的 id 忽略
}

// FileStore 把记忆存成两个 JSONL 文件（ltm/facts.jsonl、mtm/tasks.jsonl）。
// 内存切片是唯一事实来源，每次写操作锁内改内存后全量原子重写 ——
// 文件里天然没有 tombstone，DELETE 就是删掉再重写。
// 单 Mutex 覆盖前台注入读与检查点固化写，-race 安全。
type FileStore struct {
	mu       sync.Mutex
	dir      string
	ltmPath  string
	taskPath string
	taskKeep int // 保留 task 数，0 = 不裁剪
	facts    []Fact
	tasks    []Task
}

// NewFileStore 打开（或创建）memory 目录。目录不可写时返回错误，调用方降级为无记忆。
func NewFileStore(dir string, taskKeep int) (*FileStore, error) {
	ltmPath := filepath.Join(dir, "ltm", "facts.jsonl")
	taskPath := filepath.Join(dir, "mtm", "tasks.jsonl")
	for _, p := range []string{ltmPath, taskPath} {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			return nil, fmt.Errorf("memory: mkdir %s: %w", filepath.Dir(p), err)
		}
	}
	s := &FileStore{dir: dir, ltmPath: ltmPath, taskPath: taskPath, taskKeep: taskKeep}
	if err := s.load(); err != nil {
		return nil, fmt.Errorf("memory: load: %w", err)
	}
	return s, nil
}

// ---- 内部：加载与原子写 ----

// load 把两个文件读进内存。坏行跳过不报错（容忍 crash 留下的半行）。
func (s *FileStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines, err := readJSONL(s.ltmPath)
	if err != nil {
		return err
	}
	for _, line := range lines {
		var f Fact
		if json.Unmarshal(line, &f) != nil {
			slog.Warn("memory: skip corrupt fact line", "path", s.ltmPath)
			continue
		}
		s.facts = append(s.facts, f)
	}
	lines, err = readJSONL(s.taskPath)
	if err != nil {
		return err
	}
	for _, line := range lines {
		var t Task
		if json.Unmarshal(line, &t) != nil {
			slog.Warn("memory: skip corrupt task line", "path", s.taskPath)
			continue
		}
		s.tasks = append(s.tasks, t)
	}
	return nil
}

// readJSONL 读文件所有行，返回原始字节。文件不存在视为空（首次运行）。
func readJSONL(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out [][]byte
	sc := bufio.NewScanner(f)
	// 单行最大 1MB，超出视为坏行跳过（避免恶意/损坏文件吃内存）
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		if line := bytes.TrimSpace(sc.Bytes()); len(line) > 0 {
			out = append(out, slices.Clone(line))
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *FileStore) saveFactsLocked() error { return writeJSONL(s.ltmPath, s.facts) }
func (s *FileStore) saveTasksLocked() error { return writeJSONL(s.taskPath, s.tasks) }

// writeJSONL 逐条 marshal 成一行，写 tmp 再 rename（照抄 config.Save 的原子写模式）。
func writeJSONL(path string, items any) error {
	var b strings.Builder
	switch v := items.(type) {
	case []Fact:
		for _, f := range v {
			line, err := json.Marshal(f)
			if err != nil {
				return err
			}
			b.Write(line)
			b.WriteByte('\n')
		}
	case []Task:
		for _, t := range v {
			line, err := json.Marshal(t)
			if err != nil {
				return err
			}
			b.Write(line)
			b.WriteByte('\n')
		}
	}
	tmp := filepath.Join(filepath.Dir(path), ".tmp.jsonl")
	if err := os.WriteFile(tmp, []byte(b.String()), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// NewID 生成条目 id（4 字节随机 hex）。随机源失败时退到时间戳。
func NewID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("t_%d", time.Now().UnixNano())
	}
	return "t_" + hex.EncodeToString(b)
}

// ---- LTM：FactStore 实现 ----

// AddFact 追加一条事实。Content 与已有完全相同时跳过（LLM 可能反复抽同一条）。
func (s *FileStore) AddFact(f *Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.ID == "" {
		f.ID = NewID()
	}
	now := time.Now()
	if f.CreatedAt.IsZero() {
		f.CreatedAt = now
	}
	f.UpdatedAt = now
	if duplicateContent(s.facts, f.Content) {
		return nil
	}
	s.facts = append(s.facts, *f)
	return s.saveFactsLocked()
}

// UpdateFact 按 ID 替换内容，保留 CreatedAt。ID 不存在时忽略（容错模型引用错 id）。
func (s *FileStore) UpdateFact(f *Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.facts {
		if s.facts[i].ID == f.ID {
			old := s.facts[i]
			s.facts[i] = *f
			s.facts[i].CreatedAt = old.CreatedAt
			s.facts[i].UpdatedAt = time.Now()
			return s.saveFactsLocked()
		}
	}
	return nil
}

// DeleteFact 删除指定 id 的条目。不存在不报错。
func (s *FileStore) DeleteFact(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.facts[:0]
	for _, f := range s.facts {
		if f.ID != id {
			kept = append(kept, f)
		}
	}
	if len(kept) == len(s.facts) {
		return nil
	}
	s.facts = kept
	return s.saveFactsLocked()
}

// SearchFacts 关键词检索（见 search.go 的打分规则），分数降序取 topK。
func (s *FileStore) SearchFacts(query string, topK int) ([]Fact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SearchScored(query, s.facts, topK), nil
}

// ListFacts 返回最近更新的 limit 条事实。
func (s *FileStore) ListFacts(limit int) ([]Fact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Clone(s.facts)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

// ---- MTM：TaskStore 实现 ----

// UpsertTask 新建（ID 空 → 生成、Status 默认 open、CreatedAt=now）或更新
// （按 ID 替换字段，保留 CreatedAt、追加 Runs、刷新 UpdatedAt）。
func (s *FileStore) UpsertTask(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if t.ID == "" {
		t.ID = NewID()
		if t.Status == "" {
			t.Status = "open"
		}
		if t.CreatedAt.IsZero() {
			t.CreatedAt = now
		}
		t.UpdatedAt = now
		if t.Runs == nil {
			t.Runs = []string{}
		}
		s.tasks = append(s.tasks, *t)
		return s.trimAndSaveLocked()
	}
	for i := range s.tasks {
		if s.tasks[i].ID == t.ID {
			old := s.tasks[i]
			t.CreatedAt = old.CreatedAt
			if len(t.Runs) > 0 {
				t.Runs = append(append([]string{}, old.Runs...), t.Runs...)
			} else {
				t.Runs = old.Runs
			}
			t.UpdatedAt = now
			s.tasks[i] = *t
			return s.trimAndSaveLocked()
		}
	}
	return nil // ID 不存在：忽略（容错模型引用错 id）
}

// OpenTasks 返回最近的 limit 个 open task（UpdatedAt 降序）。
func (s *FileStore) OpenTasks(limit int) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Task
	for _, t := range s.tasks {
		if t.Status == "open" {
			out = append(out, t)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

// SearchTasks 关键词检索 task（打分见 search.go 的 scoreTask），分数降序取 topK。
// includeClosed=false 只检索 open（注入提醒）；true 连 closed 一起（历史档案，
// 供 memory_search 工具主动回顾 —— 工具是 agent 主动查档，过期快照不构成误导）。
func (s *FileStore) SearchTasks(query string, topK int, includeClosed bool) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if includeClosed {
		return SearchTasksScored(query, s.tasks, topK), nil
	}
	open := make([]Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		if t.Status == "open" {
			open = append(open, t)
		}
	}
	return SearchTasksScored(query, open, topK), nil
}

// ListTasks 返回最近的 limit 个 task（含 closed），UpdatedAt 降序。
func (s *FileStore) ListTasks(limit int) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Clone(s.tasks)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out, nil
}

// CloseTask 把 task 标记为 closed。不存在的 id 忽略。
func (s *FileStore) CloseTask(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.tasks {
		if s.tasks[i].ID == id && s.tasks[i].Status != "closed" {
			s.tasks[i].Status = "closed"
			s.tasks[i].UpdatedAt = time.Now()
			return s.saveTasksLocked()
		}
	}
	return nil
}

// trimAndSaveLocked 超 taskKeep 时裁掉最旧（含 closed）再写盘。
func (s *FileStore) trimAndSaveLocked() error {
	if s.taskKeep > 0 && len(s.tasks) > s.taskKeep {
		// 保留最近 taskKeep 条：按 UpdatedAt 排序后裁
		sort.SliceStable(s.tasks, func(i, j int) bool {
			return s.tasks[i].UpdatedAt.After(s.tasks[j].UpdatedAt)
		})
		s.tasks = s.tasks[:s.taskKeep]
	}
	return s.saveTasksLocked()
}

// Close 关闭 store。每次写操作已即时原子落盘，这里无需刷盘，仅保留接口对称性。
func (s *FileStore) Close() error {
	return nil
}

// ---- 小工具 ----

func duplicateContent(facts []Fact, content string) bool {
	target := strings.ToLower(strings.TrimSpace(content))
	for _, f := range facts {
		if strings.ToLower(strings.TrimSpace(f.Content)) == target {
			return true
		}
	}
	return false
}
