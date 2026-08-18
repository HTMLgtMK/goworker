package memory

import (
	"context"
	"log/slog"
	"sort"
)

// Client 是调用方的统一入口：组合存储（FileStore）、检索（Retriever）与
// 串行化固化（Checkpoint）。它完整委托 Store 的全部读写方法 —— 使 *Client
// 自身满足 Store 接口，固化写入（ApplyCheckpoint/ApplyDecisions 收 Store）可直接传它。
type Client struct {
	store     *FileStore
	retriever Retriever
	// checkpointCh 是固化串行化信号量（容量 1）。所有会话/触发点的固化共用，
	// 保证 read→LLM→write 整个周期不交错（见 consolidate.go）。
	checkpointCh chan struct{}
}

// NewClient 打开（或创建）记忆存储，装配默认关键词检索（KeywordRetriever）。
// 目录不可写时返回错误。
func NewClient(dir string, taskKeep int) (*Client, error) {
	return NewClientWithRetriever(dir, taskKeep, nil)
}

// NewClientWithRetriever 用自定义检索后端打开记忆存储 —— 换 RAG/embedding 后端
// 的扩展入口，实现 Retriever 接口传入即可。retriever 为 nil 时退回默认关键词检索。
func NewClientWithRetriever(dir string, taskKeep int, retriever Retriever) (*Client, error) {
	fs, err := NewFileStore(dir, taskKeep)
	if err != nil {
		return nil, err
	}
	if retriever == nil {
		retriever = NewKeywordRetriever(fs)
	}
	return &Client{store: fs, retriever: retriever, checkpointCh: make(chan struct{}, 1)}, nil
}

// Search 统一检索（注入用）：open task 命中 + LTM 事实命中，按分数降序。
// taskTopK/factTopK 分别约束两类结果数量，对应调用方配置的两个注入旋钮。
func (c *Client) Search(ctx context.Context, query string, taskTopK, factTopK int) ([]Result, error) {
	return c.search(ctx, query, taskTopK, factTopK, false)
}

// SearchAll 全量检索（agent 工具用）：含 closed tasks（历史档案）+ LTM 事实。
// 与 Search 的区别只在 includeClosed —— agent 主动查历史时，过期快照不构成误导。
func (c *Client) SearchAll(ctx context.Context, query string, taskTopK, factTopK int) ([]Result, error) {
	return c.search(ctx, query, taskTopK, factTopK, true)
}

func (c *Client) search(ctx context.Context, query string, taskTopK, factTopK int, includeClosed bool) ([]Result, error) {
	tasks, err := c.retriever.SearchTasks(ctx, query, taskTopK, includeClosed)
	if err != nil {
		return nil, err
	}
	facts, err := c.retriever.SearchFacts(ctx, query, factTopK)
	if err != nil {
		return nil, err
	}
	slog.Debug("memory: search", "query", query, "include_closed", includeClosed, "tasks", len(tasks), "facts", len(facts))
	merged := make([]Result, 0, len(tasks)+len(facts))
	merged = append(merged, tasks...)
	merged = append(merged, facts...)
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].Score != merged[j].Score {
			return merged[i].Score > merged[j].Score
		}
		return merged[i].updatedAt().After(merged[j].updatedAt())
	})
	return merged, nil
}

// ---- 委托 store：*Client 满足 Store（FactStore + TaskStore + Close）----

func (c *Client) AddFact(f *Fact) error      { return c.store.AddFact(f) }
func (c *Client) UpdateFact(f *Fact) error   { return c.store.UpdateFact(f) }
func (c *Client) DeleteFact(id string) error { return c.store.DeleteFact(id) }
func (c *Client) SearchFacts(query string, topK int) ([]Fact, error) {
	return c.store.SearchFacts(query, topK)
}
func (c *Client) ListFacts(limit int) ([]Fact, error) {
	return c.store.ListFacts(limit)
}
func (c *Client) UpsertTask(t *Task) error { return c.store.UpsertTask(t) }
func (c *Client) OpenTasks(limit int) ([]Task, error) {
	return c.store.OpenTasks(limit)
}
func (c *Client) ListTasks(limit int) ([]Task, error) {
	return c.store.ListTasks(limit)
}
func (c *Client) SearchTasks(query string, topK int, includeClosed bool) ([]Task, error) {
	return c.store.SearchTasks(query, topK, includeClosed)
}
func (c *Client) CloseTask(id string) error { return c.store.CloseTask(id) }
func (c *Client) Close() error              { return c.store.Close() }
