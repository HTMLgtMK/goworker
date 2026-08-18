package memory

import (
	"context"
	"time"
)

// Result 是统一检索的一条结果：要么是 task，要么是 fact（二者至多一个非 nil）。
type Result struct {
	Score float64
	Task  *Task
	Fact  *Fact
}

// updatedAt 返回该结果的更新时间，供跨类型排序（同分时最近的排前）。
func (r Result) updatedAt() time.Time {
	if r.Task != nil {
		return r.Task.UpdatedAt
	}
	if r.Fact != nil {
		return r.Fact.UpdatedAt
	}
	return time.Time{}
}

// Retriever 抽象检索面：换 RAG/embedding/KG 只换实现，调用方不感知。
// 默认实现是 KeywordRetriever（零依赖），未来可换成向量/图检索后端。
type Retriever interface {
	SearchFacts(ctx context.Context, query string, topK int) ([]Result, error)
	// SearchTasks 检索 task。includeClosed=false 只召 open（注入提醒）；
	// true 连 closed 一起（agent 主动查历史档案，如 memory_search 工具）。
	SearchTasks(ctx context.Context, query string, topK int, includeClosed bool) ([]Result, error)
}

// KeywordRetriever 关键词检索默认实现，底层走 FileStore 的 SearchFacts/SearchTasks。
type KeywordRetriever struct {
	store *FileStore
}

func NewKeywordRetriever(s *FileStore) *KeywordRetriever {
	return &KeywordRetriever{store: s}
}

// SearchFacts 按关键词召回 LTM 事实。store.SearchFacts 已按分数排序，
// 这里对 topK 重算 score 回填到 Result（topK 很小，重算成本可忽略）。
func (r *KeywordRetriever) SearchFacts(ctx context.Context, query string, topK int) ([]Result, error) {
	facts, err := r.store.SearchFacts(query, topK)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tokens := tokenize(query)
	out := make([]Result, 0, len(facts))
	for i := range facts {
		out = append(out, Result{Fact: &facts[i], Score: scoreFact(&facts[i], tokens, now)})
	}
	return out, nil
}

// SearchTasks 按关键词召回 task，closed 是否参与由 includeClosed 决定。
func (r *KeywordRetriever) SearchTasks(ctx context.Context, query string, topK int, includeClosed bool) ([]Result, error) {
	tasks, err := r.store.SearchTasks(query, topK, includeClosed)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tokens := tokenize(query)
	out := make([]Result, 0, len(tasks))
	for i := range tasks {
		out = append(out, Result{Task: &tasks[i], Score: scoreTask(&tasks[i], tokens, now)})
	}
	return out, nil
}
