package memory

import (
	"context"
	"testing"
)

func TestClient_NewClientAssembles(t *testing.T) {
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	if c.store == nil || c.retriever == nil {
		t.Fatal("client should assemble store and retriever")
	}
}

func TestClient_SearchMergesTaskAndFact(t *testing.T) {
	c, _ := NewClient(t.TempDir(), 10)
	defer c.Close()
	c.UpsertTask(&Task{Title: "修复长沙部署脚本", Status: "open"})
	c.AddFact(&Fact{Content: "长沙项目用 yaml 配置", Topic: "config"})

	results, err := c.Search(context.Background(), "长沙部署 yaml", 5, 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	var hasTask, hasFact bool
	for _, r := range results {
		if r.Task != nil {
			hasTask = true
		}
		if r.Fact != nil {
			hasFact = true
		}
	}
	if !hasTask || !hasFact {
		t.Fatalf("search should hit both task and fact: %+v", results)
	}
	// 按分数降序：task/fact 都命中时有序
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			t.Fatalf("results not sorted desc: %+v", results)
		}
	}
}

func TestClient_SearchEmptyQuery(t *testing.T) {
	c, _ := NewClient(t.TempDir(), 10)
	defer c.Close()
	c.UpsertTask(&Task{Title: "anything", Status: "open"})
	c.AddFact(&Fact{Content: "anything"})

	results, err := c.Search(context.Background(), "", 5, 5)
	if err != nil {
		t.Fatalf("Search empty: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("empty query = %+v, want none", results)
	}
}

func TestClient_SearchAllIncludesClosed(t *testing.T) {
	c, _ := NewClient(t.TempDir(), 10)
	defer c.Close()
	c.UpsertTask(&Task{Title: "查询长沙天气", Status: "open"})
	c.UpsertTask(&Task{Title: "查询桂东天气", Status: "closed"})
	c.AddFact(&Fact{Content: "wttr.in 支持中文", Topic: "天气查询API"})

	// Search（注入）：closed task 不得泄漏 —— query 只命中 closed 时返回空
	inject, _ := c.Search(context.Background(), "桂东", 5, 5)
	for _, r := range inject {
		if r.Task != nil && r.Task.Status == "closed" {
			t.Fatalf("inject Search leaked closed task: %+v", r.Task)
		}
	}

	// SearchAll（工具）：closed task 也能召回
	all, _ := c.SearchAll(context.Background(), "桂东", 5, 5)
	var foundClosed bool
	for _, r := range all {
		if r.Task != nil && r.Task.Status == "closed" && r.Task.Title == "查询桂东天气" {
			foundClosed = true
		}
	}
	if !foundClosed {
		t.Fatalf("SearchAll should recall closed task, got %+v", all)
	}
}

// TestClient_ImplementsStore 编译期断言：ApplyCheckpoint/ApplyDecisions 收 Store，
// *Client 必须满足它。
func TestClient_ImplementsStore(t *testing.T) {
	var _ Store = (*Client)(nil)
}

func TestClient_NewClientWithRetriever(t *testing.T) {
	// 自定义 retriever：只回一个空结果，验证注入生效
	fake := &stubRetriever{}
	c, err := NewClientWithRetriever(t.TempDir(), 10, fake)
	if err != nil {
		t.Fatalf("NewClientWithRetriever: %v", err)
	}
	defer c.Close()
	results, err := c.Search(context.Background(), "anything", 1, 1)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 || results[0].Fact == nil {
		t.Fatalf("custom retriever not used: %+v", results)
	}

	// nil retriever 退回默认 keyword
	c2, _ := NewClientWithRetriever(t.TempDir(), 10, nil)
	defer c2.Close()
	if _, ok := c2.retriever.(*KeywordRetriever); !ok {
		t.Fatalf("nil retriever should fall back to keyword, got %T", c2.retriever)
	}
}

// stubRetriever 固定返回一条 fact，用于验证自定义检索注入。
type stubRetriever struct{}

func (s *stubRetriever) SearchFacts(context.Context, string, int) ([]Result, error) {
	return []Result{{Score: 1, Fact: &Fact{Content: "stub"}}}, nil
}
func (s *stubRetriever) SearchTasks(context.Context, string, int, bool) ([]Result, error) {
	return nil, nil
}
