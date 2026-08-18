package memory

import (
	"context"
	"testing"
)

func TestKeywordRetriever_SearchTasksIncludeClosed(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	s.UpsertTask(&Task{Title: "修复长沙部署脚本", Status: "open"})
	s.UpsertTask(&Task{Title: "修复长沙部署脚本", Status: "closed"})

	r := NewKeywordRetriever(s)

	// includeClosed=false（注入）：只召 open
	results, err := r.SearchTasks(context.Background(), "长沙部署", 10, false)
	if err != nil {
		t.Fatalf("SearchTasks: %v", err)
	}
	if len(results) != 1 || results[0].Task == nil || results[0].Task.Status != "open" {
		t.Fatalf("includeClosed=false: got %+v, want only the open task", results)
	}
	if results[0].Score <= 0 {
		t.Errorf("score = %v, want > 0", results[0].Score)
	}

	// includeClosed=true（工具）：open + closed 都召回
	all, err := r.SearchTasks(context.Background(), "长沙部署", 10, true)
	if err != nil {
		t.Fatalf("SearchTasks all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("includeClosed=true: got %d results, want 2 (%+v)", len(all), all)
	}
}

func TestKeywordRetriever_SearchFactsScores(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	s.AddFact(&Fact{Content: "长沙的项目用 yaml 配置", Topic: "config"})

	r := NewKeywordRetriever(s)
	results, err := r.SearchFacts(context.Background(), "长沙", 10)
	if err != nil {
		t.Fatalf("SearchFacts: %v", err)
	}
	if len(results) != 1 || results[0].Fact == nil || results[0].Score <= 0 {
		t.Fatalf("got %+v, want 1 scored fact", results)
	}
}
