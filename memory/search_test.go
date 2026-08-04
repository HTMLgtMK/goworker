package memory

import (
	"reflect"
	"testing"
	"time"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"hello world", []string{"hello", "world"}},
		{"GoLang-ci-run", []string{"golang", "ci", "run"}},
		{"数据库迁移", []string{"数据", "据库", "库迁", "迁移"}}, // CJK 段按相邻 bigram 切
		{"长沙部署", []string{"长沙", "沙部", "部署"}},
		{"长", []string{"长"}},                                        // 单字 CJK 退化为 unigram
		{"mix 中文 mixed-123", []string{"mix", "中文", "mixed", "123"}}, // "中文"两字 → 单个 bigram
		{"  spaces\tand\nnewline  ", []string{"spaces", "and", "newline"}},
		{"", []string{}},
	}
	for _, tc := range tests {
		if got := tokenize(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("tokenize(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestScoreFact_RecencyDecay(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	tokens := tokenize("postgres")

	fresh := &Fact{Content: "database is postgres", UpdatedAt: now}
	stale := &Fact{Content: "database is postgres", UpdatedAt: now.Add(-48 * time.Hour)}

	freshScore := scoreFact(fresh, tokens, now)
	staleScore := scoreFact(stale, tokens, now)
	if !(freshScore > staleScore) {
		t.Errorf("fresh = %v should outscore stale = %v", freshScore, staleScore)
	}
	if staleScore >= freshScore {
		t.Errorf("stale not decayed: stale %v, fresh %v", staleScore, freshScore)
	}
}

func TestScoreFact_NoMatchZero(t *testing.T) {
	f := &Fact{Content: "deploy pipeline", Topic: "ci"}
	if s := scoreFact(f, tokenize("kafka"), time.Now()); s != 0 {
		t.Errorf("no-match score = %v, want 0", s)
	}
}

func TestSearchScored_RankingAndTopK(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	facts := []Fact{
		{ID: "a", Content: "deploy uses make deploy", Topic: "deploy", UpdatedAt: now.Add(-24 * time.Hour)},
		{ID: "b", Content: "ci runs golangci-lint", Topic: "ci", UpdatedAt: now},
		{ID: "c", Content: "deploy to staging is manual", Topic: "deploy", UpdatedAt: now.Add(-1 * time.Hour)},
		{ID: "d", Content: "database is postgres", Topic: "db", UpdatedAt: now},
	}

	got := SearchScored("deploy", facts, 2)
	if len(got) != 2 {
		t.Fatalf("topK=2 got %d: %+v", len(got), got)
	}
	// c 命中 deploy，a 也命中 deploy；同 token 数时 c 更新排前
	if got[0].ID != "c" {
		t.Errorf("got[0] = %s, want c (newer wins ties)", got[0].ID)
	}
	// a 和 c 都命中 1 个 token；a 时间更旧排后
	if got[1].ID != "a" {
		t.Errorf("got[1] = %s, want a", got[1].ID)
	}

	// topK 超过结果数时全返回
	all := SearchScored("deploy", facts, 100)
	if len(all) != 2 {
		t.Errorf("unbounded search = %d, want 2", len(all))
	}

	// 空 query 返回 nil
	if got := SearchScored("", facts, 10); got != nil {
		t.Errorf("empty query = %+v, want nil", got)
	}
}

func TestSearchScored_ZeroTopKDisables(t *testing.T) {
	// topK<=0 是"禁用该类检索"，不是全返回 —— 配置 0 时应得到 nil
	facts := []Fact{{ID: "a", Content: "deploy uses make", Topic: "deploy"}}
	if got := SearchScored("deploy", facts, 0); got != nil {
		t.Errorf("topK=0 should disable facts, got %+v", got)
	}
	tasks := []Task{{ID: "t1", Title: "deploy config", Status: "open"}}
	if got := SearchTasksScored("deploy", tasks, 0); got != nil {
		t.Errorf("topK=0 should disable tasks, got %+v", got)
	}
}

func TestScoreTask_MatchesRegardlessOfStatus(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	tokens := tokenize("长沙")
	open := &Task{Title: "修复长沙部署脚本", Status: "open", UpdatedAt: now}
	closed := &Task{Title: "修复长沙部署脚本", Status: "closed", UpdatedAt: now}
	// scoreTask 不区分状态 —— open/closed 过滤由调用方（SearchTasks 的 includeClosed）负责
	if s := scoreTask(open, tokens, now); s <= 0 {
		t.Errorf("open task score = %v, want > 0", s)
	}
	if s := scoreTask(closed, tokens, now); s <= 0 {
		t.Errorf("closed task score = %v, want > 0 too", s)
	}
	if s := scoreTask(&Task{Title: "升级 postgres", Status: "open", UpdatedAt: now}, tokens, now); s != 0 {
		t.Errorf("no-match score = %v, want 0", s)
	}
}

func TestSearchTasksScored_BigramRecall(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	tasks := []Task{
		{ID: "a", Title: "修复长沙部署脚本", Status: "open", UpdatedAt: now},
		{ID: "b", Title: "升级 postgres", Status: "open", UpdatedAt: now},
		{ID: "c", Title: "修复长沙部署脚本", Status: "closed", UpdatedAt: now},
	}
	// "长沙部署" 的 bigram 命中 a、c 两条（scoreTask 不区分状态）
	got := SearchTasksScored("长沙部署", tasks, 10)
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2 (%+v)", len(got), got)
	}
	if got := SearchTasksScored("", tasks, 10); got != nil {
		t.Errorf("empty query = %+v, want nil", got)
	}
}
