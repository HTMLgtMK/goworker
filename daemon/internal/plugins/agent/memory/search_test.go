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
		{"数据库迁移", []string{"数据库迁移"}}, // 中文无空格成单 token，靠子串匹配
		{"mix 中文 mixed-123", []string{"mix", "中文", "mixed", "123"}},
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
