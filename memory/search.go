package memory

import (
	"sort"
	"strings"
	"time"
	"unicode"
)

// 关键词检索的取舍：LTM 条目是 LLM 已提炼的短事实，文件名/命令/API 名都是
// 强 token，字面命中率够用，故不引倒排索引/向量库。将来换 embedding/RAG 时
// 只需换 Retriever 的实现（retriever.go），检索面不用动。

// tokenize 小写后切词：拉丁字母/数字连续段照旧成 token；CJK（汉字）连续段
// 按相邻双字切 bigram —— 中文无空格，"长沙"成 ["长沙"]、"数据库迁移"成
// ["数据","据库","库迁","迁移"]，靠子串匹配命中含这些词的记忆。
//
// 局限（诚实标注）：query 不含实体词的模糊召回（如"之前查过哪些城市"）
// 关键词检索无解 —— 那是 embedding/RAG 的活，Retriever 抽象正是为此。
func tokenize(s string) []string {
	s = strings.ToLower(s)
	out := make([]string, 0) // 非 nil，空输入返回空切片（与旧 tokenize 行为一致）
	var latin strings.Builder
	var cjk []rune

	flushLatin := func() {
		if latin.Len() > 0 {
			out = append(out, latin.String())
			latin.Reset()
		}
	}
	flushCJK := func() {
		switch len(cjk) {
		case 1:
			out = append(out, string(cjk[0]))
		default:
			for i := 0; i+1 < len(cjk); i++ {
				out = append(out, string(cjk[i:i+2]))
			}
		}
		cjk = cjk[:0]
	}

	for _, r := range s {
		switch {
		case unicode.In(r, unicode.Han):
			flushLatin()
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			flushCJK()
			latin.WriteRune(r)
		default:
			flushLatin()
			flushCJK()
		}
	}
	flushLatin()
	flushCJK()
	return out
}

// scoreFact 给一条事实打分：命中 token 数 × recency。
// recency = 1/(1+小时差/24)，半天衰减一半 —— 近的记忆更容易被想起来。
func scoreFact(f *Fact, tokens []string, now time.Time) float64 {
	if len(tokens) == 0 {
		return 0
	}
	haystack := strings.ToLower(f.Content + " " + f.Topic)
	matched := 0
	for _, t := range tokens {
		if strings.Contains(haystack, t) {
			matched++
		}
	}
	if matched == 0 {
		return 0
	}
	hours := now.Sub(f.UpdatedAt).Hours()
	if hours < 0 {
		hours = 0 // 时钟回拨防御
	}
	return float64(matched) / (1.0 + hours/24.0)
}

// scoreTask 给一条 task 打分，逻辑同 scoreFact，检索主体是
// Title+Summary+NextSteps。open/closed 过滤由调用方决定（注入只取 open，
// 工具全量检索含 closed）。
func scoreTask(t *Task, tokens []string, now time.Time) float64 {
	if len(tokens) == 0 {
		return 0
	}
	haystack := strings.ToLower(t.Title + " " + t.Summary + " " + strings.Join(t.NextSteps, " "))
	matched := 0
	for _, tok := range tokens {
		if strings.Contains(haystack, tok) {
			matched++
		}
	}
	if matched == 0 {
		return 0
	}
	hours := now.Sub(t.UpdatedAt).Hours()
	if hours < 0 {
		hours = 0
	}
	return float64(matched) / (1.0 + hours/24.0)
}

// SearchScored 返回 query 命中的事实，按分数降序、同分按更新时间降序，取 topK。
// 纯函数，方便测试；FileStore.SearchFacts 在锁内调用它。
func SearchScored(query string, facts []Fact, topK int) []Fact {
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return nil
	}
	now := time.Now()
	type scored struct {
		f     Fact
		score float64
	}
	scoredAll := make([]scored, 0, len(facts))
	for _, f := range facts {
		if s := scoreFact(&f, tokens, now); s > 0 {
			scoredAll = append(scoredAll, scored{f: f, score: s})
		}
	}
	sort.SliceStable(scoredAll, func(i, j int) bool {
		if scoredAll[i].score != scoredAll[j].score {
			return scoredAll[i].score > scoredAll[j].score
		}
		return scoredAll[i].f.UpdatedAt.After(scoredAll[j].f.UpdatedAt)
	})
	if topK <= 0 {
		return nil // topK<=0 表示"不取该类"，而不是全返回 —— 配置 0 应禁用而非放量
	}
	if topK > len(scoredAll) {
		topK = len(scoredAll)
	}
	out := make([]Fact, 0, topK)
	for i := 0; i < topK; i++ {
		out = append(out, scoredAll[i].f)
	}
	return out
}

// SearchTasksScored 返回 query 命中的 task（open/closed 由调用方先过滤），规则同 SearchScored。
// 纯函数；FileStore.SearchTasks 在锁内调用它。
func SearchTasksScored(query string, tasks []Task, topK int) []Task {
	tokens := tokenize(query)
	if len(tokens) == 0 {
		return nil
	}
	now := time.Now()
	type scored struct {
		t     Task
		score float64
	}
	scoredAll := make([]scored, 0, len(tasks))
	for _, t := range tasks {
		if s := scoreTask(&t, tokens, now); s > 0 {
			scoredAll = append(scoredAll, scored{t: t, score: s})
		}
	}
	sort.SliceStable(scoredAll, func(i, j int) bool {
		if scoredAll[i].score != scoredAll[j].score {
			return scoredAll[i].score > scoredAll[j].score
		}
		return scoredAll[i].t.UpdatedAt.After(scoredAll[j].t.UpdatedAt)
	})
	if topK <= 0 {
		return nil // topK<=0 表示"不取该类"，而不是全返回 —— 配置 0 应禁用而非放量
	}
	if topK > len(scoredAll) {
		topK = len(scoredAll)
	}
	out := make([]Task, 0, topK)
	for i := 0; i < topK; i++ {
		out = append(out, scoredAll[i].t)
	}
	return out
}
