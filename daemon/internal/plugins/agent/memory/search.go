package memory

import (
	"sort"
	"strings"
	"time"
	"unicode"
)

// 关键词检索的取舍：LTM 条目是 LLM 已提炼的短事实，文件名/命令/API 名都是
// 强 token，字面命中率够用，故不引倒排索引/向量库。将来换 embedding 时
// 只需重写 SearchFacts 的实现，检索面（SearchScored）不用动。

// tokenize 小写后按非字母数字切词。中文无空格，整个短语成单 token，
// 靠 Content 上的子串匹配命中 —— 零依赖下够用的折中。
func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
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
	if topK <= 0 || topK > len(scoredAll) {
		topK = len(scoredAll)
	}
	out := make([]Fact, 0, topK)
	for i := 0; i < topK; i++ {
		out = append(out, scoredAll[i].f)
	}
	return out
}
