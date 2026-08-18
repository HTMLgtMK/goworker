package memory

import (
	"strings"
	"time"
)

// 写路径设计（借鉴 Mem0）：不是无脑把会话追加进事实集，而是让 LLM 对每条
// 候选事实给出处置决策 —— 纯追加会让事实集快速长出重复和互相矛盾的条目。
// 决策由检查点固化（Checkpointer）统一产出并落库。

// DecisionType 是抽取模型对单条候选事实的处置。
type DecisionType string

const (
	DecisionAdd    DecisionType = "add"
	DecisionUpdate DecisionType = "update"
	DecisionDelete DecisionType = "delete"
	DecisionNoop   DecisionType = "noop"
)

// Decision 是抽取模型对一条候选事实的决策。
type Decision struct {
	Action  DecisionType `json:"action"`
	ID      string       `json:"id,omitempty"`      // update/delete 引用已有事实
	Content string       `json:"content,omitempty"` // add/update 的新内容
	Topic   string       `json:"topic,omitempty"`
}

// validActions 用于校验模型输出，坏 action 一律丢弃。
var validActions = map[DecisionType]bool{
	DecisionAdd: true, DecisionUpdate: true, DecisionDelete: true, DecisionNoop: true,
}

// maxFactContentRunes 单条事实长度上限，防模型吐小作文把检索面撑爆。
const maxFactContentRunes = 2000

// overlapFactThreshold 判定"两条 fact 是同一事实的演化版本"的覆盖度阈值。
// 覆盖度 = |new∩old| / min(|new|,|old|)：wttr.in 重复案例实测旧内容 100% 被
// 新内容覆盖（cov=1.0），0.8 留了余量，又不至于误伤不同主题的条目。
const overlapFactThreshold = 0.8

// supersetThreshold 方向判定的超集阈值：交集占旧内容的比例 >= 它，才认为新内容
// 几乎完全包含旧内容（新是超集），可以用新的覆盖旧的而不丢信息。等长但互有
// 独有 token 时此比例 < 1，判 false 保守跳过 —— 覆盖会永久丢掉旧 fact 的独有信息。
const supersetThreshold = 0.9

// ApplyDecisions 把抽取决策落到 store。
// current 必须与检查点传入的是同一份，保证 update/delete 的 id 定位一致。
// source 是事实来源前缀（如 "checkpoint:"），调用方拼接本次 runID。
// 返回实际执行（add+update+delete）的条数 + add/update 的明细行 + delete 的明细行。
// 明细只含真正落库的条目（被过滤的坏 action/空内容/重复项不在其中）。
func ApplyDecisions(store Store, decisions []Decision, current []Fact, source string) (int, []string, []string, error) {
	byID := make(map[string]Fact, len(current))
	for _, f := range current {
		byID[f.ID] = f
	}
	now := time.Now()
	applied := 0
	var appliedFacts, deletedFacts []string
	// 同批去重：duplicateContent(current) 只比对固化前快照，不含本批次内已 add 的 ——
	// 不补这个集合，同批两条相同 content 的 add 会在明细里虚报（store 侧 AddFact 静默去重）。
	added := make(map[string]bool)
	// 本批已 delete 的 id：后续 update / add 覆盖引用它时忽略 —— 模型可能在同一批
	// 里先 delete 又"改进"同一条（矛盾输出），此时 store 里该 id 已删，update 是空转。
	deleted := make(map[string]bool)
	for _, d := range decisions {
		switch d.Action {
		case DecisionAdd:
			key := strings.ToLower(strings.TrimSpace(d.Content))
			if key == "" || duplicateContent(current, d.Content) || added[key] {
				continue
			}
			// 兜底：模型漏判的重复 add —— 新 fact 与已有 fact 高度重叠时，不新增，
			// 而是按覆盖方向转 update（新盖旧）或跳过（旧盖新）。这是检索式召回
			// 之外的第二道保险：prompt 是软约束，模型会偷懒，这里确定性拦截。
			if hit, old, newCovers := findOverlapFact(current, d.Content); hit {
				added[key] = true // 兜底处理后视为已处理，同批重复 add 不再重复判定
				if newCovers {
					if deleted[old.ID] {
						continue // 该旧 fact 本批已被 delete，覆盖是空转
					}
					nf, err := applyUpdate(store, *old, d.Content, d.Topic)
					if err != nil {
						return applied, appliedFacts, deletedFacts, err
					}
					applied++
					appliedFacts = append(appliedFacts, factLine(nf.Topic, nf.Content))
				}
				// 旧盖新 / 互有独有：新内容不是旧事实的超集，保留旧 fact
				continue
			}
			if err := store.AddFact(&Fact{
				Content: d.Content, Topic: d.Topic, Source: source,
				CreatedAt: now, UpdatedAt: now,
			}); err != nil {
				return applied, appliedFacts, deletedFacts, err
			}
			applied++
			added[key] = true
			appliedFacts = append(appliedFacts, factLine(d.Topic, d.Content))
		case DecisionUpdate:
			old, ok := byID[d.ID]
			if !ok || strings.TrimSpace(d.Content) == "" || deleted[d.ID] {
				continue // 模型引用了不存在的 id、空内容、或该 id 本批已被 delete，忽略
			}
			// prompt 只要求 update 带 id+content，topic 常缺省 —— 空则保留旧值，
			// 否则按主题关键词检索的命中率会被清空 topic 悄悄拉低。
			nf, err := applyUpdate(store, old, d.Content, d.Topic)
			if err != nil {
				return applied, appliedFacts, deletedFacts, err
			}
			applied++
			appliedFacts = append(appliedFacts, factLine(nf.Topic, nf.Content))
		case DecisionDelete:
			old, ok := byID[d.ID]
			if !ok {
				continue
			}
			if err := store.DeleteFact(d.ID); err != nil {
				return applied, appliedFacts, deletedFacts, err
			}
			deleted[d.ID] = true // 同批后续 update/add 覆盖该 id 时忽略（防矛盾输出空转）
			applied++
			deletedFacts = append(deletedFacts, factLine(old.Topic, old.Content))
		}
	}
	return applied, appliedFacts, deletedFacts, nil
}

// factLine 把一条已应用的事实压成单行摘要供回显："topic: content"（topic 空则纯 content）。
func factLine(topic, content string) string {
	c := singleLine(content)
	if topic != "" {
		return topic + ": " + c
	}
	return c
}

// tokenSet 把文本 tokenize 后转成集合，供覆盖度判定用。
func tokenSet(s string) map[string]bool {
	out := make(map[string]bool)
	for _, t := range tokenize(s) {
		out[t] = true
	}
	return out
}

// findOverlapFact 在已有 facts 里找与 newContent 属于同一事实演化版本的一条。
// 判定指标是单方向覆盖度（不是 Jaccard）：
//
//	覆盖度 = |new ∩ old| / min(|new|, |old|)   —— 较短一方被较长一方覆盖的比例
//
// 覆盖度 >= overlapFactThreshold 视为"同一事实的演化版本"。命中返回
// (true, fact, newCoversOld)：
//   - newCoversOld=true  新内容覆盖旧内容（新信息更多），应 update 旧 fact
//   - newCoversOld=false 旧内容覆盖新内容（新信息是子集），应跳过新增
//
// 多条命中返回覆盖度最高的那一条；无一命中返回 (false, nil, false)。
func findOverlapFact(facts []Fact, newContent string) (bool, *Fact, bool) {
	newSet := tokenSet(newContent)
	if len(newSet) == 0 {
		return false, nil, false
	}
	var best *Fact
	var bestCov float64
	bestNewCovers := false
	for i := range facts {
		oldSet := tokenSet(facts[i].Content)
		if len(oldSet) == 0 {
			continue
		}
		inter := 0
		for t := range oldSet {
			if newSet[t] {
				inter++
			}
		}
		if inter == 0 {
			continue
		}
		minLen := len(oldSet)
		if len(newSet) < minLen {
			minLen = len(newSet)
		}
		cov := float64(inter) / float64(minLen)
		if cov >= overlapFactThreshold && cov > bestCov {
			best = &facts[i]
			bestCov = cov
			// 方向：交集占 old 的比例 >= supersetThreshold 才认为"新内容覆盖旧内容"
			// （新是旧的超集），用新的替换不丢信息。等长但互有独有 token 时此比例
			// < 1，判 false —— 保守跳过，覆盖会丢旧 fact 的独有信息。
			bestNewCovers = float64(inter)/float64(len(oldSet)) >= supersetThreshold
		}
	}
	if best == nil {
		return false, nil, false
	}
	return true, best, bestNewCovers
}

// applyUpdate 按 ID 替换 fact 内容，保留旧 Topic（topic 空时）、Source、CreatedAt。
// 返回应用后的 Fact（含解析后的 topic），供调用方拼明细。ID 不存在时 UpdateFact 空转返回 nil。
func applyUpdate(store Store, old Fact, content, topic string) (Fact, error) {
	if topic == "" {
		topic = old.Topic // prompt 只要求 update 带 id+content，topic 常缺省 —— 空则保留旧值
	}
	nf := Fact{ID: old.ID, Content: content, Topic: topic, Source: old.Source, CreatedAt: old.CreatedAt}
	return nf, store.UpdateFact(&nf)
}
