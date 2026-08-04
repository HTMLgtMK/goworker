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
	for _, d := range decisions {
		switch d.Action {
		case DecisionAdd:
			key := strings.ToLower(strings.TrimSpace(d.Content))
			if key == "" || duplicateContent(current, d.Content) || added[key] {
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
			if !ok || strings.TrimSpace(d.Content) == "" {
				continue // 模型引用了不存在的 id 或空内容，忽略
			}
			// prompt 只要求 update 带 id+content，topic 常缺省 —— 空则保留旧值，
			// 否则按主题关键词检索的命中率会被清空 topic 悄悄拉低。
			topic := old.Topic
			if d.Topic != "" {
				topic = d.Topic
			}
			if err := store.UpdateFact(&Fact{
				ID: old.ID, Content: d.Content, Topic: topic,
				Source: old.Source, CreatedAt: old.CreatedAt,
			}); err != nil {
				return applied, appliedFacts, deletedFacts, err
			}
			applied++
			appliedFacts = append(appliedFacts, factLine(topic, d.Content))
		case DecisionDelete:
			old, ok := byID[d.ID]
			if !ok {
				continue
			}
			if err := store.DeleteFact(d.ID); err != nil {
				return applied, appliedFacts, deletedFacts, err
			}
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
