package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Checkpointer 在"固化检查点"时把当前 conversation 沉淀为 task 更新 + LTM 事实。
// 一次 LLM 调用打包两件事（省一次推理）：
//   - 判定每个 candidate task 归属已有 open task / 新建 / 完成关闭
//   - 从对话抽取长期事实（分类决策，走 LTM 管线）
//
// 触发点都是低频事件：/compact、进程退出、/new、/task checkpoint。
// 平时（每 run）不做，会话内连续性由 STM 提供。
type Checkpointer struct {
	provider LLM // 本包定义的极简 LLM 面（llm.go），调用方用适配器注入
	cwd      string
}

func NewCheckpointer(provider LLM, cwd string) *Checkpointer {
	return &Checkpointer{provider: provider, cwd: cwd}
}

// TaskUpdate 是检查点模型对单条候选 task 的判定。
type TaskUpdate struct {
	ID           string   `json:"id,omitempty"` // 已有 open task 的 id；空 = 新建
	Title        string   `json:"title,omitempty"`
	Done         bool     `json:"done"` // true = 该 task 已完成，关闭
	SummaryDelta string   `json:"summary_delta,omitempty"`
	KeyFiles     []string `json:"key_files,omitempty"`
	Commands     []string `json:"commands,omitempty"`
	NextSteps    []string `json:"next_steps,omitempty"`
}

// CheckpointResult 是一次检查点 LLM 调用的完整输出。
type CheckpointResult struct {
	Tasks     []TaskUpdate `json:"tasks"`
	Decisions []Decision   `json:"decisions"`
}

// Run 处理全量 conversation，返回 task 更新 + LTM 决策。
// openTasks 与 facts 必须与后续 ApplyCheckpoint 传入的是同一份（id 定位一致）。
func (c *Checkpointer) Run(ctx context.Context, conversation []Message, openTasks []Task, facts []Fact) (*CheckpointResult, error) {
	req := &ChatRequest{
		Model: c.provider.Model(),
		Messages: append(
			[]Message{{Role: "system", Content: checkpointPrompt(openTasks, facts)}},
			conversation...,
		),
	}
	resp, err := c.provider.Chat(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("empty response")
	}
	return parseCheckpoint(resp.Choices[0].Message.Content)
}

// checkpointPrompt 构造固化指令。要点：判定 task 归属（现有 id 或新建）、
// 摘要增量（task 跨检查点累积，不重述历史）、done 标记、输出 ONLY JSON。
func checkpointPrompt(openTasks []Task, facts []Fact) string {
	var b strings.Builder
	b.WriteString("You are a task archivist for a coding assistant. You are given the full conversation\n")
	b.WriteString("of a working session and must update the task list and long-term memory.\n\n")
	b.WriteString("Open tasks (id | title | summary):\n")
	if len(openTasks) == 0 {
		b.WriteString("(none)\n")
	} else {
		for _, t := range openTasks {
			fmt.Fprintf(&b, "- %s | %s | %s\n", t.ID, t.Title, singleLine(t.Summary))
		}
	}
	b.WriteString("\nFor the work done in the conversation, emit one entry per distinct task touched.\n")
	b.WriteString("For each entry:\n")
	b.WriteString(`- "id": the id of an open task this work belongs to; omit to create a new task.
- "title": a short task title (required when creating a new task).
- "done": true if this task is now complete.
- "summary_delta": NEW progress only — facts and decisions not already captured in the task summary. Do not restate the existing summary.
- "key_files"/"commands"/"next_steps": updated values (full replacement lists, not deltas).
`)
	b.WriteString("Then extract durable cross-session facts into \"decisions\" (see below).\n")
	if len(facts) > 0 {
		b.WriteString("\nExisting facts (id | content):\n")
		for _, f := range facts {
			fmt.Fprintf(&b, "- %s | %s\n", f.ID, singleLine(f.Content))
		}
	}
	b.WriteString(`
Decision actions:
- "add": new fact worth remembering. Provide content and topic.
- "update": refines/contradicts an existing fact. Set id, give new content.
- "delete": an existing fact is now wrong. Set id.
- "noop": trivial or already covered.

Output ONLY a JSON object, no commentary, no markdown:
{"tasks":[{"id":"t_xxx","title":"...","done":false,"summary_delta":"...","key_files":[],"commands":[],"next_steps":[]}],
 "decisions":[{"action":"add","content":"...","topic":"...","id":""}]}
If nothing is worth saving, output {"tasks":[],"decisions":[]}.`)
	return b.String()
}

// parseCheckpoint 容错解析模型输出：剥围栏、取 JSON 窗口、逐条校验。
func parseCheckpoint(s string) (*CheckpointResult, error) {
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no JSON object in checkpoint output")
	}
	var out CheckpointResult
	if err := json.Unmarshal([]byte(s[start:end+1]), &out); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	// 校验：新建 task 必须有 title；done 与 id 冲突（新建又 done）降级为新建并关闭
	valid := out.Tasks[:0]
	for _, t := range out.Tasks {
		if t.ID == "" && strings.TrimSpace(t.Title) == "" {
			continue // 既没引用 id 也没给 title，没法归档
		}
		if r := []rune(t.SummaryDelta); len(r) > 4000 {
			t.SummaryDelta = string(r[:4000])
		}
		valid = append(valid, t)
	}
	out.Tasks = valid
	// 决策校验：坏 action 丢弃、content 限长
	decs := out.Decisions[:0]
	for _, d := range out.Decisions {
		if !validActions[d.Action] {
			continue
		}
		if r := []rune(d.Content); len(r) > maxFactContentRunes {
			d.Content = string(r[:maxFactContentRunes])
		}
		decs = append(decs, d)
	}
	out.Decisions = decs
	return &out, nil
}

// AppliedSummary 是一次检查点应用的实际结果明细，供调用方回显/留痕。
// 只含真正落库的条目：被过滤的坏 action、空内容、重复 fact、未知 task id 都不算。
type AppliedSummary struct {
	UpdatedTasks []string // 本次新建或归入已有 task 的标题
	ClosedTasks  []string // 本次被标记完成（done）的 task 标题
	Facts        []string // 已 add/update 的事实，格式 "topic: content"（topic 空则纯 content）
	DeletedFacts []string // 已删除的事实，格式同 Facts
}

// Count 返回应用条目总数（task 更新 + task 关闭 + fact 增删）。
func (s *AppliedSummary) Count() int {
	return len(s.UpdatedTasks) + len(s.ClosedTasks) + len(s.Facts) + len(s.DeletedFacts)
}

// ApplyCheckpoint 把检查点结果落到 store：更新/新建 task、应用 LTM 决策。
// openTasks 与 facts 必须与 Run 传入的是同一份。返回实际应用的明细。
func ApplyCheckpoint(store Store, res *CheckpointResult, openTasks []Task, facts []Fact, runID string, tokens int, cwd string) (*AppliedSummary, error) {
	byID := make(map[string]Task, len(openTasks))
	for _, t := range openTasks {
		byID[t.ID] = t
	}
	sum := &AppliedSummary{}
	// 同 id 多条 TaskUpdate（LLM 输出异常）只按最终 done 状态记一条明细，
	// 否则同一 task 会同时进 Updated/Closed 重复渲染、计数虚高。
	type taskRec struct {
		title  string
		closed bool
	}
	existing := make(map[string]taskRec)
	var existingOrder []string
	for _, u := range res.Tasks {
		if u.ID != "" {
			merged, ok := byID[u.ID]
			if !ok {
				continue // 引用了不存在的 task id，忽略
			}
			merged.Summary = mergeText(merged.Summary, u.SummaryDelta)
			// prompt 要求 key_files/commands/next_steps 为 full replacement lists ——
			// 直接替换而非并集，否则模型想删的条目（输出空列表）永远删不掉。
			merged.KeyFiles = u.KeyFiles
			merged.Commands = u.Commands
			merged.NextSteps = u.NextSteps
			if u.Done {
				merged.Status = "closed"
			}
			merged.Runs = []string{runID}
			merged.TokenUsage = byID[u.ID].TokenUsage + tokens
			// 写回 live map：同 id 出现多条 TaskUpdate 时，后一条基于前一条的
			// 合并结果增量叠加，而不是回到初始快照覆盖（否则前一条更新丢失）。
			byID[u.ID] = merged
			if err := store.UpsertTask(&merged); err != nil {
				return sum, err
			}
			if _, ok := existing[u.ID]; !ok {
				existingOrder = append(existingOrder, u.ID)
			}
			existing[u.ID] = taskRec{title: merged.Title, closed: u.Done}
			continue
		}
		// 新建 task
		nt := &Task{
			Title: u.Title, Status: "open",
			Cwd: cwd, CreatedAt: time.Now(),
			Runs:     []string{runID},
			Summary:  u.SummaryDelta,
			KeyFiles: u.KeyFiles, Commands: u.Commands, NextSteps: u.NextSteps,
			TokenUsage: tokens,
		}
		if u.Done {
			nt.Status = "closed"
		}
		if err := store.UpsertTask(nt); err != nil {
			return sum, err
		}
		if u.Done {
			sum.ClosedTasks = append(sum.ClosedTasks, nt.Title)
		} else {
			sum.UpdatedTasks = append(sum.UpdatedTasks, nt.Title)
		}
	}
	for _, id := range existingOrder {
		if r := existing[id]; r.closed {
			sum.ClosedTasks = append(sum.ClosedTasks, r.title)
		} else {
			sum.UpdatedTasks = append(sum.UpdatedTasks, r.title)
		}
	}
	if _, appliedFacts, deletedFacts, err := ApplyDecisions(store, res.Decisions, facts, "checkpoint:"+runID); err != nil {
		return sum, err
	} else {
		sum.Facts = appliedFacts
		sum.DeletedFacts = deletedFacts
	}
	return sum, nil
}

// mergeText 把增量拼到已有摘要后，避免重复拼接空内容。
func mergeText(existing, delta string) string {
	existing = strings.TrimSpace(existing)
	delta = strings.TrimSpace(delta)
	if delta == "" {
		return existing
	}
	if existing == "" {
		return delta
	}
	return existing + "\n" + delta
}

// singleLine 把多行文本折叠成单行，供清单里逐条展示。
func singleLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
