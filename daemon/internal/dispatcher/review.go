package dispatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

type reviewUsage struct {
	Used     json.Number
	Size     json.Number
	Cost     json.Number
	Currency string
}

type reviewDTO struct {
	Task     reviewTaskDTO   `json:"task"`
	Statuses []string        `json:"statuses"`
	Usage    *reviewUsageDTO `json:"usage,omitempty"`
	Tools    []reviewToolDTO `json:"tools"`
	Artifact string          `json:"artifact,omitempty"`
	Code     *reviewCodeDTO  `json:"code,omitempty"`
}

type reviewTaskDTO struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Worker    string `json:"worker"`
	Status    string `json:"status"`
	Prompt    string `json:"prompt"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type reviewUsageDTO struct {
	Used     json.Number `json:"used"`
	Size     json.Number `json:"size"`
	Cost     json.Number `json:"cost,omitempty"`
	Currency string      `json:"currency,omitempty"`
}

type reviewToolDTO struct {
	Title  string `json:"title"`
	Status string `json:"status"`
}

type reviewCodeDTO struct {
	Repo       string   `json:"repo"`
	Worktree   string   `json:"worktree"`
	Branch     string   `json:"branch"`
	BaseCommit string   `json:"base_commit"`
	Commits    []string `json:"commits"`
	Conclusion string   `json:"conclusion,omitempty"`
}

type reviewTool struct {
	Title  string
	Status string
}

type taskReview struct {
	Statuses []task.Status
	Text     string
	Usage    *reviewUsage
	Tools    []reviewTool
}

func (p *DispatcherPlugin) handleReview(ctx *plugin.Context, id string) error {
	p.doReview(id, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) review(id string) (reviewDTO, bool) {
	t, ok := p.store.Get(id)
	if !ok {
		return reviewDTO{}, false
	}
	events, _ := p.eventLog.EventsAfter(id, 0)
	review := summarizeTaskEvents(events)
	return buildReviewDTO(t, review), true
}

func buildReviewDTO(t task.Task, review taskReview) reviewDTO {
	result := reviewDTO{
		Task: reviewTaskDTO{
			ID:        cleanReviewText(t.ID),
			Kind:      cleanReviewText(string(t.Kind)),
			Worker:    cleanReviewText(t.Worker),
			Status:    cleanReviewText(string(t.Status)),
			Prompt:    cleanReviewText(t.Prompt),
			CreatedAt: formatReviewTime(t.CreatedAt),
			UpdatedAt: formatReviewTime(t.UpdatedAt),
		},
		Tools: make([]reviewToolDTO, 0, len(review.Tools)),
	}
	for _, status := range review.Statuses {
		result.Statuses = append(result.Statuses, cleanReviewText(string(status)))
	}
	for _, tool := range review.Tools {
		result.Tools = append(result.Tools, reviewToolDTO{
			Title: cleanReviewText(tool.Title), Status: cleanReviewText(tool.Status),
		})
	}
	if review.Usage != nil {
		result.Usage = &reviewUsageDTO{
			Used: review.Usage.Used, Size: review.Usage.Size,
			Cost: review.Usage.Cost, Currency: cleanReviewText(review.Usage.Currency),
		}
	}
	text := cleanReviewText(review.Text)
	if t.Kind == task.KindCode {
		commits := make([]string, 0, len(t.Commits))
		for _, commit := range t.Commits {
			commits = append(commits, cleanReviewText(commit))
		}
		result.Code = &reviewCodeDTO{
			Repo: cleanReviewText(t.Repo), Worktree: cleanReviewText(t.Worktree),
			Branch: cleanReviewText(t.Branch), BaseCommit: cleanReviewText(t.BaseCommit),
			Commits: commits, Conclusion: text,
		}
		return result
	}
	result.Artifact = text
	return result
}

func (p *DispatcherPlugin) doReview(id string, write func(string)) {
	t, ok := p.store.Get(id)
	if !ok {
		write(fmt.Sprintf("✘ 任务 %s 不存在\n", cleanReviewText(id)))
		return
	}

	events, _ := p.eventLog.EventsAfter(id, 0)
	review := summarizeTaskEvents(events)
	writeReviewHeader(write, t, review)
	writeReviewAction(write, t)
	if t.Kind == task.KindCode {
		writeCodeReview(write, t, review.Text)
		return
	}
	writeGeneralReview(write, review.Text)
}

func summarizeTaskEvents(events []task.TaskEvent) taskReview {
	review := taskReview{}
	tools := make(map[string]reviewTool)
	for _, event := range events {
		if event.Type == task.EventStatus {
			review.Statuses = append(review.Statuses, event.To)
			continue
		}

		var update protocol.SessionUpdateBody
		if err := json.Unmarshal(event.Update, &update); err != nil {
			continue
		}
		switch update.SessionUpdate {
		case protocol.UpdateAgentMessageChunk:
			if update.Content != nil {
				review.Text += update.Content.Text
			}
		case protocol.UpdateToolCall, protocol.UpdateToolCallUpdate:
			if update.Title == "" {
				continue
			}
			key := update.ToolCallID
			if key == "" {
				key = update.Title
			}
			tools[key] = reviewTool{Title: update.Title, Status: update.Status}
		case "usage_update":
			if usage, ok := parseReviewUsage(event.Update); ok {
				review.Usage = &usage
			}
		}
	}
	for _, tool := range tools {
		review.Tools = append(review.Tools, tool)
	}
	sort.Slice(review.Tools, func(i, j int) bool {
		return review.Tools[i].Title < review.Tools[j].Title
	})
	return review
}

func parseReviewUsage(data []byte) (reviewUsage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var update struct {
		Used json.Number `json:"used"`
		Size json.Number `json:"size"`
		Cost *struct {
			Amount   json.Number `json:"amount"`
			Currency string      `json:"currency"`
		} `json:"cost"`
	}
	if err := decoder.Decode(&update); err != nil || update.Size == "" {
		return reviewUsage{}, false
	}
	usage := reviewUsage{Used: update.Used, Size: update.Size}
	if update.Cost != nil {
		usage.Cost = update.Cost.Amount
		usage.Currency = update.Cost.Currency
	}
	return usage, true
}

func writeReviewHeader(write func(string), t task.Task, review taskReview) {
	write(fmt.Sprintf("任务：%s\n类型：%s\nWorker：%s\n状态：%s\n", cleanReviewText(t.ID), cleanReviewText(string(t.Kind)), cleanReviewText(t.Worker), cleanReviewText(string(t.Status))))
	write(fmt.Sprintf("创建：%s\n最后更新：%s\n", formatReviewTime(t.CreatedAt), formatReviewTime(t.UpdatedAt)))
	write("需求：\n" + cleanReviewText(t.Prompt) + "\n")
	if len(review.Statuses) > 0 {
		statuses := make([]string, len(review.Statuses))
		for i, status := range review.Statuses {
			statuses[i] = cleanReviewText(string(status))
		}
		write("状态轨迹：" + strings.Join(statuses, " → ") + "\n")
	}
	writeReviewUsage(write, review.Usage)
	if len(review.Tools) > 0 {
		write("工具：\n")
		for _, tool := range review.Tools {
			write(fmt.Sprintf("- %s %s\n", cleanReviewText(tool.Title), cleanReviewText(tool.Status)))
		}
	}
}

func writeReviewUsage(write func(string), usage *reviewUsage) {
	if usage == nil {
		write("用量：worker 未上报\n")
		return
	}
	write(fmt.Sprintf("上下文：%s / %s\n", usage.Used, usage.Size))
	if usage.Cost == "" || usage.Currency == "" {
		write("成本：worker 未上报\n")
		return
	}
	write(fmt.Sprintf("成本：%s %s\n", usage.Cost, cleanReviewText(usage.Currency)))
}

func writeReviewAction(write func(string), t task.Task) {
	switch t.Status {
	case task.StatusAwaitingReview:
		write(fmt.Sprintf("可验收：/dispatch approve %s 通过，或 /dispatch reject %s 拒绝\n", cleanReviewText(t.ID), cleanReviewText(t.ID)))
	case task.StatusQueued, task.StatusDispatching, task.StatusWorking, task.StatusMerging:
		write(fmt.Sprintf("尚未收口：执行 /dispatch tail %s 查看实时进度\n", cleanReviewText(t.ID)))
	case task.StatusFailed:
		write("任务失败：" + reviewError(t) + "\n")
	case task.StatusCancelled:
		write("任务已取消：" + reviewError(t) + "\n")
	case task.StatusDone:
		write("任务已完成\n")
	case task.StatusRejected:
		write("任务已拒绝，产物已清理\n")
	}
}

func writeGeneralReview(write func(string), result string) {
	write("\n任务产物：\n")
	if result == "" {
		write("（worker 未输出可验收文本）\n")
		return
	}
	result = cleanReviewText(result)
	write(result)
	if !strings.HasSuffix(result, "\n") {
		write("\n")
	}
}

func writeCodeReview(write func(string), t task.Task, result string) {
	write("\n代码产物：\n")
	write("Worktree：" + cleanReviewText(t.Worktree) + "\n")
	write("分支：" + cleanReviewText(t.Branch) + "\n")
	write("基线：" + cleanReviewText(t.BaseCommit) + "\n")
	if len(t.Commits) == 0 {
		write("Commits：（worker 未记录 commit）\n")
	} else {
		write("Commits：\n")
		for _, commit := range t.Commits {
			write("- " + cleanReviewText(commit) + "\n")
		}
	}
	if t.Repo != "" && t.BaseCommit != "" && t.Branch != "" {
		write(fmt.Sprintf("Diff：git -C %s diff %s..%s\n", shellQuote(t.Repo), shellQuote(t.BaseCommit), shellQuote(t.Branch)))
	}
	if result == "" {
		return
	}
	result = cleanReviewText(result)
	write("\nworker 结论：\n" + result)
	if !strings.HasSuffix(result, "\n") {
		write("\n")
	}
}

func reviewError(t task.Task) string {
	if t.Error != "" {
		return cleanReviewText(t.Error)
	}
	return "worker 未提供错误详情"
}

func cleanReviewText(value string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, value)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(cleanReviewText(value), "'", "'\"'\"'") + "'"
}

func formatReviewTime(value time.Time) string {
	if value.IsZero() {
		return "未记录"
	}
	return value.Format(time.RFC3339)
}
