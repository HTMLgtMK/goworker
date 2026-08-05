package agent

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/spec"
	"github.com/tinguo/goworker/memory"
)

// 本文件是记忆与任务档案命令：/memory 与 /task。

// ---- /memory 命令 ----

func (p *AgentPlugin) handleMemory(ctx *spec.Context) error {
	if p.memory == nil {
		ctx.Writer("memory 未启用（检查 config.yaml 的 memory.enabled 与存储目录权限）\n")
		return nil
	}
	args := ctx.Args
	if len(args) == 0 {
		p.memoryHelp(ctx)
		return nil
	}

	switch args[0] {
	case "help":
		p.memoryHelp(ctx)

	case "add":
		if len(args) < 2 {
			ctx.Writer("用法: /memory add <要记住的事实>\n")
			return nil
		}
		content := strings.TrimSpace(strings.Join(args[1:], " "))
		f := &memory.Fact{Content: content, Topic: "manual", Source: "user"}
		if err := p.memory.AddFact(f); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ added %s\n", f.ID))

	case "search":
		if len(args) < 2 {
			ctx.Writer("用法: /memory search <关键词> [条数]\n")
			return nil
		}
		n := 8
		q := strings.Join(args[1:], " ")
		if len(args) >= 3 {
			if v, err := strconv.Atoi(args[len(args)-1]); err == nil && v > 0 {
				n = v
				q = strings.Join(args[1:len(args)-1], " ")
			}
		}
		facts, err := p.memory.SearchFacts(q, n)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(facts) == 0 {
			ctx.Writer("(no matching facts)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("facts: %d matched\n\n", len(facts)))
		for i, f := range facts {
			ctx.Writer(fmt.Sprintf("  #%d [%s|%s] %s (%s)\n",
				i+1, f.ID, f.Topic, truncate(oneLine(f.Content), 160), f.UpdatedAt.Format("01-02 15:04")))
		}

	case "list":
		n := 20
		if len(args) >= 2 {
			if v, err := strconv.Atoi(args[1]); err == nil && v > 0 {
				n = v
			}
		}
		facts, err := p.memory.ListFacts(n)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(facts) == 0 {
			ctx.Writer("(no facts yet — run /agent to auto-extract, or /memory add <text>)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("facts: %d\n\n", len(facts)))
		for i, f := range facts {
			ctx.Writer(fmt.Sprintf("  #%d [%s|%s] %s (%s)\n",
				i+1, f.ID, f.Topic, truncate(oneLine(f.Content), 160), f.UpdatedAt.Format("01-02 15:04")))
		}

	case "tasks":
		n := 10
		if len(args) >= 2 {
			if v, err := strconv.Atoi(args[1]); err == nil && v > 0 {
				n = v
			}
		}
		tasks, err := p.memory.ListTasks(n)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(tasks) == 0 {
			ctx.Writer("(no tasks yet — run /agent then /compact or /new to checkpoint)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("tasks: %d\n\n", len(tasks)))
		for i, t := range tasks {
			ctx.Writer(fmt.Sprintf("  #%d [%s|%s] %s\n     %s\n",
				i+1, t.ID, t.Status, truncate(oneLine(t.Title), 80), truncate(oneLine(t.Summary), 160)))
			if len(t.NextSteps) > 0 {
				ctx.Writer("     下一步: " + strings.Join(t.NextSteps, "；") + "\n")
			}
		}

	case "forget":
		if len(args) < 2 {
			ctx.Writer("用法: /memory forget <id> [id...]\n")
			return nil
		}
		for _, id := range args[1:] {
			if err := p.memory.DeleteFact(id); err != nil {
				ctx.Writer(fmt.Sprintf("✘ %v\n", err))
				return nil
			}
		}
		ctx.Writer(fmt.Sprintf("✔ forgot %d\n", len(args)-1))

	case "profile":
		if p.instructions == nil || p.instructions.Profile() == nil {
			ctx.Writer("declarative instructions disabled (memory.enabled=false or load failed)\n")
			return nil
		}
		prof := p.instructions.Profile()
		used, max := prof.Capacity()
		ctx.Writer(fmt.Sprintf("USER.md capacity: %d/%d chars\n", used, max))
		if strings.TrimSpace(prof.Content()) == "" {
			ctx.Writer("(empty — have the agent write via the profile tool, or edit ~/.config/goworker/USER.md by hand)\n")
			return nil
		}
		ctx.Writer(prof.Content() + "\n")

	default:
		ctx.Writer(fmt.Sprintf("未知子命令: %s（使用 /memory help 查看用法）\n", args[0]))
	}
	return nil
}

func (p *AgentPlugin) memoryHelp(ctx *spec.Context) {
	ctx.Writer("Usage:\n")
	ctx.Writer("  /memory                 — show this help\n")
	ctx.Writer("  /memory add <text>      — manually remember a fact\n")
	ctx.Writer("  /memory search <keyword> [n] — search related facts\n")
	ctx.Writer("  /memory list [n]        — list recent facts (default 20)\n")
	ctx.Writer("  /memory tasks [n]       — list task archive (incl. closed)\n")
	ctx.Writer("  /memory forget <id>...  — delete facts\n")
	ctx.Writer("  /memory profile         — show user profile USER.md (capacity/content)\n")
	ctx.Writer("  /task list|start|end    — manage open tasks\n")
	ctx.Writer("  /rules                  — show effective declarative instructions (AGENTS.md + USER.md)\n")
	ctx.Writer("  /new                    — end session: consolidate + clear STM + reload instructions + inject reminders\n")
}

// ---- /task 命令 ----

func (p *AgentPlugin) handleTask(ctx *spec.Context) error {
	if p.memory == nil {
		ctx.Writer("memory 未启用（检查 config.yaml 的 memory.enabled 与存储目录权限）\n")
		return nil
	}
	args := ctx.Args
	if len(args) == 0 {
		p.taskHelp(ctx)
		return nil
	}

	switch args[0] {
	case "help":
		p.taskHelp(ctx)

	case "list":
		tasks, err := p.memory.OpenTasks(20)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		if len(tasks) == 0 {
			ctx.Writer("(no open tasks — run /agent then checkpoint via /compact or /new)\n")
			return nil
		}
		ctx.Writer(fmt.Sprintf("tasks: %d open\n\n", len(tasks)))
		for i, t := range tasks {
			ctx.Writer(fmt.Sprintf("  #%d [%s] %s\n     %s\n",
				i+1, t.ID, truncate(oneLine(t.Title), 80), truncate(oneLine(t.Summary), 160)))
			if len(t.NextSteps) > 0 {
				ctx.Writer("     下一步: " + strings.Join(t.NextSteps, "；") + "\n")
			}
		}

	case "start":
		if len(args) < 2 {
			ctx.Writer("用法: /task start <任务标题>\n")
			return nil
		}
		title := strings.TrimSpace(strings.Join(args[1:], " "))
		t := &memory.Task{Title: title, Status: "open"}
		if err := p.memory.UpsertTask(t); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ 已创建 task %s，后续对话将在检查点时归入\n", t.ID))

	case "end":
		id := ""
		if len(args) >= 2 {
			id = args[1]
		} else if tasks, err := p.memory.OpenTasks(1); err == nil && len(tasks) > 0 {
			id = tasks[0].ID // 默认关最近活跃的 open task
		}
		if id == "" {
			ctx.Writer("(no open task to close)\n")
			return nil
		}
		if err := p.memory.CloseTask(id); err != nil {
			ctx.Writer(fmt.Sprintf("✘ %v\n", err))
			return nil
		}
		ctx.Writer(fmt.Sprintf("✔ 已关闭 task %s\n", id))

	case "checkpoint":
		sum, err := p.checkpointMemory(ctx.Ctx)
		if err != nil {
			ctx.Writer(fmt.Sprintf("✘ Consolidation failed: %v\n", err))
			return nil
		}
		if notice := renderCheckpointNotice(sum); notice != "" {
			ctx.Writer(notice)
		} else {
			ctx.Writer("✔ Session consolidated (no new memory)\n")
		}

	default:
		ctx.Writer(fmt.Sprintf("未知子命令: %s（使用 /task help 查看用法）\n", args[0]))
	}
	return nil
}

func (p *AgentPlugin) taskHelp(ctx *spec.Context) {
	ctx.Writer("用法:\n")
	ctx.Writer("  /task               — 显示本帮助\n")
	ctx.Writer("  /task list          — 列出未完成任务\n")
	ctx.Writer("  /task start <标题>   — 手动开始一个新任务（LLM 判定不准时干预）\n")
	ctx.Writer("  /task end [id]      — 关闭任务（默认最近活跃的）\n")
	ctx.Writer("  /task checkpoint    — 立即把当前会话固化进任务档案\n")
}
