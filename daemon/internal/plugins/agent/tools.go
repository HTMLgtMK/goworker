package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/sandbox"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// 本文件是工具收集：把 hub 命令 / skill / memory / profile / MCP 工具统一
// 组装成 agent 可用的 []core.Tool（collectTools），及 profile 写工具实现。

func (p *AgentPlugin) sandboxConfig() sandbox.Config {
	return *sandbox.NewFromConfig(&p.hub.Config.Sandbox)
}

func (p *AgentPlugin) collectTools(cfg *sandbox.Config) []core.Tool {
	var tools []core.Tool
	tools = append(tools, DefaultTools(cfg)...)

	for _, t := range p.hub.Tools() {
		tool := t
		tools = append(tools, core.Tool{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  parseSchema(tool.Schema),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				var buf strings.Builder
				err := p.hub.Eval(spec.NewContext(context.Background(), func(s string) { buf.WriteString(s) }, nil, nil), "/"+tool.Name)
				if err != nil {
					return buf.String(), err
				}
				return strings.TrimSpace(buf.String()), nil
			},
		})
	}

	// skill 注册成工具：LLM 自己判断何时加载，调用即把指令正文取回上下文。
	// 内容不常驻 system prompt，按需进上下文，省 token。
	for _, s := range p.skills {
		skill := s
		tools = append(tools, core.Tool{
			Name:        "skill_" + skill.Name,
			Description: fmt.Sprintf("Load the %s skill. Call it when you need to: %s", skill.Name, skill.Description),
			Parameters:  normalizeSchema(nil),
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				return skill.Content, nil
			},
		})
	}

	// memory_search：agent 主动回顾历史的结构化入口。不依赖 agent 猜档案路径 ——
	// 检索含 closed 的历史档案 + LTM 事实（SearchAll 全量，区别于注入只取 open，
	// agent 主动查档时过期快照不构成误导）。
	if p.memory != nil {
		tools = append(tools, core.Tool{
			Name:        "memory_search",
			Description: "Search historical memory: task archive (including completed tasks) and long-term facts. Use when the user asks about past sessions, previous work, or things you did before. Returns ranked task summaries and facts.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string", "description": "search keywords"},
					"top_k": map[string]any{"type": "number", "description": "max results to return"},
				},
				"required": []string{"query"},
			},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				query, _ := args["query"].(string)
				if strings.TrimSpace(query) == "" {
					return "", fmt.Errorf("memory_search: empty query")
				}
				topK := 5
				if n, ok := args["top_k"].(float64); ok && int(n) > 0 {
					topK = int(n)
				}
				if topK > 20 {
					topK = 20 // 防模型传超大 top_k 把全部档案渲染进上下文撑爆窗口
				}
				results, err := p.memory.SearchAll(ctx, query, topK, topK)
				if err != nil {
					return "", fmt.Errorf("memory_search: %w", err)
				}
				if len(results) == 0 {
					return "(no matching memory)", nil
				}
				var b strings.Builder
				for _, r := range results {
					switch {
					case r.Task != nil:
						t := r.Task
						fmt.Fprintf(&b, "- task [%s] %s\n", t.Status, oneLine(t.Title))
						if t.Summary != "" {
							b.WriteString("  " + oneLine(t.Summary) + "\n")
						}
					case r.Fact != nil:
						f := r.Fact
						if f.Topic != "" {
							fmt.Fprintf(&b, "- fact [%s] %s\n", f.Topic, oneLine(f.Content))
						} else {
							fmt.Fprintf(&b, "- fact %s\n", oneLine(f.Content))
						}
					}
				}
				return b.String(), nil
			},
		})
	}

	// profile：agent 写入用户画像 USER.md 的结构化入口。声明式指令层在，
	// 才给 agent 写自己档案的能力 —— 写入即持久化，语义与 memory_search 的只读相对。
	if p.deps.Instruction != nil {
		tools = append(tools, p.profileTool())
	}

	// MCP 工具：Init 时已从 server 拉取定义，这里是静态复用。
	// 工具是外部进程执行，不受本地 sandbox 管控，信任由"用户显式配置了哪个 server"建立。
	tools = append(tools, p.mcpTools...)
	return tools
}

// ---- profile 工具（agent 写 USER.md） ----

// profileTool 返回 agent 更新用户画像的工具。画像条目必须短、信息密度高；
// replace/remove 用唯一子串定位（Hermes 式），写多命中直接报错防误伤。
// 写入语义：落盘持久化，但注入快照冻结 —— 本次会话不生效，/new 或重启后可见。
func (p *AgentPlugin) profileTool() core.Tool {
	return core.Tool{
		Name: "profile",
		Description: "Update the user profile (USER.md): durable facts about the user's " +
			"preferences, communication style, identity, and corrections. Entries are compact, " +
			"one line each. action=add appends a new entry; action=replace swaps the entry " +
			"uniquely matching old_text; action=remove deletes it. Returns the result and current " +
			"capacity. Changes are saved to disk and take effect next session (/new or restart).",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"action":   map[string]any{"type": "string", "description": "add | replace | remove"},
				"content":  map[string]any{"type": "string", "description": "new entry text (for add/replace)"},
				"old_text": map[string]any{"type": "string", "description": "unique substring matching the entry to replace/remove"},
			},
			"required": []string{"action"},
		},
		Execute: func(ctx context.Context, args map[string]any) (string, error) {
			return p.execProfileTool(args)
		},
	}
}

func (p *AgentPlugin) execProfileTool(args map[string]any) (string, error) {
	action, _ := args["action"].(string)
	content, _ := args["content"].(string)
	oldText, _ := args["old_text"].(string)
	if p.deps.Instruction == nil || p.deps.Instruction.Profile() == nil {
		return "", fmt.Errorf("profile: instructions not loaded")
	}
	switch action {
	case "add":
		if strings.TrimSpace(content) == "" {
			return "", fmt.Errorf("profile: content required for add")
		}
	case "replace":
		if strings.TrimSpace(content) == "" {
			return "", fmt.Errorf("profile: content required for replace")
		}
		fallthrough
	case "remove":
		if strings.TrimSpace(oldText) == "" {
			return "", fmt.Errorf("profile: old_text required for %s", action)
		}
	default:
		return "", fmt.Errorf("profile: unknown action %q (add|replace|remove)", action)
	}

	res, err := p.applyProfileAction(action, content, oldText)
	if err != nil {
		return "", err
	}
	used, max := p.deps.Instruction.Profile().Capacity()
	return fmt.Sprintf("%s (USER.md %d/%d chars). Saved to disk; takes effect next session (/new or restart).", res, used, max), nil
}

// applyProfileAction 把一次画像写操作落到 Profile。add/replace/remove 的参数约定：
// add/replace 用 content，replace/remove 用 oldText 定位。
func (p *AgentPlugin) applyProfileAction(action, content, oldText string) (string, error) {
	prof := p.deps.Instruction.Profile()
	switch action {
	case "add":
		return prof.AddEntry(content)
	case "replace":
		return prof.ReplaceEntry(oldText, content)
	case "remove":
		return prof.RemoveEntry(oldText)
	default:
		return "", fmt.Errorf("profile: unknown action %q", action)
	}
}

// ---- Util ----

func parseSchema(schema []byte) map[string]any {
	if len(schema) == 0 {
		return map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
	}
	var m map[string]any
	json.Unmarshal(schema, &m) // best effort
	return m
}
