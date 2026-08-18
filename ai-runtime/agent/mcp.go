package agent

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-core/spec"
	"github.com/tinguo/goworker/ai-runtime/mcp"
)

// 本文件是 MCP 集成：server 连接（loadMCP）与工具桥接（mcpToolToCore），
// 以及 /mcp 状态查看命令。

// ---- /mcp 命令 ----

func (p *AgentPlugin) handleMCP(ctx *spec.Context) error {
	servers := p.cfg.MCP.Servers
	if len(servers) == 0 {
		ctx.Writer("(no MCP servers configured — add mcp.servers to config.yaml)\n")
		return nil
	}
	ctx.Writer(fmt.Sprintf("mcp: %d configured\n\n", len(servers)))
	for _, srv := range servers {
		status := "❌"
		if _, ok := p.mcpClients[srv.Name]; ok {
			status = "✔"
		}
		ctx.Writer(fmt.Sprintf("  %-14s %s  %s %s\n", srv.Name, status, srv.Command, strings.Join(srv.Args, " ")))
	}
	if n := len(p.mcpTools); n > 0 {
		ctx.Writer(fmt.Sprintf("\n  tools: %d loaded\n", n))
	}
	return nil
}

// loadMCP 连接配置的 MCP server 并拉取工具清单。
// MCP server 是外部进程，可能没起/连不上——单个失败只降级跳过，不阻塞插件启动。
// 每个 server 握手+列工具共限时 10s，避免坏配置卡住启动。
func (p *AgentPlugin) loadMCP() {
	for _, srv := range p.cfg.MCP.Servers {
		// 重复 name：先连接的那个会被 map 覆盖成孤儿进程，直接跳过
		if _, exists := p.mcpClients[srv.Name]; exists {
			slog.Warn("duplicate mcp server name, skipping", "server", srv.Name)
			continue
		}
		c, err := mcp.NewStdioClient(srv.Command, srv.Args)
		if err != nil {
			slog.Warn("mcp connect failed, skipping", "server", srv.Name, "err", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := c.Initialize(ctx, &mcp.InitializeRequest{
			ProtocolVersion: mcp.LatestProtocolVersion,
			ClientInfo:      mcp.Implementation{Name: "goworker", Version: "0.1.0"},
		}); err != nil {
			cancel()
			c.Close()
			slog.Warn("mcp initialize failed, skipping", "server", srv.Name, "err", err)
			continue
		}
		tools, err := c.ListTools(ctx, &mcp.ListToolsRequest{})
		cancel()
		if err != nil {
			c.Close()
			slog.Warn("mcp tools/list failed, skipping", "server", srv.Name, "err", err)
			continue
		}

		p.mcpClients[srv.Name] = c
		for _, t := range tools.Tools {
			if tool, ok := p.mcpToolToCore(c, srv.Name, t); ok {
				p.mcpTools = append(p.mcpTools, tool)
			}
		}
		slog.Info("mcp connected", "server", srv.Name, "tools", len(tools.Tools))
	}
}

// mcpToolNameRe 限定合成工具名只含 OpenAI 允许的字符（^[a-zA-Z0-9_-]{1,64}$）。
// server/tool 名来自配置或外部 server，可能带空格/点/冒号，必须在源头拦截。
var mcpToolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// mcpToolToCore 把 MCP 工具转成 agent 工具。工具名带 server 前缀防跨 server 冲突。
// 名字非法字符或超长时跳过 —— 一个坏工具名会让整个 tools 数组被 OpenAI 400 拒绝。
func (p *AgentPlugin) mcpToolToCore(c mcp.Client, server string, t mcp.Tool) (core.Tool, bool) {
	name := "mcp_" + server + "_" + t.Name
	if !mcpToolNameRe.MatchString(name) {
		slog.Warn("mcp tool name has invalid characters, skipping", "server", server, "tool", t.Name, "name", name)
		return core.Tool{}, false
	}
	if len(name) > 64 {
		slog.Warn("mcp tool name too long, skipping", "server", server, "tool", t.Name, "name", name)
		return core.Tool{}, false
	}
	return core.Tool{
		Name:        name,
		Description: t.Description,
		Parameters:  normalizeSchema(t.InputSchema),
		Execute: func(ctx context.Context, args map[string]any) (string, error) {
			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			res, err := c.CallTool(ctx, &mcp.CallToolRequest{Name: t.Name, Arguments: args})
			if err != nil {
				return "", fmt.Errorf("mcp %s: %w", t.Name, err)
			}
			if res.IsError {
				return "", fmt.Errorf("mcp %s: %s", t.Name, mcpContentText(res))
			}
			return mcpContentText(res), nil
		},
	}, true
}

// mcpContentText 把 MCP 返回的 content 块拼成单块文本。
func mcpContentText(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, blk := range res.Content {
		b.WriteString(blk.Text)
	}
	return b.String()
}
