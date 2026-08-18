package agent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-sandbox"
)

// DefaultTools 返回 Agent 的默认工具集。
// cfg 为沙箱配置，nil 表示不启用沙箱。
func DefaultTools(cfg *sandbox.Config) []core.Tool {
	return []core.Tool{
		{
			Name:        "bash",
			Description: "Execute a shell command. Returns stdout + stderr.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"command": map[string]any{"type": "string", "description": "shell command"},
					"timeout": map[string]any{"type": "number", "description": "timeout in seconds"},
				},
				"required": []string{"command"},
			},
			// NOTE: sandbox 检查不在 tool.Execute 里做，而是在 agent.Run 的 tool call 循环中统一处理。
			// 这样 sandbox 可以通过 interrupt token + decisions channel 与前端交互。
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				cmdStr, _ := args["command"].(string)
				if cmdStr == "" {
					return "", fmt.Errorf("bash: empty command")
				}
				timeout := 30
				if t, ok := args["timeout"].(float64); ok {
					timeout = int(t)
				}
				ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "bash", "-c", cmdStr)
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				// context 取消时 kill 整个进程组（含管道子进程），
				// exec.CommandContext 只 kill bash 本身，子进程变孤儿可能阻塞 CombinedOutput
				go func() {
					<-ctx.Done()
					if cmd.Process != nil {
						syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					}
				}()
				if cfg != nil && cfg.AllowedWorkDir != "" {
					cmd.Dir = cfg.AllowedWorkDir
				}
				out, err := cmd.CombinedOutput()
				output := string(out)
				if err != nil {
					if ctx.Err() == context.DeadlineExceeded {
						return output + "\n[timed out]", nil
					}
					return output + "\n" + err.Error(), nil
				}
				return output, nil
			},
		},
		{
			Name:        "read_file",
			Description: "Read a file, with optional offset/limit for large files.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":   map[string]any{"type": "string", "description": "file path"},
					"offset": map[string]any{"type": "number", "description": "line offset"},
					"limit":  map[string]any{"type": "number", "description": "max lines"},
				},
				"required": []string{"path"},
			},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				path, _ := args["path"].(string)
				if path == "" {
					return "", fmt.Errorf("read_file: empty path")
				}
				// os.ReadFile 是同步 syscall，不认 ctx —— goroutine + select 包装让取消
				// 能提前返回。极端（如 NFS 永久挂起）泄漏一个 goroutine，但比整个 ReAct
				// 循环无限挂住可接受。
				type readRes struct {
					data []byte
					err  error
				}
				resCh := make(chan readRes, 1)
				go func() {
					data, err := os.ReadFile(path)
					resCh <- readRes{data, err}
				}()
				var data []byte
				var err error
				select {
				case r := <-resCh:
					data, err = r.data, r.err
				case <-ctx.Done():
					return "", fmt.Errorf("read_file: %w", ctx.Err())
				}
				if err != nil {
					return "", fmt.Errorf("read_file: %w", err)
				}
				lines := strings.Split(string(data), "\n")
				offset := 0
				if o, ok := args["offset"].(float64); ok {
					offset = int(o)
				}
				// LLM 可能传负 offset，slice 负索引直接 panic，clamp 到 0
				if offset < 0 {
					offset = 0
				}
				limit := len(lines)
				if l, ok := args["limit"].(float64); ok && int(l) > 0 {
					limit = offset + int(l)
				}
				if offset > len(lines) {
					offset = len(lines)
				}
				if limit > len(lines) {
					limit = len(lines)
				}
				if offset > 0 || limit < len(lines) {
					var b strings.Builder
					for i, line := range lines[offset:limit] {
						b.WriteString(fmt.Sprintf("%d\t%s\n", offset+i+1, line))
					}
					return b.String(), nil
				}
				return string(data), nil
			},
		},
		{
			Name:        "write_file",
			Description: "Write content to a file. Creates directories if needed.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":    map[string]any{"type": "string", "description": "file path"},
					"content": map[string]any{"type": "string", "description": "content to write"},
				},
				"required": []string{"path", "content"},
			},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				path, _ := args["path"].(string)
				content, _ := args["content"].(string)
				if path == "" {
					return "", fmt.Errorf("write_file: empty path")
				}
				if idx := strings.LastIndex(path, "/"); idx > 0 {
					if err := os.MkdirAll(path[:idx], 0755); err != nil {
						return "", fmt.Errorf("write_file: mkdir: %w", err)
					}
				}
				// os.WriteFile 是同步 syscall，不认 ctx —— goroutine + select 包装让取消可提前返回
				type writeRes struct{ err error }
				resCh := make(chan writeRes, 1)
				go func() {
					resCh <- writeRes{os.WriteFile(path, []byte(content), 0644)}
				}()
				select {
				case r := <-resCh:
					if r.err != nil {
						return "", fmt.Errorf("write_file: %w", r.err)
					}
				case <-ctx.Done():
					return "", fmt.Errorf("write_file: %w", ctx.Err())
				}
				return fmt.Sprintf("wrote %d bytes to %s", len(content), path), nil
			},
		},
	}
}
