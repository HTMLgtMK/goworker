// ACP worker 模式：把 ai-runtime Session 暴露成 ACP Agent（stdio）。
// `goworker acp` 启动后，任意 ACP Client（包括本项目的 dispatcher，或 Zed 等
// 编辑器）都能把 ZCode 当作 worker 驱动：session/prompt 的文本即 agent 输入，
// token 流经 session/update 回传。
package acpworker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	dispatch "github.com/tinguo/goworker/ai-dispatch"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// ACPWorkerDeps 是 worker 模式的最小装配输入。
type ACPWorkerDeps struct {
	Config   *runtimeconfig.Config
	AuditDir string // sandbox 审计目录（可空）
}

// acpWorker 实现 dispatch.TaskHandler：一个 ACP session 对应一个 Session 会话。
type acpWorker struct {
	deps ACPWorkerDeps

	mu          sync.Mutex
	runMu       sync.Mutex // serializes Session.Run with runtime allow-rule updates
	sessions    map[string]*runtimeagent.Session
	caller      DeviceCaller           // client-ward 调用通道（dispatch.Server），nil = 未知
	deviceTools []DeviceToolDescriptor // 探测到的客户端设备工具（连接级，一次探测全会话复用）
}

// ServeACPWorker 在给定连接上服务 ACP worker 协议，返回 Server 便于测试与关闭。
func ServeACPWorker(rwc io.ReadWriteCloser, deps ACPWorkerDeps) *dispatch.Server {
	return dispatch.ServeConn(rwc, &acpWorker{deps: deps, sessions: map[string]*runtimeagent.Session{}})
}

// SetSessionServer 实现 SessionServerAware：捕获 client-ward 调用通道并探测
// 设备工具清单（一次探测，连接内所有会话复用；失败按无能力处理，不阻塞会话建立）。
func (w *acpWorker) SetSessionServer(sessionID, cwd string, server *dispatch.Server) {
	w.mu.Lock()
	w.caller = server
	w.mu.Unlock()
	if descs := probeDeviceTools(server); len(descs) > 0 {
		w.mu.Lock()
		w.deviceTools = descs
		w.mu.Unlock()
	}
	w.setSession(sessionID, cwd)
}

// SetSession 建 Session（无 Server 引用的调用路径：设备工具不可用，仅 DefaultTools）。
func (w *acpWorker) SetSession(sessionID, cwd string) {
	w.setSession(sessionID, cwd)
}

// setSession creates a session using the cwd supplied by the trusted orchestrator.
// The cwd becomes the sandbox work directory for commands and relative file access.
// It is not checked against AllowedWorkDir: dispatch worktrees may be outside that
// prefix, and the caller is within the worker's trust boundary.
func (w *acpWorker) setSession(sessionID, cwd string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	collectTools := runtimeagent.DefaultTools
	if w.caller != nil && len(w.deviceTools) > 0 {
		relayed := RelayDeviceTools(w.caller, w.deviceTools)
		collectTools = func(cfg *sandbox.Config) []core.Tool {
			tools := runtimeagent.DefaultTools(cfg)
			return append(tools, relayed...)
		}
	}

	w.sessions[sessionID] = runtimeagent.NewSession(runtimeagent.SessionDeps{
		Config:               w.deps.Config,
		AuditDir:             w.deps.AuditDir,
		CWD:                  cwd,
		Memory:               nil,
		CollectTools:         collectTools,
		NewProvider:          ProviderFactory(acpHTTPClient()),
		Store:                nil, // worker 会话由调用方管理生命周期，本地不持久化
		PersistAllowRule:     w.persistAllowRule,
		PersistToolAllowRule: w.persistToolAllowRule,
	})
}

func (w *acpWorker) persistAllowRule(rule sandbox.AllowRule) error {
	if w.deps.Config == nil {
		return fmt.Errorf("acp worker: runtime config is nil")
	}
	if len(rule.MatchTokens) == 0 {
		return fmt.Errorf("acp worker: allow rule has empty match")
	}

	candidate := sandbox.AllowRuleConfig{
		Match:   strings.Join(rule.MatchTokens, " "),
		MaxRisk: rule.MaxRisk.String(),
		Effects: rule.Effects.Names(),
		Desc:    rule.Desc,
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, existing := range w.deps.Config.Sandbox.AllowRules {
		if existing.Match == candidate.Match &&
			existing.MaxRisk == candidate.MaxRisk &&
			existing.Desc == candidate.Desc &&
			slices.Equal(existing.Effects, candidate.Effects) {
			return nil
		}
	}
	w.deps.Config.Sandbox.AllowRules = append(
		w.deps.Config.Sandbox.AllowRules,
		candidate,
	)
	return nil
}

func (w *acpWorker) persistToolAllowRule(rule sandbox.ToolAllowRule) error {
	if w.deps.Config == nil {
		return fmt.Errorf("acp worker: runtime config is nil")
	}
	if strings.TrimSpace(rule.Key) == "" {
		return fmt.Errorf("acp worker: tool allow rule has empty key")
	}

	candidate := sandbox.ToolAllowRuleConfig{
		Key:     rule.Key,
		MaxRisk: rule.MaxRisk.String(),
		Desc:    rule.Desc,
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, existing := range w.deps.Config.Sandbox.ToolAllowRules {
		if existing.Key == candidate.Key && existing.MaxRisk == candidate.MaxRisk && existing.Desc == candidate.Desc {
			return nil
		}
	}
	w.deps.Config.Sandbox.ToolAllowRules = append(w.deps.Config.Sandbox.ToolAllowRules, candidate)
	return nil
}

// Run 执行一回合：session/prompt 文本进 Session.Run，token 流映射为 session/update。
func (w *acpWorker) Run(ctx context.Context, sessionID, prompt string, rep dispatch.Reporter) (string, error) {
	w.mu.Lock()
	session, ok := w.sessions[sessionID]
	w.mu.Unlock()
	if !ok {
		return "", statusError(fmt.Sprintf("unknown session %q (call session/new first)", sessionID))
	}

	w.runMu.Lock()
	defer w.runMu.Unlock()
	cb := runtimeagent.RunCallbacks{
		Write: func(text string) {
			rep.MessageChunk(sessionID, text)
		},
		EmitToken: func(token core.Token) {
			rep.Update(sessionID, TokenToUpdate(token))
		},
		// HITL 经 ACP 授权请求回传提交方（dispatcher 侧按 worker 的 on_permission
		// 策略决定是自动放行还是转给订阅者）。决策失败一律拒绝，见 DecideViaACP。
		Decide: model.DecideViaACP(ctx, rep, sessionID),
	}
	// stdio worker sessions are caller-managed and have no SessionRuntime, so
	// Run does not record run history here. Preserve the existing error behavior.
	if _, err := session.Run(ctx, runtimeagent.RunRequest{Input: prompt}, cb); err != nil {
		return "", err
	}
	return protocol.StopEndTurn, nil
}

// TokenToUpdate 把结构化 runtime token 映射为 ACP session/update 的唯一映射：
// stdio worker（acpWorker）与主会话 ACP 前端（core/service/acp 的 FrontendHandler）
// 两处共用，保证两条 ACP 入口的工具生命周期/思考/正文渲染一致。
func TokenToUpdate(token core.Token) protocol.SessionUpdateBody {
	switch token.Type {
	case core.TokenTypeThinking:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentThoughtChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: token.Content},
		}
	case core.TokenTypeToolCall:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCall,
			ToolCallID:    token.ToolCall.ID,
			Title:         token.Content,
			Status:        "pending",
		}
	case core.TokenTypeToolResult:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCallUpdate,
			ToolCallID:    token.ToolCallID,
			Title:         token.Content,
			Status:        "completed",
		}
	default:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentMessageChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: token.Content},
		}
	}
}

// statusError 生成 -32000 RPC 错误（protocol.Conn 会识别 *RPCError）。
func statusError(message string) error {
	return &protocol.RPCError{Code: -32000, Message: message}
}

// acpHTTPClient worker 模式的 LLM 客户端：与 agent 插件同为 2min 总超时。
func acpHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute}
}

// stdioConn 把进程 stdin/stdout 拼成 ACP 传输通道；Close 不关进程标准流。
type stdioConn struct{}

func (stdioConn) Read(b []byte) (int, error)  { return os.Stdin.Read(b) }
func (stdioConn) Write(b []byte) (int, error) { return os.Stdout.Write(b) }
func (stdioConn) Close() error                { return nil }

// Stdio 返回 worker 模式的标准传输通道。
func Stdio() io.ReadWriteCloser { return stdioConn{} }
