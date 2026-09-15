// ACP worker 模式：把 ai-runtime Session 暴露成 ACP Agent（stdio）。
// `goworker acp` 启动后，任意 ACP Client（包括本项目的 dispatcher，或 Zed 等
// 编辑器）都能把 ZCode 当作 worker 驱动：session/prompt 的文本即 agent 输入，
// token 流经 session/update 回传。
package agent

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	dispatch "github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// ACPWorkerDeps 是 worker 模式的最小装配输入。
type ACPWorkerDeps struct {
	Config   *runtimeconfig.Config
	AuditDir string // sandbox 审计目录（可空）
}

// acpWorker 实现 dispatch.TaskHandler：一个 ACP session 对应一个 Session 会话。
type acpWorker struct {
	deps ACPWorkerDeps

	mu       sync.Mutex
	sessions map[string]*runtimeagent.Session
}

// ServeACPWorker 在给定连接上服务 ACP worker 协议，返回 Server 便于测试与关闭。
func ServeACPWorker(rwc io.ReadWriteCloser, deps ACPWorkerDeps) *dispatch.Server {
	return dispatch.ServeConn(rwc, &acpWorker{deps: deps, sessions: map[string]*runtimeagent.Session{}})
}

func (w *acpWorker) SetSession(sessionID, _ string) {
	// worker 模式下 cwd 由调用方进程自行管理；预留参数对齐 SessionAware。
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sessions[sessionID] = runtimeagent.NewSession(runtimeagent.SessionDeps{
		Config:       w.deps.Config,
		AuditDir:     w.deps.AuditDir,
		Memory:       nil,
		CollectTools: runtimeagent.DefaultTools,
		NewProvider:  ProviderFactory(acpHTTPClient()),
		Store:        nil, // worker 会话由调用方管理生命周期，本地不持久化
	})
}

// Run 执行一回合：session/prompt 文本进 Session.Run，token 流映射为 session/update。
func (w *acpWorker) Run(ctx context.Context, sessionID, prompt string, rep dispatch.Reporter) (string, error) {
	w.mu.Lock()
	session, ok := w.sessions[sessionID]
	w.mu.Unlock()
	if !ok {
		return "", statusError(fmt.Sprintf("unknown session %q (call session/new first)", sessionID))
	}

	cb := runtimeagent.RunCallbacks{
		Write: func(text string) {
			rep.MessageChunk(sessionID, text)
		},
		EmitToken: func(token core.Token) {
			rep.Update(sessionID, TokenToUpdate(token))
		},
		// 无人值守：HITL 请求一律拒绝（Session SDK 对 nil Decide 的默认行为）
	}
	if err := session.Run(ctx, runtimeagent.RunRequest{Input: prompt}, cb); err != nil {
		return "", err
	}
	return protocol.StopEndTurn, nil
}

// TokenToUpdate 把结构化 runtime token 映射为 ACP session/update 的唯一映射：
// stdio worker（acpWorker）与 vscode socket 前端（frontend/vscode）两处共用，
// 保证两条 ACP 入口的工具生命周期/思考/正文渲染一致。
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
