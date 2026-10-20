package acp

import (
	"context"
	"fmt"
	"sync"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// FrontendHandler 是 daemon 主会话 ACP 入口的默认 handler：每连接一份，维护
// 「本连接上已创建的 session」台账，把 prompt 文本交给注入的 evaluate（通常是
// Engine.Eval 兜底链路）。session/list、session/load、命令补全等前端语义由
// 后续 adapter 提供，这里刻意保持最小。
//
// 它放在本包而非装配层：路由策略是 daemon domain 逻辑，所有壳层共用；装配层
// 只负责把 evaluate/Sessions 接进来。
type FrontendHandler struct {
	evaluate func(ctx *model.Context, input string) error
	sessions model.SessionSource

	mu    sync.Mutex
	known map[string]struct{} // 本连接已 session/new 的 ACP sessionID
}

var (
	_ aiacp.SessionHandler = (*FrontendHandler)(nil)
	_ aiacp.SessionCreator = (*FrontendHandler)(nil)
)

// NewFrontendHandlerFactory 返回每连接创建一个 FrontendHandler 的工厂。sessions
// 可为 nil（会话能力不可用时降级：cwd 不向 daemon 声明，prompt 仍可路由）。
func NewFrontendHandlerFactory(evaluate func(ctx *model.Context, input string) error, sessions model.SessionSource) HandlerFactory {
	return func() aiacp.SessionHandler {
		return &FrontendHandler{evaluate: evaluate, sessions: sessions}
	}
}

// CreateSession 实现 aiacp.SessionCreator：登记本连接的 session 并向 daemon
// 声明 cwd（SessionSource 不可用时跳过，不阻塞会话建立）。
func (h *FrontendHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, _ aiacp.Reporter) error {
	h.mu.Lock()
	if h.known == nil {
		h.known = make(map[string]struct{})
	}
	h.known[sessionID] = struct{}{}
	h.mu.Unlock()
	if h.sessions != nil {
		h.sessions.SetSession(sessionID, req.Cwd)
	}
	return nil
}

// Prompt 实现 aiacp.SessionHandler：拼接文本块经 evaluate 执行，输出以 ACP
// chunk 回流。HITL/命令补全等前端语义在后续 adapter 阶段接入。
func (h *FrontendHandler) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, reporter aiacp.Reporter) (string, error) {
	h.mu.Lock()
	_, known := h.known[sessionID]
	h.mu.Unlock()
	if !known {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: unknown session %q", sessionID),
		}
	}

	var input string
	for _, block := range prompt {
		if block.Type != "text" {
			continue
		}
		if input != "" {
			input += "\n"
		}
		input += block.Text
	}

	// decide 传 nil：沙箱中间件对 nil 的默认行为是拒绝，HITL 授权等
	// Reporter.RequestPermission 映射在 adapter 阶段补齐。
	commandContext := model.NewContext(ctx, func(text string) {
		reporter.MessageChunk(sessionID, text)
	}, nil, nil)
	if err := h.evaluate(commandContext, input); err != nil {
		return "", err
	}
	return protocol.StopEndTurn, nil
}
