// Service 装配：Application 启动的后台 service（ACP 主会话入口）。
//
// 业务逻辑不在这里：handler 在 daemon/internal/core/service/acp（FrontendHandler），
// 本文件只做 composition —— 解析 socket 路径、解析 SessionSource、把 Engine 的
// evaluate 接进 handler factory。壳层（cmd/goworker、mobile）经 app.New 间接使用。
package app

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/tinguo/goworker/daemon/internal/app/config"
	acpservice "github.com/tinguo/goworker/daemon/internal/core/service/acp"
)

// startACPService 启动 daemon 主会话 ACP service。socket 未配置时按 DefaultDir
// 派生 <DefaultDir>/frontend/acp.sock —— 与 vscode.sock、dispatcher 的
// <DispatchDir>/acp.sock 完全分离，三路 socket 各服务各的语义。
func startACPService(a *Application) error {
	if a.Config.Frontend.ACP.Socket == "" {
		a.Config.Frontend.ACP.Socket = filepath.Join(config.DefaultDir(), "frontend", "acp.sock")
	}

	// SessionSource 在 StartAll 后已定型：可用则声明 cwd/清单，不可用只降级
	// （FrontendHandler 的 CreateSession/Prompt 均可独立工作），与 vscode
	// frontend 的处理一致。
	sessions, err := a.SessionSource()
	if err != nil {
		if !errors.Is(err, ErrCapabilityUnavailable) {
			return fmt.Errorf("resolve session source: %w", err)
		}
		slog.Warn("acp service: session capability unavailable, sessions will not be declared to the daemon")
		sessions = nil
	}

	service := acpservice.NewService(acpservice.NewFrontendHandlerFactory(a.Engine.Eval, sessions))
	if err := service.Start(a.Config.Frontend.ACP.Socket); err != nil {
		return fmt.Errorf("start acp service: %w", err)
	}
	a.ACP = service
	a.Log.Info("acp service listening", "socket", a.Config.Frontend.ACP.Socket)
	return nil
}
