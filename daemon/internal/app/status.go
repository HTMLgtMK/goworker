// status 查询面：daemon 把运行状态以一行 JSON 快照发给每个连上
// <ConfigDir>/status.sock 的客户端（`goworker status` 是唯一预期消费者）。
// 启动 banner 与 status 服务共用 StatusInfo，保证 `goworker status` 看到的
// 和启动时打印的同源 —— 新增端点只改 StatusInfo 一处。
//
// 只在 CLI 壳声明了 mode 时注册（见 serviceRegistrations）：mobile 等进程内
// 装配不应在用户目录落查询 socket。
package app

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/tinguo/goworker/daemon/internal/app/config"
	acpservice "github.com/tinguo/goworker/daemon/internal/core/service/acp"
	"github.com/tinguo/goworker/daemon/internal/version"
)

// StatusInfo 是 status socket 下发与启动 banner 共用的运行状态快照。
type StatusInfo struct {
	Version        string `json:"version"` // version.Get().String() 单行摘要
	PID            int    `json:"pid"`
	Mode           string `json:"mode"`       // "daemon" / "tui"；空 = 非 CLI 壳装配
	StartedAt      string `json:"started_at"` // RFC3339
	UptimeSeconds  int64  `json:"uptime_seconds"`
	ConfigDir      string `json:"config_dir"`
	ConfigPath     string `json:"config_path"`
	LogFile        string `json:"log_file,omitempty"`
	ACPSocket      string `json:"acp_socket,omitempty"` // frontend.acp 关闭时为空
	DispatchSocket string `json:"dispatch_socket,omitempty"`
	StatusSocket   string `json:"status_socket"`
}

// StatusInfo 汇总当前装配的运行状态。a.Mode 为空（mobile 装配）时字段照常填充，
// 只是快照不会被任何 socket 发布。
func (a *Application) StatusInfo() StatusInfo {
	info := StatusInfo{
		Version:        version.Get().String(),
		PID:            os.Getpid(),
		Mode:           a.Mode,
		StartedAt:      a.startedAt.Format(time.RFC3339),
		UptimeSeconds:  int64(time.Since(a.startedAt).Seconds()),
		ConfigDir:      config.DefaultDir(),
		ConfigPath:     config.DefaultPath(),
		StatusSocket:   StatusSocketPath(),
		DispatchSocket: DispatchSocketPath(),
	}
	if a.Config != nil {
		info.LogFile = a.Config.Log.File
		if a.Config.Frontend.ACP.Enabled {
			info.ACPSocket = a.Config.Frontend.ACP.Socket
			if info.ACPSocket == "" {
				info.ACPSocket = FrontendSocketPath()
			}
		}
	}
	return info
}

// statusService 是只读旁路服务：每个连接回一行 JSON 快照即断开。
type statusService struct {
	app      *Application
	listener acpservice.Listener

	mu      sync.Mutex
	started bool
}

// Name identifies the status service to the application service registry.
func (s *statusService) Name() string { return "status" }

// Start 在 StatusSocketPath 上开监听并进入 accept 循环。Start 幂等；监听器
// 复用 acpservice.NewUnixListener（0600 socket / 0700 目录 / 残留 socket 探测）。
func (s *statusService) Start() error {
	if s == nil || s.app == nil {
		return fmt.Errorf("status: nil service")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}

	path := StatusSocketPath()
	listener, err := acpservice.NewUnixListener(path)
	if err != nil {
		return fmt.Errorf("status: create listener: %w", err)
	}
	s.listener = listener
	s.started = true
	if s.app.Log != nil {
		s.app.Log.Info("status service listening", "socket", path)
	}

	go func() {
		// Serve 依赖 Close 才返回；status 是旁路面，accept 出错只记日志，
		// 不值得为它拖垮整个装配。
		if err := listener.Serve(s.serveConn); err != nil && s.app.Log != nil {
			s.app.Log.Error("status service stopped", "error", err)
		}
	}()
	return nil
}

// serveConn 把快照写给单个查询方并关闭连接。快照极小，必然一次写完；
// 读取侧拿到对端 EOF 即视为完整一行。
func (s *statusService) serveConn(conn io.ReadWriteCloser) {
	defer conn.Close()
	data, err := json.Marshal(s.app.StatusInfo())
	if err != nil {
		return
	}
	_, _ = conn.Write(append(data, '\n'))
}

// Stop 关闭监听并等 accept 循环与在途快照写完。幂等。
func (s *statusService) Stop() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil
	}
	s.started = false

	if err := s.listener.Close(); err != nil {
		return err
	}
	<-s.listener.Done()
	return nil
}
