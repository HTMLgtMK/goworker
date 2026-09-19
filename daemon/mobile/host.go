// Package mobile 是 Android（gobind）入口壳：进程内嵌入 ACP worker 的管道桥。
//
// 架构：Kotlin 侧实现 ACP Client（JSON-RPC over 换行分隔帧），经本包与 Go 侧
// service.ServeACPWorker 通信 —— 与 VS Code/ZCode 前端共享同一套协议语义
// （session/prompt 流式输出、session/request_permission HITL、session/load 回放）。
// 本包只桥接字节流，不解释协议：JSON-RPC 编解码、session 生命周期、HITL 对话框
// 交互全部在 Kotlin ACP 客户端完成。协议细节见 PROTOCOL.md。
//
// gobind 边界约束：仅 string/[]byte/error/可绑定接口（无 map/func 字段/变参）；
// 一切导出方法带 recover 壳 —— Go panic 会 abort 整个 App 进程。
package mobile

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	dispatch "github.com/tinguo/goworker/ai-dispatch"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/service"
)

// Callbacks 是 worker → Kotlin 的上行回调（Kotlin 实现，gobind 生成 proxy）。
// 回调来自后台 goroutine —— Kotlin 侧碰 UI 必须自行切主线程。
type Callbacks interface {
	// OnData 递送 worker 发出的原始字节流：换行分隔的 JSON-RPC 消息
	//（对请求的响应 + session/update / session/request_permission 等通知与请求）。
	// chunk 是一次底层 Read 的切片，可能含半条消息 —— 客户端必须按行累积组帧。
	OnData(chunk []byte)
	// OnClose worker 侧退出（EOF / 致命错误 / Close）后回调恰一次。
	OnClose(message string)
}

// pipeConn 把一对 io.Pipe 拼成 worker 侧的 io.ReadWriteCloser：
// 读 = client→worker 管道（Kotlin Write 的一端），写 = worker→client 管道。
type pipeConn struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (c *pipeConn) Read(b []byte) (int, error)  { return c.r.Read(b) }
func (c *pipeConn) Write(b []byte) (int, error) { return c.w.Write(b) }
func (c *pipeConn) Close() error {
	_ = c.r.Close()
	_ = c.w.Close()
	return nil
}

// Host 是 Android 侧持有的 ACP worker 容器：Start 后 worker 在后台 goroutine
// 服务 ACP 协议，Kotlin 经 Write/OnData 与之全双工通信。非并发安全：
// Close 之后 Host 不可再用；Write 需在 Start 之后。
type Host struct {
	mu sync.Mutex
	cb Callbacks

	runtimeCfg *runtimeconfig.Config
	log        *logger.Logger

	clientW *io.PipeWriter // Kotlin Write → worker 读端
	pumpR   *io.PipeReader // worker 写端 → pump → OnData
	server  *dispatch.Server

	started     bool
	closed      bool
	closeOnce   sync.Once
	closeReport sync.Once
}

// NewHost 装载 worker 运行时。与 app.RunACPWorker 共享同一套装配点：
// 配置/日志装载、GOWORKER_SANDBOX_MODE 门禁覆盖；分叉的只有传输层 ——
// 管道而非 stdio。configDir 经 GOWORKER_CONFIG_DIR 注入
// （App 传 getFilesDir() 派生路径；空串走默认 ~/.config/goworker）。
func NewHost(configDir string, cb Callbacks) (h *Host, err error) {
	defer func() {
		if r := recover(); r != nil {
			h, err = nil, fmt.Errorf("mobile host panic: %v", r)
		}
	}()
	if cb == nil {
		return nil, errors.New("callbacks is nil")
	}
	if configDir != "" {
		os.Setenv("GOWORKER_CONFIG_DIR", configDir)
	}
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)
	if cfg == nil {
		return nil, fmt.Errorf("配置无效: %s，详见日志；修好后重试，或删掉该文件用默认配置", cfgPath)
	}
	// 与 app.RunACPWorker 一致：无人值守 worker 的沙箱门禁可经环境收紧，
	// 非法值拒绝启动（App 侧如需 strict 可在启动前设置该 env）。
	runtimeCfg := cfg.ToRuntime()
	if mode := os.Getenv("GOWORKER_SANDBOX_MODE"); mode != "" {
		switch mode {
		case "normal", "strict", "readonly", "off":
			runtimeCfg.Sandbox.Mode = mode
		default:
			return nil, fmt.Errorf("invalid GOWORKER_SANDBOX_MODE %q (normal|strict|readonly|off)", mode)
		}
		slog.Info("mobile worker: sandbox mode overridden", "mode", mode)
	}
	log, err := logger.Setup(cfg.Log)
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
	}
	slog.SetDefault(log.Logger)
	return &Host{cb: cb, runtimeCfg: runtimeCfg, log: log}, nil
}

// Start 启动 worker：io.Pipe 桥接 + ServeACPWorker 后台服务。
// 之后 Kotlin 即可经 Write 发送 ACP 请求（首条约定为 initialize）。
func (h *Host) Start() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("start panic: %v", r)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return errors.New("host already started")
	}
	if h.closed {
		return errors.New("host closed")
	}

	toWorkerR, toWorkerW := io.Pipe()     // client→worker
	fromWorkerR, fromWorkerW := io.Pipe() // worker→client
	h.clientW = toWorkerW
	h.pumpR = fromWorkerR
	h.server = service.ServeACPWorker(&pipeConn{r: toWorkerR, w: fromWorkerW}, service.ACPWorkerDeps{
		Config:   h.runtimeCfg,
		AuditDir: filepath.Join(config.DefaultDir(), "audit"),
	})
	h.started = true
	go h.pump()
	go h.watchDone()
	return nil
}

// Write 发送一条 client→worker 的 JSON-RPC 消息。Host 负责帧定界：
// 每次 Write 追加 '\n' —— 一次 Write 一条完整消息，chunk 无需自带换行。
func (h *Host) Write(chunk []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write panic: %v", r)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.started || h.closed {
		return errors.New("host not running")
	}
	if _, err := h.clientW.Write(chunk); err != nil {
		return fmt.Errorf("write acp pipe: %w", err)
	}
	if _, err := h.clientW.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("write acp frame: %w", err)
	}
	return nil
}

// Close 停止 worker 并释放资源。幂等；Close 后 OnClose 恰好回调一次。
func (h *Host) Close() {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		started := h.started
		if h.clientW != nil {
			_ = h.clientW.Close() // EOF → worker 读循环退出 → Done
		}
		if h.pumpR != nil {
			_ = h.pumpR.Close() // pump 退出
		}
		h.closed = true
		h.mu.Unlock()
		if started && h.server != nil {
			_ = h.server.Close()
		}
		if h.log != nil {
			h.log.Close()
		}
	})
}

// pump 把 worker 输出桥到 OnData（worker 写端 → Kotlin）。
func (h *Host) pump() {
	buf := make([]byte, 32*1024)
	for {
		n, err := h.pumpR.Read(buf)
		if n > 0 {
			// gobind 的 upcall 是同步的，buf 复用本安全；拷贝一份留出
			// 回调异步化的余地，代价可忽略。
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			h.safe(func() { h.cb.OnData(chunk) })
		}
		if err != nil {
			h.reportClose("worker output closed: " + err.Error())
			return
		}
	}
}

// watchDone 等待 worker 读循环退出（对端断开/Close），收尾并上报。
func (h *Host) watchDone() {
	<-h.server.Done()
	h.reportClose("worker stopped")
	h.Close()
}

// reportClose 保证 OnClose 恰好递送一次（pump 与 watchDone 谁先到都行）。
func (h *Host) reportClose(message string) {
	h.closeReport.Do(func() {
		h.safe(func() { h.cb.OnClose(message) })
	})
}

// safe 保护上行回调：Kotlin 侧抛出的异常经 gobind 变 Go panic，吞掉并记日志，
// 不让它打断 worker。
func (h *Host) safe(fn func()) {
	defer func() {
		if r := recover(); r != nil && h.log != nil {
			h.log.Error("mobile callback panic", "panic", r)
		}
	}()
	fn()
}
