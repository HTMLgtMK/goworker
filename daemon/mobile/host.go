// Package mobile 是 Android（gobind）入口壳：进程内嵌入 app.Application 后端的管道桥。
//
// 架构：Kotlin 侧实现 ACP Client（JSON-RPC over 换行分隔帧），经本包与 Go 侧
// app.Application 通信 —— ACP 经 WithACPConnection 走进程内管道，后端是完整
// 的 Engine + 插件 + services 装配（内置命令、会话持久化与 VS Code 端的
// app-service 路径同源）。本包只桥接字节流，不解释协议：JSON-RPC 编解码、
// session 生命周期、HITL 对话框交互全部在 Kotlin ACP 客户端完成。协议细节见 PROTOCOL.md。
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
	"sync"

	"github.com/tinguo/goworker/daemon/internal/app"
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

// pipeConn 把一对 io.Pipe 拼成 app 侧的 io.ReadWriteCloser：
// 读 = client→app 管道（Kotlin Write 的一端），写 = app→client 管道。
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

// Host 是 Android 侧持有的 agent 后端容器：Start 后 app.Application 在
// 进程内服务 ACP 协议，Kotlin 经 Write/OnData 与之全双工通信。非并发安全：
// Close 之后 Host 不可再用；Write 需在 Start 之后。
type Host struct {
	mu sync.Mutex
	cb Callbacks

	appl *app.Application

	clientW *io.PipeWriter // Kotlin Write → ACP server 读端
	pumpR   *io.PipeReader // ACP server 写端 → pump → OnData

	started     bool
	closed      bool
	closeOnce   sync.Once
	closeReport sync.Once
}

// NewHost 准备 Android agent 后端容器。装配（配置/日志装载、沙箱门禁覆盖、
// Engine/插件/服务生命周期）统一由 app.New 在 Start 时完成 —— NewHost 只存
// configDir 与回调，配置校验推迟到 Start，错误经 error 而非 panic 交给壳层。
// configDir 经 GOWORKER_CONFIG_DIR 注入（App 传 getFilesDir() 派生路径；
// 空串走默认 ~/.config/goworker）。
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
	return &Host{cb: cb}, nil
}

// Start 装配并启动后端：io.Pipe 桥 + app.New(WithACPConnection) 同步装配
// （配置 → Engine → 插件 → StartAll → services.Start），之后 Kotlin 即可经
// Write 发送 ACP 请求（首条约定为 initialize）。app.New 阻塞装配是预期行为；
// acp service 的 Start 只调 listener factory 后台 accept，不会卡在协议握手。
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

	toWorkerR, toWorkerW := io.Pipe()     // client→app
	fromWorkerR, fromWorkerW := io.Pipe() // app→client
	h.clientW = toWorkerW
	h.pumpR = fromWorkerR
	appl, err := app.New(app.WithACPConnection(&pipeConn{r: toWorkerR, w: fromWorkerW}))
	if err != nil {
		// Host 自己的管道自己收：关掉四个端点防泄漏；app.New 内部失败已自行 cleanup
		//（services/engine/log），管道留给它只会悬空。
		_ = toWorkerR.Close()
		_ = toWorkerW.Close()
		_ = fromWorkerR.Close()
		_ = fromWorkerW.Close()
		return err
	}
	h.appl = appl
	h.started = true
	go h.pump()
	return nil
}

// Write 发送一条 client→app 的 JSON-RPC 消息。Host 负责帧定界：
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

// Close 停止后端并释放资源。幂等；Close 后 OnClose 恰好回调一次。
// 顺序：先关 Kotlin 写端（EOF → ACP server 读循环退出），再 appl.Stop()
// （services → engine → log）—— Stop 会等活跃 prompt 取消完成，可能阻塞，
// 这是有意的收口语义；期间 acp service 收口会关闭管道对端，pump 读到 EOF
// 退出并触发 OnClose。
func (h *Host) Close() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("mobile close panic", "panic", r)
		}
	}()
	h.closeOnce.Do(func() {
		h.mu.Lock()
		if h.clientW != nil {
			_ = h.clientW.Close() // EOF → ACP server 读循环退出
		}
		h.closed = true
		h.mu.Unlock()
		if h.appl != nil {
			h.appl.Stop()
			return
		}
		// Start 未跑或失败：没有后端替我们关对端，pump 只能手动收。
		h.mu.Lock()
		if h.pumpR != nil {
			_ = h.pumpR.Close()
		}
		h.mu.Unlock()
	})
}

// pump 把后端输出桥到 OnData（app 写端 → Kotlin）。
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
			// 正常 Close 走到这里的报错（closed pipe）对用户无信息量，换成稳定文案。
			h.mu.Lock()
			wasClosed := h.closed
			h.mu.Unlock()
			if wasClosed {
				h.reportClose("worker stopped")
			} else {
				h.reportClose("worker output closed: " + err.Error())
			}
			return
		}
	}
}

// reportClose 保证 OnClose 恰好递送一次。
func (h *Host) reportClose(message string) {
	h.closeReport.Do(func() {
		h.safe(func() { h.cb.OnClose(message) })
	})
}

// safe 保护上行回调：Kotlin 侧抛出的异常经 gobind 变 Go panic，吞掉并记日志，
// 不让它打断后端。此时全局 slog 已被 app.New 接上配置的 handler；装配前阶段
// 没有回调触发，用 slog.Default 兜底即可。
func (h *Host) safe(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("mobile callback panic", "panic", r)
		}
	}()
	fn()
}
