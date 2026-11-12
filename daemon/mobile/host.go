// Package mobile bridges Android's gobind ACP client to app.Application over pipes.
// Kotlin owns protocol handling and session/UI behavior; this package forwards bytes.
// Exported APIs must stay gobind-compatible, and recover panics at the boundary.
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
// 进程内服务 ACP 协议，Kotlin 经 Write/OnData 与之全双工通信。生命周期是
// 小状态机（见 Start），方法可从任意线程调用；但 Close 之后 Host 不可再用，
// Write 需在 Start 成功之后。
type Host struct {
	// mu 只守生命周期状态（started/closed/clientW/pumpR/appl），绝不跨管道
	// I/O 持有 —— 管道写可能因对端停摆而无限阻塞，锁里等它 = ANR。
	mu sync.Mutex
	cb Callbacks

	// writeMu 专管上行 I/O：序列化 chunk+'\n' 两段写保证一帧不交错。
	// Close 刻意不碰它 —— 关 clientW 就能解救卡在管道上的 Write。
	writeMu sync.Mutex

	appl *app.Application

	clientW *io.PipeWriter // Kotlin Write → ACP server 读端
	pumpR   *io.PipeReader // ACP server 写端 → pump → OnData

	// 生命周期状态机：new → starting → running → closed。starting 期间
	// app.New 在锁外装配 —— 昂贵装配绝不拦 Close（Android 后台初始化时
	// 组件销毁要能立刻收口，不能等装配跑完）。
	started     bool
	starting    bool
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

// rollbackStart 统一回收 Start 装配失败（含 app.New panic）的半成品：四个管道
// 端点全部关闭，Host 回到可重试的 new 状态。若 Close 已抢先收口，则补发一次
// OnClose；closeReport 保证与其他退出路径竞争时仍只递送一次。
func (h *Host) rollbackStart(toWorkerR *io.PipeReader, toWorkerW *io.PipeWriter, fromWorkerR *io.PipeReader, fromWorkerW *io.PipeWriter) {
	if toWorkerR != nil {
		_ = toWorkerR.Close()
	}
	if toWorkerW != nil {
		_ = toWorkerW.Close()
	}
	if fromWorkerR != nil {
		_ = fromWorkerR.Close()
	}
	if fromWorkerW != nil {
		_ = fromWorkerW.Close()
	}

	h.mu.Lock()
	if h.clientW == toWorkerW {
		h.clientW = nil
	}
	if h.pumpR == fromWorkerR {
		h.pumpR = nil
	}
	h.starting = false
	closed := h.closed
	h.mu.Unlock()
	if closed {
		h.reportClose("host closed during start")
	}
}

// Start builds the pipe-backed ACP service. app.New runs outside the state lock so
// Close can finish during assembly; the completion path then stops the new app and
// reports OnClose. After a successful start, OnClose is delivered once on shutdown;
// a start that never succeeded does not report it.
func (h *Host) Start() (err error) {
	var (
		toWorkerR   *io.PipeReader
		toWorkerW   *io.PipeWriter
		fromWorkerR *io.PipeReader
		fromWorkerW *io.PipeWriter
	)
	assembling := false
	defer func() {
		if r := recover(); r != nil {
			// app.New 是唯一装配入口。它在状态锁外执行，panic 时可安全复用
			// 普通启动失败的 rollback；别只吞 panic 留下 starting=true 的僵尸 Host。
			if assembling {
				h.rollbackStart(toWorkerR, toWorkerW, fromWorkerR, fromWorkerW)
			}
			err = fmt.Errorf("start panic: %v", r)
		}
	}()

	// 阶段一（锁内）：登记 starting 并建好管道。管道留在锁内创建是有意的：
	// clientW 对 Write 可见即完整，快照语义不破。
	h.mu.Lock()
	if h.started {
		h.mu.Unlock()
		return errors.New("host already started")
	}
	if h.starting {
		h.mu.Unlock()
		return errors.New("host start already in progress")
	}
	if h.closed {
		h.mu.Unlock()
		return errors.New("host closed")
	}
	h.starting = true
	toWorkerR, toWorkerW = io.Pipe()     // client→app
	fromWorkerR, fromWorkerW = io.Pipe() // app→client
	h.clientW = toWorkerW
	h.pumpR = fromWorkerR
	h.mu.Unlock()

	// 阶段二（锁外）：app.New 是唯一 Application 创建入口。装配可耗时数秒，
	// 绝不持有状态锁；panic 由上方 defer 捕获并复用同一条 rollback 路径。
	assembling = true
	appl, err := app.New(app.WithACPConnection(&pipeConn{r: toWorkerR, w: fromWorkerW}))
	assembling = false
	if err != nil {
		// app.New 内部失败已自行 cleanup（services/engine/log）；Host 只回收
		// 自己创建的管道并复位生命周期状态。
		h.rollbackStart(toWorkerR, toWorkerW, fromWorkerR, fromWorkerW)
		return err
	}

	// 阶段三（锁内快照）：装配期间若 Close 已收口，本方法接手善后。
	h.mu.Lock()
	closed := h.closed
	clientW, pumpR := h.clientW, h.pumpR
	if !closed {
		h.appl = appl
		h.started = true
		h.starting = false
		h.mu.Unlock()
		go h.pump()
		return nil
	}
	h.mu.Unlock()

	// 装配撞上 Close：拆掉刚搭好的后端。appl.Stop 会经 acp service 收口关掉
	// 管道对端；clientW/pumpR 与 Close 各自的关闭可能重叠 —— io.Pipe 端点
	// 幂等，重复 Close 无害。
	appl.Stop()
	_ = clientW.Close()
	_ = pumpR.Close()
	h.reportClose("host closed during start")
	h.mu.Lock()
	h.starting = false
	h.mu.Unlock()
	return errors.New("host closed during start")
}

// Write 发送一条 client→app 的 JSON-RPC 消息。Host 负责帧定界：
// 每次 Write 追加 '\n' —— 一次 Write 一条完整消息，chunk 无需自带换行。
//
// 两段式加锁：先在 mu 下校验状态并快照写端（绝不持 mu 等管道），再在 writeMu
// 下真正落笔。Close 随时可以拿 mu 关 clientW —— 卡在管道上的 Write 会立刻
// 以 ErrClosedPipe 醒来，而不是抱着状态锁把 Close 一起拖死。装配中（starting）
// started 仍为 false，同样报 host not running —— 装配没完成就没有可写的后端。
func (h *Host) Write(chunk []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write panic: %v", r)
		}
	}()
	h.mu.Lock()
	if !h.started || h.closed {
		h.mu.Unlock()
		return errors.New("host not running")
	}
	w := h.clientW
	h.mu.Unlock()

	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if _, err := w.Write(chunk); err != nil {
		return fmt.Errorf("write acp pipe: %w", err)
	}
	if _, err := w.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("write acp frame: %w", err)
	}
	return nil
}

// Close stops the backend and releases resources. It is idempotent. A successful
// Start waits for app shutdown and the output pump to report OnClose; Close during
// assembly leaves reporting to Start's completion path. Close never takes writeMu:
// closing the pipe must unblock a blocked Write. Snapshot appl under mu to avoid a race.
func (h *Host) Close() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("mobile close panic", "panic", r)
		}
	}()
	h.closeOnce.Do(func() {
		h.mu.Lock()
		appl := h.appl // 锁内快照，见锁纪律注释
		if h.clientW != nil {
			_ = h.clientW.Close() // EOF → ACP server 读循环退出
		}
		h.closed = true
		h.mu.Unlock()
		if appl != nil {
			appl.Stop()
			return
		}
		// Start 未跑、失败或仍在装配：没有后端替我们关对端，pump 只能手动收。
		// 若是仍在装配，OnClose 由 Start 的阶段三/失败路径补报（见 Start）。
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
