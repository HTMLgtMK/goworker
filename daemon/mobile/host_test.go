package mobile

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// collectCB 收集 OnData 字节流并记录 OnClose，供往返断言。
type collectCB struct {
	mu      sync.Mutex
	buffer  strings.Builder
	closed  chan string
	partial string            // 跨 chunk 的半行残留
	onLine  func(line string) // 完整行回调（可选）：模拟客户端对 server 请求的自动应答
}

func (c *collectCB) OnData(chunk []byte) {
	c.mu.Lock()
	c.buffer.Write(chunk)
	text := c.partial + string(chunk)
	lines := strings.Split(text, "\n")
	c.partial = lines[len(lines)-1]
	complete := lines[:len(lines)-1]
	c.mu.Unlock()
	for _, line := range complete {
		if line = strings.TrimSpace(line); line != "" && c.onLine != nil {
			c.onLine(line)
		}
	}
}

func (c *collectCB) OnClose(message string) {
	select {
	case c.closed <- message:
	default:
	}
}

// waitResponse 轮询字节流，按行组帧后找指定 id 的 JSON-RPC 响应。
func (c *collectCB) waitResponse(t *testing.T, id float64) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		data := c.buffer.String()
		c.mu.Unlock()
		for _, line := range strings.Split(data, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var msg map[string]any
			if err := json.Unmarshal([]byte(line), &msg); err != nil {
				continue // 半条消息，等下一轮累积
			}
			if msgID, ok := msg["id"].(float64); ok && msgID == id && msg["result"] != nil {
				return msg
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no response with id %v within deadline; got: %s", id, c.buffer.String())
	return nil
}

// TestHostBridgeRoundTrip 经桥做真实协议往返：initialize → session/new。
// Session 构造是惰性的（provider/LLM 到 prompt 才触网），本测试离线可跑。
func TestHostBridgeRoundTrip(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	if err := h.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// 模拟客户端契约：对未知方法请求自动回 -32601（Kotlin AcpClient 同款义务）——
	// 否则 worker 的 x-device/tools 探测会吃满超时，session/new 被拖住。
	cb.onLine = func(line string) {
		obj := map[string]any{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			return
		}
		method, _ := obj["method"].(string)
		id, hasID := obj["id"].(float64)
		if method == "" || !hasID {
			return
		}
		if strings.HasPrefix(method, "session/") {
			return // 正常 RPC 由 waitResponse 断言
		}
		_ = h.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32601,"message":"method not found"}}`, int64(id))))
	}

	// initialize 往返
	if err := h.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)); err != nil {
		t.Fatalf("write initialize: %v", err)
	}
	initResp := cb.waitResponse(t, 1)
	if _, ok := initResp["result"].(map[string]any); !ok {
		t.Fatalf("initialize result missing: %v", initResp)
	}

	// session/new 往返
	if err := h.Write([]byte(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp","mcpServers":[]}}`)); err != nil {
		t.Fatalf("write session/new: %v", err)
	}
	newResp := cb.waitResponse(t, 2)
	result, _ := newResp["result"].(map[string]any)
	if result == nil || result["sessionId"] == "" {
		t.Fatalf("session/new missing sessionId: %v", newResp)
	}

	// Close 后 OnClose 恰好一次，且 Close 幂等
	h.Close()
	select {
	case <-cb.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("OnClose not delivered after Close")
	}
	h.Close()
	select {
	case msg := <-cb.closed:
		t.Fatalf("OnClose delivered twice: %q", msg)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestHostLifecycle 校验生命周期守卫：Start 前 Write 报错、重复 Start 报错。
func TestHostLifecycle(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	if err := h.Write([]byte(`{}`)); err == nil {
		t.Fatal("Write before Start: want error")
	}
	if err := h.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := h.Start(); err == nil {
		t.Fatal("double Start: want error")
	}
}

// TestHostStartRejectsInvalidSandboxMode 校验 GOWORKER_SANDBOX_MODE 门禁已随装配
// 移交 app.New：NewHost 不再提前校验（它根本不碰配置），Start 拒绝启动。
func TestHostStartRejectsInvalidSandboxMode(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())
	t.Setenv("GOWORKER_SANDBOX_MODE", "yolo")
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	if err := h.Start(); err == nil {
		t.Fatal("Start with invalid GOWORKER_SANDBOX_MODE: want error")
	}
}

// TestHostStartValidatesConfig 校验配置校验也已推迟到 Start（app.New 统一装载）：
// NewHost 对损坏的 config.yaml 照常成功，Start 报配置错误。
func TestHostStartValidatesConfig(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("llm: [unclosed"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost(dir, cb)
	if err != nil {
		t.Fatalf("NewHost with broken config: %v", err)
	}
	defer h.Close()
	if err := h.Start(); err == nil {
		t.Fatal("Start with broken config: want error")
	}
}

// TestHostStartRollbackClearsState 钉住启动失败与 panic 共用的 rollback：
// Host 必须离开 starting，且不再暴露已经关闭的管道端点。
func TestHostStartRollbackClearsState(t *testing.T) {
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	toWorkerR, toWorkerW := io.Pipe()
	fromWorkerR, fromWorkerW := io.Pipe()
	h.mu.Lock()
	h.starting = true
	h.clientW = toWorkerW
	h.pumpR = fromWorkerR
	h.mu.Unlock()

	h.rollbackStart(toWorkerR, toWorkerW, fromWorkerR, fromWorkerW)

	h.mu.Lock()
	starting, clientW, pumpR := h.starting, h.clientW, h.pumpR
	h.mu.Unlock()
	if starting || clientW != nil || pumpR != nil {
		t.Fatalf("start rollback: starting=%v clientW=%v pumpR=%v, want false/nil/nil", starting, clientW, pumpR)
	}
}

// TestHostWriteBlockedCloseStillCloses 白盒复现 P1：app 侧读循环停摆时管道写
// 无限阻塞，旧实现持 mu 写管道会让 Close 永远拿不到锁（Android 上就是 ANR）。
// 不绕 app.New，直接手工摆一个无人读的管道对制造确定性阻塞。
func TestHostWriteBlockedCloseStillCloses(t *testing.T) {
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	// 白盒布置：started=true + 无读者的管道 —— 任何 Write 都必然阻塞在管道上。
	toR, toW := io.Pipe()
	defer func() { _ = toR.Close() }()
	h.mu.Lock()
	h.started = true
	h.clientW = toW
	h.mu.Unlock()

	// 卡死的 writer：持有 writeMu 后阻塞在管道写上。
	errCh := make(chan error, 1)
	go func() { errCh <- h.Write([]byte(`{"jsonrpc":"2.0","id":1}`)) }()

	// 等 Write 真正占住 writeMu（TryLock 成功说明它还没拿到，重试到拿不到为止）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.writeMu.TryLock() {
			h.writeMu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}

	// 核心断言：卡死的 Write 拦不住 Close。旧实现这里必死锁，5 秒看门狗兜底。
	closeDone := make(chan struct{})
	go func() { h.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked by stuck Write: state lock still starved by pipe I/O")
	}

	// Close 关了 clientW，卡死的 Write 应以错误返回而非永远悬着。
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("stuck Write after Close: want error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stuck Write not unblocked after Close")
	}

	// Close 之后的常规 Write 也要报错（生命周期守卫保留）。
	if err := h.Write([]byte(`{}`)); err == nil {
		t.Fatal("Write after Close: want error")
	}
}

// TestHostCloseWithoutStartIsImmediate Close 在 Start 之前调用必须立刻返回
// （状态机下无锁外装配可等），且不上报 OnClose —— 历史契约：后端从未跑起来
// 就没有可关的流。收口后再 Start 必须拒绝。
func TestHostCloseWithoutStartIsImmediate(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()

	done := make(chan struct{})
	go func() { h.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close before Start did not return immediately")
	}
	select {
	case msg := <-cb.closed:
		t.Fatalf("unexpected OnClose before any Start: %q", msg)
	case <-time.After(200 * time.Millisecond):
	}
	if err := h.Start(); err == nil {
		t.Fatal("Start after Close: want error")
	}
}

// TestHostStartCloseRace 压测 Start∥Close 全交织：Close 可能在 Start 登记
// starting 之前、锁外装配中、装配完成之后落地。任何交织下 Start/Close 都要
// 收敛（装配不持状态锁是本次修复的核心，看门狗兜底）；OnClose 至多一次，
// Start 成功后 Close 必须给出恰一次。顺带覆盖：装配中重入 Start 快速拒绝、
// 收口后 Start 拒绝。
func TestHostStartCloseRace(t *testing.T) {
	const iterations = 10
	for i := 0; i < iterations; i++ {
		dir := t.TempDir()
		cb := &collectCB{closed: make(chan string, 2)} // 容量 2：多送一次能当场数出来
		h, err := NewHost(dir, cb)
		if err != nil {
			t.Fatalf("iteration %d: NewHost: %v", i, err)
		}

		startDone := make(chan error, 1)
		go func() { startDone <- h.Start() }()

		// 等第一发 Start 登记好状态，之后的第二发 Start 必吃快速拒绝 ——
		// 同时证明装配中的重入 Start 不排队等装配完成。
		regDeadline := time.Now().Add(2 * time.Second)
		for {
			h.mu.Lock()
			registered := h.starting || h.closed || h.started
			h.mu.Unlock()
			if registered {
				break
			}
			if time.Now().After(regDeadline) {
				t.Fatalf("iteration %d: Start never reached a registered state", i)
			}
			time.Sleep(time.Millisecond)
		}
		if err := h.Start(); err == nil {
			t.Fatalf("iteration %d: Start while starting/started: want error", i)
		}

		// 抖动让 Close 落进生命周期不同阶段（登记前/装配中/装配后）。
		time.Sleep(time.Duration(i%4) * time.Millisecond)
		closeDone := make(chan struct{})
		go func() { h.Close(); close(closeDone) }()

		var startErr error
		select {
		case startErr = <-startDone:
		case <-time.After(15 * time.Second):
			t.Fatalf("iteration %d: Start did not converge under Close race", i)
		}
		select {
		case <-closeDone:
		case <-time.After(15 * time.Second):
			t.Fatalf("iteration %d: Close did not converge during assembly", i)
		}

		if startErr == nil {
			// Start 成功 → Close 走 appl.Stop → pump EOF → OnClose 恰一次。
			select {
			case <-cb.closed:
			case <-time.After(3 * time.Second):
				t.Fatalf("iteration %d: Start succeeded but OnClose missing after Close", i)
			}
		}
		if n := len(cb.closed); n > 1 {
			t.Fatalf("iteration %d: OnClose delivered %d times, want at most 1", i, n)
		}

		// 收口后的 Host 彻底作废：再 Start 必须拒绝。
		if err := h.Start(); err == nil {
			t.Fatalf("iteration %d: Start after Close: want error", i)
		}
	}
}

// TestHostWriteCloseRace 真实 Start 下的并发压测：多 goroutine 狂发 Write，
// 主 goroutine 同时 Close —— 全程 -race，Close 必须能及时收口、无死锁。
func TestHostWriteCloseRace(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())
	cb := &collectCB{closed: make(chan string, 1)}
	h, err := NewHost("", cb)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	if err := h.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const writers = 4
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				// 大块 payload 提高管道写阻塞概率，让 Write∥Close 真正撞上。
				_ = h.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"ping","params":{"pad":"%s"}}`, j, strings.Repeat("x", 4096))))
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() { h.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish while writers were active")
	}
	close(stop)
	wg.Wait()

	if err := h.Write([]byte(`{}`)); err == nil {
		t.Fatal("Write after Close: want error")
	}
}
