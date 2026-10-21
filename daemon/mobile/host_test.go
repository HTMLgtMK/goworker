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
