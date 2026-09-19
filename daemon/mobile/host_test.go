package mobile

import (
	"encoding/json"
	"fmt"
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
