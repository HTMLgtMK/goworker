// Package protocol 实现 ACP（Agent Client Protocol）的 wire 层：
// JSON-RPC 2.0 over newline-delimited stdio，双端共用同一套 Conn。
package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// ErrClosed 连接关闭后的 Call 与写入返回它。
var ErrClosed = errors.New("acp: connection closed")

const jsonRPCVersion = "2.0"

// Request 是 JSON-RPC 2.0 请求；ID 缺省即为 notification。
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// RPCError 是 JSON-RPC 2.0 error object。
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("rpc %d: %s", e.Code, e.Message)
}

// Response 是 JSON-RPC 2.0 响应。
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// Handler 处理对端请求，返回值序列化为 result。
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

// Notifier 处理对端 notification。
type Notifier func(params json.RawMessage)

// Conn 是双向 JSON-RPC 连接，Client/Agent 两端共用。
// Serve 在后台逐行读取并分发。分发语义是有意不对称的：
//   - 请求各自在独立 goroutine 中处理 —— 长任务 handler（session/prompt）
//     不得阻塞读循环，否则并发的 session/cancel 通知永远到不了；
//   - 通知也是异步分发（handler 阻塞不得卡死读循环），但串成 FIFO 链：
//     线上顺序即送达顺序，session/update 的流式渲染依赖这一点
//     （chunk 必须先于它触发的 tool_call 被消费端观察到）。
type Conn struct {
	rwc io.ReadWriteCloser

	writeMu sync.Mutex

	mu      sync.Mutex
	nextID  int64
	pending map[string]chan *Response
	methods map[string]Handler
	notif   map[string]Notifier

	// notification FIFO 链：notifLast 是上一条通知的完成信号。
	// 每条通知的 goroutine 先等它再跑 handler，串行化送达顺序；
	// 读循环本身只做入链，永不被慢 handler 阻塞（见 Conn 文档）。
	notifMu   sync.Mutex
	notifLast chan struct{}

	closed   chan struct{}
	once     sync.Once
	serveErr error
}

func NewConn(rwc io.ReadWriteCloser) *Conn {
	return &Conn{
		rwc:     rwc,
		pending: make(map[string]chan *Response),
		methods: make(map[string]Handler),
		notif:   make(map[string]Notifier),
		closed:  make(chan struct{}),
	}
}

// Handle 注册对端请求的处理器，重复注册覆盖前者。
func (c *Conn) Handle(method string, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.methods[method] = h
}

// HandleNotification 注册对端 notification 的处理器。
func (c *Conn) HandleNotification(method string, h Notifier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notif[method] = h
}

// dispatchNotif 把一条 notification 挂上 FIFO 链：goroutine 先等前一条完成，
// 再跑自己的 handler —— 送达顺序与线上顺序一致（见 Conn 文档的分发语义）。
func (c *Conn) dispatchNotif(h Notifier, params json.RawMessage) {
	c.notifMu.Lock()
	wait := c.notifLast
	done := make(chan struct{})
	c.notifLast = done
	c.notifMu.Unlock()

	go func() {
		if wait != nil {
			<-wait
		}
		defer close(done)
		h(params)
	}()
}

// Serve 阻塞读循环，直到连接关闭或对端 EOF。
// Close 之后的读错误视为正常关闭，返回 nil。
func (c *Conn) Serve() error {
	reader := bufio.NewReader(c.rwc)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			c.dispatch(line)
		}
		if err != nil {
			if errors.Is(err, io.EOF) && c.isClosed() {
				return nil
			}
			c.closeWith(err)
			return err
		}
	}
}

func (c *Conn) dispatch(line []byte) {
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		return // 非 JSON 行（对端噪音）直接忽略
	}
	if req.Method == "" {
		var resp Response
		if err := json.Unmarshal(line, &resp); err == nil {
			c.deliver(&resp)
		}
		return
	}
	if len(req.ID) == 0 || string(req.ID) == "null" {
		c.mu.Lock()
		h := c.notif[req.Method]
		c.mu.Unlock()
		if h != nil {
			c.dispatchNotif(h, req.Params)
		}
		return
	}

	id := req.ID
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// 连接关闭时放弃正在执行的 handler（ctx 感知的实现自行退出）
		select {
		case <-c.closed:
			return
		default:
		}
		c.mu.Lock()
		h := c.methods[req.Method]
		c.mu.Unlock()

		var resp Response
		resp.JSONRPC = jsonRPCVersion
		resp.ID = id
		if h == nil {
			resp.Error = &RPCError{Code: -32601, Message: "method not found: " + req.Method}
		} else {
			result, err := h(ctx, req.Params)
			if err != nil {
				var rpcErr *RPCError
				if errors.As(err, &rpcErr) {
					resp.Error = rpcErr
				} else {
					resp.Error = &RPCError{Code: -32000, Message: err.Error()}
				}
			} else {
				data, mErr := json.Marshal(result)
				if mErr != nil {
					resp.Error = &RPCError{Code: -32603, Message: "marshal result: " + mErr.Error()}
				} else {
					resp.Result = data
				}
			}
		}
		_ = c.write(resp)
	}()
}

func (c *Conn) deliver(resp *Response) {
	key := string(resp.ID)
	c.mu.Lock()
	ch, ok := c.pending[key]
	if ok {
		delete(c.pending, key)
	}
	c.mu.Unlock()
	if ok {
		ch <- resp
	}
}

// Call 发送请求并等待响应。
func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	key := strconv.FormatInt(id, 10)
	ch := make(chan *Response, 1)
	c.pending[key] = ch
	c.mu.Unlock()

	req := Request{JSONRPC: jsonRPCVersion, Method: method}
	req.ID, _ = json.Marshal(id)
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			c.dropPending(key)
			return fmt.Errorf("marshal %s params: %w", method, err)
		}
		req.Params = data
	}
	if err := c.write(req); err != nil {
		c.dropPending(key)
		return err
	}

	select {
	case resp := <-ch:
		if resp == nil { // closeWith 关闭了 pending channel
			return ErrClosed
		}
		if resp.Error != nil {
			return resp.Error
		}
		if result != nil {
			return json.Unmarshal(resp.Result, result)
		}
		return nil
	case <-ctx.Done():
		c.dropPending(key)
		return ctx.Err()
	case <-c.closed:
		c.dropPending(key)
		return ErrClosed
	}
}

func (c *Conn) dropPending(key string) {
	c.mu.Lock()
	delete(c.pending, key)
	c.mu.Unlock()
}

// Notify 发送 notification（无 ID，无响应）。
func (c *Conn) Notify(method string, params any) error {
	req := Request{JSONRPC: jsonRPCVersion, Method: method}
	if params != nil {
		data, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("marshal %s params: %w", method, err)
		}
		req.Params = data
	}
	return c.write(req)
}

func (c *Conn) write(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	data = append(data, '\n')

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	_, err = c.rwc.Write(data)
	return err
}

// Close 关闭底层连接；所有挂起 Call 立即以 ErrClosed 返回。
func (c *Conn) Close() error {
	c.closeWith(nil)
	return c.rwc.Close()
}

func (c *Conn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *Conn) closeWith(err error) {
	c.once.Do(func() {
		if err != nil {
			c.serveErr = err
		}
		c.mu.Lock()
		for key, ch := range c.pending {
			close(ch) // 让 select 落到 closed 分支前先唤醒
			delete(c.pending, key)
		}
		c.mu.Unlock()
		close(c.closed)
	})
}

// ServeErr 返回读循环退出时的错误（Close 导致的除外）。
func (c *Conn) ServeErr() error {
	return c.serveErr
}
