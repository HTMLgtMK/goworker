// Package protocol implements the ACP wire subset used by goworker:
// JSON-RPC 2.0 over newline-delimited streams, with one Conn shared by both ends.
package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strconv"
	"sync"
)

var (
	// ErrClosed 连接关闭后的 Call 与写入返回它。
	ErrClosed = errors.New("acp: connection closed")

	// ErrMessageTooLarge 表示单条 JSON-RPC frame 超过连接限制。
	ErrMessageTooLarge = errors.New("acp: message too large")
)

const (
	JSONRPCVersion         = "2.0"
	DefaultMaxMessageBytes = 1 << 20

	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

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

// ConnOptions 配置一条 JSON-RPC 连接。
type ConnOptions struct {
	// MaxMessageBytes 是 JSON payload 的最大字节数，不包含末尾换行符。
	MaxMessageBytes int
}

// Conn 是双向 JSON-RPC 连接，Client/Agent 两端共用。
// Serve 在后台逐行读取并分发。分发语义是有意不对称的：
//
//   - 请求各自在独立 goroutine 中处理 —— 长任务 handler（session/prompt）
//     不得阻塞读循环，否则并发的 session/cancel 通知永远到不了；
//   - 请求 handler 会在连接关闭或读循环终止时收到取消；
//   - 通知也是异步分发（handler 阻塞不得卡死读循环），但串成 FIFO 链：
//     线上顺序即送达顺序，session/update 的流式渲染依赖这一点；
//   - handler 的 panic 被就地兜住：请求回 -32603，通知丢弃并记日志，
//     连接保持存活 —— handler 代码烂了不该拖垮整个进程。
type Conn struct {
	rwc             io.ReadWriteCloser
	maxMessageBytes int

	writeMu   sync.Mutex
	closeOnce sync.Once

	mu      sync.Mutex
	nextID  int64
	pending map[string]chan *Response
	methods map[string]Handler
	notif   map[string]Notifier

	requestSeq     uint64
	requestCancels map[uint64]context.CancelFunc

	// notification FIFO 链：notifLast 是上一条通知的完成信号。
	notifMu   sync.Mutex
	notifLast chan struct{}

	closed   chan struct{}
	once     sync.Once
	serveErr error
}

// NewConn 使用安全默认消息上限创建连接。
func NewConn(rwc io.ReadWriteCloser) *Conn {
	conn, err := NewConnWithOptions(rwc, ConnOptions{MaxMessageBytes: DefaultMaxMessageBytes})
	if err != nil {
		panic(err)
	}
	return conn
}

// NewConnWithOptions 使用显式配置创建连接。
func NewConnWithOptions(rwc io.ReadWriteCloser, options ConnOptions) (*Conn, error) {
	if rwc == nil {
		return nil, errors.New("acp: nil transport")
	}
	if options.MaxMessageBytes <= 0 {
		return nil, fmt.Errorf("acp: max message bytes must be positive: %d", options.MaxMessageBytes)
	}
	return &Conn{
		rwc:             rwc,
		maxMessageBytes: options.MaxMessageBytes,
		pending:         make(map[string]chan *Response),
		methods:         make(map[string]Handler),
		notif:           make(map[string]Notifier),
		requestCancels:  make(map[uint64]context.CancelFunc),
		closed:          make(chan struct{}),
	}, nil
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
// 再跑自己的 handler —— 送达顺序与线上顺序一致。handler panic 在此就地兜住
// （丢弃该通知并记日志），FIFO 链照常推进，连接不中断。
func (c *Conn) dispatchNotif(method string, h Notifier, params json.RawMessage) {
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
		defer func() {
			if r := recover(); r != nil {
				slog.Error("acp: notification handler panic", "method", method, "panic", r, "stack", string(debug.Stack()))
			}
		}()
		h(params)
	}()
}

// Serve 阻塞读循环，直到连接关闭、对端 EOF 或协议错误。
func (c *Conn) Serve() error {
	reader := bufio.NewReader(c.rwc)
	for {
		line, err := c.readFrame(reader)
		if errors.Is(err, ErrMessageTooLarge) {
			c.closeWith(err)
			_ = c.closeTransport()
			return err
		}
		if len(line) > 0 {
			c.dispatch(line)
		}
		if err != nil {
			if c.isClosed() {
				return nil
			}
			c.closeWith(err)
			_ = c.closeTransport()
			return err
		}
	}
}

func (c *Conn) readFrame(reader *bufio.Reader) ([]byte, error) {
	frame := make([]byte, 0, min(c.maxMessageBytes, 4096))
	for {
		part, err := reader.ReadSlice('\n')
		payloadLen := len(part)
		if payloadLen > 0 && part[payloadLen-1] == '\n' {
			payloadLen--
		}
		if len(frame)+payloadLen > c.maxMessageBytes {
			return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrMessageTooLarge, c.maxMessageBytes)
		}
		frame = append(frame, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return frame, err
	}
}

func (c *Conn) dispatch(line []byte) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		c.writeError(nil, CodeParseError, "parse error")
		return
	}
	if fields == nil {
		c.writeError(nil, CodeInvalidRequest, "invalid request")
		return
	}

	id := fields["id"]
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  *string         `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		c.writeError(id, CodeInvalidRequest, "invalid request")
		return
	}
	if envelope.JSONRPC != JSONRPCVersion || !validID(envelope.ID) {
		c.writeError(id, CodeInvalidRequest, "invalid request")
		return
	}

	hasMethod := envelope.Method != nil || hasField(fields, "method")
	hasResult := hasField(fields, "result")
	hasError := hasField(fields, "error")
	if hasMethod {
		if envelope.Method == nil || *envelope.Method == "" || hasResult || hasError {
			c.writeError(id, CodeInvalidRequest, "invalid request")
			return
		}
		c.dispatchRequest(envelope.ID, *envelope.Method, envelope.Params)
		return
	}
	if (!hasResult && !hasError) || (hasResult && hasError) || len(fields["id"]) == 0 {
		c.writeError(id, CodeInvalidRequest, "invalid request")
		return
	}

	var response Response
	if err := json.Unmarshal(line, &response); err != nil || (hasError && response.Error == nil) {
		c.writeError(id, CodeInvalidRequest, "invalid response")
		return
	}
	c.deliver(&response)
}

func (c *Conn) dispatchRequest(id json.RawMessage, method string, params json.RawMessage) {
	ctx, cancel := context.WithCancel(context.Background())
	requestID, ok := c.registerRequest(cancel)
	if !ok {
		cancel()
		return
	}

	go func() {
		defer cancel()
		defer c.unregisterRequest(requestID)
		defer func() {
			if r := recover(); r != nil {
				slog.Error("acp: request handler panic", "method", method, "panic", r, "stack", string(debug.Stack()))
				_ = c.write(Response{
					JSONRPC: JSONRPCVersion,
					ID:      id,
					Error:   &RPCError{Code: CodeInternalError, Message: "internal error: handler panic"},
				})
			}
		}()

		c.mu.Lock()
		h := c.methods[method]
		c.mu.Unlock()

		var response Response
		response.JSONRPC = JSONRPCVersion
		response.ID = id
		if len(id) == 0 || string(id) == "null" {
			c.dispatchNotification(method, params)
			return
		}
		if h == nil {
			response.Error = &RPCError{Code: CodeMethodNotFound, Message: "method not found: " + method}
		} else {
			result, err := h(ctx, params)
			if err != nil {
				var rpcErr *RPCError
				if errors.As(err, &rpcErr) {
					response.Error = rpcErr
				} else {
					response.Error = &RPCError{Code: CodeInternalError, Message: err.Error()}
				}
			} else {
				data, marshalErr := json.Marshal(result)
				if marshalErr != nil {
					response.Error = &RPCError{Code: CodeInternalError, Message: "marshal result: " + marshalErr.Error()}
				} else {
					response.Result = data
				}
			}
		}
		_ = c.write(response)
	}()
}

func (c *Conn) dispatchNotification(method string, params json.RawMessage) {
	c.mu.Lock()
	h := c.notif[method]
	c.mu.Unlock()
	if h != nil {
		c.dispatchNotif(method, h, params)
	}
}

func (c *Conn) registerRequest(cancel context.CancelFunc) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed() {
		return 0, false
	}
	c.requestSeq++
	c.requestCancels[c.requestSeq] = cancel
	return c.requestSeq, true
}

func (c *Conn) unregisterRequest(requestID uint64) {
	c.mu.Lock()
	delete(c.requestCancels, requestID)
	c.mu.Unlock()
}

func validID(id json.RawMessage) bool {
	if len(id) == 0 || string(id) == "null" {
		return true
	}
	var value any
	if err := json.Unmarshal(id, &value); err != nil {
		return false
	}
	switch value.(type) {
	case string, float64:
		return true
	default:
		return false
	}
}

func hasField(fields map[string]json.RawMessage, name string) bool {
	_, ok := fields[name]
	return ok
}

func (c *Conn) writeError(id json.RawMessage, code int, message string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	_ = c.write(Response{
		JSONRPC: JSONRPCVersion,
		ID:      id,
		Error:   &RPCError{Code: code, Message: message},
	})
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

	req := Request{JSONRPC: JSONRPCVersion, Method: method}
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
		if resp == nil {
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
	req := Request{JSONRPC: JSONRPCVersion, Method: method}
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
	if len(data) > c.maxMessageBytes {
		err = fmt.Errorf("%w: payload exceeds %d bytes", ErrMessageTooLarge, c.maxMessageBytes)
		c.closeWith(err)
		_ = c.closeTransport()
		return err
	}
	data = append(data, '\n')

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isClosed() {
		return ErrClosed
	}
	n, err := c.rwc.Write(data)
	if err != nil {
		c.closeWith(err)
		_ = c.closeTransport()
		return err
	}
	if n != len(data) {
		err = io.ErrShortWrite
		c.closeWith(err)
		_ = c.closeTransport()
		return err
	}
	return nil
}

// Close 关闭底层连接；所有挂起 Call 与 request handler 立即收到关闭信号。
func (c *Conn) Close() error {
	c.closeWith(nil)
	return c.closeTransport()
}

func (c *Conn) closeTransport() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.rwc.Close()
	})
	return err
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
		c.mu.Lock()
		if err != nil {
			c.serveErr = err
		}
		c.pending = make(map[string]chan *Response)
		cancels := make([]context.CancelFunc, 0, len(c.requestCancels))
		for requestID, cancel := range c.requestCancels {
			cancels = append(cancels, cancel)
			delete(c.requestCancels, requestID)
		}
		close(c.closed)
		c.mu.Unlock()

		for _, cancel := range cancels {
			cancel()
		}
	})
}

// ServeErr 返回导致连接终止的错误；主动 Close 返回 nil。
func (c *Conn) ServeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.serveErr
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
