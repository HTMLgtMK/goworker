package dispatch

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
)

func hexEncode(b []byte) string { return hex.EncodeToString(b) }

// WorkerSpec 描述一个以子进程方式接入的 ACP worker。
type WorkerSpec struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args,omitempty"`
	Env     []string `json:"env,omitempty"` // 追加环境变量 KEY=VALUE
}

// Opener 抽象「连接 worker」这一步，测试注入 net.Pipe 代替真实子进程。
type Opener func(ctx context.Context, spec WorkerSpec) (io.ReadWriteCloser, io.Closer, error)

// ProcessOpener 默认实现：exec 子进程，stdin/stdout 作为 ACP 通道。
func ProcessOpener(ctx context.Context, spec WorkerSpec) (io.ReadWriteCloser, io.Closer, error) {
	cmd := exec.CommandContext(ctx, spec.Command, spec.Args...)
	cmd.Env = append(os.Environ(), spec.Env...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("dispatch: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("dispatch: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("dispatch: start worker %q: %w", spec.Command, err)
	}
	pc := &processConn{r: stdout, w: stdin, cmd: cmd}
	return pc, pc, nil
}

// processConn 聚合子进程管道；Close 等待进程退出。
type processConn struct {
	r   io.ReadCloser
	w   io.WriteCloser
	cmd *exec.Cmd
}

func (p *processConn) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *processConn) Write(b []byte) (int, error) { return p.w.Write(b) }

func (p *processConn) Close() error {
	_ = p.w.Close()
	// stdin 关闭后 worker 正常退出；Wait 回收资源，避免僵尸进程
	return p.cmd.Wait()
}

// ClientCallbacks 驱动任务回合时的回调。
type ClientCallbacks struct {
	// OnUpdate 消费 worker 的 session/update 流（进度/工具调用/plan）。
	OnUpdate func(protocol.SessionUpdate)
	// OnPermission 应答 worker 的 session/request_permission，返回选中的 optionId；
	// 为 nil 时一律拒绝（无人值守默认拒绝，避免 worker 卡死等不到人）。
	OnPermission func(ctx context.Context, req protocol.PermissionRequest) (string, error)
}

// Client 是 ACP Client 角色：连接一个 worker agent，驱动任务到 stop reason。
// 一个 Client 可承载多个 session（多数 worker 支持），任务级隔离由上层负责。
type Client struct {
	name    string
	conn    *protocol.Conn
	closeFn func() error

	cbMu         sync.Mutex
	onUpdate     func(protocol.SessionUpdate)
	onPermission func(context.Context, protocol.PermissionRequest) (string, error)
}

// NewClient 基于现成连接构造（测试或自定义 transport）。
func NewClient(name string, rwc io.ReadWriteCloser, closeFn func() error) *Client {
	c := &Client{name: name, conn: protocol.NewConn(rwc), closeFn: closeFn}
	c.conn.HandleNotification(protocol.MethodSessionUpdate, c.handleUpdate)
	c.conn.Handle(protocol.MethodSessionRequestPermission, c.handlePermission)
	go func() { _ = c.conn.Serve() }()
	return c
}

// StartWorker spawn worker 子进程并建立 ACP 连接。
func StartWorker(ctx context.Context, spec WorkerSpec) (*Client, error) {
	return StartWorkerWithOpener(ctx, spec, ProcessOpener)
}

// StartWorkerWithOpener 允许替换连接建立方式（测试注入 net.Pipe 等）。
func StartWorkerWithOpener(ctx context.Context, spec WorkerSpec, opener Opener) (*Client, error) {
	rwc, closer, err := opener(ctx, spec)
	if err != nil {
		return nil, err
	}
	return NewClient(spec.Name, rwc, closer.Close), nil
}

func (c *Client) SetCallbacks(cb ClientCallbacks) {
	c.cbMu.Lock()
	defer c.cbMu.Unlock()
	c.onUpdate = cb.OnUpdate
	c.onPermission = cb.OnPermission
}

// Initialize 完成协议握手。
func (c *Client) Initialize(ctx context.Context) (protocol.InitializeResponse, error) {
	var resp protocol.InitializeResponse
	req := protocol.InitializeRequest{ProtocolVersion: protocol.Version}
	err := c.conn.Call(ctx, protocol.MethodInitialize, req, &resp)
	return resp, err
}

// NewSession 在 cwd 下开一个 worker 会话。
func (c *Client) NewSession(ctx context.Context, cwd string) (string, error) {
	var resp protocol.NewSessionResponse
	req := protocol.NewSessionRequest{
		Cwd:        cwd,
		McpServers: []map[string]any{},
	}
	if err := c.conn.Call(ctx, protocol.MethodSessionNew, req, &resp); err != nil {
		return "", err
	}
	if resp.SessionID == "" {
		return "", fmt.Errorf("dispatch: worker %q returned empty sessionId", c.name)
	}
	return resp.SessionID, nil
}

// SessionLoad 恢复 worker 上已有的会话（崩溃恢复路径）；worker 须声明
// loadSession 能力。ACP 的 load 响应只含可选 modes（常序列化为 {}）、不回显
// sessionId；Call 成功即视为 worker 接受了该会话，返回请求的 sessionID 作确认。
func (c *Client) SessionLoad(ctx context.Context, sessionID, cwd string) (string, error) {
	req := protocol.LoadSessionRequest{
		SessionID:  sessionID,
		Cwd:        cwd,
		McpServers: []map[string]any{},
	}
	var resp protocol.LoadSessionResponse
	if err := c.conn.Call(ctx, protocol.MethodSessionLoad, req, &resp); err != nil {
		return "", err
	}
	return sessionID, nil
}

// Prompt 提交一回合任务，阻塞到 worker 返回 stop reason。
func (c *Client) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock) (string, error) {
	var resp protocol.PromptResponse
	req := protocol.PromptRequest{SessionID: sessionID, Prompt: prompt}
	if err := c.conn.Call(ctx, protocol.MethodSessionPrompt, req, &resp); err != nil {
		return "", err
	}
	return resp.StopReason, nil
}

// Cancel 中断 worker 正在执行的回合（notification，不等待确认）。
func (c *Client) Cancel(sessionID string) error {
	return c.conn.Notify(protocol.MethodSessionCancel, protocol.CancelNotification{SessionID: sessionID})
}

// Close 断开连接并回收 worker 进程。
func (c *Client) Close() error {
	if c.closeFn != nil {
		return c.closeFn()
	}
	return c.conn.Close()
}

// Name 返回 worker 名。
func (c *Client) Name() string { return c.name }

func (c *Client) handleUpdate(params json.RawMessage) {
	var update protocol.SessionUpdate
	if err := json.Unmarshal(params, &update); err != nil {
		return
	}
	c.cbMu.Lock()
	onUpdate := c.onUpdate
	c.cbMu.Unlock()
	if onUpdate != nil {
		onUpdate(update)
	}
}

func (c *Client) handlePermission(ctx context.Context, params json.RawMessage) (any, error) {
	var req protocol.PermissionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, fmt.Errorf("dispatch: decode permission request: %w", err)
	}
	c.cbMu.Lock()
	onPermission := c.onPermission
	c.cbMu.Unlock()
	if onPermission == nil {
		return nil, fmt.Errorf("dispatch: no permission handler, deny tool call %q", req.ToolCall.Title)
	}
	optionID, err := onPermission(ctx, req)
	if err != nil {
		return nil, err
	}
	return protocol.PermissionResponse{OptionID: optionID}, nil
}
