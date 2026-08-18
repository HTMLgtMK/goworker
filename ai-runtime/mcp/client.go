package mcp

import (
	"context"
	"fmt"
)

// client 是 Client 接口的 stdio 实现。
// 后续接入 HTTP/SSE 传输时新增实现，接口不变。
type client struct {
	transport *StdioTransport
}

// NewStdioClient 启动 command 进程并返回 MCP 客户端。
// 返回的 client 尚未握手，调用方需先调用 Initialize。
// 返回 Client 接口（而非具体类型），后续换 HTTP/SSE 实现时调用方不变。
func NewStdioClient(command string, args []string) (Client, error) {
	tr, err := NewStdioTransport(command, args)
	if err != nil {
		return nil, err
	}
	return &client{transport: tr}, nil
}

// Initialize 执行 MCP 握手，随后发 initialized notification。
func (c *client) Initialize(ctx context.Context, req *InitializeRequest) (*InitializeResult, error) {
	var res InitializeResult
	if err := c.transport.Call(ctx, "initialize", req, &res); err != nil {
		return nil, fmt.Errorf("initialize: %w", err)
	}
	// 规范要求握手后通知 server 客户端已就绪。notification 无响应，best-effort。
	_ = c.transport.Notify(ctx, "notifications/initialized", struct{}{})
	return &res, nil
}

func (c *client) ListTools(ctx context.Context, req *ListToolsRequest) (*ListToolsResult, error) {
	var res ListToolsResult
	if err := c.transport.Call(ctx, "tools/list", req, &res); err != nil {
		return nil, fmt.Errorf("tools/list: %w", err)
	}
	return &res, nil
}

func (c *client) CallTool(ctx context.Context, req *CallToolRequest) (*CallToolResult, error) {
	var res CallToolResult
	if err := c.transport.Call(ctx, "tools/call", req, &res); err != nil {
		return nil, fmt.Errorf("tools/call: %w", err)
	}
	return &res, nil
}

func (c *client) Close() error {
	return c.transport.Close()
}
