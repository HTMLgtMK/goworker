// Package mcp 提供 Model Context Protocol 客户端。
//
// 手写实现，但类型和接口形状对齐 mark3labs/mcp-go 的 protocol 包：
// 换库时只替换内部实现，调用方（agent 集成层）不动。
package mcp

import "context"

// LatestProtocolVersion 是当前 MCP 规范协议版本。
const LatestProtocolVersion = "2025-06-18"

// Client 是 MCP 客户端接口。
// 对齐 protocol.Client 的核心方法；后续按需扩展 Resource/Prompt。
type Client interface {
	// Initialize 握手，协商协议版本与能力。
	Initialize(ctx context.Context, req *InitializeRequest) (*InitializeResult, error)
	// ListTools 列出 server 暴露的工具。
	ListTools(ctx context.Context, req *ListToolsRequest) (*ListToolsResult, error)
	// CallTool 调用一个工具。
	CallTool(ctx context.Context, req *CallToolRequest) (*CallToolResult, error)
	// Close 关闭底层连接并回收进程。
	Close() error
}

// Implementation 描述客户端或服务器的身份。
type Implementation struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ClientCapabilities 客户端能力声明。当前没有需要协商的能力，保留空结构。
type ClientCapabilities struct{}

// InitializeRequest 是 initialize 请求参数。
type InitializeRequest struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ClientCapabilities `json:"capabilities"`
	ClientInfo      Implementation     `json:"clientInfo"`
}

// InitializeResult 是 initialize 响应。
type InitializeResult struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ServerCapabilities `json:"capabilities"`
	ServerInfo      Implementation     `json:"serverInfo"`
	Instructions    string             `json:"instructions,omitempty"`
}

// ServerCapabilities 服务器能力声明。tools 是主要用到的能力。
type ServerCapabilities struct {
	Tools *struct{} `json:"tools,omitempty"`
}

// ListToolsRequest 是 tools/list 请求参数。
type ListToolsRequest struct {
	Cursor string `json:"cursor,omitempty"` // 分页游标，空 = 首页
}

// ListToolsResult 是 tools/list 响应。
type ListToolsResult struct {
	Tools      []Tool `json:"tools"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// CallToolRequest 是 tools/call 请求参数。
type CallToolRequest struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

// CallToolResult 是 tools/call 响应。
type CallToolResult struct {
	Content []ContentBlock `json:"content"`
	IsError bool           `json:"isError,omitempty"`
}

// ContentBlock 是工具返回的内容块。目前只关心 text 类型。
type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// Tool 是 MCP server 暴露的工具定义。
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}
