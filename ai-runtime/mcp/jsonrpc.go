package mcp

import (
	"encoding/json"
	"fmt"
)

// rpcMessage 是 JSON-RPC 2.0 的请求/响应统一外壳。
// 请求带 Method，响应带 Result/Error，两者共用 ID 配对。
type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// rpcError 是 JSON-RPC 2.0 错误对象。
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message)
}

// encodeRequest 序列化一个带 id 的请求。
func encodeRequest(id int, method string, params any) ([]byte, error) {
	var paramsRaw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("encode params: %w", err)
		}
		paramsRaw = b
	}
	msg := rpcMessage{
		JSONRPC: "2.0",
		ID:      json.RawMessage(fmt.Sprintf("%d", id)),
		Method:  method,
		Params:  paramsRaw,
	}
	return json.Marshal(msg)
}

// encodeNotification 序列化一个无 id 的 notification（fire-and-forget）。
func encodeNotification(method string, params any) ([]byte, error) {
	var paramsRaw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("encode notification params: %w", err)
		}
		paramsRaw = b
	}
	msg := rpcMessage{
		JSONRPC: "2.0",
		Method:  method,
		Params:  paramsRaw,
	}
	return json.Marshal(msg)
}
