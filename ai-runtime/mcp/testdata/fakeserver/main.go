// 假 MCP server：在 stdio 上跑 newline-delimited JSON-RPC 2.0。
// 仅供 internal/mcp 客户端测试使用，非生产代码。
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			continue
		}
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": handle(req)}
		b, err := json.Marshal(resp)
		if err != nil {
			continue
		}
		fmt.Println(string(b))
	}
}

func handle(req rpcRequest) any {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "fakeserver", "version": "1.0"},
		}
	case "tools/list":
		return map[string]any{
			"tools": []any{
				map[string]any{
					"name":        "echo",
					"description": "echo arguments back",
					"inputSchema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"text": map[string]any{"type": "string"},
						},
					},
				},
			},
		}
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		json.Unmarshal(req.Params, &params)
		text := fmt.Sprintf("called %s with %v", params.Name, params.Arguments)
		return map[string]any{
			"content": []any{map[string]any{"type": "text", "text": text}},
		}
	default:
		return map[string]any{"error": map[string]any{"code": -32601, "message": "method not found"}}
	}
}
