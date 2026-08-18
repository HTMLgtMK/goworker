package core

// ToolSpec 返回 OpenAI 兼容的工具定义。
func (t Tool) ToolSpec() map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"parameters":  t.Parameters,
		},
	}
}
