package spec

// RenderKind 标记前端渲染类型，由 frontend 根据类型决定样式。
type RenderKind string

const (
	KindText       RenderKind = "text"        // 所有文本输出，一律 markdown 渲染
	KindToolCall   RenderKind = "tool_call"   // 工具调用：● name(args)
	KindToolResult RenderKind = "tool_result" // 工具结果：⎿ output
)
