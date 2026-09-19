package task

// dispatcher 的事件契约：daemon/internal/dispatcher 发布（经 Engine 事件总线），
// 前端 statusbar addon 订阅渲染。契约跟类型走，与本包 Task/Status 同源。
//
// 发布语义：
//   - EventTaskStatus 低频必达（状态迁移时各发一条）
//   - EventTaskProgress 高频可丢（addon 端 channel 缓冲 + drop 兜底）
const (
	EventTaskStatus   = "task_status"
	EventTaskProgress = "task_progress"
)

// StatusEvent 是 EventTaskStatus 的载荷：一次状态迁移。
type StatusEvent struct {
	TaskID string
	From   Status
	To     Status
	Kind   Kind
	Worker string
}

// ProgressEvent 是 EventTaskProgress 的载荷：worker 执行进度的文本摘要。
type ProgressEvent struct {
	TaskID  string
	Kind    string // session/update 子类型（agent_message_chunk/tool_call/…）
	Summary string // 文本摘要（Content.Text 截断），结构化更新为空
}

// Summarize 从 session/update 子类型产出进度摘要：非文本更新用 kind 描述。
func Summarize(updateKind, text string, maxLen int) string {
	if text == "" {
		return updateKind
	}
	r := []rune(text)
	if len(r) > maxLen {
		return string(r[:maxLen]) + "…"
	}
	return string(r)
}
