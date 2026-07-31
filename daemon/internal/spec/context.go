package spec

import "context"

// NewContext 创建一个命令执行上下文。
// ctx 为请求上下文，从调用链继承用于超时/取消传播。
// writer 由前端注入，负责将输出返回给用户。
// decide 由前端注入，负责 HITL 决策会话（nil 表示不支持交互式确认）。
func NewContext(ctx context.Context, writer func(string), decide func(*InterruptRequest) HITLDecision, args []string) *Context {
	return &Context{
		Ctx:  ctx,
		Args: args,
		FrontendContext: FrontendContext{
			Writer: writer,
			Decide: decide,
		},
		Values: make(map[string]any),
	}
}
