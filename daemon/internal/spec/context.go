package spec

import "context"

// NewContext 创建一个命令执行上下文。
// ctx 为请求上下文，从调用链继承用于超时/取消传播。
// writer 由前端注入，负责将输出返回给用户。
// readLine 由前端注入，负责交互式输入（nil 表示不支持）。
func NewContext(ctx context.Context, writer func(string), readLine func() (string, error), args []string) *Context {
	return &Context{
		Ctx:      ctx,
		Writer:   writer,
		ReadLine: readLine,
		Args:     args,
		Values:   make(map[string]any),
	}
}
