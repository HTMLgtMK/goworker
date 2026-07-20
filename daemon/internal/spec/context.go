package spec

// NewContext 创建一个命令执行上下文。
// writer 由前端注入，负责将输出返回给用户。
func NewContext(writer func(string), args []string) *Context {
	return &Context{
		Writer: writer,
		Args:   args,
		Values: make(map[string]any),
	}
}
