package spec

// NewContext 创建一个命令执行上下文。
// writer 由前端注入，负责将输出返回给用户。
// readLine 由前端注入，负责交互式输入（nil 表示不支持）。
func NewContext(writer func(string), readLine func() (string, error), args []string) *Context {
	return &Context{
		Writer:   writer,
		ReadLine: readLine,
		Args:     args,
		Values:   make(map[string]any),
	}
}
