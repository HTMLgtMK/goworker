// Package spec 定义 goworker 引擎的核心类型。
//
// 这是纯协议层——只有接口和结构体，零实现。
// 不依赖任何其他内部包。
package spec

// Plugin 是每个插件必须实现的接口。
//
// 生命周期：
//  1. Init(hub) — 注册命令、建立连接
//  2. Start()   — 启动后台 goroutine
//  3. Stop()    — 优雅关闭资源
type Plugin interface {
	Name() string
	Init(h *Hub) error
	Start() error
	Stop() error
}

// DependentPlugin 是可选接口，声明依赖的其他插件。
// Hub 会在 Init 前校验依赖是否已注册。
type DependentPlugin interface {
	Plugin
	Dependencies() []string
}

// EventAwarePlugin 是可选接口，接收生命周期事件通知。
type EventAwarePlugin interface {
	Plugin
	OnEvent(event Event) error
}

// Hub 是插件注册、事件广播的抽象接口。
// 具体实现由 core.Engine 完成，插件在 Init 时通过 *Hub 注册命令。
type Hub struct {
	RegisterCommand   func(cmd Command) error
	RegisterTool      func(tool Tool) error
	AddEventListener  func(fn func(Event))
	Plugin            func(name string) Plugin
	Plugins           func() []string
	Tools             func() []Tool
	Notify            func(event Event)
	Eval              func(ctx *Context, input string) error
}

// ---- 命令 ----

// Command 是一个命名的可执行操作。
type Command struct {
	Name        string
	Aliases     []string
	Description string
	Handler     func(ctx *Context) error
}

// ---- MCP / AI Tool ----

// Tool 是给 LLM 调用的工具（MCP 风格）。
type Tool struct {
	Name        string
	Description string
	Schema      []byte // JSON Schema
	Handler     func(ctx *Context, params []byte) (any, error)
}

// ---- 事件 ----

type EventType string

const (
	EventPluginStarted EventType = "plugin:started"
	EventPluginStopped EventType = "plugin:stopped"
	EventLogin         EventType = "login"
	EventLogout        EventType = "logout"
	EventQuit          EventType = "quit"
)

type Event struct {
	Type    EventType
	Payload any
}

// ---- 执行上下文 ----

// Context 是命令执行的上下文。
// 由前端创建，经 Engine 传入命令 Handler。
type Context struct {
	Args    []string       // 命令参数，由 Engine.Eval 解析填入
	Writer  func(string)   // 输出回调，由前端注入
	Session Session        // 用户会话，由前端/中间件注入
	Values  map[string]any // 扩展数据，中间件间传递
}

// Session 是只读的用户会话信息。
type Session struct {
	UserID   string
	Username string
	Token    string
}

func (s Session) IsValid() bool {
	return s.UserID != ""
}

// ---- 中间件 ----

// Middleware 是命令执行拦截器。
// 可以读取/修改 Context，或提前返回错误中断执行。
type Middleware func(ctx *Context, next func() error) error

// SessionProvider 根据凭证还原 Session。
type SessionProvider interface {
	Resume(token string) (Session, bool)
}
