package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/mcp"
	runtimeopenai "github.com/tinguo/goworker/ai-runtime/provider/openai"
	"github.com/tinguo/goworker/ai-runtime/skills"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// testHub 构造一个最小可用的 plugin.Hub，只暴露测试需要的字段。
func testHub(cfg *runtimeconfig.Config) (*plugin.Hub, *[]*runtimeconfig.Config) {
	var saved []*runtimeconfig.Config
	hub := &plugin.Hub{
		Config: cfg,
		SaveConfig: func(c any) error {
			rc, ok := c.(*runtimeconfig.Config)
			if !ok {
				return errors.New("unexpected config type")
			}
			next := *rc
			next.LLM = rc.LLM.Clone()
			*cfg = next
			saved = append(saved, rc)
			return nil
		},
		Tools: func() []plugin.Tool { return nil }, // collectTools 依赖，缺了会 nil 函数 panic
	}
	return hub, &saved
}

// newAgentPlugin 构造带完整依赖的插件：NewProvider 走当前默认的 OpenAI provider。
// 需要固定 provider 的测试用 newAgentPluginP。
func newAgentPlugin(hub *plugin.Hub) *AgentPlugin {
	cfg, _ := hub.Config.(*runtimeconfig.Config)
	p := &AgentPlugin{hub: hub, cfg: cfg}
	// 与 Init 编排一致：资源确认后组装 deps + 创建会话
	p.deps = runtimeagent.SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       p.memory,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(cfg *runtimeconfig.Config) (core.Provider, error) {
			_, provider, err := cfg.LLM.ResolveDefault()
			if err != nil {
				return nil, err
			}
			return runtimeopenai.NewProvider("mock", provider.Endpoint, provider.APIKey, provider.Model, &http.Client{}), nil
		},
	}
	p.refreshSession()
	return p
}

// newAgentPluginP 注入固定 provider：深播种测试不依赖真实 LLM 端点。
func newAgentPluginP(hub *plugin.Hub, pv core.Provider) *AgentPlugin {
	cfg, _ := hub.Config.(*runtimeconfig.Config)
	p := &AgentPlugin{hub: hub, cfg: cfg}
	p.deps = runtimeagent.SessionDeps{
		Config:       cfg,
		AuditDir:     "",
		Memory:       p.memory,
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
	}
	p.refreshSession()
	return p
}

// fixedProvider 每次调用返回固定 assistant 内容，用于播种会话/压缩。
type fixedProvider struct{ content string }

func (p fixedProvider) Name() string  { return "fixed" }
func (p fixedProvider) Model() string { return "m" }
func (p fixedProvider) Chat(context.Context, *core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: p.content}}}}, nil
}
func (p fixedProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// seedRuns 走公开 Run API 播种会话：每轮追加 user input + assistant 回复。
// daemon 拿不到 Session 内部字段，这是唯一正当的填充路径。
func seedRuns(p *AgentPlugin, inputs ...string) {
	var out strings.Builder
	for _, in := range inputs {
		_ = p.session.Run(context.Background(), runtimeagent.RunRequest{Input: in}, runtimeagent.RunCallbacks{
			Write: func(s string) { out.WriteString(s) },
		})
	}
}

// setMemory 设置插件 memory 并重建会话，模拟 Init 的编排顺序（资源就绪后创建会话）。
// SessionDeps 是值拷贝：直接改 p.memory 不会传导到已创建会话的 deps，必须重建。
func (p *AgentPlugin) setMemory(c *memory.Client) {
	p.memory = c
	p.deps.Memory = c
	p.refreshSession()
}

// refreshSession 用当前 deps 重建会话，对齐 startSession 的会话边界构造
// （测试不读真实文件 —— deps 由测试直接注入）。
func (p *AgentPlugin) refreshSession() {
	p.session = runtimeagent.NewSession(p.deps)
}

// newContext 构造带输出捕获的 plugin.Context。
// WriteToken 与 Writer 都写进同一 buffer —— Session.Run 的流式输出测试需要它。
func newContext(args ...string) (*plugin.Context, *strings.Builder) {
	var buf strings.Builder
	return &plugin.Context{
		Ctx:  context.Background(),
		Args: args,
		FrontendContext: plugin.FrontendContext{
			Writer:     func(s string) { buf.WriteString(s) },
			WriteToken: func(_ plugin.RenderKind, s string) { buf.WriteString(s) },
		},
	}, &buf
}

func TestHandleModel_SetProviderAndGlobalKeys(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, saved := testHub(cfg)
	p := newAgentPlugin(hub)

	for _, setting := range []string{
		"global.compress_at=0.9",
		"global.compact_keep=25",
		"global.thinking_show=false",
		"thinking_request_mode=reasoning_effort",
		"thinking_effort=high",
	} {
		ctx, buf := newContext("set", setting)
		if err := p.handleModel(ctx); err != nil {
			t.Fatalf("handleModel(%s): %v", setting, err)
		}
		if !strings.Contains(buf.String(), "✔") {
			t.Errorf("handleModel(%s) output = %q, want success", setting, buf.String())
		}
	}
	provider := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
	if cfg.LLM.CompressAt != 0.9 || cfg.LLM.CompactKeep != 25 || cfg.LLM.Thinking.Show {
		t.Errorf("global LLM config = %+v", cfg.LLM)
	}
	if provider.Thinking.RequestMode != runtimeconfig.ThinkingRequestEffort {
		t.Errorf("RequestMode = %q, want reasoning_effort", provider.Thinking.RequestMode)
	}
	if provider.Thinking.Effort != runtimeconfig.ThinkingEffortHigh {
		t.Errorf("Effort = %q, want high", provider.Thinking.Effort)
	}
	if len(*saved) != 5 {
		t.Errorf("SaveConfig called %d times, want 5", len(*saved))
	}
}

func TestHandleModel_ListUseAndFailedSaveRollback(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.LLM.Providers["cc-switch"] = runtimeconfig.ProviderConfig{
		Type:          runtimeconfig.ProviderTypeAnthropic,
		Endpoint:      "http://127.0.0.1:15721",
		Model:         "claude-sonnet-4-6",
		APIKey:        "PROXY_MANAGED",
		AuthType:      runtimeconfig.AnthropicAuthBearer,
		MaxTokens:     8192,
		ContextWindow: 1048576,
	}
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	ctx, list := newContext("list")
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel(list): %v", err)
	}
	if !strings.Contains(list.String(), "* openai") || !strings.Contains(list.String(), "cc-switch") || strings.Contains(list.String(), "PROXY_MANAGED") {
		t.Errorf("list output = %q", list.String())
	}

	ctx, out := newContext("use", "cc-switch")
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel(use): %v", err)
	}
	if cfg.LLM.DefaultProvider != "cc-switch" || !strings.Contains(out.String(), "已切换") {
		t.Errorf("use output = %q, default = %q", out.String(), cfg.LLM.DefaultProvider)
	}

	originalModel := cfg.LLM.Providers["cc-switch"].Model
	p.hub.SaveConfig = func(any) error { return errors.New("disk full") }
	ctx, out = newContext("set", "model=changed")
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel(set): %v", err)
	}
	if cfg.LLM.Providers["cc-switch"].Model != originalModel || !strings.Contains(out.String(), "保存失败") {
		t.Errorf("failed save mutated live config or missed error: %q", out.String())
	}
}

func TestHandleModel_ThinkingHelpStatusAndValidation(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	ctx, help := newContext("help")
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel(help): %v", err)
	}
	for _, key := range []string{"thinking_show", "thinking_request_mode", "thinking_effort"} {
		if !strings.Contains(help.String(), key) {
			t.Errorf("help missing %q:\n%s", key, help.String())
		}
	}

	ctx, status := newContext()
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel(status): %v", err)
	}
	for _, value := range []string{"Thinking Show:", "Thinking Mode:", "Thinking Effort:"} {
		if !strings.Contains(status.String(), value) {
			t.Errorf("status missing %q:\n%s", value, status.String())
		}
	}

	for _, setting := range []string{
		"global.thinking_show=sometimes",
		"thinking_request_mode=guess",
		"thinking_effort=extreme",
	} {
		beforeGlobal := cfg.LLM.Thinking
		beforeProvider := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
		ctx, out := newContext("set", setting)
		if err := p.handleModel(ctx); err != nil {
			t.Fatalf("handleModel(%s): %v", setting, err)
		}
		if !strings.Contains(out.String(), "✘") {
			t.Errorf("handleModel(%s) output = %q, want rejection", setting, out.String())
		}
		if cfg.LLM.Thinking != beforeGlobal || cfg.LLM.Providers[cfg.LLM.DefaultProvider] != beforeProvider {
			t.Errorf("handleModel(%s) mutated config after rejection", setting)
		}
	}
}

func TestHandleModel_RejectsBadCompressionKeys(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	for _, bad := range []string{"compress_at=1.5", "compress_at=abc", "compact_keep=0", "compact_keep=-2"} {
		ctx, buf := newContext("set", bad)
		if err := p.handleModel(ctx); err != nil {
			t.Fatalf("handleModel(%s): %v", bad, err)
		}
		if !strings.Contains(buf.String(), "✘") {
			t.Errorf("handleModel(%s) should reject, output = %q", bad, buf.String())
		}
	}
}

func TestHandleCompact_CompressesConversation(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.LLM.CompactKeep = 2
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "COMPACTED"})

	// 播种 3 轮 user/assistant 对（走公开 Run API）
	seedRuns(p, "q", "q", "q")
	if n := len(p.session.Conversation()); n != 6 {
		t.Fatalf("seeded conversation = %d msgs, want 6", n)
	}

	ctx, buf := newContext()
	if err := p.handleCompact(ctx); err != nil {
		t.Fatalf("handleCompact: %v", err)
	}

	conv := p.session.Conversation()
	if len(conv) >= 6 {
		t.Fatalf("conversation not shrunk: %d → %d", 6, len(conv))
	}
	if conv[0].Role != "system" {
		t.Errorf("conversation[0] = %+v, want system summary", conv[0])
	}
	if !strings.Contains(buf.String(), "✔ 已压缩") {
		t.Errorf("output = %q, want success message", buf.String())
	}
}

func TestHandleUsage_ShowsCompaction(t *testing.T) {
	// /compact 后 /usage 必须能看出压缩：消耗进账 + 当前历史占用
	cfg := runtimeconfig.Default()
	cfg.LLM.CompactKeep = 2
	hub, _ := testHub(cfg)
	p := newAgentPluginP(hub, fixedProvider{content: "COMPACTED"})

	seedRuns(p, "q", "q", "q")

	compactCtx, _ := newContext()
	if err := p.handleCompact(compactCtx); err != nil {
		t.Fatalf("handleCompact: %v", err)
	}

	ctx, buf := newContext()
	if err := p.handleUsage(ctx); err != nil {
		t.Fatalf("handleUsage: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "compact: 1 time") {
		t.Errorf("usage output missing compact record:\n%s", out)
	}
	if !strings.Contains(out, "history:") {
		t.Errorf("usage output missing current history estimate:\n%s", out)
	}
}

func TestHandleHistory_ShowsMessages(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	// 长回复用于验证截断渲染
	p := newAgentPluginP(hub, fixedProvider{content: strings.Repeat("x", 300)})

	seedRuns(p, "第一个问题")

	ctx, buf := newContext()
	if err := p.handleHistory(ctx); err != nil {
		t.Fatalf("handleHistory: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "history: 2 messages") {
		t.Errorf("output missing message count:\n%s", out)
	}
	if !strings.Contains(out, "[user") || !strings.Contains(out, "第一个问题") {
		t.Errorf("output missing user message:\n%s", out)
	}
	if !strings.Contains(out, "…") {
		t.Errorf("output should Truncate long content:\n%s", out)
	}
}

func TestHandleCompact_EmptyConversation(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	ctx, buf := newContext()
	if err := p.handleCompact(ctx); err != nil {
		t.Fatalf("handleCompact: %v", err)
	}
	if !strings.Contains(buf.String(), "no conversation history yet") {
		t.Errorf("output = %q, want empty-history hint", buf.String())
	}
}

func TestCollectTools_IncludesSkills(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.skills = []skills.Skill{{Name: "gofmt", Description: "格式化 Go 代码", Content: "用 gofmt 清理代码。"}}

	tools := p.collectTools(nil)
	var found *core.Tool
	for i := range tools {
		if tools[i].Name == "skill_gofmt" {
			found = &tools[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("skill tool not registered, got: %+v", tools)
	}
	if !strings.Contains(found.Description, "格式化 Go 代码") {
		t.Errorf("description should carry skill purpose: %q", found.Description)
	}
	out, err := found.Execute(context.Background(), map[string]any{})
	if err != nil || out != "用 gofmt 清理代码。" {
		t.Errorf("Execute = %q, %v", out, err)
	}
}

func TestHandleSkills_ListsSkills(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.skills = []skills.Skill{
		{Name: "gofmt", Description: "格式化 Go 代码", Content: "..."},
		{Name: "db", Description: "数据库操作", Content: "..."},
	}

	ctx, buf := newContext()
	if err := p.handleSkills(ctx); err != nil {
		t.Fatalf("handleSkills: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "skills: 2 loaded") {
		t.Errorf("missing count: %q", out)
	}
	if !strings.Contains(out, "gofmt") || !strings.Contains(out, "格式化 Go 代码") {
		t.Errorf("missing skill detail: %q", out)
	}
}

func TestHandleSkills_Empty(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	ctx, buf := newContext()
	if err := p.handleSkills(ctx); err != nil {
		t.Fatalf("handleSkills: %v", err)
	}
	if !strings.Contains(buf.String(), "no skills loaded") {
		t.Errorf("output = %q, want empty hint", buf.String())
	}
}

// ---- MCP ----

// fakeMCPClient 是 mcp.Client 的内存替身，测转换逻辑不拉起真实进程。
type fakeMCPClient struct {
	callRes *mcp.CallToolResult
	callErr error
}

func (f *fakeMCPClient) Initialize(context.Context, *mcp.InitializeRequest) (*mcp.InitializeResult, error) {
	return &mcp.InitializeResult{ProtocolVersion: mcp.LatestProtocolVersion}, nil
}
func (f *fakeMCPClient) ListTools(context.Context, *mcp.ListToolsRequest) (*mcp.ListToolsResult, error) {
	return &mcp.ListToolsResult{}, nil
}
func (f *fakeMCPClient) CallTool(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return f.callRes, f.callErr
}
func (f *fakeMCPClient) Close() error { return nil }

// buildFakeMCPServer 编译 internal/mcp/testdata 的假 MCP server，返回可执行路径。
func buildFakeMCPServer(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// daemon/internal/agent → 仓库根（3 级上溯）→ ai-runtime/mcp/testdata/fakeserver
	pkgDir := filepath.Join(wd, "..", "..", "..", "ai-runtime", "mcp", "testdata", "fakeserver")
	out := filepath.Join(t.TempDir(), "fakeserver")
	cmd := exec.Command("go", "build", "-o", out, pkgDir)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build fakeserver: %v", err)
	}
	return out
}

func TestLoadMCP_ConnectsFakeServer(t *testing.T) {
	serverPath := buildFakeMCPServer(t)
	cfg := runtimeconfig.Default()
	cfg.MCP.Servers = []runtimeconfig.MCPServer{{Name: "fake", Command: serverPath}}
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.mcpClients = make(map[string]mcp.Client)

	p.loadMCP()

	if len(p.mcpClients) != 1 {
		t.Fatalf("clients = %d, want 1", len(p.mcpClients))
	}
	if len(p.mcpTools) != 1 {
		t.Fatalf("tools = %+v, want 1", p.mcpTools)
	}
	tool := p.mcpTools[0]
	if tool.Name != "mcp_fake_echo" {
		t.Errorf("tool name = %q, want mcp_fake_echo", tool.Name)
	}
	out, err := tool.Execute(context.Background(), map[string]any{"text": "hi"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(out, "hi") {
		t.Errorf("Execute output = %q, want contains hi", out)
	}
	p.Stop() // 回收子进程
}

func TestLoadMCP_UnreachableServerSkips(t *testing.T) {
	cfg := runtimeconfig.Default()
	// 不存在的命令：loadMCP 应降级跳过，不 panic 不阻塞
	cfg.MCP.Servers = []runtimeconfig.MCPServer{{Name: "ghost", Command: "/nonexistent/cmd"}}
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.mcpClients = make(map[string]mcp.Client)

	p.loadMCP()

	if len(p.mcpClients) != 0 || len(p.mcpTools) != 0 {
		t.Errorf("ghost server should be skipped, clients=%d tools=%d", len(p.mcpClients), len(p.mcpTools))
	}
}

func TestMCPToolToCore_ExecuteCallsServer(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	c := &fakeMCPClient{callRes: &mcp.CallToolResult{Content: []mcp.ContentBlock{{Type: "text", Text: "result!"}}}}

	tool, ok := p.mcpToolToCore(c, "db", mcp.Tool{Name: "query", Description: "run sql", InputSchema: map[string]any{"type": "object"}})
	if !ok {
		t.Fatal("conversion failed")
	}
	if tool.Name != "mcp_db_query" {
		t.Errorf("name = %q, want mcp_db_query", tool.Name)
	}
	out, err := tool.Execute(context.Background(), map[string]any{"sql": "select 1"})
	if err != nil || out != "result!" {
		t.Errorf("Execute = %q, %v", out, err)
	}
}

func TestMCPToolToCore_LongNameSkipped(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	c := &fakeMCPClient{}

	// name 超长 → 工具名 > 64 会被 OpenAI 拒绝，这里应跳过
	_, ok := p.mcpToolToCore(c, "server", mcp.Tool{Name: strings.Repeat("x", 60)})
	if ok {
		t.Fatal("long tool name should be skipped")
	}
}

func TestMCPToolToCore_InvalidNameSkipped(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	c := &fakeMCPClient{}

	// 工具名带空格 → 合成名含非法字符，OpenAI 会 400 整个 tools 数组，必须跳过
	if _, ok := p.mcpToolToCore(c, "server", mcp.Tool{Name: "my tool"}); ok {
		t.Fatal("tool name with space should be skipped")
	}
	// server 名带点同样拦截
	if _, ok := p.mcpToolToCore(c, "my.server", mcp.Tool{Name: "read"}); ok {
		t.Fatal("server name with dot should be skipped")
	}
}

func TestMCPToolToCore_EmptySchemaNormalized(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	c := &fakeMCPClient{callRes: &mcp.CallToolResult{}}

	tool, ok := p.mcpToolToCore(c, "db", mcp.Tool{Name: "query"}) // InputSchema 为空
	if !ok {
		t.Fatal("conversion failed")
	}
	if tool.Parameters["type"] != "object" {
		t.Errorf("empty schema should be normalized to object, got %+v", tool.Parameters)
	}
}

func TestHandleMCP_NoServers(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	ctx, buf := newContext()
	if err := p.handleMCP(ctx); err != nil {
		t.Fatalf("handleMCP: %v", err)
	}
	if !strings.Contains(buf.String(), "no MCP servers configured") {
		t.Errorf("output = %q, want no-server hint", buf.String())
	}
}

func TestHandleMCP_ShowsServers(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.MCP.Servers = []runtimeconfig.MCPServer{{Name: "filesystem", Command: "npx", Args: []string{"-y", "x"}}}
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.mcpClients = make(map[string]mcp.Client)
	p.mcpTools = []core.Tool{{Name: "mcp_filesystem_read", Description: "x"}}

	ctx, buf := newContext()
	if err := p.handleMCP(ctx); err != nil {
		t.Fatalf("handleMCP: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "mcp: 1 configured") {
		t.Errorf("missing count: %q", out)
	}
	if !strings.Contains(out, "filesystem") || !strings.Contains(out, "❌") {
		t.Errorf("missing server/status: %q", out)
	}
	if !strings.Contains(out, "tools: 1 loaded") {
		t.Errorf("missing tool count: %q", out)
	}
}

// newMemoryPlugin 构造带真实 memory Client 的插件，供 /memory 命令测试。
func newMemoryPlugin(t *testing.T) *AgentPlugin {
	t.Helper()
	cfg := runtimeconfig.Default()
	// 测试必须关会话持久化：handleNew 会走真实 startSession，若 Enabled 默认 true
	// 会 Open 用户真实 sessions 目录并恢复 ActiveView，污染数据 + 断言错乱。
	cfg.Session.Enabled = false
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	c, err := memory.NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	p.setMemory(c)
	return p
}

func TestHandleMemory_AddListSearch(t *testing.T) {
	p := newMemoryPlugin(t)

	ctx, _ := newContext("add", "记住：编译命令是 go build ./...")
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("add: %v", err)
	}

	ctx, buf := newContext("list")
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("list: %v", err)
	}
	if !strings.Contains(buf.String(), "go build ./...") {
		t.Errorf("list output missing fact: %q", buf.String())
	}

	ctx, buf = newContext("search", "build")
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("search: %v", err)
	}
	if !strings.Contains(buf.String(), "go build ./...") {
		t.Errorf("search output missing match: %q", buf.String())
	}

	ctx, buf = newContext("search", "kafka")
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("search no-match: %v", err)
	}
	if !strings.Contains(buf.String(), "no matching") {
		t.Errorf("search no-match output: %q", buf.String())
	}
}

func TestHandleMemory_Forget(t *testing.T) {
	p := newMemoryPlugin(t)
	p.memory.AddFact(&memory.Fact{Content: "manual fact", Source: "user"})
	facts, _ := p.memory.ListFacts(10)
	id := facts[0].ID

	ctx, buf := newContext("forget", id)
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if !strings.Contains(buf.String(), "✔ forgot 1") {
		t.Errorf("forget output: %q", buf.String())
	}
	left, _ := p.memory.ListFacts(10)
	if len(left) != 0 {
		t.Errorf("fact survived forget: %+v", left)
	}
}

func TestHandleMemory_DisabledShowsHint(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub) // p.memory 为 nil

	ctx, buf := newContext("list")
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("handleMemory: %v", err)
	}
	if !strings.Contains(buf.String(), "未启用") {
		t.Errorf("output = %q, want disabled hint", buf.String())
	}
}

func TestHandleMemory_UnknownSubcommand(t *testing.T) {
	p := newMemoryPlugin(t)
	ctx, buf := newContext("bogus")
	if err := p.handleMemory(ctx); err != nil {
		t.Fatalf("handleMemory: %v", err)
	}
	if !strings.Contains(buf.String(), "未知子命令") {
		t.Errorf("output = %q, want unknown-subcommand hint", buf.String())
	}
}

// lockedBuf 是线程安全的字符串收集器：/new 的后台固化 goroutine 会并发写 writer，
// strings.Builder 直接写会有 data race，测试统一走它。
type lockedBuf struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuf) Write(s string) {
	b.mu.Lock()
	b.buf.WriteString(s)
	b.mu.Unlock()
}

func (b *lockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitFor 轮询 b.String() 直到包含 want 或超时。
func (b *lockedBuf) waitFor(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !strings.Contains(b.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %q, output = %q", want, b.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandleNew_EmptyConversationSkipsCheckpoint(t *testing.T) {
	p := newMemoryPlugin(t) // conversation 为空，不启动后台固化，不碰 LLM

	ctx, buf := newContext()
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	if len(p.session.Conversation()) != 0 {
		t.Errorf("conversation not cleared: %d", len(p.session.Conversation()))
	}
	if !strings.Contains(buf.String(), "New session started") {
		t.Errorf("output = %q", buf.String())
	}
}

func TestHandleNew_CheckpointsConversation(t *testing.T) {
	// mock LLM 固化：返回一条 task + 一条 fact
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: `{
				"tasks":[{"title":"fix config","summary_delta":"moved to yaml"}],
				"decisions":[{"action":"add","content":"project uses yaml config","topic":"config"}]}`}}},
		})
	}))
	defer srv.Close()

	cfg := runtimeconfig.Default()
	provider := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
	provider.Endpoint = srv.URL
	cfg.LLM.Providers[cfg.LLM.DefaultProvider] = provider
	cfg.Session.Enabled = false // 避免 handleNew 走真实 store 路径，污染用户会话目录
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	ms, _ := memory.NewClient(t.TempDir(), 10)
	p.setMemory(ms)
	seedRuns(p, "q")

	// 固化是后台 goroutine 执行的，writer 必须线程安全
	var out lockedBuf
	ctx := &plugin.Context{
		Ctx:  context.Background(),
		Args: nil,
		FrontendContext: plugin.FrontendContext{
			Writer: out.Write,
		},
	}
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}

	// STM 立即清空（同步路径，不等后台）
	if len(p.session.Conversation()) != 0 {
		t.Errorf("conversation not cleared: %d", len(p.session.Conversation()))
	}
	if !strings.Contains(out.String(), "New session started") {
		t.Errorf("output = %q", out.String())
	}

	// 等后台固化完成：提示回显 + 落库
	out.waitFor(t, "Memory updated", 3*time.Second)
	tasks, _ := ms.ListTasks(10)
	if len(tasks) != 1 || tasks[0].Title != "fix config" {
		t.Errorf("task not checkpointed: %+v", tasks)
	}
	facts, _ := ms.ListFacts(10)
	if len(facts) != 1 || facts[0].Topic != "config" {
		t.Errorf("fact not extracted: %+v", facts)
	}
}

func TestHandleTask_StartListEnd(t *testing.T) {
	p := newMemoryPlugin(t)

	ctx, buf := newContext("start", "write", "docs")
	if err := p.handleTask(ctx); err != nil {
		t.Fatalf("task start: %v", err)
	}
	if !strings.Contains(buf.String(), "已创建 task") {
		t.Errorf("start output: %q", buf.String())
	}

	ctx, buf = newContext("list")
	if err := p.handleTask(ctx); err != nil {
		t.Fatalf("task list: %v", err)
	}
	if !strings.Contains(buf.String(), "write docs") {
		t.Errorf("list output missing task: %q", buf.String())
	}

	ctx, buf = newContext("end")
	if err := p.handleTask(ctx); err != nil {
		t.Fatalf("task end: %v", err)
	}
	if !strings.Contains(buf.String(), "✔ 已关闭") {
		t.Errorf("end output: %q", buf.String())
	}

	ctx, buf = newContext("list")
	if err := p.handleTask(ctx); err != nil {
		t.Fatalf("task list after end: %v", err)
	}
	if !strings.Contains(buf.String(), "no open tasks") {
		t.Errorf("list after end should be empty: %q", buf.String())
	}
}

func TestHandleTask_DisabledShowsHint(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub) // p.memory 为 nil

	ctx, buf := newContext("list")
	if err := p.handleTask(ctx); err != nil {
		t.Fatalf("handleTask: %v", err)
	}
	if !strings.Contains(buf.String(), "未启用") {
		t.Errorf("output = %q, want disabled hint", buf.String())
	}
}

func TestInit_OpensMemory(t *testing.T) {
	cfg := runtimeconfig.Default()
	cfg.Memory.Dir = t.TempDir()
	hub := &plugin.Hub{
		Config:             cfg,
		RegisterCommand:    func(plugin.Command) error { return nil },
		SetFallbackHandler: func(func(*plugin.Context) error) {},
		Tools:              func() []plugin.Tool { return nil },
	}
	p := &AgentPlugin{cfg: cfg}
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if p.memory == nil {
		t.Fatal("memory should be opened")
	}
}

func TestCollectTools_RegistersMemorySearch(t *testing.T) {
	p := newMemoryPlugin(t)
	// 历史事件存在 closed task 里 —— memory_search 必须能捞出来
	p.memory.UpsertTask(&memory.Task{Title: "查询长沙天气", Status: "closed", Summary: "昨天查的"})

	cfg := p.sandboxConfig()
	tools := p.collectTools(&cfg)
	var ms *core.Tool
	for i := range tools {
		if tools[i].Name == "memory_search" {
			ms = &tools[i]
		}
	}
	if ms == nil {
		t.Fatal("memory_search tool not registered when memory enabled")
	}

	out, err := ms.Execute(context.Background(), map[string]any{"query": "长沙"})
	if err != nil {
		t.Fatalf("execute memory_search: %v", err)
	}
	if !strings.Contains(out, "查询长沙天气") {
		t.Errorf("memory_search output missing closed task: %q", out)
	}
	if !strings.Contains(out, "closed") {
		t.Errorf("memory_search output missing task status: %q", out)
	}

	// memory disabled：工具不注册
	hub2, _ := testHub(runtimeconfig.Default())
	p2 := newAgentPlugin(hub2)
	p2.memory = nil
	cfg2 := p2.sandboxConfig()
	for _, tl := range p2.collectTools(&cfg2) {
		if tl.Name == "memory_search" {
			t.Fatal("memory_search should not register when memory disabled")
		}
	}
}

func TestHandleNew_LtmExtractDisabledSkipsFacts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: `{
				"tasks":[{"title":"fix config","summary_delta":"moved to yaml"}],
				"decisions":[{"action":"add","content":"should not land","topic":"config"}]}`}}},
		})
	}))
	defer srv.Close()

	cfg := runtimeconfig.Default()
	provider := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
	provider.Endpoint = srv.URL
	cfg.LLM.Providers[cfg.LLM.DefaultProvider] = provider
	cfg.Memory.LtmExtract = false
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	ms, _ := memory.NewClient(t.TempDir(), 10)
	p.setMemory(ms)
	seedRuns(p, "q")

	var out lockedBuf
	ctx := &plugin.Context{
		Ctx:  context.Background(),
		Args: nil,
		FrontendContext: plugin.FrontendContext{
			Writer: out.Write,
		},
	}
	if err := p.handleNew(ctx); err != nil {
		t.Fatalf("handleNew: %v", err)
	}
	out.waitFor(t, "Memory updated", 3*time.Second)
	tasks, _ := ms.ListTasks(10)
	if len(tasks) != 1 {
		t.Errorf("tasks = %d, want 1 (task extraction stays on)", len(tasks))
	}
	facts, _ := ms.ListFacts(10)
	if len(facts) != 0 {
		t.Errorf("facts = %d, want 0 when ltm_extract=false", len(facts))
	}
}
