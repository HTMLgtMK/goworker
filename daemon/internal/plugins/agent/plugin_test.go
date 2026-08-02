package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/config"
	"github.com/tinguo/goworker/daemon/internal/mcp"
	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/daemon/internal/skills"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// testHub 构造一个最小可用的 spec.Hub，只暴露测试需要的字段。
func testHub(cfg *config.Config) (*spec.Hub, *[]*config.Config) {
	var saved []*config.Config
	hub := &spec.Hub{
		Config: cfg,
		SaveConfig: func(c *config.Config) error {
			saved = append(saved, c)
			return nil
		},
		Tools: func() []spec.Tool { return nil }, // collectTools 依赖，缺了会 nil 函数 panic
	}
	return hub, &saved
}

// newAgentPlugin 构造带完整依赖的插件（usage tracker 由 Init 初始化，测试里直接给上）。
func newAgentPlugin(hub *spec.Hub) *AgentPlugin {
	return &AgentPlugin{hub: hub, usage: core.NewUsageTracker()}
}

// newContext 构造带输出捕获的 spec.Context。
func newContext(args ...string) (*spec.Context, *strings.Builder) {
	var buf strings.Builder
	return &spec.Context{
		Ctx:  context.Background(),
		Args: args,
		FrontendContext: spec.FrontendContext{
			Writer: func(s string) { buf.WriteString(s) },
		},
	}, &buf
}

func TestHandleModel_SetCompressionKeys(t *testing.T) {
	cfg := config.Default()
	hub, saved := testHub(cfg)
	p := newAgentPlugin(hub)

	ctx, _ := newContext("set", "compress_at=0.9")
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel: %v", err)
	}
	if cfg.LLM.CompressAt != 0.9 {
		t.Errorf("CompressAt = %v, want 0.9", cfg.LLM.CompressAt)
	}

	ctx, _ = newContext("set", "compact_keep=25")
	if err := p.handleModel(ctx); err != nil {
		t.Fatalf("handleModel: %v", err)
	}
	if cfg.LLM.CompactKeep != 25 {
		t.Errorf("CompactKeep = %d, want 25", cfg.LLM.CompactKeep)
	}

	if len(*saved) != 2 {
		t.Errorf("SaveConfig called %d times, want 2", len(*saved))
	}
}

func TestHandleModel_RejectsBadCompressionKeys(t *testing.T) {
	cfg := config.Default()
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
	// mock OpenAI 兼容端点：压缩调用返回固定摘要
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "COMPACTED"}}},
		})
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.LLM.Endpoint = srv.URL
	cfg.LLM.CompactKeep = 2
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	// 6 条会话历史
	var conv []core.Message
	for i := 0; i < 3; i++ {
		conv = append(conv, core.Message{Role: "user", Content: "q"}, core.Message{Role: "assistant", Content: "a"})
	}
	p.conversation = conv

	ctx, buf := newContext()
	if err := p.handleCompact(ctx); err != nil {
		t.Fatalf("handleCompact: %v", err)
	}

	if len(p.conversation) >= len(conv) {
		t.Fatalf("conversation not shrunk: %d → %d", len(conv), len(p.conversation))
	}
	if p.conversation[0].Role != "system" || p.conversation[0].Content != "COMPACTED" {
		t.Errorf("conversation[0] = %+v, want COMPACTED summary", p.conversation[0])
	}
	if !strings.Contains(buf.String(), "✔ 已压缩") {
		t.Errorf("output = %q, want success message", buf.String())
	}
}

func TestHandleUsage_ShowsCompaction(t *testing.T) {
	// /compact 后 /usage 必须能看出压缩：消耗进账 + 当前历史占用
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(core.ChatResponse{
			Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "COMPACTED"}}},
		})
	}))
	defer srv.Close()

	cfg := config.Default()
	cfg.LLM.Endpoint = srv.URL
	cfg.LLM.CompactKeep = 2
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	var conv []core.Message
	for i := 0; i < 3; i++ {
		conv = append(conv, core.Message{Role: "user", Content: "q"}, core.Message{Role: "assistant", Content: "a"})
	}
	p.conversation = conv

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
	cfg := config.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.conversation = []core.Message{
		{Role: "user", Content: "第一个问题"},
		{Role: "assistant", Content: "第一个回答"},
		{Role: "tool", Content: strings.Repeat("x", 300), ToolCallID: "c1"},
	}

	ctx, buf := newContext()
	if err := p.handleHistory(ctx); err != nil {
		t.Fatalf("handleHistory: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "history: 3 messages") {
		t.Errorf("output missing message count:\n%s", out)
	}
	if !strings.Contains(out, "[user") || !strings.Contains(out, "第一个问题") {
		t.Errorf("output missing user message:\n%s", out)
	}
	if !strings.Contains(out, "…") {
		t.Errorf("output should truncate long content:\n%s", out)
	}
}

func TestDescribeMessage_ToolCallExpandsName(t *testing.T) {
	m := core.Message{
		Role: "assistant",
		ToolCalls: []core.ToolCall{{
			Function: core.ToolCallFunction{Name: "bash", Arguments: `{"command":"ls -la"}`},
		}},
	}
	got := describeMessage(m)
	if !strings.Contains(got, "bash(") || !strings.Contains(got, "ls -la") {
		t.Errorf("describeMessage = %q, want tool call name+args", got)
	}
	if strings.Contains(got, "\n") {
		t.Errorf("describeMessage should be single line, got %q", got)
	}
}

func TestDescribeMessage_FoldsMultiline(t *testing.T) {
	m := core.Message{Role: "tool", Content: "line1\nline2\nline3"}
	got := describeMessage(m)
	if strings.Contains(got, "\n") {
		t.Errorf("multiline not folded: %q", got)
	}
	if !strings.Contains(got, "⏎") {
		t.Errorf("fold marker missing: %q", got)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("truncate(short) = %q", got)
	}
	long := "中文内容特别长，用来验证不会切半字符"
	got := truncate(long, 6)
	if len([]rune(got)) != 7 { // 6 rune + …
		t.Errorf("truncate got %d runes, want 7: %q", len([]rune(got)), got)
	}
	if got := truncate("x", 0); got != "x" {
		t.Errorf("truncate n=0 should not truncate: %q", got)
	}
}

func TestHandleCompact_EmptyConversation(t *testing.T) {
	cfg := config.Default()
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
	cfg := config.Default()
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
	cfg := config.Default()
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
	cfg := config.Default()
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
	pkgDir := filepath.Join(wd, "..", "..", "mcp", "testdata", "fakeserver")
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
	cfg := config.Default()
	cfg.MCP.Servers = []config.MCPServer{{Name: "fake", Command: serverPath}}
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
	cfg := config.Default()
	// 不存在的命令：loadMCP 应降级跳过，不 panic 不阻塞
	cfg.MCP.Servers = []config.MCPServer{{Name: "ghost", Command: "/nonexistent/cmd"}}
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)
	p.mcpClients = make(map[string]mcp.Client)

	p.loadMCP()

	if len(p.mcpClients) != 0 || len(p.mcpTools) != 0 {
		t.Errorf("ghost server should be skipped, clients=%d tools=%d", len(p.mcpClients), len(p.mcpTools))
	}
}

func TestMCPToolToCore_ExecuteCallsServer(t *testing.T) {
	cfg := config.Default()
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
	cfg := config.Default()
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
	cfg := config.Default()
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
	cfg := config.Default()
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
	cfg := config.Default()
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
	cfg := config.Default()
	cfg.MCP.Servers = []config.MCPServer{{Name: "filesystem", Command: "npx", Args: []string{"-y", "x"}}}
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
