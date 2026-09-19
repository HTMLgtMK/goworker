package vscode

import (
	"context"
	"strings"
	"testing"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// fakeReporter 收集 MessageChunk 发出的文本片段（顺序即发出顺序）。
type fakeReporter struct {
	chunks []string
}

func (r *fakeReporter) Update(string, protocol.SessionUpdateBody) {}
func (r *fakeReporter) MessageChunk(_, text string)               { r.chunks = append(r.chunks, text) }

// RequestPermission 本文件不触发 HITL，空实现仅为满足接口。
func (r *fakeReporter) RequestPermission(context.Context, string, protocol.PermissionRequest) (string, error) {
	return "", nil
}

// joined 把全部片段拼回一条流：webview 侧就是这么累积的。
func (r *fakeReporter) joined() string { return strings.Join(r.chunks, "") }

func newCommandOutput() (*commandOutput, *fakeReporter) {
	rep := &fakeReporter{}
	return &commandOutput{rep: rep, sessionID: "sess"}, rep
}

// 命令输出必须被围栏包住：这是「让 webview 用等宽渲染」的唯一手段。
func TestCommandOutput_WrapsInFence(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("  #1   in 1.2k  out 340\n")
	w.Close()

	got := rep.joined()
	if !strings.HasPrefix(got, commandFence+"\n") {
		t.Errorf("输出未以围栏开头: %q", got)
	}
	if !strings.HasSuffix(got, commandFence+"\n") {
		t.Errorf("输出未以围栏结尾: %q", got)
	}
	if !strings.Contains(got, "  #1   in 1.2k  out 340\n") {
		t.Errorf("原始排版被改动（列对齐必须原样保留）: %q", got)
	}
}

// 没有输出时不发任何东西：命令失败前就返回时不该留一个空代码块。
func TestCommandOutput_EmptyWritesNothing(t *testing.T) {
	w, rep := newCommandOutput()

	w.Close()

	if len(rep.chunks) != 0 {
		t.Errorf("chunks = %q, want none", rep.chunks)
	}
}

// 纯空白不构成输出：不因一个孤立的换行就开出一个空代码块。
//
// 这不是假想情况 —— agent 路径（/agent 与裸输入走同一个 fallback handler）每轮
// 结束都会无条件 cb.Write("\n")，不拦的话每次对话后都会多出一个空代码块。
func TestCommandOutput_WhitespaceOnlyDoesNotOpenFence(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("\n")
	w.Close()

	if len(rep.chunks) != 0 {
		t.Errorf("chunks = %q, want none（纯空白不应开启围栏）", rep.chunks)
	}
}

// 围栏开启后，空白是内容的一部分，必须原样透传（表格里的空行不能吞）。
func TestCommandOutput_WhitespaceInsideFenceIsKept(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("line\n")
	w.Write("\n")
	w.Write("after gap\n")
	w.Close()

	if got := rep.joined(); !strings.Contains(got, "line\n\nafter gap\n") {
		t.Errorf("围栏内空行被吞: %q", got)
	}
}

// 流式可见性：首次写入后立即就能看到合法（尚未闭合）的代码块。
// CommonMark 规定未闭合围栏延伸到文档末尾，中途渲染不会漏出裸文本。
func TestCommandOutput_StreamsBeforeClose(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("line one\n")

	streamed := rep.joined()
	if !strings.HasPrefix(streamed, commandFence+"\n") {
		t.Fatalf("首块就应带上开启围栏: %q", streamed)
	}
	if strings.Count(streamed, commandFence) != 1 {
		t.Errorf("闭合前不应有闭合围栏: %q", streamed)
	}
}

// 内容不以换行结尾时，Close 要补一个，否则闭合围栏会跟最后一行粘在一起。
func TestCommandOutput_CloseAddsMissingNewline(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("no trailing newline")
	w.Close()

	got := rep.joined()
	if !strings.Contains(got, "no trailing newline\n"+commandFence) {
		t.Errorf("闭合前未补换行: %q", got)
	}
}

// 已经有换行时不应多补一个（避免块尾多出空行）。
func TestCommandOutput_CloseDoesNotDoubleNewline(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("line\n")
	w.Close()

	got := rep.joined()
	if strings.Contains(got, "\n\n"+commandFence) {
		t.Errorf("闭合前多补了换行: %q", got)
	}
}

// 命令输出会回显用户内容（/history、/rules），其中出现 ``` 完全可能 ——
// 4 反引号围栏必须让它原样保留、不提前闭合。
func TestCommandOutput_InnerTripleBackticksStayInside(t *testing.T) {
	w, rep := newCommandOutput()

	w.Write("```\ncode sample\n```\n")
	w.Close()

	got := rep.joined()
	// 内容里的 ``` 不能被当成闭合围栏：全文只应有开/闭两个 commandFence。
	if count := strings.Count(got, commandFence); count != 2 {
		t.Errorf("commandFence 出现 %d 次, want 2（内容里的 ``` 不应闭合）: %q", count, got)
	}
	if !strings.Contains(got, "```\ncode sample\n```") {
		t.Errorf("内容里的三反引号被破坏: %q", got)
	}
}

// 端到端：ingress.Run 让命令的 Writer 输出变成一条带围栏的 message chunk。
func TestIngress_RunWrapsCommandOutputInFence(t *testing.T) {
	rep := &fakeReporter{}
	h := &ingress{sessions: map[string]struct{}{"sess": {}}}
	var captured *model.Context
	h.evaluate = func(ctx *model.Context, _ string) error {
		captured = ctx
		ctx.Writer("  key:  value\n")
		ctx.Writer("  key2: value2\n")
		return nil
	}

	if _, err := h.Run(context.Background(), "sess", "/whatever", rep); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if captured == nil {
		t.Fatal("evaluate 未被调用")
	}

	got := rep.joined()
	want := commandFence + "\n  key:  value\n  key2: value2\n" + commandFence + "\n"
	if got != want {
		t.Errorf("输出 = %q\nwant %q", got, want)
	}
}

// 命令返回错误时，已经流出的内容仍要闭合围栏再返回错误，
// 否则 webview 会留一个未闭合的代码块。
func TestIngress_RunClosesFenceBeforeError(t *testing.T) {
	rep := &fakeReporter{}
	h := &ingress{sessions: map[string]struct{}{"sess": {}}}
	h.evaluate = func(ctx *model.Context, _ string) error {
		ctx.Writer("✘ 出错了\n")
		return context.Canceled
	}

	_, err := h.Run(context.Background(), "sess", "/whatever", rep)
	if err == nil {
		t.Fatal("应返回错误")
	}

	got := rep.joined()
	if !strings.HasSuffix(got, commandFence+"\n") {
		t.Errorf("返回错误前未闭合围栏: %q", got)
	}
}

// agent 路径（/agent 与裸输入走同一个 fallback handler）每轮结束会无条件写一个
// "\n"：不能因此冒出一个空代码块。这是最常走的路径，回归代价最高。
func TestIngress_RunIgnoresAgentTrailingNewline(t *testing.T) {
	rep := &fakeReporter{}
	h := &ingress{sessions: map[string]struct{}{"sess": {}}}
	h.evaluate = func(ctx *model.Context, _ string) error {
		// 模拟一轮 agent：正文走 EmitToken，收尾 cb.Write("\n")（session.go）。
		ctx.EmitToken(core.Token{Type: core.TokenTypeText, Content: "hi"})
		ctx.Writer("\n")
		return nil
	}

	if _, err := h.Run(context.Background(), "sess", "hi", rep); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(rep.chunks) != 0 {
		t.Errorf("chunks = %q, want none（agent 的收尾换行不该开代码块）", rep.chunks)
	}
}

// 命令没有任何输出（如未知子命令前的早退）：不该发出空代码块。
func TestIngress_RunWithNoOutputEmitsNothing(t *testing.T) {
	rep := &fakeReporter{}
	h := &ingress{sessions: map[string]struct{}{"sess": {}}}
	h.evaluate = func(*model.Context, string) error { return nil }

	if _, err := h.Run(context.Background(), "sess", "/whatever", rep); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(rep.chunks) != 0 {
		t.Errorf("chunks = %q, want none（无输出不留空块）", rep.chunks)
	}
}
