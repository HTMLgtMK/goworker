package stdin

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// testCommands 构造一组固定命令用于补全测试。
func testCommands() []plugin.Command {
	return []plugin.Command{
		{Name: "/agent", Aliases: []string{"/llm", "/ai"}, Description: "与 AI Agent 对话"},
		{Name: "/compact", Description: "压缩对话历史"},
		{Name: "/config", Aliases: []string{"/cfg"}, Description: "查看/修改配置"},
		{Name: "/help", Description: "列出所有命令"},
	}
}

// captureEditorStderr 在 fn 期间重定向 os.Stderr 并返回捕获到的输出
// （handleTab/redrawInput 直写 stderr）。
func captureEditorStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stderr = old
	return <-done
}

// ---- newCompleter：构建与排序 ----

func TestNewCompleter_SortedAndMerged(t *testing.T) {
	c := newCompleter(testCommands())

	// 字典序：/agent < /compact < /config < /help，末尾追加 /quit
	var names []string
	for _, cand := range c.candidates {
		names = append(names, cand.completion)
	}
	want := []string{"/agent", "/compact", "/config", "/help", "/quit"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("candidates = %v, want %v", names, want)
	}

	// 别名归并进 tokens，completion 仍是 canonical
	for _, cand := range c.candidates {
		if cand.completion == "/agent" {
			if len(cand.tokens) != 3 || cand.tokens[1] != "/llm" || cand.tokens[2] != "/ai" {
				t.Errorf("/agent tokens = %v, want canonical + 2 aliases", cand.tokens)
			}
		}
	}
}

// ---- match：前缀匹配（canonical + 别名，大小写不敏感） ----

func TestCompleter_Match(t *testing.T) {
	c := newCompleter(testCommands())
	tests := []struct {
		input string
		want  []string
	}{
		{"/", []string{"/agent", "/compact", "/config", "/help", "/quit"}},
		{"/c", []string{"/compact", "/config"}},
		{"/C", []string{"/compact", "/config"}}, // 大小写不敏感
		{"/ll", []string{"/agent"}},             // 别名命中 → canonical
		{"/CFG", []string{"/config"}},           // 别名大小写不敏感
		{"/q", []string{"/quit"}},               // 静态 /quit
		{"/exit", []string{"/quit"}},            // /quit 别名
		{"/zz", nil},                            // 无匹配
	}
	for _, tt := range tests {
		got := c.match(tt.input)
		var names []string
		for _, cand := range got {
			names = append(names, cand.completion)
		}
		if strings.Join(names, ",") != strings.Join(tt.want, ",") {
			t.Errorf("match(%q) = %v, want %v", tt.input, names, tt.want)
		}
	}
}

// ---- complete：唯一补全 / cycle / 无匹配 / 漂移保护 ----

func TestCompleter_CompleteUnique(t *testing.T) {
	c := newCompleter(testCommands())

	outcome, text, list, idx := c.complete("/hel", true)
	if outcome != tabUnique || text != "/help " || list != nil || idx != 0 {
		t.Fatalf("got (%d, %q, %v, %d), want (tabUnique, \"/help \", nil, 0)", outcome, text, list, idx)
	}
	// 唯一补全不应进入 cycle 态
	if c.cycling {
		t.Error("unique completion should not enter cycling state")
	}
}

func TestCompleter_CompleteCycle(t *testing.T) {
	c := newCompleter(testCommands())

	// 首次 Tab：进入 cycle，落到排序第一个候选
	outcome, text, list, idx := c.complete("/c", true)
	if outcome != tabCycling || text != "/compact" || idx != 0 || len(list) != 2 {
		t.Fatalf("first tab: got (%d, %q, idx=%d, n=%d)", outcome, text, idx, len(list))
	}

	// 前进：→ /config
	_, text, _, idx = c.complete("/compact", true)
	if text != "/config" || idx != 1 {
		t.Fatalf("forward: text=%q idx=%d, want /config 1", text, idx)
	}

	// 前进回绕：→ /compact
	_, text, _, idx = c.complete("/config", true)
	if text != "/compact" || idx != 0 {
		t.Fatalf("wrap forward: text=%q idx=%d, want /compact 0", text, idx)
	}

	// 后退回绕：→ /config
	_, text, _, idx = c.complete("/compact", false)
	if text != "/config" || idx != 1 {
		t.Fatalf("wrap backward: text=%q idx=%d, want /config 1", text, idx)
	}

	// reset 退出 cycle 后重新匹配
	c.reset()
	outcome, _, _, _ = c.complete("/c", true)
	if outcome != tabCycling {
		t.Fatalf("after reset should start a fresh cycle, got %d", outcome)
	}
}

func TestCompleter_CompleteNoMatch(t *testing.T) {
	c := newCompleter(testCommands())

	outcome, text, list, _ := c.complete("/zz", true)
	if outcome != tabNoMatch || text != "/zz" || list != nil {
		t.Fatalf("got (%d, %q, %v), want (tabNoMatch, \"/zz\", nil)", outcome, text, list)
	}
}

func TestCompleter_DriftExitsCycle(t *testing.T) {
	c := newCompleter(testCommands())

	c.complete("/c", true) // 进入 cycle
	// cycle 期间输入被改（用户编辑了但没有触发 reset 的路径兜底）：重新匹配
	outcome, text, _, _ := c.complete("/he", true)
	if outcome != tabUnique || text != "/help " {
		t.Fatalf("drift should re-match fresh: got (%d, %q)", outcome, text)
	}
}

// ---- editor：资格判定 / handleTab 状态迁移 / 候选区生命周期 ----

func TestTabEligible(t *testing.T) {
	ed, _ := testLineEditor()
	tests := []struct {
		name string
		buf  string
		pos  int
		want bool
	}{
		{"slash at end", "/c", 2, true},
		{"bare slash", "/", 1, true},
		{"no slash", "c", 1, false},
		{"cursor mid-word", "/co", 1, false},
		{"trailing space (second word)", "/compact ", 9, false},
		{"space before cursor", "/a b", 4, false},
		{"empty", "", 0, false},
	}
	for _, tt := range tests {
		ed.buf = []rune(tt.buf)
		ed.pos = tt.pos
		if got := ed.tabEligible(); got != tt.want {
			t.Errorf("%s: tabEligible = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestEditor_HandleTabFlow(t *testing.T) {
	ed, _ := testLineEditor()
	ed.SetCompleter(newCompleter(testCommands()))
	ed.termWidth = 80

	// 首次 Tab：补全为第一个候选，候选区展示
	ed.buf = []rune("/c")
	ed.pos = 2
	captureEditorStderr(t, func() { ed.handleTab(true) })
	if string(ed.buf) != "/compact" || ed.pos != len(ed.buf) {
		t.Fatalf("buf = %q, want /compact", string(ed.buf))
	}
	if len(ed.acList) != 2 || ed.acIdx != 0 || ed.popupRows != 2 {
		t.Fatalf("popup: list=%d idx=%d rows=%d, want 2/0/2", len(ed.acList), ed.acIdx, ed.popupRows)
	}

	// 再 Tab：cycle 到下一个
	captureEditorStderr(t, func() { ed.handleTab(true) })
	if string(ed.buf) != "/config" || ed.acIdx != 1 {
		t.Fatalf("buf = %q idx = %d, want /config 1", string(ed.buf), ed.acIdx)
	}

	// 编辑键退出 cycle 并清候选区
	ed.buf = []rune("/config")
	ed.pos = 7
	ed.dropPopup()
	if ed.acList != nil || ed.popupRows != 0 {
		t.Fatalf("dropPopup: list=%v rows=%d, want cleared", ed.acList, ed.popupRows)
	}
}

func TestEditor_TabNoMatchBell(t *testing.T) {
	ed, _ := testLineEditor()
	ed.SetCompleter(newCompleter(testCommands()))

	ed.buf = []rune("/zz")
	ed.pos = 3
	captureEditorStderr(t, func() { ed.handleTab(true) })
	if string(ed.buf) != "/zz" {
		t.Fatalf("buf = %q, want unchanged", string(ed.buf))
	}
	if ed.acList != nil {
		t.Fatalf("popup should stay closed")
	}
}

func TestEditor_TabIneligibleNoop(t *testing.T) {
	ed, _ := testLineEditor()
	ed.SetCompleter(newCompleter(testCommands()))

	// 非补全语境（无 / 前缀）：Tab 无动作
	ed.buf = []rune("hello")
	ed.pos = 5
	captureEditorStderr(t, func() { ed.handleTab(true) })
	if string(ed.buf) != "hello" {
		t.Fatalf("buf = %q, want unchanged", string(ed.buf))
	}
}

func TestEditor_FinishLineClearsPopup(t *testing.T) {
	ed, _ := testLineEditor()
	ed.SetCompleter(newCompleter(testCommands()))

	ed.buf = []rune("/c")
	ed.pos = 2
	captureEditorStderr(t, func() { ed.handleTab(true) })
	if ed.acList == nil {
		t.Fatal("popup should be open before finishLine")
	}
	captureEditorStderr(t, func() { ed.finishLine() })
	if ed.acList != nil || ed.popupRows != 0 {
		t.Fatalf("finishLine: list=%v rows=%d, want cleared", ed.acList, ed.popupRows)
	}
}

// ---- ghostHint：行内灰色提示 ----

func TestGhostHint(t *testing.T) {
	ed, _ := testLineEditor()
	ed.SetCompleter(newCompleter(testCommands()))
	ed.termWidth = 80

	tests := []struct {
		name       string
		buf        string
		pos        int
		wantPlain  string // 期望的纯文本提示；"" = 无提示
		wantSubstr string // styled 中必须包含的片段（校验灰色/内容）
	}{
		{
			name: "partial input shows remainder + desc",
			buf:  "/c", pos: 2,
			wantPlain:  "ompact  压缩对话历史",
			wantSubstr: "\x1b[38;5;244mompact",
		},
		{
			name: "exact match shows desc only",
			buf:  "/compact", pos: 8,
			wantPlain:  "  压缩对话历史",
			wantSubstr: "\x1b[38;5;244m  压缩对话历史",
		},
		{
			name: "no match no hint", buf: "/zz", pos: 3,
			wantPlain: "",
		},
		{
			name: "space breaks first word", buf: "/compact ", pos: 9,
			wantPlain: "",
		},
		{
			name: "cursor not at end", buf: "/co", pos: 1,
			wantPlain: "",
		},
		{
			name: "non slash input", buf: "c", pos: 1,
			wantPlain: "",
		},
	}
	for _, tt := range tests {
		ed.buf = []rune(tt.buf)
		ed.pos = tt.pos
		styled, plain := ed.ghostHint()
		if plain != tt.wantPlain {
			t.Errorf("%s: plain = %q, want %q", tt.name, plain, tt.wantPlain)
		}
		if tt.wantPlain != "" && !strings.Contains(styled, tt.wantSubstr) {
			t.Errorf("%s: styled = %q, want contains %q", tt.name, styled, tt.wantSubstr)
		}
	}

	// 无 completer：静默
	ed2, _ := testLineEditor()
	ed2.buf = []rune("/c")
	ed2.pos = 2
	if s, p := ed2.ghostHint(); s != "" || p != "" {
		t.Errorf("nil completer: got (%q, %q), want empty", s, p)
	}
}

func TestRedrawInput_RendersGhost(t *testing.T) {
	ed, _ := testLineEditor()
	ed.SetCompleter(newCompleter(testCommands()))
	ed.termWidth = 80
	ed.buf = []rune("/c")
	ed.pos = 2

	out := captureEditorStderr(t, func() { ed.redrawInput() })
	// 行内提示：灰色 "ompact" 紧跟已输入文本之后
	if !strings.Contains(out, "\x1b[38;5;244mompact") {
		t.Errorf("redraw output missing ghost hint, got %q", out)
	}
	// 光标回到行尾输入处（提示不改变光标语义）
	if ed.cursorLineIdx != 0 {
		t.Errorf("cursorLineIdx = %d, want 0 (single row input)", ed.cursorLineIdx)
	}
}
