package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// sessionFixture 用真实 Store 生成 current + archive 数据：归档 archives 条
// （每条一条 user 消息），current 留一条 user 消息且 head 停在其上。
// 返回插件、store 与会话目录。
func sessionFixture(t *testing.T, archives int) (*AgentPlugin, *session.Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	for i := 0; i < archives; i++ {
		msg := core.Message{Role: "user", Content: "历史会话标题\n第二行", MsgID: session.NewMsgID()}
		if _, err := st.Commit([]core.Message{msg}); err != nil {
			t.Fatalf("commit archive %d: %v", i, err)
		}
		// 拉开归档文件 mtime，让倒序排序可断言
		time.Sleep(5 * time.Millisecond)
		if err := st.Archive(); err != nil {
			t.Fatalf("archive %d: %v", i, err)
		}
	}
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "当前会话", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit current: %v", err)
	}

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir}}
	return withStore(cfg, st), st, dir
}

// archiveIDs 返回归档目录下的 sessionId（文件名去 .jsonl），按文件名排序。
func archiveIDs(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "archive"))
	if err != nil {
		t.Fatalf("read archive dir: %v", err)
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, strings.TrimSuffix(entry.Name(), ".jsonl"))
	}
	return ids
}

func TestAgentPlugin_ListSessionsPinsCurrentAndOrdersArchivesByUpdatedAt(t *testing.T) {
	p, st, dir := sessionFixture(t, 2)
	headBefore := st.Head()

	got := p.ListSessions()
	if len(got) != 3 {
		t.Fatalf("ListSessions = %+v, want current + 2 archived", got)
	}
	if got[0].SessionID != headBefore {
		t.Errorf("sessions[0].sessionId = %q, want current head %q", got[0].SessionID, headBefore)
	}
	if got[0].Title != "当前会话" {
		t.Errorf("sessions[0].title = %q, want 当前会话", got[0].Title)
	}
	if got[1].Title != "历史会话标题 第二行" {
		t.Errorf("sessions[1].title = %q, want newline-collapsed 历史会话标题 第二行", got[1].Title)
	}
	// 归档按 mtime 倒序：后归档的文件名（UnixNano）更大，应排在前面
	ids := archiveIDs(t, dir)
	if len(ids) != 2 {
		t.Fatalf("archive ids = %v, want 2", ids)
	}
	if got[1].SessionID != ids[1] || got[2].SessionID != ids[0] {
		t.Errorf("archived sessionIds = [%q %q], want newest-first [%q %q]", got[1].SessionID, got[2].SessionID, ids[1], ids[0])
	}
	for i, info := range got {
		if info.Cwd != "" {
			t.Errorf("sessions[%d].cwd = %q, want empty (jsonl 不落 cwd)", i, info.Cwd)
		}
		if _, err := time.Parse(time.RFC3339, info.UpdatedAt); err != nil {
			t.Errorf("sessions[%d].updatedAt = %q, want RFC3339: %v", i, info.UpdatedAt, err)
		}
	}

	// 清单是只读扫描：不得改动 store 状态
	if after := st.Head(); after != headBefore {
		t.Errorf("store head changed by ListSessions: %q → %q", headBefore, after)
	}
	if view := st.ActiveView(); len(view) != 1 {
		t.Errorf("active view = %d messages after ListSessions, want 1", len(view))
	}
}

func TestAgentPlugin_ListSessionsKeepsCurrentFirstEvenWhenArchiveIsNewer(t *testing.T) {
	p, st, dir := sessionFixture(t, 1)

	// 把归档文件 mtime 拨到未来：当前会话仍须置顶（不参与 updatedAt 排序）
	archived := filepath.Join(dir, "archive", archiveIDs(t, dir)[0]+".jsonl")
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(archived, future, future); err != nil {
		t.Fatalf("chtimes archive: %v", err)
	}

	got := p.ListSessions()
	if len(got) != 2 {
		t.Fatalf("ListSessions = %+v, want current + 1 archived", got)
	}
	if got[0].SessionID != st.Head() {
		t.Errorf("sessions[0].sessionId = %q, want current head %q pinned first", got[0].SessionID, st.Head())
	}
}

func TestAgentPlugin_ListSessionsWithoutCurrentOrPersistence(t *testing.T) {
	// 空会话（无 head）：current 不出现，返回非 nil 空清单
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := withStore(&runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir}}, st)
	if got := p.ListSessions(); got == nil || len(got) != 0 {
		t.Errorf("ListSessions = %#v, want non-nil empty", got)
	}
	if got := p.CurrentSessionID(); got != "" {
		t.Errorf("CurrentSessionID = %q, want empty for headless session", got)
	}

	// store 未启用（nil）：清单为空、当前会话 id 为空
	disabled := &AgentPlugin{cfg: &runtimeconfig.Config{}}
	if got := disabled.ListSessions(); got == nil || len(got) != 0 {
		t.Errorf("disabled ListSessions = %#v, want non-nil empty", got)
	}
	if got := disabled.CurrentSessionID(); got != "" {
		t.Errorf("disabled CurrentSessionID = %q, want empty", got)
	}
}

func TestAgentPlugin_ReloadCurrentSessionRefreshesView(t *testing.T) {
	p, st, _ := sessionFixture(t, 0)
	p.session.Store(runtimeagent.NewSession(runtimeagent.SessionDeps{Config: p.cfg, Store: st}))

	before := len(p.session.Load().Conversation())
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "追加一条", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := p.ReloadCurrentSession(); err != nil {
		t.Fatalf("ReloadCurrentSession: %v", err)
	}
	if after := len(p.session.Load().Conversation()); after != before+1 {
		t.Errorf("conversation length = %d after reload, want %d", after, before+1)
	}

	empty := &AgentPlugin{}
	if err := empty.ReloadCurrentSession(); err == nil || !strings.Contains(err.Error(), "no active session") {
		t.Errorf("ReloadCurrentSession without session = %v, want no active session", err)
	}
}

// historyFixture 用真实 Store 构建多轮会话（每轮提交后打 checkpoint，模拟
// ai-runtime/agent 的 Run 提交流程）：
//
//	轮1（ckpt）：user → assistant(2 个 tool_calls) → tool(c1) → tool(c2) → assistant 收尾
//	轮2（ckpt）：user → assistant（空正文无 tool_calls）
//	轮3（ckpt）：user → assistant 正文
//	尾段（checkpoint 漏打）：user → 孤立 tool（配不到 tool_call）→ system（compact 摘要）
//
// 返回插件与 store。视图共 12 条消息、3 个 checkpoint。
func historyFixture(t *testing.T) (*AgentPlugin, *session.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	commit := func(msgs ...core.Message) {
		t.Helper()
		if _, err := st.Commit(msgs); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	checkpoint := func() {
		t.Helper()
		if err := st.Checkpoint("preview"); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
	}

	// 轮 1：多工具轮（连续两个 tool_calls + 对应 tool 消息）
	commit(
		core.Message{Role: "user", Content: "第一轮：帮我看看", MsgID: session.NewMsgID()},
		core.Message{
			Role: "assistant",
			ToolCalls: []core.ToolCall{
				{ID: "call_1", Type: "function", Function: core.ToolCallFunction{Name: "read_file", Arguments: `{"path":"a.go"}`}},
				{ID: "call_2", Type: "function", Function: core.ToolCallFunction{Name: "grep", Arguments: `not-json`}},
			},
			MsgID: session.NewMsgID(),
		},
		core.Message{Role: "tool", Content: "文件内容A", ToolCallID: "call_1", MsgID: session.NewMsgID()},
		core.Message{Role: "tool", Content: "匹配结果B", ToolCallID: "call_2", MsgID: session.NewMsgID()},
		core.Message{Role: "assistant", Content: "第一轮结论", MsgID: session.NewMsgID()},
	)
	checkpoint()

	// 轮 2：空 assistant（空正文且无 tool_calls，重放时跳过）
	commit(
		core.Message{Role: "user", Content: "第二轮：空的助手", MsgID: session.NewMsgID()},
		core.Message{Role: "assistant", Content: "", MsgID: session.NewMsgID()},
	)
	checkpoint()

	// 轮 3：纯对话轮
	commit(
		core.Message{Role: "user", Content: "第三轮", MsgID: session.NewMsgID()},
		core.Message{Role: "assistant", Content: "第三轮回答", MsgID: session.NewMsgID()},
	)
	checkpoint()

	// 尾段：checkpoint 漏打；孤立 tool 消息（call_orphan 无配对 tool_call）+
	// system（compact 摘要，重放时跳过）
	commit(
		core.Message{Role: "user", Content: "第四轮（漏打）", MsgID: session.NewMsgID()},
		core.Message{Role: "tool", Content: "孤立输出", ToolCallID: "call_orphan", MsgID: session.NewMsgID()},
		core.Message{Role: "system", Content: "compact 摘要", MsgID: session.NewMsgID()},
	)

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir}}
	return withStore(cfg, st), st
}

// rawBodyFields 解码 Raw 透传体为字段表，便于逐字段断言 wire 形状。
func rawBodyFields(t *testing.T, body protocol.SessionUpdateBody) map[string]any {
	t.Helper()
	if len(body.Raw) == 0 {
		t.Fatalf("body.Raw 为空，期望透传载荷：%+v", body)
	}
	var fields map[string]any
	if err := json.Unmarshal(body.Raw, &fields); err != nil {
		t.Fatalf("unmarshal raw body %s: %v", body.Raw, err)
	}
	return fields
}

func TestAgentPlugin_CurrentSessionHistoryReplaysRoundsWithToolPairing(t *testing.T) {
	p, st := historyFixture(t)
	headBefore := st.Head()
	viewBefore := len(st.ActiveView())
	cksBefore := len(st.Checkpoints(0))

	got := p.CurrentSessionHistory()
	if got == nil {
		t.Fatal("CurrentSessionHistory = nil, want non-nil")
	}
	// 11 条通知：user×4 + tool_call×2 + tool_call_update×3 + agent×2；
	// 空 assistant 与 system 跳过。
	if len(got) != 11 {
		t.Fatalf("CurrentSessionHistory = %d bodies, want 11", len(got))
	}

	wantKinds := []string{
		protocol.UpdateUserMessageChunk,  // 第一轮 user
		protocol.UpdateToolCall,          // call_1
		protocol.UpdateToolCall,          // call_2
		protocol.UpdateToolCallUpdate,    // call_1 结果
		protocol.UpdateToolCallUpdate,    // call_2 结果
		protocol.UpdateAgentMessageChunk, // 第一轮结论
		protocol.UpdateUserMessageChunk,  // 第二轮 user（空 assistant 跳过）
		protocol.UpdateUserMessageChunk,  // 第三轮 user
		protocol.UpdateAgentMessageChunk, // 第三轮回答
		protocol.UpdateUserMessageChunk,  // 第四轮 user（漏打尾巴段）
		protocol.UpdateToolCallUpdate,    // 孤立 call_orphan 降级
	}
	// Raw 透传体的判别值在载荷内部（同 availableCommandsUpdateBody），typed 字段为空
	bodyKind := func(body protocol.SessionUpdateBody) string {
		if len(body.Raw) > 0 {
			kind, _ := rawBodyFields(t, body)["sessionUpdate"].(string)
			return kind
		}
		return body.SessionUpdate
	}
	for i, kind := range wantKinds {
		if gotKind := bodyKind(got[i]); gotKind != kind {
			t.Errorf("history[%d].sessionUpdate = %q, want %q", i, gotKind, kind)
		}
	}

	// user/assistant 全文保留（全文一条，不截断）
	wantChunks := map[int]string{
		0: "第一轮：帮我看看",
		5: "第一轮结论",
		6: "第二轮：空的助手",
		7: "第三轮",
		8: "第三轮回答",
		9: "第四轮（漏打）",
	}
	for i, text := range wantChunks {
		if got[i].Content == nil || got[i].Content.Type != "text" || got[i].Content.Text != text {
			t.Errorf("history[%d] content = %+v, want text %q", i, got[i].Content, text)
		}
	}

	// tool_call：title=name(args)（与 live 路径同构）、status=in_progress、rawInput 合法 JSON 内嵌为对象
	call1 := rawBodyFields(t, got[1])
	if call1["toolCallId"] != "call_1" || call1["title"] != "read_file({\"path\":\"a.go\"})" || call1["status"] != "in_progress" {
		t.Errorf("history[1] = %v, want call_1/read_file(args)/in_progress", call1)
	}
	if _, has := call1["kind"]; has {
		t.Errorf("history[1] carries kind %v, want none (kind is semantic, folding is length-driven)", call1["kind"])
	}
	input1, ok := call1["rawInput"].(map[string]any)
	if !ok || input1["path"] != "a.go" {
		t.Errorf("history[1].rawInput = %v, want object {path: a.go}", call1["rawInput"])
	}
	// 非法 JSON 参数退化为字符串透传
	call2 := rawBodyFields(t, got[2])
	if call2["toolCallId"] != "call_2" || call2["title"] != "grep(not-json)" || call2["status"] != "in_progress" {
		t.Errorf("history[2] = %v, want call_2/grep(args)/in_progress", call2)
	}
	if call2["rawInput"] != "not-json" {
		t.Errorf("history[2].rawInput = %v, want string not-json", call2["rawInput"])
	}

	// tool_call_update：同 id 配对、status=completed、输出文本走 rawOutput
	for i, want := range []struct{ id, output string }{
		{"call_1", "文件内容A"},
		{"call_2", "匹配结果B"},
	} {
		fields := rawBodyFields(t, got[3+i])
		if fields["sessionUpdate"] != protocol.UpdateToolCallUpdate ||
			fields["toolCallId"] != want.id || fields["status"] != "completed" || fields["rawOutput"] != want.output {
			t.Errorf("history[%d] = %v, want tool_call_update %s completed rawOutput %q", i+3, fields, want.id, want.output)
		}
	}

	// 孤立 tool 消息降级：completed 的 tool_call_update，id 用消息自带 ToolCallID
	orphan := rawBodyFields(t, got[10])
	if orphan["sessionUpdate"] != protocol.UpdateToolCallUpdate ||
		orphan["toolCallId"] != "call_orphan" || orphan["status"] != "completed" || orphan["rawOutput"] != "孤立输出" {
		t.Errorf("history[10] = %v, want degraded update for call_orphan", orphan)
	}

	// 只读导出：不得改动 store 状态（head / 视图 / checkpoint 数均不变）
	if headAfter := st.Head(); headAfter != headBefore {
		t.Errorf("store head changed by export: %q → %q", headBefore, headAfter)
	}
	if viewAfter := len(st.ActiveView()); viewAfter != viewBefore || viewBefore != 12 {
		t.Errorf("active view = %d messages after export, want %d", viewAfter, viewBefore)
	}
	if cksAfter := len(st.Checkpoints(0)); cksAfter != cksBefore || cksBefore != 3 {
		t.Errorf("checkpoints = %d after export, want %d", cksAfter, cksBefore)
	}
}

func TestAgentPlugin_CurrentSessionHistoryRoundBoundariesMatchCheckpoints(t *testing.T) {
	_, st := historyFixture(t)

	view := st.ActiveView()
	rounds := segmentByCheckpoints(view, checkpointAnchors(st))
	// 3 个 checkpoint 各收一轮（5/2/2 条），漏打 checkpoint 的尾巴段单独成轮（3 条）
	wantSizes := []int{5, 2, 2, 3}
	if len(rounds) != len(wantSizes) {
		t.Fatalf("rounds = %d, want %d", len(rounds), len(wantSizes))
	}
	for i, size := range wantSizes {
		if len(rounds[i]) != size {
			t.Errorf("rounds[%d] = %d messages, want %d", i, len(rounds[i]), size)
		}
	}
	// 前 3 轮的最后一条消息 id 与对应 checkpoint 的 at 一致（Checkpoints 倒序：最新在前）
	cks := st.Checkpoints(0)
	for i, round := range rounds[:3] {
		if last := round[len(round)-1].MsgID; last != cks[2-i].At {
			t.Errorf("rounds[%d] ends at %q, want checkpoint at %q", i, last, cks[2-i].At)
		}
	}
	// 每轮都以 user 开头（fixture 数据规整；段落开头非 user 的场景在
	// TestSegmentByCheckpoints 白盒覆盖）
	for i, round := range rounds {
		if round[0].Role != "user" {
			t.Errorf("rounds[%d] starts with %q, want user", i, round[0].Role)
		}
	}
}

func TestSegmentByCheckpoints(t *testing.T) {
	view := []core.Message{
		{Role: "user", MsgID: "a"},
		{Role: "assistant", MsgID: "b"},
		{Role: "user", MsgID: "c"},
		{Role: "assistant", MsgID: "d"},
		{Role: "user", MsgID: "e"},
	}
	roundIDs := func(rounds [][]core.Message) [][]string {
		out := make([][]string, 0, len(rounds))
		for _, round := range rounds {
			ids := make([]string, 0, len(round))
			for _, m := range round {
				ids = append(ids, m.MsgID)
			}
			out = append(out, ids)
		}
		return out
	}
	want := func(ids ...[]string) [][]string { return ids }

	cases := []struct {
		name    string
		anchors map[string]bool
		want    [][]string
	}{
		{
			name:    "锚点命中 b/d 切三轮",
			anchors: map[string]bool{"b": true, "d": true},
			want:    want([]string{"a", "b"}, []string{"c", "d"}, []string{"e"}),
		},
		{
			name:    "视图外锚点（rewind/compact 失效）跳过",
			anchors: map[string]bool{"b": true, "d": true, "ghost": true},
			want:    want([]string{"a", "b"}, []string{"c", "d"}, []string{"e"}),
		},
		{
			name:    "无锚点整个视图一段",
			anchors: nil,
			want:    want([]string{"a", "b", "c", "d", "e"}),
		},
		{
			name:    "锚点在最后一条无空尾巴段",
			anchors: map[string]bool{"e": true},
			want:    want([]string{"a", "b", "c", "d", "e"}),
		},
		{
			name:    "尾巴段漏打锚点单独成段",
			anchors: map[string]bool{"b": true},
			want:    want([]string{"a", "b"}, []string{"c", "d", "e"}),
		},
		{
			name:    "段中锚点致段首非 user，如实成段重放时顺延前段",
			anchors: map[string]bool{"a": true},
			want:    want([]string{"a"}, []string{"b", "c", "d", "e"}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := roundIDs(segmentByCheckpoints(view, tc.anchors))
			if len(got) != len(tc.want) {
				t.Fatalf("rounds = %v, want %v", got, tc.want)
			}
			for i := range got {
				if len(got[i]) != len(tc.want[i]) {
					t.Fatalf("rounds = %v, want %v", got, tc.want)
				}
				for j := range got[i] {
					if got[i][j] != tc.want[i][j] {
						t.Fatalf("rounds = %v, want %v", got, tc.want)
					}
				}
			}
		})
	}

	// 无 MsgID 的消息不参与锚点匹配
	noID := []core.Message{{Role: "user"}, {Role: "assistant", MsgID: "b"}}
	if rounds := segmentByCheckpoints(noID, map[string]bool{"b": true}); len(rounds) != 1 || len(rounds[0]) != 2 {
		t.Errorf("rounds with id-less message = %v, want single 2-message round", rounds)
	}
}

func TestAgentPlugin_CurrentSessionHistoryEmpty(t *testing.T) {
	// 空会话（无 head）：非 nil 空切片
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := withStore(&runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir}}, st)
	if got := p.CurrentSessionHistory(); got == nil || len(got) != 0 {
		t.Errorf("CurrentSessionHistory = %#v, want non-nil empty", got)
	}

	// 持久化未启用（store 为 nil）：同样非 nil 空切片
	disabled := &AgentPlugin{cfg: &runtimeconfig.Config{}}
	if got := disabled.CurrentSessionHistory(); got == nil || len(got) != 0 {
		t.Errorf("disabled CurrentSessionHistory = %#v, want non-nil empty", got)
	}
}

// archiveFixture 在 current 提交一整轮（user → assistant(2 tool_calls + thinking)
// → tool → assistant 收尾）并打 checkpoint，随后 Archive 归档，再为 current 提交
// 一条新消息。返回插件、store、归档文件 id 与归档前视图。
func archiveFixture(t *testing.T) (*AgentPlugin, *session.Store, string, []core.Message) {
	t.Helper()
	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if _, err := st.Commit([]core.Message{
		core.Message{Role: "user", Content: "归档里的问题", MsgID: session.NewMsgID()},
		core.Message{
			Role: "assistant",
			ToolCalls: []core.ToolCall{
				{ID: "call_arch_1", Type: "function", Function: core.ToolCallFunction{Name: "read_file", Arguments: `{"path":"a.go"}`}},
			},
			Thinking: core.Thinking{Text: "归档里的思考"},
			MsgID:    session.NewMsgID(),
		},
		core.Message{Role: "tool", Content: "归档里的工具输出", ToolCallID: "call_arch_1", MsgID: session.NewMsgID()},
		core.Message{Role: "assistant", Content: "归档里的回答", MsgID: session.NewMsgID()},
	}); err != nil {
		t.Fatalf("commit archive round: %v", err)
	}
	if err := st.Checkpoint("archive round"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	want := st.ActiveView()
	if err := st.Archive(); err != nil {
		t.Fatalf("archive: %v", err)
	}
	// 归档后 current 提交新消息，保证清单有「当前会话」条目可断言
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "当前会话", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit current: %v", err)
	}

	cfg := &runtimeconfig.Config{Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir}}
	return withStore(cfg, st), st, archiveIDs(t, dir)[0], want
}

// TestAgentPlugin_ArchivedSessionHistoryReplaysArchive 对真实归档文件做重放映射：
// user/assistant/tool 各至少一条，顺序与落盘一致；解析为只读，不触碰 store 与文件。
func TestAgentPlugin_ArchivedSessionHistoryReplaysArchive(t *testing.T) {
	p, st, archiveID, wantView := archiveFixture(t)
	headBefore := st.Head()
	archiveFile := filepath.Join(p.cfg.Session.Dir, "archive", archiveID+".jsonl")
	rawBefore, err := os.ReadFile(archiveFile)
	if err != nil {
		t.Fatalf("read archive file: %v", err)
	}

	got, err := p.ArchivedSessionHistory(archiveID)
	if err != nil {
		t.Fatalf("ArchivedSessionHistory: %v", err)
	}
	// 期望序列：user chunk → thought → tool_call → tool_call_update → agent chunk
	wantKinds := []string{
		protocol.UpdateUserMessageChunk,
		protocol.UpdateAgentThoughtChunk,
		protocol.UpdateToolCall,
		protocol.UpdateToolCallUpdate,
		protocol.UpdateAgentMessageChunk,
	}
	bodyKind := func(body protocol.SessionUpdateBody) string {
		if len(body.Raw) > 0 {
			fields := rawBodyFields(t, body)
			return fields["sessionUpdate"].(string)
		}
		return body.SessionUpdate
	}
	if len(got) != len(wantKinds) {
		t.Fatalf("ArchivedSessionHistory = %d bodies, want %d: %+v", len(got), len(wantKinds), got)
	}
	for i, kind := range wantKinds {
		if gotKind := bodyKind(got[i]); gotKind != kind {
			t.Errorf("history[%d].sessionUpdate = %q, want %q", i, gotKind, kind)
		}
	}
	// 文本与 tool 配对保真
	if got[0].Content == nil || got[0].Content.Text != "归档里的问题" {
		t.Errorf("history[0] = %+v, want user chunk 归档里的问题", got[0])
	}
	if got[1].Content == nil || got[1].Content.Text != "归档里的思考" {
		t.Errorf("history[1] = %+v, want thought chunk", got[1])
	}
	call := rawBodyFields(t, got[2])
	if call["toolCallId"] != "call_arch_1" || call["status"] != "in_progress" {
		t.Errorf("history[2] = %v, want tool_call call_arch_1 in_progress", call)
	}
	result := rawBodyFields(t, got[3])
	if result["toolCallId"] != "call_arch_1" || result["status"] != "completed" || result["rawOutput"] != "归档里的工具输出" {
		t.Errorf("history[3] = %v, want completed tool_call_update with rawOutput", result)
	}
	if got[4].Content == nil || got[4].Content.Text != "归档里的回答" {
		t.Errorf("history[4] = %+v, want agent chunk 归档里的回答", got[4])
	}

	// 只读：store head 不变、归档文件字节不变（wantView 长度与归档前视图一致仅作夹具自检）
	if after := st.Head(); after != headBefore {
		t.Errorf("store head changed by archive replay: %q → %q", headBefore, after)
	}
	rawAfter, err := os.ReadFile(archiveFile)
	if err != nil {
		t.Fatalf("re-read archive file: %v", err)
	}
	if string(rawBefore) != string(rawAfter) {
		t.Error("archive file changed by replay (must be read-only)")
	}
	if len(wantView) != 4 {
		t.Errorf("fixture view = %d messages, want 4 (自检)", len(wantView))
	}
}

// TestAgentPlugin_ArchivedSessionHistoryErrors 断言非法 id 与缺失归档的报错路径。
func TestAgentPlugin_ArchivedSessionHistoryErrors(t *testing.T) {
	p, _, archiveID, _ := archiveFixture(t)

	// 不存在的归档 id → 报错（含 OpenArchiveView 的文件缺失错误）
	if _, err := p.ArchivedSessionHistory("9999999999999999999"); err == nil {
		t.Error("missing archive id = nil error, want error")
	} else if !strings.Contains(err.Error(), "open archive") {
		t.Errorf("missing archive error = %v, want open archive failure", err)
	}

	// traversal id：路径分隔符 / 「.」前缀一律拒绝（防穿越）
	for _, bad := range []string{"../escape", "a/b", `a\b`, "..", ".", ".hidden"} {
		if _, err := p.ArchivedSessionHistory(bad); err == nil {
			t.Errorf("traversal id %q = nil error, want error", bad)
		} else if strings.Contains(err.Error(), "open archive") {
			t.Errorf("traversal id %q rejected too late (reached file open): %v", bad, err)
		}
	}

	// 持久化目录未配置 → 明确报错
	disabled := &AgentPlugin{cfg: &runtimeconfig.Config{}}
	if _, err := disabled.ArchivedSessionHistory(archiveID); err == nil || !strings.Contains(err.Error(), "persistence disabled") {
		t.Errorf("disabled persistence error = %v, want persistence disabled", err)
	}
}

// TestAgentPlugin_ListSessionsMarksCurrentAndCwd 验证 IsCurrent 标记与当前会话
// cwd 回显：当前条目 isCurrent=true（TS 侧据此禁用归档输入框），SetSession 声明
// 的 cwd 规范化为绝对路径后回显；归档条目两项皆空。
func TestAgentPlugin_ListSessionsMarksCurrentAndCwd(t *testing.T) {
	p, st, dir := sessionFixture(t, 2)
	head := st.Head()

	got := p.ListSessions()
	if len(got) != 3 {
		t.Fatalf("ListSessions = %+v, want 3 entries", got)
	}
	if !got[0].IsCurrent || got[1].IsCurrent || got[2].IsCurrent {
		t.Errorf("isCurrent flags = [%v %v %v], want [true false false]", got[0].IsCurrent, got[1].IsCurrent, got[2].IsCurrent)
	}
	if got[0].SessionID != head {
		t.Errorf("sessions[0].sessionId = %q, want head %q", got[0].SessionID, head)
	}
	if got[0].Cwd != "" {
		t.Errorf("sessions[0].cwd = %q before SetSession, want empty", got[0].Cwd)
	}

	// AllowedWorkDir 为空 = 不限制：任意 cwd 接受并回显（绝对路径）
	p.SetSession("sess_transport", filepath.Join(dir, "workspace"))
	got = p.ListSessions()
	wantCwd, err := filepath.Abs(filepath.Join(dir, "workspace"))
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	if got[0].Cwd != wantCwd {
		t.Errorf("sessions[0].cwd = %q, want declared %q", got[0].Cwd, wantCwd)
	}
	if got[1].Cwd != "" || got[2].Cwd != "" {
		t.Errorf("archived cwds = [%q %q], want empty", got[1].Cwd, got[2].Cwd)
	}
}

// TestAgentPlugin_SetSessionValidatesSandboxBoundary 验证 cwd 声明的 sandbox 边界：
// AllowedWorkDir 非空时界内接受、界外存无效标记（handleAgent prompt 报
// invalid session cwd），无效标记随 startSession 重建作废。
func TestAgentPlugin_SetSessionValidatesSandboxBoundary(t *testing.T) {
	boundary := t.TempDir()
	inside := filepath.Join(boundary, "project")
	if err := os.MkdirAll(inside, 0o700); err != nil {
		t.Fatalf("mkdir inside: %v", err)
	}
	outside := t.TempDir()

	dir := t.TempDir()
	st, err := session.Open(dir)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.Commit([]core.Message{{Role: "user", Content: "当前会话", MsgID: session.NewMsgID()}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	cfg := &runtimeconfig.Config{
		Session: runtimeconfig.SessionConfig{Enabled: true, Dir: dir},
		Sandbox: sandbox.SandboxConfig{AllowedWorkDir: boundary},
	}
	p := withStore(cfg, st)
	p.session.Store(runtimeagent.NewSession(runtimeagent.SessionDeps{Config: cfg, Store: st}))

	// 界内：接受，cwd 落到插件与会话
	p.SetSession("sess_a", inside)
	if err := p.sessionCWDError(); err != nil {
		t.Fatalf("inside cwd rejected: %v", err)
	}
	if got := p.currentSessionCwd(); got != inside {
		t.Errorf("sessionCwd = %q, want %q", got, inside)
	}

	// 符号链接指向界内目录：EvalSymlinks 解析后放行
	link := filepath.Join(boundary, "link-to-project")
	if err := os.Symlink(inside, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	p.SetSession("sess_link", link)
	if err := p.sessionCWDError(); err != nil {
		t.Fatalf("symlinked inside cwd rejected: %v", err)
	}

	// 界外：存为无效标记，sessionCwd 清空
	p.SetSession("sess_b", outside)
	if err := p.sessionCWDError(); err == nil || !strings.Contains(err.Error(), "invalid session cwd") {
		t.Fatalf("outside cwd error = %v, want invalid session cwd", err)
	}
	if got := p.currentSessionCwd(); got != "" {
		t.Errorf("sessionCwd after invalid = %q, want empty", got)
	}
	// 无效标记下 prompt 被挡（handleAgent 首查 cwdErr）
	perr := p.handleAgent(&model.Context{Args: []string{"hi"}})
	if perr == nil || !strings.Contains(perr.Error(), "invalid session cwd") {
		t.Errorf("handleAgent under invalid cwd = %v, want invalid session cwd", perr)
	}

	// 不存在的 cwd：EvalSymlinks 失败 → 无效
	p.SetSession("sess_c", filepath.Join(boundary, "missing-dir"))
	if err := p.sessionCWDError(); err == nil || !strings.Contains(err.Error(), "invalid session cwd") {
		t.Errorf("missing cwd error = %v, want invalid session cwd", err)
	}

	// 重新声明界内 cwd：无效标记清除
	p.SetSession("sess_d", inside)
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("cwd error after redeclare = %v, want nil", err)
	}

	// /new（startSession 重建）：无效标记不跨会话保留，最后声明无效 → 新会话无 cwd
	p.SetSession("sess_e", outside) // 置无效
	p.startSession()
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("cwdErr after startSession = %v, want nil (无效标记随旧会话作废)", err)
	}
	if got := p.currentSessionCwd(); got != "" {
		t.Errorf("sessionCwd after startSession = %q, want empty", got)
	}

	// 有效声明跨会话保留：重新声明界内 cwd 后重建，新会话继续生效
	p.SetSession("sess_f", inside)
	p.startSession()
	if err := p.sessionCWDError(); err != nil {
		t.Errorf("cwdErr after redeclare+startSession = %v, want nil", err)
	}
	if got := p.currentSessionCwd(); got != inside {
		t.Errorf("sessionCwd after redeclare+startSession = %q, want %q", got, inside)
	}
}

// TestAgentPlugin_SessionModesFromConfig 验证 modes 数据源：providers 按名字排序、
// name = "provider (model)"、currentModeId = default_provider；无 providers 返回 nil。
func TestAgentPlugin_SessionModesFromConfig(t *testing.T) {
	p := &AgentPlugin{cfg: &runtimeconfig.Config{
		LLM: runtimeconfig.LLMConfig{
			DefaultProvider: "deepseek",
			Providers: map[string]runtimeconfig.ProviderConfig{
				"anthropic": {Model: "claude-sonnet"},
				"deepseek":  {Model: "deepseek-chat"},
				"kimi":      {Model: "kimi-k2"},
			},
		},
	}}
	got := p.SessionModes()
	if got == nil {
		t.Fatal("SessionModes = nil, want state")
	}
	if got.CurrentModeID != "deepseek" {
		t.Errorf("currentModeId = %q, want deepseek", got.CurrentModeID)
	}
	wantIDs := []string{"anthropic", "deepseek", "kimi"}
	wantNames := []string{"anthropic (claude-sonnet)", "deepseek (deepseek-chat)", "kimi (kimi-k2)"}
	if len(got.AvailableModes) != len(wantIDs) {
		t.Fatalf("availableModes = %+v, want %d entries", got.AvailableModes, len(wantIDs))
	}
	for i, mode := range got.AvailableModes {
		if mode.ID != wantIDs[i] || mode.Name != wantNames[i] || mode.Description != "" {
			t.Errorf("availableModes[%d] = {%s %s %s}, want {%s %s }", i, mode.ID, mode.Name, mode.Description, wantIDs[i], wantNames[i])
		}
	}

	// 无 providers：nil（响应省略 modes）
	if empty := (&AgentPlugin{cfg: &runtimeconfig.Config{}}).SessionModes(); empty != nil {
		t.Errorf("SessionModes without providers = %+v, want nil", empty)
	}
}

// withStore 测试构造辅助：atomic.Pointer 字段无法在 struct 字面量里直接赋值。
func withStore(cfg *runtimeconfig.Config, st *session.Store) *AgentPlugin {
	p := &AgentPlugin{cfg: cfg}
	p.store.Store(st)
	return p
}
