// sessions.go 提供 agent 插件的会话清单能力：扫描 Session.Dir（current.jsonl +
// archive/*.jsonl）产出 ACP session/list 的会话摘要，供 vscode ACP 前端做会话
// 列表与 load 判定。只读扫描：不取 store.mu、不触发 Archive、归档文件只读打开
// ——清单绝不干扰运行中的 store（追加中的半行按坏行跳过，与 store.loadRecords 同策略）。
package agent

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-runtime/session"
)

const (
	// maxListSessions 是一次会话清单返回的条数上限。
	maxListSessions = 50
	// titleMaxRunes 是会话标题截断长度（rune 计，中文安全）。
	titleMaxRunes = 60
)

// ListSessions 汇总会话清单：当前会话（store 有 head 时）置顶且不参与排序，
// 归档会话按 updatedAt 倒序随后，最多 maxListSessions 条。
//
// 取值规则（store 的记录模型不落 cwd）：
//   - sessionId：当前会话 = store 当前 head id（与 session/load 的「当前会话」
//     判定同源，保证列表给出的 id 可直接 load）；归档会话 = 归档文件名去
//     .jsonl（归档时的 UnixNano 时间戳，稳定且唯一）。
//   - cwd：空字符串（jsonl 记录不含 cwd；Session.Dir 是存储目录而非项目目录）。
//   - title：文件内首条 user 消息内容，空白折叠为单行后截断到 titleMaxRunes。
//   - updatedAt：文件 mtime（RFC3339）。
func (p *AgentPlugin) ListSessions() []protocol.SessionInfo {
	out := make([]protocol.SessionInfo, 0, 4)
	if p.store == nil || p.cfg.Session.Dir == "" {
		return out
	}
	dir := p.cfg.Session.Dir

	if head := p.store.Head(); head != "" {
		if m := describeSessionFile(filepath.Join(dir, "current.jsonl"), head); m != nil {
			out = append(out, m.info)
		}
	}
	for _, m := range listArchivedSessions(filepath.Join(dir, "archive")) {
		out = append(out, m.info)
	}
	if len(out) > maxListSessions {
		out = out[:maxListSessions]
	}
	return out
}

// CurrentSessionID 返回当前活动会话的 head id（空 = 无活动会话/持久化未启用）。
// vscode 前端用它判定 session/load 的目标是否为当前会话。
func (p *AgentPlugin) CurrentSessionID() string {
	if p.store == nil {
		return ""
	}
	return p.store.Head()
}

// ReloadCurrentSession 确保内存会话视图与 store 最新状态一致。session/load
// 命中当前会话时调用——单活动会话模型下当前会话始终处于已加载状态，这里只做
// 视图刷新（ReloadFromStore），不重建会话。
func (p *AgentPlugin) ReloadCurrentSession() error {
	if p.session == nil {
		return fmt.Errorf("agent: no active session")
	}
	return p.session.ReloadFromStore()
}

// CurrentSessionHistory 导出当前会话的完整对话历史，映射为 ACP session/update
// 通知序列，供 vscode 前端在 session/load 返回响应前向客户端重放。数据源
// store.ActiveView()（store 锁内取只读快照，单活动会话模型下即当前视图）。
//
// 分段：每轮 Run 提交后 store 自动打 checkpoint（at = 该轮最后一条消息 id）。
// 这里取全部 checkpoint 的 at 作轮次锚点，按 ActiveView 顺序切分；锚点不在当前
// 视图内（rewind/compact 后失效）自然不命中即跳过，checkpoint 漏打的结尾消息
// 如实并入最后一段。checkpoint 只作边界，不进重放流。
//
// 段内映射（wire 形状对齐 vscode-acp-ui 的 sessionUpdateMapping）：
//   - user → user_message_chunk（全文一条）；
//   - assistant 带 ToolCalls → 每个 tool call 一条 tool_call（toolCallId=call id，
//     title=function 名，status=in_progress；rawInput 带调用参数供 UI 副标题）；
//   - tool（有 ToolCallID）→ tool_call_update（同 id，status=completed，输出文本
//     走 rawOutput——mapping 在无 content 数组时以 rawOutput 为折叠内容来源；
//     配不到 tool_call 的孤立 tool 消息同样降级为 completed 更新，id 用消息自带
//     ToolCallID，webview 对未知 id 会新建行）；
//   - assistant 有正文无 tool_calls → agent_message_chunk（全文一条）；空正文且
//     无 tool_calls 跳过；
//   - system（compact 摘要）与其余角色跳过。
//
// 全量重放，不设轮次/条数上限。无持久化（store 为 nil）或空会话返回非 nil 空切片。
func (p *AgentPlugin) CurrentSessionHistory() []protocol.SessionUpdateBody {
	out := []protocol.SessionUpdateBody{}
	if p.store == nil {
		return out
	}
	view := p.store.ActiveView()
	if len(view) == 0 {
		return out
	}
	anchors := checkpointAnchors(p.store)
	for _, round := range segmentByCheckpoints(view, anchors) {
		out = append(out, roundUpdates(round)...)
	}
	return out
}

// checkpointAnchors 汇总全部 checkpoint 的 at 锚点 id 集合。Checkpoints 以
// limit<=0 请求全部（返回按时间倒序，这里只用集合，顺序无关）。
func checkpointAnchors(st *session.Store) map[string]bool {
	anchors := make(map[string]bool)
	for _, ck := range st.Checkpoints(0) {
		if ck.At != "" {
			anchors[ck.At] = true
		}
	}
	return anchors
}

// segmentByCheckpoints 按锚点把视图切成轮次：命中锚点的消息是它所在轮的最后一条。
// 锚点不在视图内（rewind/compact 后失效）自然不命中；结尾未命中锚点的消息
// （checkpoint 漏打）并入最后一段。空视图返回空轮次。
func segmentByCheckpoints(view []core.Message, anchors map[string]bool) [][]core.Message {
	rounds := make([][]core.Message, 0, len(anchors)+1)
	cur := make([]core.Message, 0, len(view))
	for _, msg := range view {
		cur = append(cur, msg)
		if msg.MsgID != "" && anchors[msg.MsgID] {
			rounds = append(rounds, cur)
			cur = make([]core.Message, 0, len(view))
		}
	}
	if len(cur) > 0 {
		rounds = append(rounds, cur)
	}
	return rounds
}

// roundUpdates 把一个轮次映射为重放通知序列，严格保持消息顺序（同一 assistant
// 的多个 tool_calls 依次发出）。
func roundUpdates(round []core.Message) []protocol.SessionUpdateBody {
	out := make([]protocol.SessionUpdateBody, 0, len(round))
	for _, msg := range round {
		switch msg.Role {
		case "user":
			out = append(out, protocol.SessionUpdateBody{
				SessionUpdate: protocol.UpdateUserMessageChunk,
				Content:       &protocol.ContentBlock{Type: "text", Text: msg.Content},
			})
		case "assistant":
			// thinking 先于 tools/正文：对应实时流里思考块的位置，重放后
			// AgentThoughtBlock 以收起态渲染（streaming=false）。
			if msg.Thinking.Text != "" {
				out = append(out, protocol.SessionUpdateBody{
					SessionUpdate: protocol.UpdateAgentThoughtChunk,
					Content:       &protocol.ContentBlock{Type: "text", Text: msg.Thinking.Text},
				})
			}
			if len(msg.ToolCalls) > 0 {
				for _, tc := range msg.ToolCalls {
					if body, ok := toolCallBody(tc); ok {
						out = append(out, body)
					}
				}
			}
			if msg.Content != "" {
				out = append(out, protocol.SessionUpdateBody{
					SessionUpdate: protocol.UpdateAgentMessageChunk,
					Content:       &protocol.ContentBlock{Type: "text", Text: msg.Content},
				})
			}
		case "tool":
			if body, ok := toolCallUpdateBody(msg.ToolCallID, msg.Content); ok {
				out = append(out, body)
			}
		}
		// system（compact 摘要）与其余角色跳过——摘要展示留待后续版本以占位形式补。
	}
	return out
}

// acpToolCall 是 tool_call 的 Raw 透传载荷：rawInput 需在对象/字符串间混型，
// SessionUpdateBody 的类型化字段表达不了，整体经 Raw 透传（同
// availableCommandsUpdateBody 模式）。
type acpToolCall struct {
	SessionUpdate string `json:"sessionUpdate"`
	ToolCallID    string `json:"toolCallId"`
	Title         string `json:"title,omitempty"`
	Status        string `json:"status,omitempty"`
	// RawInput 是调用参数：合法 JSON 原样内嵌为对象（mapping 可从中提取
	// path/pattern 作副标题），否则退化为字符串（formatToolRawInput 两者都能展示）。
	RawInput any `json:"rawInput,omitempty"`
}

// acpToolCallUpdate 是 tool_call_update 的 Raw 透传载荷：输出文本走 rawOutput——
// mapping 在没有 content 数组时以 rawOutput 作折叠内容来源（字符串分支，
// formatRawToolOutput）。
type acpToolCallUpdate struct {
	SessionUpdate string `json:"sessionUpdate"`
	ToolCallID    string `json:"toolCallId"`
	Status        string `json:"status,omitempty"`
	RawOutput     string `json:"rawOutput,omitempty"`
}

// toolCallBody 构造一条 tool_call 透传体；空 call id 无法与 tool 结果配对，跳过。
// 这些字段 json.Marshal 不会失败（失败即跳过该条，与 availableCommandsUpdateBody
// 同策略）。
func toolCallBody(tc core.ToolCall) (protocol.SessionUpdateBody, bool) {
	if tc.ID == "" {
		return protocol.SessionUpdateBody{}, false
	}
	// title 与 live 路径同构（name(args)）：webview 的 generic 分支渲染
	// "tool · <name>"，args 收进折叠区；不报 kind（枚举是语义描述，不为
	// 渲染门控撒谎——折叠由 SDK 按输出长度判定）。
	title := tc.Function.Name
	if args := tc.Function.Arguments; args != "" {
		title = fmt.Sprintf("%s(%s)", title, args)
	}
	data, err := json.Marshal(acpToolCall{
		SessionUpdate: protocol.UpdateToolCall,
		ToolCallID:    tc.ID,
		Title:         title,
		Status:        "in_progress",
		RawInput:      rawInputValue(tc.Function.Arguments),
	})
	if err != nil {
		return protocol.SessionUpdateBody{}, false
	}
	return protocol.SessionUpdateBody{Raw: data}, true
}

// toolCallUpdateBody 构造一条 tool_call_update 透传体（status=completed，输出文本
// 走 rawOutput）；id 为空（tool 消息没有 ToolCallID）无法配对，跳过。
func toolCallUpdateBody(id, output string) (protocol.SessionUpdateBody, bool) {
	if id == "" {
		return protocol.SessionUpdateBody{}, false
	}
	data, err := json.Marshal(acpToolCallUpdate{
		SessionUpdate: protocol.UpdateToolCallUpdate,
		ToolCallID:    id,
		Status:        "completed",
		RawOutput:     output,
	})
	if err != nil {
		return protocol.SessionUpdateBody{}, false
	}
	return protocol.SessionUpdateBody{Raw: data}, true
}

// rawInputValue 把调用参数包装成 SDK 友好的形状：合法 JSON 内嵌为对象，其余退化
// 为字符串；空串返回 nil（omitempty 下省略字段）。
func rawInputValue(args string) any {
	args = strings.TrimSpace(args)
	if args == "" {
		return nil
	}
	if json.Valid([]byte(args)) {
		return json.RawMessage(args)
	}
	return args
}

// sessionMeta 是扫描单个 jsonl 的内部摘要；at 保留 time.Time 供排序。
type sessionMeta struct {
	info protocol.SessionInfo
	at   time.Time
}

// listArchivedSessions 读取归档目录，按文件 mtime 倒序返回归档会话摘要。
// 目录不存在（从未归档）返回空。
func listArchivedSessions(archiveDir string) []sessionMeta {
	entries, err := os.ReadDir(archiveDir)
	if err != nil {
		return nil
	}
	out := make([]sessionMeta, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		// sessionId = 归档文件名去扩展名（Archive 以 UnixNano 命名，稳定唯一）
		m := describeSessionFile(filepath.Join(archiveDir, name), strings.TrimSuffix(name, ".jsonl"))
		if m == nil {
			continue
		}
		out = append(out, *m)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].at.After(out[j].at) })
	return out
}

// describeSessionFile 取文件 mtime 与标题，组装一条会话摘要（sessionId 由调用方
// 给定）。文件不存在或不可读返回 nil。
func describeSessionFile(path, sessionID string) *sessionMeta {
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() {
		return nil
	}
	return &sessionMeta{
		info: protocol.SessionInfo{
			SessionID: sessionID,
			Title:     firstUserTitle(path),
			UpdatedAt: fi.ModTime().Format(time.RFC3339),
		},
		at: fi.ModTime(),
	}
}

// firstUserTitle 只读扫描 jsonl，返回首条 user 消息的单行化截断标题。
// 无 user 消息（或全是坏行/空文件）返回空串（omitempty 下标题缺省）。
func firstUserTitle(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1<<20) // 与 store.loadRecords 同限：单行最大 1MB
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var r session.Record
		if err := json.Unmarshal(line, &r); err != nil {
			continue // 坏行跳过
		}
		// kind/role 字面量对齐 session 包的磁盘格式契约（record_format_test 兜底）：
		// kind == "msg" 且 role == "user" 即用户消息。
		if r.Kind == "msg" && r.Msg != nil && r.Msg.Role == "user" {
			return truncateTitle(r.Msg.Content)
		}
	}
	return ""
}

// truncateTitle 把内容折叠为单行并按 rune 截断，超长补 …（规则对齐 session 包
// 的 truncate，只是先做空白折叠让标题单行化）。
func truncateTitle(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if len(runes) > titleMaxRunes {
		return string(runes[:titleMaxRunes]) + "…"
	}
	return s
}
