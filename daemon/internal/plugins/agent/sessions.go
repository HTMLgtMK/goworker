// sessions.go 提供 agent 插件的会话清单与元数据能力：扫描当前会话目录（current.jsonl +
// archive/*.jsonl）产出 ACP session/list 的会话摘要、session/new|load 响应的 modes
// 状态与归档只读重放，供主会话 ACP service 做会话列表、load 判定与模型显示。
// P1-3 起持久化换轨 Repository（每会话独立目录 + legacy 根槽兼容挂载），归档随
// 会话目录轮转：清单取数经 currentStore() 解析，扫描「当前会话目录」下的
// current.jsonl 与 archive/（legacy 挂载期即 Session.Dir，与旧布局逐字段一致；
// 其余 per-id 目录的并集留待 P5 并入）。
// 只读扫描：不取 store.mu、不触发 Archive、归档文件只读打开
// ——清单绝不干扰运行中的 store（追加中的半行按坏行跳过，与 store.loadRecords 同策略）。
package service

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
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
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
// 数据源（P1-3 换轨）：当前会话 = currentStore()（经 current runtime → repo 实例
// 唯一缓存解析，与命令互斥同源）；归档 = 当前会话目录（currentSessionDir）的
// archive/ 扫描——legacy 挂载期即 Session.Dir（与旧布局输出一致），独立目录会话
// 扫 <root>/<id>/archive/；其余 per-id 目录的并集留待 P5 并入，本步输出逐字段
// 规则与旧版一致。
//
// 取值规则（store 的记录模型不落 cwd）：
//   - sessionId：当前会话 = store 的稳定 logical SessionID（与 session/load 的「当前会话」
//     判定同源，保证列表给出的 id 可直接 load）；Store.Head 只用于判断当前文件是否有
//     消息，绝不作为 ACP sessionId；归档会话 = 归档 metadata 中的 logical SessionID。
//   - cwd：当前会话填最近一次有效声明的会话 cwd（未声明为空）；归档会话为空。
//   - active：daemon domain summary 标记当前会话；ACP SessionInfo 映射时不会写入该字段。
//   - title：文件内首条 user 消息内容，空白折叠为单行后截断到 titleMaxRunes。
//   - updatedAt：文件 mtime（RFC3339）。
func (p *AgentPlugin) ListSessions() []model.SessionSummary {
	out := make([]model.SessionSummary, 0, 4)
	store := p.currentStore()
	dir := p.currentSessionDir()
	if store == nil || dir == "" {
		return out
	}

	if head := store.Head(); head != "" {
		if m := describeSessionFile(filepath.Join(dir, "current.jsonl"), store.SessionID()); m != nil {
			m.info.Active = true
			m.info.Cwd = p.currentSessionCwd()
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

// SessionModes 实现 modes 探测（经 ACP service 委托给 dispatch.Server）：
// 数据源 cfg.LLM，只显示不切换 —— currentModeId = default_provider，
// availableModes = providers 按名字排序（id = provider 名，name = "名 (model)"，
// description 留空）。无 providers 或无配置返回 nil（响应省略 modes）。
func (p *AgentPlugin) SessionModes() *model.SessionModeState {
	if p.cfg == nil || len(p.cfg.LLM.Providers) == 0 {
		return nil
	}
	names := make([]string, 0, len(p.cfg.LLM.Providers))
	for name := range p.cfg.LLM.Providers {
		names = append(names, name)
	}
	sort.Strings(names)
	modes := make([]model.SessionMode, 0, len(names))
	for _, name := range names {
		modes = append(modes, model.SessionMode{
			ID:   name,
			Name: fmt.Sprintf("%s (%s)", name, p.cfg.LLM.Providers[name].Model),
		})
	}
	return &model.SessionModeState{
		CurrentModeID:  p.cfg.LLM.DefaultProvider,
		AvailableModes: modes,
	}
}

// currentSessionCwd 返回最近一次有效声明的会话 cwd（锁内快照，未声明为空）。
func (p *AgentPlugin) currentSessionCwd() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessionCwd
}

// CurrentSessionID returns the stable logical identity, not the mutable head.
// P1-3 起经 currentStore() 解析（current runtime → repo 缓存），返回值语义不变。
func (p *AgentPlugin) CurrentSessionID() string {
	if store := p.currentStore(); store != nil {
		return store.SessionID()
	}
	return ""
}

// ReloadCurrentSession 确保内存会话视图与 store 最新状态一致。session/load
// 命中当前会话时调用——单活动会话模型下当前会话始终处于已加载状态，这里只做
// 视图刷新（ReloadFromStore），不重建会话。
func (p *AgentPlugin) ReloadCurrentSession() error {
	s := p.boundSession()
	if s == nil {
		return fmt.Errorf("agent: no active session")
	}
	return s.ReloadFromStore()
}

// CurrentSessionHistory 导出当前会话的完整对话历史，映射为 ACP session/update
// 通知序列，供 ACP service 在 session/load 返回响应前向客户端重放。数据源
// store.ActiveView()（store 锁内取只读快照，单活动会话模型下即当前视图），
// 经 replayUpdates 管线（与归档只读查看共用）输出。
//
// 分段：每轮 Run 提交后 store 自动打 checkpoint（at = 该轮最后一条消息 id），
// 取全部 checkpoint 的 at 作轮次锚点，按 ActiveView 顺序切分；锚点不在当前视图
// 内（rewind/compact 后失效）自然不命中即跳过，checkpoint 漏打的结尾消息如实
// 并入最后一段。checkpoint 只作边界，不进重放流。
//
// 全量重放，不设轮次/条数上限。无持久化（currentStore 为 nil）或空会话返回非 nil 空切片。
func (p *AgentPlugin) CurrentSessionHistory() []model.SessionUpdate {
	store := p.currentStore()
	if store == nil {
		return []model.SessionUpdate{}
	}
	view := store.ActiveView()
	if len(view) == 0 {
		return []model.SessionUpdate{}
	}
	return replayUpdates(view, checkpointAnchors(store))
}

// ArchivedSessionHistory 只读解析归档会话并映射为 ACP 重放通知序列（方案 B
// 「归档只读查看」的数据面）：按 archiveId 定位归档文件，OpenArchiveView 构建
// ActiveView 与 checkpoint 锚点，经与 CurrentSessionHistory 相同的
// segmentByCheckpoints + roundUpdates 管线输出。不持写句柄、不 rename、不触碰
// current 与 store。
//
// 归档路径解析（P1-3）：归档随会话目录轮转（Archive 以 store 自身 dir 为基准），
// 先查当前会话目录的 archive/，legacy 根槽（<Session.Dir>/archive/，兼容挂载的
// 既有归档）次之；两处都只读 OpenArchiveView，其余 per-id 目录的并集留待 P5。
func (p *AgentPlugin) ArchivedSessionHistory(sessionID string) ([]model.SessionUpdate, error) {
	if p.cfg == nil || p.cfg.Session.Dir == "" {
		return nil, fmt.Errorf("agent: session persistence disabled")
	}
	// id 校验前置（防 traversal）：非法 id 在触碰任何文件前拒绝，报错文案与旧版一致。
	if _, ok := archiveFilePath(p.cfg.Session.Dir, sessionID); !ok {
		return nil, fmt.Errorf("agent: invalid archive session id %q", sessionID)
	}
	dirs := make([]string, 0, 2)
	if dir := p.currentSessionDir(); dir != "" && dir != p.cfg.Session.Dir {
		dirs = append(dirs, dir)
	}
	dirs = append(dirs, p.cfg.Session.Dir)
	path := ""
	for _, dir := range dirs {
		candidate, ok := archivePathForSession(dir, sessionID)
		if !ok {
			continue // 已前置校验，防御分支
		}
		if path == "" {
			path = candidate // 保留首个候选：全部未命中时按它给出 open archive 报错
		}
		if _, err := os.Stat(candidate); err == nil {
			path = candidate
			break
		}
	}
	if path == "" {
		return nil, fmt.Errorf("agent: invalid archive session id %q", sessionID)
	}
	view, err := session.OpenArchiveView(path)
	if err != nil {
		return nil, fmt.Errorf("agent: open archive %q: %w", sessionID, err)
	}
	return replayUpdates(view.ActiveView(), anchorSet(view.Checkpoints(0))), nil
}

// currentSessionDir 返回当前会话的会话目录（P1-3 归档扫描基准）。归档随会话目录
// 轮转，目录两种形态：
//   - 新布局 <Session.Dir>/<store SessionID>/（Repository.Create 创建，目录名即
//     文件内 SessionID）；
//   - legacy 根槽 = Session.Dir 本身（兼容挂载，current.jsonl 直在根下）。
//
// 判定：会话 id 命中新布局目录（其下有 current.jsonl）取该目录；否则根下有
// legacy 槽取根；都没有返回空串。store 被 Archive 轮转的瞬间 metadata id 会换成
// 新随机 id 而 store 目录不变，新 id 不命中任何目录 → 短暂返回空串；/new 在同一
// gate 临界区内随即换轨新会话，窗口只暴露给无锁的只读清单方，与旧版在归档瞬间
// 读清单的抖动同级。
func (p *AgentPlugin) currentSessionDir() string {
	root := p.cfg.Session.Dir
	store := p.currentStore()
	if root == "" || store == nil {
		return ""
	}
	if d := filepath.Join(root, store.SessionID()); hasCurrentFile(d) {
		return d
	}
	if hasCurrentFile(root) {
		return root
	}
	return ""
}

// hasCurrentFile 报告 dir 下是否存在 current.jsonl。
func hasCurrentFile(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "current.jsonl"))
	return err == nil
}

// archiveFilePath 按 archiveId 定位归档文件路径。id 必须是不含路径分隔符、
// 不以「.」开头的纯文件名（归档 id = UnixNano 时间戳），其余一律拒绝防 traversal。
func archiveFilePath(sessionDir, archiveID string) (string, bool) {
	if archiveID == "" || strings.HasPrefix(archiveID, ".") {
		return "", false
	}
	if strings.ContainsAny(archiveID, `/\`) || strings.ContainsRune(archiveID, os.PathSeparator) {
		return "", false
	}
	return filepath.Join(sessionDir, "archive", archiveID+".jsonl"), true
}

func archivePathForSession(sessionDir, sessionID string) (string, bool) {
	direct, valid := archiveFilePath(sessionDir, sessionID)
	if !valid {
		return "", false
	}

	// Keep accepting pre-metadata callers that used the timestamp filename as
	// an archive ID. New listings always expose the logical metadata ID.
	if view, err := session.OpenArchiveView(direct); err == nil {
		if view.SessionID() == sessionID || strings.TrimSuffix(filepath.Base(direct), ".jsonl") == sessionID {
			return direct, true
		}
	}

	entries, err := os.ReadDir(filepath.Join(sessionDir, "archive"))
	if err != nil {
		return direct, true
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(sessionDir, "archive", entry.Name())
		view, viewErr := session.OpenArchiveView(path)
		if viewErr == nil && view.SessionID() == sessionID {
			return path, true
		}
	}
	return direct, true
}

// 当前会话导出（CurrentSessionHistory）与归档只读查看（ArchivedSessionHistory）共用。
// 无命中锚点时整个视图一段（checkpoint 漏打的结尾消息并入最后一段）。
func replayUpdates(view []core.Message, anchors map[string]bool) []model.SessionUpdate {
	out := []model.SessionUpdate{}
	for _, round := range segmentByCheckpoints(view, anchors) {
		out = append(out, roundUpdates(round)...)
	}
	return out
}

// checkpointAnchors 汇总 store 全部 checkpoint 的 at 锚点 id 集合。
func checkpointAnchors(st *session.Store) map[string]bool {
	return anchorSet(st.Checkpoints(0))
}

// anchorSet 把检查点列表折成 at 锚点集合。Checkpoints 以 limit<=0 请求全部
// （返回按时间倒序，这里只用集合，顺序无关）。
func anchorSet(cks []session.Checkpoint) map[string]bool {
	anchors := make(map[string]bool, len(cks))
	for _, ck := range cks {
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
// 的多个 tool_calls 依次发出）。段内映射（wire 形状对齐 vscode-acp-ui 的
// sessionUpdateMapping）：
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
func roundUpdates(round []core.Message) []model.SessionUpdate {
	out := make([]model.SessionUpdate, 0, len(round))
	for _, msg := range round {
		switch msg.Role {
		case "user":
			out = append(out, model.SessionUpdate{
				Kind:    model.SessionUpdateUserMessageChunk,
				Content: &model.SessionContent{Type: "text", Text: msg.Content},
			})
		case "assistant":
			// thinking 先于 tools/正文：对应实时流里思考块的位置，重放后
			// AgentThoughtBlock 以收起态渲染（streaming=false）。
			if msg.Thinking.Text != "" {
				out = append(out, model.SessionUpdate{
					Kind:    model.SessionUpdateAgentThoughtChunk,
					Content: &model.SessionContent{Type: "text", Text: msg.Thinking.Text},
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
				out = append(out, model.SessionUpdate{
					Kind:    model.SessionUpdateAgentMessageChunk,
					Content: &model.SessionContent{Type: "text", Text: msg.Content},
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
func toolCallBody(tc core.ToolCall) (model.SessionUpdate, bool) {
	if tc.ID == "" {
		return model.SessionUpdate{}, false
	}
	// title 与 live 路径同构（name(args)）：webview 的 generic 分支渲染
	// "tool · <name>"，args 收进折叠区；不报 kind（枚举是语义描述，不为
	// 渲染门控撒谎——折叠由 SDK 按输出长度判定）。
	title := tc.Function.Name
	if args := tc.Function.Arguments; args != "" {
		title = fmt.Sprintf("%s(%s)", title, args)
	}
	data, err := json.Marshal(acpToolCall{
		SessionUpdate: model.SessionUpdateToolCall,
		ToolCallID:    tc.ID,
		Title:         title,
		Status:        "in_progress",
		RawInput:      rawInputValue(tc.Function.Arguments),
	})
	if err != nil {
		return model.SessionUpdate{}, false
	}
	return model.SessionUpdate{Raw: data}, true
}

// toolCallUpdateBody 构造一条 tool_call_update 透传体（status=completed，输出文本
// 走 rawOutput）；id 为空（tool 消息没有 ToolCallID）无法配对，跳过。
func toolCallUpdateBody(id, output string) (model.SessionUpdate, bool) {
	if id == "" {
		return model.SessionUpdate{}, false
	}
	data, err := json.Marshal(acpToolCallUpdate{
		SessionUpdate: model.SessionUpdateToolCallUpdate,
		ToolCallID:    id,
		Status:        "completed",
		RawOutput:     output,
	})
	if err != nil {
		return model.SessionUpdate{}, false
	}
	return model.SessionUpdate{Raw: data}, true
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
	info model.SessionSummary
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
		path := filepath.Join(archiveDir, name)
		sessionID := strings.TrimSuffix(name, ".jsonl")
		if view, viewErr := session.OpenArchiveView(path); viewErr == nil && view.SessionID() != "" {
			sessionID = view.SessionID()
		}
		m := describeSessionFile(path, sessionID)
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
		info: model.SessionSummary{
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
