// sessions.go 提供 agent 插件的会话清单与元数据能力：清单（ListSessions）在
// Plan A3 起改为全仓并集扫描——session.ScanSessionDigests 一次只读解析 Session.Dir
// 下全部会话（per-id 目录 v2/v1 双探测 + legacy 根槽），三态判定后按 updatedAt
// 降序输出；本文件另承载 session/new|load 响应的 modes 状态、当前/归档会话的
// 历史重放与 cwd 取数，供主会话 ACP service 做会话列表、load 判定与模型显示。
// 只读纪律不变：不取 store.mu、不触发归档、坏行跳过——清单绝不干扰运行中的
// store（追加中的半行按坏行跳过，与 store.loadRecords 同策略）。
package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
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

// ListSessions 汇总会话清单（Plan A3 并集重写）：扫描 Session.Dir 下全部会话
// （per-id 目录 v2/v1 双探测 + legacy 根槽，session.ScanSessionDigests 一次性
// 只读解析），三态判定（定案需求 #3）后按 updatedAt 降序输出、
// maxListSessions 截断。旧版「当前会话置顶且不参与排序」的特殊逻辑取消——
// 多会话下统一排序（plan A3），当前会话只是普通条目（cwd 取值有特例，见下）。
//
// 数据源（Plan A3）：session.ScanSessionDigests——不进实例唯一缓存、不取写
// 句柄、不加目录锁的只读解析面（digest.go 文件头论证：清单是高频操作，逐会话
// Open 的缓存/flock 常驻不可接受；OpenArchiveView 同级只读纪律，坏行跳过）。
// 旧版的「当前会话目录 archive/ 扫描」随之退役：Plan A1 起 archived 是原位
// 终态标记（不再搬文件），显式归档与迁移的会话都在数据文件位置。
// Store.Archive() 轮转时代的存量归档（root/archive/）已由 A4 一次性迁移
// （session.MigrateLegacyLayout，启动链调用）以 archived 态迁回 per-id 目录后
// 回归本清单；未迁移的旧快照（迁移前备份、异常中断的现场）上它们暂不可列，
// ArchivedSessionHistory 的归档重放路径不受影响（已知 id 仍可 load）。
//
// 取值规则：
//   - sessionId：metadata 的稳定 logical SessionID（与 session/load 判定同源，
//     可直接 load）；legacy 槽为合成 id（metadata 缺失时）。
//   - state（定案需求 #3/#8，archived 优先）：archived 终态标记 → archived；
//     连接绑定（BindingRegistry）或 run 在飞（rt.Active 非 nil；挂起收口中的
//     会话同样在飞——isSuspendedRelease 取数面覆盖「终态已清、台账未摘」的
//     瞬间窗口）或 current 便利指针持有 → active；否则 idle。current 是
//     CLI/stdin 的便利绑定（plan A2「current 收缩为 CLI/stdin 便利指针」），
//     持有即活跃——否则 CLI 用户看自己的会话是 idle。
//   - cwd（A7 设计点 5）：active 的 current 会话以 runtime 声明为最新事实
//     （startSession 的声明迁移只落 runtime 不落 workspace 记录——/new 后
//     新会话无记录，声明是唯一事实源）；其余会话从 workspace 记录读（declareSessionCWD
//     声明即双写，记录与声明同步；idle/archived 的记录是唯一事实源）。
//   - title：文件内首条 user 消息内容，空白折叠为单行后截断到 titleMaxRunes。
//   - updatedAt：数据文件 mtime（RFC3339 展示；排序用 time.Time 避免字符串
//     比较的时区陷阱）。
func (p *AgentPlugin) ListSessions() []model.SessionSummary {
	out := make([]model.SessionSummary, 0, 8)
	if p.cfg == nil || p.cfg.Session.Dir == "" {
		return out
	}
	digests, err := session.ScanSessionDigests(p.cfg.Session.Dir)
	if err != nil {
		// root 不可读（目录被移走/权限收紧）：空清单诚实降级，warn 留痕。
		slog.Warn("agent: session list scan failed", "dir", p.cfg.Session.Dir, "err", err)
		return out
	}
	currentID := p.CurrentSessionID()
	type entry struct {
		info model.SessionSummary
		at   time.Time
	}
	entries := make([]entry, 0, len(digests))
	for _, d := range digests {
		id := d.Metadata.SessionID
		info := model.SessionSummary{
			SessionID: id,
			Title:     truncateTitle(d.FirstUserText),
			Cwd:       d.Workspace,
			UpdatedAt: d.ModTime.Format(time.RFC3339),
			Archived:  d.Archived,
		}
		switch {
		case d.Archived:
			info.State = model.SessionStateArchived
		case p.sessionIsLive(id):
			info.State = model.SessionStateActive
			info.Active = true
			if id == currentID {
				// A7⑤：current 会话的 cwd 以 runtime 声明为最新事实（声明迁移
				// 不落记录）；无效声明下 currentSessionCwd 返回空（cwdErr 标记）。
				info.Cwd = p.currentSessionCwd()
			}
		default:
			info.State = model.SessionStateIdle
		}
		entries = append(entries, entry{info: info, at: d.ModTime})
	}
	// updatedAt 降序（time.Time 比较，无时区字符串陷阱）；稳定排序保持同刻
	// 文件的扫描序（os.ReadDir 语义）。
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].at.After(entries[j].at) })
	for _, e := range entries {
		out = append(out, e.info)
	}
	if len(out) > maxListSessions {
		out = out[:maxListSessions]
	}
	return out
}

// sessionIsLive 报告会话处于 active 态（Plan A3/定案需求 #8 的判定底座）：
// 连接绑定（BindingRegistry，生命周期引用的事实源）、run 在飞（rt.Active 非
// nil——后台 run 与挂起收口中的会话同样在飞）、current 便利指针持有（CLI/stdin
// 形态）。三个判据任一命中即活；全零即 idle（引用归零的会话由 A2a 生命周期
// 收口注销，idle 是收口前后的过渡态）。
//
// 绑定集合的跨层取数通路（A3 任务书勘察项）：取 p.control（agentcontrol.Service，
// 插件装配时已持有的实例）的 SessionBindings 透传，而非新建 ConnectionBinder
// 同款可选接口——插件与 Service 是组合关系（control 字段），零新增接口面；
// 新接口反而要在 model 层再造一个 type-assert 面，侵入更大。control 为 nil
// （测试直构插件）按无绑定处理，与 lifecycle.go 的 nil 容忍同款。
func (p *AgentPlugin) sessionIsLive(id string) bool {
	if p.control != nil && len(p.control.SessionBindings(id)) > 0 {
		return true
	}
	if rt := p.manager.Get(id); rt != nil && rt.Active() != nil {
		return true
	}
	if p.isSuspendedRelease(id) {
		return true
	}
	if cur := p.currentSession(); cur != nil && cur.ID == id {
		return true
	}
	return false
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

// currentSessionCwd 返回当前会话最近一次有效声明的 cwd（P1-4 起声明随 current
// runtime 走：无效标记下 cwd 恒空、无活动 runtime 或未声明返回空）。
func (p *AgentPlugin) currentSessionCwd() string {
	rt := p.currentSession()
	if rt == nil {
		return ""
	}
	cwd, err := rt.DeclaredCWD()
	if err != nil {
		return "" // 无效标记：DeclareCWD 已保证 cwd 清空，这里防御性兜底
	}
	return cwd
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
// 「归档只读查看」的数据面）。两条解析路径：
//   - archive/ 目录（方案 B 原路径）：按 archiveId 定位归档文件，OpenArchiveView
//     构建 ActiveView 与 checkpoint 锚点，经与 CurrentSessionHistory 相同的
//     segmentByCheckpoints + roundUpdates 管线输出。不持写句柄、不 rename、
//     不触碰 current 与 store。归档路径先查当前会话目录的 archive/，legacy
//     根槽（<Session.Dir>/archive/，兼容挂载的既有归档）次之。
//   - repo 层 per-id 数据文件（Plan A2b1 扩展，判定源从「清单扫描」改为
//     repo 探测）：Plan A1 起 archived 是原位终态标记（MarkArchived 不搬文件，
//     Store.Archive() 已随定案 #9 删除），显式归档（A3）与迁移（A4）的产物
//     停在 <root>/<id>/<数据文件>——这些会话不在任何 archive/ 目录里，经
//     repo.Open（v2/v1 双探测 + legacy 槽，与 LoadBoundSession 的重开同链）
//     解析后查 store.Archived()。命中 → 快照重放后释放（见下）；未命中
//     archived 标记 → 诚实报错「not archived」（handler 只在 SessionArchived
//     探测为真后到达这里，此分支是防御面）。
//
// 释放守卫（repo 分支）：快照先于关闭——updates 是纯值切片，Close 后可安全
// 流式下发。CloseSession 前置「非在册 runtime」判定（RouteSession 命中即
// 跳过）：会话仍在 manager 注册表（如 A3 起的绑定态归档）时其 store 归
// runtime 所有，绝不能随只读重放关闭；不在册的终态会话不常驻缓存（flock
// 与句柄随快照释放，SessionArchived 探测留下的缓存条目一并逐出）。
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
	found := ""
	for _, dir := range dirs {
		candidate, ok := archivePathForSession(dir, sessionID)
		if !ok {
			continue // 已前置校验，防御分支
		}
		if path == "" {
			path = candidate // 保留首个候选：全部未命中时按它给出 open archive 报错
		}
		if _, err := os.Stat(candidate); err == nil {
			found = candidate
			break
		}
	}
	if found != "" {
		view, err := session.OpenArchiveView(found)
		if err != nil {
			return nil, fmt.Errorf("agent: open archive %q: %w", sessionID, err)
		}
		return replayUpdates(view.ActiveView(), anchorSet(view.Checkpoints(0))), nil
	}
	// repo 层 per-id 分支（Plan A2b1 扩展，见函数头）：archive/ 目录未命中时
	// 会话可能以原位终态形态存在。
	if p.repo != nil {
		st, err := p.repo.Open(sessionID)
		if err == nil {
			if !st.Archived() {
				return nil, fmt.Errorf("agent: session %q is not archived", sessionID)
			}
			updates := replayUpdates(st.ActiveView(), anchorSet(st.Checkpoints(0)))
			if p.RouteSession(sessionID) == nil {
				if err := p.repo.CloseSession(sessionID); err != nil {
					slog.Warn("agent: archived store release failed", "session", sessionID, "err", err)
				}
			}
			return updates, nil
		}
	}
	if path != "" {
		// 存量语义兜底：archive 候选在但文件缺失、且 repo 层也无此会话——按旧
		// 报错形态给出 open archive 失败（TestAgentPlugin_ArchivedSessionHistoryErrors
		// 钉死的文案路径）。
		view, err := session.OpenArchiveView(path)
		if err != nil {
			return nil, fmt.Errorf("agent: open archive %q: %w", sessionID, err)
		}
		return replayUpdates(view.ActiveView(), anchorSet(view.Checkpoints(0))), nil
	}
	return nil, fmt.Errorf("agent: invalid archive session id %q", sessionID)
}

// currentSessionDir 返回当前会话的会话目录（P1-3 归档扫描基准）。目录两种形态：
//   - 新布局 <Session.Dir>/<store SessionID>/（Repository.Create 创建，目录名即
//     文件内 SessionID）；
//   - legacy 根槽 = Session.Dir 本身（兼容挂载，current.jsonl 直在根下）。
//
// 判定：会话 id 命中新布局目录（其下有 current.jsonl）取该目录；否则根下有
// legacy 槽取根；都没有返回空串。
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
