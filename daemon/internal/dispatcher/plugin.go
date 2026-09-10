// Package dispatcher 是 commit dispatcher 的 daemon 插件适配器：
// 把 ai-dispatch 的编排器（Task 状态机 × ACP worker）装进 plugin.Plugin，
// 提供 /dispatch /workers 命令族与审批入口。设计见 docs/dispatcher.md。
package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-dispatch"
	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-dispatch/task"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/plugin"
)

// DispatcherPlugin 是 dispatcher 插件入口：持有任务 store、编排器与运行中任务表。
type DispatcherPlugin struct {
	cfg   *runtimeconfig.Config
	paths runtimeconfig.Paths
	hub   *plugin.Hub

	store    *task.Store
	eventLog *task.EventLog
	orch     *dispatch.Orchestrator

	// running 追踪在跑的任务：taskID → cancel。Stop 时统一取消并等待退出。
	mu      sync.Mutex
	running map[string]context.CancelFunc
	wg      sync.WaitGroup

	// ACP 入口：socket listener、活跃连接与任务→连接的进度路由
	listener   net.Listener
	acpServers []*dispatch.Server
	acpRoutes  map[string][]acpRoute
}

type acpRoute struct {
	server    *dispatch.Server
	sessionID string
}

func (p *DispatcherPlugin) addACPRoute(taskID string, route acpRoute) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acpRoutes[taskID] = append(p.acpRoutes[taskID], route)
}

func (p *DispatcherPlugin) removeACPRoute(taskID string, route acpRoute) {
	p.mu.Lock()
	defer p.mu.Unlock()

	routes := p.acpRoutes[taskID]
	for index, current := range routes {
		if current == route {
			routes = append(routes[:index:index], routes[index+1:]...)
			break
		}
	}
	if len(routes) == 0 {
		delete(p.acpRoutes, taskID)
		return
	}
	p.acpRoutes[taskID] = routes
}

func (p *DispatcherPlugin) snapshotACPRoutes(taskID string) []acpRoute {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]acpRoute(nil), p.acpRoutes[taskID]...)
}

// NewPlugin 构造 dispatcher 插件。cfg.Dispatch.Enabled=false 时 main 不会注册本插件。
func NewPlugin(cfg *runtimeconfig.Config, paths runtimeconfig.Paths) *DispatcherPlugin {
	return &DispatcherPlugin{
		cfg:       cfg,
		paths:     paths,
		running:   make(map[string]context.CancelFunc),
		acpRoutes: make(map[string][]acpRoute),
	}
}

func (p *DispatcherPlugin) Name() string { return "dispatcher" }

func (p *DispatcherPlugin) Init(h *plugin.Hub) error {
	p.hub = h

	store, err := task.Open(filepath.Join(p.paths.DispatchDir, "tasks.jsonl"))
	if err != nil {
		return fmt.Errorf("dispatcher: open task store: %w", err)
	}
	p.store = store
	eventLog, err := task.OpenEventLog(filepath.Join(p.paths.DispatchDir, "task-events.jsonl"))
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("dispatcher: open task event log: %w", err)
	}
	p.eventLog = eventLog
	p.orch = dispatch.NewOrchestrator(store)
	p.orch.SetStatusListener(func(t *task.Task, from, to task.Status) {
		p.notifyStatus(t, from, to)
	})
	p.orch.SetProgress(func(taskID string, u protocol.SessionUpdateBody) {
		if err := p.eventLog.Append(task.TaskEvent{
			TaskID: taskID,
			Type:   task.EventUpdate,
			Update: updatePayload(u),
		}); err != nil {
			slog.Error("dispatch: append task update", "task", taskID, "error", err)
		}
		// ACP 提交方的任务：进度路由回对应连接；REPL 任务只留 debug 日志
		routes := p.snapshotACPRoutes(taskID)
		if len(routes) > 0 {
			for _, route := range routes {
				route.server.Update(route.sessionID, u)
			}
		} else {
			slog.Debug("dispatch progress", "task", taskID, "kind", u.SessionUpdate)
		}
		// statusbar 进度广播（addon 端 channel drop 兜底）
		if u.Content != nil {
			p.hub.Notify(plugin.Event{
				Type: plugin.EventType(task.EventTaskProgress),
				Payload: task.ProgressEvent{
					TaskID:  taskID,
					Kind:    u.SessionUpdate,
					Summary: task.Summarize(u.SessionUpdate, u.Content.Text, 48),
				},
			})
		}
	})
	p.resumeInterrupted()

	if err := h.RegisterCommand(plugin.Command{
		Name:        "/dispatch",
		Description: "commit dispatcher：加任务/查任务/审批，用法见 /dispatch help",
		Handler:     p.handleDispatch,
	}); err != nil {
		return err
	}
	return h.RegisterCommand(plugin.Command{
		Name:        "/workers",
		Description: "查看已配置的 ACP worker 清单",
		Handler:     p.handleWorkers,
	})
}

// resumeInterrupted 重启恢复：store 里停在 queued/dispatching/working 的任务
// 全部重新派发。working 任务带 WorkerSession 时 orchestrator 优先 session/load
// 续接 worker 上下文（能力协商失败则降级重跑）。
func (p *DispatcherPlugin) resumeInterrupted() {
	for _, t := range p.store.List() {
		if t.Status.Terminal() || t.Status == task.StatusAwaitingReview {
			continue // awaiting_review 等人审批，不需要重派
		}
		slog.Info("dispatcher: resuming interrupted task", "task", t.ID, "status", t.Status)
		p.launch(&t, p.runOptions(t.Worker)...)
	}
}

// SetOpener 注入 worker 连接方式（测试用）。
func (p *DispatcherPlugin) SetOpener(op dispatch.Opener) { p.orch.SetOpener(op) }

// Start 打开 ACP 入口（unix socket），接受外部 ACP Client 提交任务。
// socket 路径：<DispatchDir>/acp.sock。
func (p *DispatcherPlugin) Start() error {
	sockPath := filepath.Join(p.paths.DispatchDir, "acp.sock")
	_ = os.Remove(sockPath) // 残留 socket 会 bind 失败
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("dispatcher: listen %s: %w", sockPath, err)
	}
	p.listener = ln

	p.wg.Add(1)
	go p.acceptLoop()
	slog.Info("dispatcher: acp ingress listening", "socket", sockPath)
	return nil
}

func (p *DispatcherPlugin) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return // listener 已关闭
		}
		server := dispatch.ServeConn(conn, &acpIngress{plugin: p})
		p.mu.Lock()
		p.acpServers = append(p.acpServers, server)
		p.mu.Unlock()
	}
}

func (p *DispatcherPlugin) Stop() error {
	if p.listener != nil {
		_ = p.listener.Close()
	}
	p.mu.Lock()
	for _, server := range p.acpServers {
		_ = server.Close()
	}
	for _, cancel := range p.running {
		cancel()
	}
	p.mu.Unlock()
	p.wg.Wait()
	_ = os.Remove(filepath.Join(p.paths.DispatchDir, "acp.sock"))
	if p.eventLog != nil {
		if err := p.eventLog.Close(); err != nil {
			return err
		}
	}
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// acpIngress 是 ACP 入口侧的 TaskHandler：session/prompt 文本即任务，
// 复用与 REPL 完全相同的 orchestrator 派发链路。
type acpIngress struct {
	plugin *DispatcherPlugin

	mu       sync.Mutex
	sessions map[string]string // ACP sessionID → 提交方声明的工作目录
}

func (h *acpIngress) SetSession(sessionID, cwd string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessions == nil {
		h.sessions = make(map[string]string)
	}
	h.sessions[sessionID] = cwd
}

func (h *acpIngress) cwd(sessionID string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessions[sessionID]
}

// Run 处理一回合提交。除同步派发外支持控制指令（外部编排无需长连接）：
//
//	--detach <prompt>       异步入队即返回（提交方断连不影响任务）
//	--ls [status]           任务列表
//	--status <task_id>      任务详情快照
//	--approve <task_id>     审批通过（code 任务尝试 ff 合并）
//	--complete <task_id>    人工合并后收尾
//	--reject <task_id>      拒绝
//	--cancel <task_id>      取消
func (h *acpIngress) Run(ctx context.Context, sessionID, prompt string, rep dispatch.Reporter) (string, error) {
	p := h.plugin
	cwd := h.cwd(sessionID)
	if cwd == "" {
		return "", statusError("missing session cwd")
	}
	write := func(text string) { rep.MessageChunk(sessionID, text) }
	trim := strings.TrimSpace

	switch {
	case trim(prompt) == "" || strings.HasPrefix(trim(prompt), "#"):
		return "", statusError("empty prompt")
	case strings.HasPrefix(prompt, "--detach "):
		return h.detach(ctx, sessionID, cwd, trim(strings.TrimPrefix(prompt, "--detach ")), rep)
	case prompt == "--ls" || strings.HasPrefix(prompt, "--ls "):
		p.doList(trim(strings.TrimPrefix(prompt, "--ls")), write)
		return protocol.StopEndTurn, nil
	case strings.HasPrefix(prompt, "--status "):
		p.doShow(trim(strings.TrimPrefix(prompt, "--status ")), write)
		return protocol.StopEndTurn, nil
	case strings.HasPrefix(prompt, "--review "):
		return h.review(sessionID, trim(strings.TrimPrefix(prompt, "--review ")), rep)
	case strings.HasPrefix(prompt, "--events "):
		return h.events(ctx, sessionID, trim(strings.TrimPrefix(prompt, "--events ")), rep)
	case prompt == "--attach" || strings.HasPrefix(prompt, "--attach "):
		return h.attach(ctx, sessionID, trim(strings.TrimPrefix(prompt, "--attach")), rep)
	case strings.HasPrefix(prompt, "--approve "):
		p.doApprove(ctx, trim(strings.TrimPrefix(prompt, "--approve ")), write)
		return protocol.StopEndTurn, nil
	case strings.HasPrefix(prompt, "--complete "):
		p.doComplete(ctx, trim(strings.TrimPrefix(prompt, "--complete ")), write)
		return protocol.StopEndTurn, nil
	case strings.HasPrefix(prompt, "--reject "):
		p.doReject(ctx, trim(strings.TrimPrefix(prompt, "--reject ")), write)
		return protocol.StopEndTurn, nil
	case strings.HasPrefix(prompt, "--cancel "):
		p.doCancel(ctx, trim(strings.TrimPrefix(prompt, "--cancel ")), write)
		return protocol.StopEndTurn, nil
	case strings.HasPrefix(trim(prompt), "--"):
		return "", statusError(fmt.Sprintf("unknown directive %q (detach/ls/status/review/events/attach/approve/complete/reject/cancel)", trim(prompt)))
	}
	return h.dispatchSync(ctx, sessionID, cwd, prompt, rep)
}

type ingressEnvelope struct {
	Version    int             `json:"version"`
	Type       string          `json:"type"`
	Task       *reviewTaskDTO  `json:"task,omitempty"`
	Review     *reviewDTO      `json:"review,omitempty"`
	Event      *task.TaskEvent `json:"event,omitempty"`
	Cursor     int             `json:"cursor,omitempty"`
	NextCursor int             `json:"next_cursor,omitempty"`
	Status     string          `json:"status,omitempty"`
}

func (h *acpIngress) review(sessionID, id string, rep dispatch.Reporter) (string, error) {
	review, ok := h.plugin.review(id)
	if !ok {
		return "", statusError(fmt.Sprintf("task %q not found", id))
	}
	if err := writeIngressEnvelope(rep, sessionID, ingressEnvelope{
		Version: 1, Type: "review", Task: &review.Task, Review: &review,
	}); err != nil {
		return "", err
	}
	return protocol.StopEndTurn, nil
}

func (h *acpIngress) events(ctx context.Context, sessionID, args string, rep dispatch.Reporter) (string, error) {
	id, cursor, err := parseEventsArgs(args)
	if err != nil {
		return "", statusError(err.Error())
	}
	if _, ok := h.plugin.store.Get(id); !ok {
		return "", statusError(fmt.Sprintf("task %q not found", id))
	}

	history, historyCursor, live, unsubscribe := h.plugin.eventLog.Subscribe(id)
	defer unsubscribe()
	if cursor > historyCursor {
		return "", statusError(fmt.Sprintf("cursor %d exceeds task event history", cursor))
	}
	if err := h.writeEvents(rep, sessionID, id, cursor, history[cursor:]); err != nil {
		return "", err
	}
	cursor = historyCursor
	current, ok := h.plugin.store.Get(id)
	if !ok {
		return "", statusError(fmt.Sprintf("task %q not found", id))
	}
	if tailComplete(current.Status) {
		return h.completeEvents(rep, sessionID, cursor, current.Status)
	}

	for {
		select {
		case <-ctx.Done():
			return protocol.StopEndTurn, nil
		case <-live:
			events, next := h.plugin.eventLog.EventsAfter(id, cursor)
			if err := h.writeEvents(rep, sessionID, id, cursor, events); err != nil {
				return "", err
			}
			cursor = next
			for _, event := range events {
				if event.Type == task.EventStatus && tailComplete(event.To) {
					return h.completeEvents(rep, sessionID, cursor, event.To)
				}
			}
		}
	}
}

func (h *acpIngress) attach(ctx context.Context, sessionID, args string, rep dispatch.Reporter) (string, error) {
	id, err := parseAttachArgs(args)
	if err != nil {
		return "", statusError(err.Error())
	}
	if _, ok := h.plugin.store.Get(id); !ok {
		return "", statusError(fmt.Sprintf("task %q not found", id))
	}

	history, cursor, live, unsubscribe := h.plugin.eventLog.Subscribe(id)
	defer unsubscribe()
	h.writeAttachedUpdates(id, 0, rep, sessionID, history, false)
	current, ok := h.plugin.store.Get(id)
	if !ok {
		return "", statusError(fmt.Sprintf("task %q not found", id))
	}
	if tailComplete(current.Status) {
		return protocol.StopEndTurn, nil
	}

	for {
		select {
		case <-ctx.Done():
			return protocol.StopEndTurn, nil
		case <-live:
			events, next := h.plugin.eventLog.EventsAfter(id, cursor)
			complete := h.writeAttachedUpdates(id, cursor, rep, sessionID, events, true)
			cursor = next
			if complete {
				return protocol.StopEndTurn, nil
			}
		}
	}
}

func parseAttachArgs(args string) (string, error) {
	parts := strings.Fields(args)
	if len(parts) != 1 {
		return "", fmt.Errorf("usage: --attach <task_id>")
	}
	return parts[0], nil
}

func (h *acpIngress) writeAttachedUpdates(taskID string, cursor int, rep dispatch.Reporter, sessionID string, events []task.TaskEvent, completeOnStatus bool) bool {
	complete := false
	for index, event := range events {
		if event.Type == task.EventStatus {
			complete = complete || completeOnStatus && tailComplete(event.To)
			continue
		}
		if event.Type != task.EventUpdate {
			continue
		}

		var update protocol.SessionUpdateBody
		if err := json.Unmarshal(event.Update, &update); err != nil || update.SessionUpdate == "" {
			if err == nil {
				err = fmt.Errorf("missing sessionUpdate")
			}
			slog.Warn("dispatch: skip malformed task update", "task", taskID, "cursor", cursor+index, "error", err)
			continue
		}
		rep.Update(sessionID, update)
	}
	return complete
}

func parseEventsArgs(args string) (string, int, error) {
	parts := strings.Fields(args)
	if len(parts) == 0 || len(parts) > 2 {
		return "", 0, fmt.Errorf("usage: --events <task_id> [cursor]")
	}
	if len(parts) == 1 {
		return parts[0], 0, nil
	}
	cursor, err := strconv.Atoi(parts[1])
	if err != nil || cursor < 0 {
		return "", 0, fmt.Errorf("invalid event cursor %q", parts[1])
	}
	return parts[0], cursor, nil
}

func (h *acpIngress) writeEvents(rep dispatch.Reporter, sessionID, id string, cursor int, events []task.TaskEvent) error {
	for index, event := range events {
		event.TaskID = id
		if err := writeIngressEnvelope(rep, sessionID, ingressEnvelope{
			Version: 1, Type: "event", Event: &event, Cursor: cursor + index + 1,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (h *acpIngress) completeEvents(rep dispatch.Reporter, sessionID string, cursor int, status task.Status) (string, error) {
	if err := writeIngressEnvelope(rep, sessionID, ingressEnvelope{
		Version: 1, Type: "complete", NextCursor: cursor, Status: string(status),
	}); err != nil {
		return "", err
	}
	return protocol.StopEndTurn, nil
}

func writeIngressEnvelope(rep dispatch.Reporter, sessionID string, envelope ingressEnvelope) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return statusError(fmt.Sprintf("encode ingress envelope: %v", err))
	}
	rep.MessageChunk(sessionID, string(data))
	return nil
}

// newTaskFromIngress 按 cwd 判型创建任务（不落盘，orchestrator 首次 save 负责）。
func (p *DispatcherPlugin) newTaskFromIngress(cwd, worker, prompt string) *task.Task {
	kind := task.KindCode
	if !dispatch.DetectGit(context.Background(), cwd) {
		kind = task.KindGeneral
	}
	now := time.Now()
	return &task.Task{
		ID: task.NewID(), Source: "acp", Kind: kind, Prompt: prompt,
		Repo: cwd, Worker: worker, Status: task.StatusQueued,
		CreatedAt: now, UpdatedAt: now,
	}
}

// detach 异步入队：注册进度路由的副本无效（无人在连接上看），任务照常广播
// task_status/task_progress 事件；prompt 立即返回 task ID，提交方可安全断连，
// 之后用 --status 查询、--approve/--reject 审批（补偿接口）。
func (h *acpIngress) detach(ctx context.Context, sessionID, cwd, prompt string, rep dispatch.Reporter) (string, error) {
	p := h.plugin
	worker, ok := p.resolveWorker("", prompt, func(string) {})
	if !ok {
		return "", statusError("no worker configured")
	}
	t := p.newTaskFromIngress(cwd, worker, prompt)
	p.audit("acp-submit", t.ID, cwd+" (detach)")
	p.launch(t, p.runOptions(worker)...)
	rep.MessageChunk(sessionID, fmt.Sprintf("任务 %s 已异步入队（%s），可用 --status %s 查询", t.ID, t.Kind, t.ID))
	return protocol.StopEndTurn, nil
}

// dispatchSync 同步派发：连接存续期间跑完任务，进度实时回流。
func (h *acpIngress) dispatchSync(ctx context.Context, sessionID, cwd, prompt string, rep dispatch.Reporter) (string, error) {
	p := h.plugin
	worker, ok := p.resolveWorker("", prompt, func(string) {})
	if !ok {
		return "", statusError("no worker configured")
	}
	t := p.newTaskFromIngress(cwd, worker, prompt)

	// 进度路由：orchestrator 的全局 ProgressFunc 按 taskID 转回本连接
	if server, ok := rep.(*dispatch.Server); ok {
		route := acpRoute{server: server, sessionID: sessionID}
		p.addACPRoute(t.ID, route)
		defer p.removeACPRoute(t.ID, route)
	}

	p.audit("acp-submit", t.ID, cwd)
	outcome, err := p.orch.Run(ctx, t, p.workerSpec(worker), p.runOptions(worker)...)
	if err != nil {
		return "", err
	}
	if len(outcome.Task.Commits) > 0 {
		rep.MessageChunk(sessionID, fmt.Sprintf("任务完成，产出 %d 个 commit，等待审批（--approve %s）",
			len(outcome.Task.Commits), t.ID))
	}
	return outcome.StopReason, nil
}

// ---- /dispatch ----

func (p *DispatcherPlugin) handleDispatch(ctx *plugin.Context) error {
	args := ctx.Args
	if len(args) == 0 || args[0] == "help" {
		p.writeUsage(ctx)
		return nil
	}

	switch args[0] {
	case "ls":
		return p.handleList(ctx, args[1:])
	case "show":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch show <task_id>\n")
			return nil
		}
		return p.handleShow(ctx, args[1])
	case "tail":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch tail <task_id>\n")
			return nil
		}
		return p.handleTail(ctx, args[1])
	case "review":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch review <task_id>\n")
			return nil
		}
		return p.handleReview(ctx, args[1])
	case "approve":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch approve <task_id>\n")
			return nil
		}
		return p.handleApprove(ctx, args[1])
	case "complete":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch complete <task_id>   ← 人工合并完成后收尾\n")
			return nil
		}
		return p.handleComplete(ctx, args[1])
	case "reject":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch reject <task_id>\n")
			return nil
		}
		return p.handleReject(ctx, args[1])
	case "cancel":
		if len(args) < 2 {
			ctx.Writer("用法: /dispatch cancel <task_id>\n")
			return nil
		}
		return p.handleCancel(ctx, args[1])
	}

	// 加任务：[/worker|--general] 前缀 + prompt；未显式指定 worker 时走关键词路由
	worker := ""
	general := false
	promptArgs := args
	for len(promptArgs) > 0 {
		arg := promptArgs[0]
		if strings.HasPrefix(arg, "@") && len(arg) > 1 {
			worker = arg[1:]
			promptArgs = promptArgs[1:]
			continue
		}
		if arg == "--general" {
			general = true
			promptArgs = promptArgs[1:]
			continue
		}
		break
	}
	if len(promptArgs) == 0 {
		p.writeUsage(ctx)
		return nil
	}
	return p.addTask(ctx, worker, general, strings.Join(promptArgs, " "))
}

// resolveWorker 派发 worker：显式指定 > 关键词路由 > default_worker。
func (p *DispatcherPlugin) resolveWorker(explicit, prompt string, write func(string)) (string, bool) {
	if explicit != "" {
		if _, ok := p.cfg.Dispatch.Worker(explicit); !ok {
			write(fmt.Sprintf("✘ worker %q 未配置（/workers 查看）\n", explicit))
			return "", false
		}
		return explicit, true
	}
	if name, ok := p.cfg.Dispatch.MatchWorker(prompt); ok {
		return name, true
	}
	name, err := p.cfg.Dispatch.ResolveDefaultWorker()
	if err != nil {
		write("✘ " + err.Error() + "\n")
		return "", false
	}
	return name, true
}

func (p *DispatcherPlugin) writeUsage(ctx *plugin.Context) {
	ctx.Writer(`用法:
  /dispatch <prompt>              ← 当前目录是 git 仓库 → code 任务（worktree 隔离）；否则 general 任务
  /dispatch --general <prompt>    ← 强制 general 任务
  /dispatch @claude <prompt>      ← 指定 worker
  /dispatch ls [status]           ← 任务列表
  /dispatch show <id>             ← 任务详情
  /dispatch tail <id>             ← 实时查看任务输出
  /dispatch review <id>           ← 查看可验收的任务产物
  /dispatch approve <id>          ← 审批通过（code 任务尝试 ff 合并）
  /dispatch complete <id>         ← 人工合并完成后收尾（清 worktree、标记 done）
  /dispatch reject <id>           ← 拒绝（弃置任务产物）
  /dispatch cancel <id>           ← 取消（运行中任务会中断 worker）
`)
}

func (p *DispatcherPlugin) addTask(ctx *plugin.Context, explicitWorker string, general bool, prompt string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	worker, ok := p.resolveWorker(explicitWorker, prompt, ctx.Writer)
	if !ok {
		return nil
	}
	kind := task.KindCode
	if general || !dispatch.DetectGit(ctx.Ctx, cwd) {
		kind = task.KindGeneral
	}

	p.mu.Lock()
	maxParallel := p.cfg.Dispatch.MaxParallel
	if maxParallel <= 0 {
		maxParallel = 1
	}
	if len(p.running) >= maxParallel {
		p.mu.Unlock()
		ctx.Writer(fmt.Sprintf("✘ 已有 %d 个任务在运行（max_parallel=%d），稍后再派\n", len(p.running), maxParallel))
		return nil
	}
	p.mu.Unlock()

	now := time.Now()
	t := &task.Task{
		ID:        task.NewID(),
		Source:    "repl",
		Kind:      kind,
		Prompt:    prompt,
		Repo:      cwd,
		Worker:    worker,
		Status:    task.StatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	p.launch(t, p.runOptions(worker)...)
	// 入队落盘由 orchestrator 首次 save 完成（并触发 queued 状态事件）
	ctx.Writer(fmt.Sprintf("⏳ 任务 %s 已入队（%s → %s worker）\n", t.ID, kind, worker))
	return nil
}

// notifyStatus 广播状态迁移事件（save diff 与插件侧流转共用）。
func (p *DispatcherPlugin) notifyStatus(t *task.Task, from, to task.Status) {
	if from == to {
		return
	}
	if err := p.eventLog.Append(task.TaskEvent{
		TaskID: t.ID,
		Type:   task.EventStatus,
		From:   from,
		To:     to,
	}); err != nil {
		slog.Error("dispatch: append task status", "task", t.ID, "error", err)
	}
	p.hub.Notify(plugin.Event{
		Type: plugin.EventType(task.EventTaskStatus),
		Payload: task.StatusEvent{
			TaskID: t.ID, From: from, To: to, Kind: t.Kind, Worker: t.Worker,
		},
	})
}

// updateTask 插件侧流转（approve/reject/cancel）的落盘 + 事件广播。
func (p *DispatcherPlugin) updateTask(t *task.Task) error {
	old, _ := p.store.Get(t.ID)
	if err := p.store.Update(t); err != nil {
		return err
	}
	p.notifyStatus(t, old.Status, t.Status)
	return nil
}

// runOptions 按 worker 配置生成派发选项：无人值守权限应答策略。
func (p *DispatcherPlugin) runOptions(worker string) []dispatch.RunOption {
	w, ok := p.cfg.Dispatch.Worker(worker)
	if !ok || w.OnPermission != "allow" {
		return nil // 默认拒绝
	}
	return []dispatch.RunOption{dispatch.WithPermissionPolicy(
		func(_ context.Context, req protocol.PermissionRequest) (string, error) {
			for _, opt := range req.Options {
				if strings.Contains(opt.Kind, "allow") {
					return opt.OptionID, nil
				}
			}
			if len(req.Options) > 0 {
				return req.Options[0].OptionID, nil
			}
			return "", fmt.Errorf("worker requested permission with no options")
		},
	)}
}

// launch 后台执行任务。orchestrator 负责状态机流转与 worktree 清理。
func (p *DispatcherPlugin) launch(t *task.Task, opts ...dispatch.RunOption) {
	runCtx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.running[t.ID] = cancel
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.running, t.ID)
			p.mu.Unlock()
		}()

		spec := p.workerSpec(t.Worker)
		outcome, err := p.orch.Run(runCtx, t, spec, opts...)
		if err != nil {
			slog.Warn("dispatch task ended", "task", t.ID, "err", err)
			return
		}
		slog.Info("dispatch task awaiting review", "task", t.ID, "stop", outcome.StopReason,
			"commits", len(outcome.Task.Commits))
	}()
}

func (p *DispatcherPlugin) workerSpec(name string) dispatch.WorkerSpec {
	if w, ok := p.cfg.Dispatch.Worker(name); ok {
		return dispatch.WorkerSpec{Name: w.Name, Command: w.Command, Args: w.Args}
	}
	return dispatch.WorkerSpec{Name: name}
}

func (p *DispatcherPlugin) handleList(ctx *plugin.Context, args []string) error {
	filter := ""
	if len(args) > 0 {
		filter = args[0]
	}
	p.doList(filter, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) doList(filter string, write func(string)) {
	tasks := p.store.List()
	if len(tasks) == 0 {
		write("（暂无任务）\n")
		return
	}
	for _, t := range tasks {
		if filter != "" && string(t.Status) != filter {
			continue
		}
		write(fmt.Sprintf("%s  %-15s %-8s %-8s %s\n", t.ID, t.Status, t.Kind, t.Worker, truncate(t.Prompt, 48)))
	}
}

func (p *DispatcherPlugin) handleShow(ctx *plugin.Context, id string) error {
	p.doShow(id, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) doShow(id string, write func(string)) {
	t, ok := p.store.Get(id)
	if !ok {
		write(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return
	}
	data, _ := json.MarshalIndent(t, "", "  ")
	write(string(data) + "\n")
}

func (p *DispatcherPlugin) handleTail(ctx *plugin.Context, id string) error {
	if _, ok := p.store.Get(id); !ok {
		ctx.Writer(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return nil
	}

	history, cursor, live, unsubscribe := p.eventLog.Subscribe(id)
	defer unsubscribe()
	for _, event := range history {
		writeTaskEvent(ctx.Writer, event)
	}
	current, ok := p.store.Get(id)
	if !ok || tailComplete(current.Status) || historyHasCompletion(history) {
		if ok {
			writeTaskError(ctx.Writer, current)
		}
		return nil
	}

	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		case <-live:
			events, next := p.eventLog.EventsAfter(id, cursor)
			cursor = next
			for _, event := range events {
				writeTaskEvent(ctx.Writer, event)
				if event.Type == task.EventStatus && tailComplete(event.To) {
					if current, ok := p.store.Get(id); ok {
						writeTaskError(ctx.Writer, current)
					}
					return nil
				}
			}
		}
	}
}

func historyHasCompletion(history []task.TaskEvent) bool {
	for _, event := range history {
		if event.Type == task.EventStatus && tailComplete(event.To) {
			return true
		}
	}
	return false
}

func updatePayload(update protocol.SessionUpdateBody) json.RawMessage {
	if len(update.Raw) > 0 {
		return append(json.RawMessage(nil), update.Raw...)
	}
	data, err := json.Marshal(update)
	if err != nil {
		slog.Error("dispatch: encode task update", "kind", update.SessionUpdate, "error", err)
		return nil
	}
	return data
}

func tailComplete(status task.Status) bool {
	return status == task.StatusAwaitingReview || status.Terminal()
}

func writeTaskError(write func(string), t task.Task) {
	if t.Error != "" {
		write("✘ " + t.Error + "\n")
	}
}

func writeTaskEvent(write func(string), event task.TaskEvent) {
	if event.Type == task.EventStatus {
		write(fmt.Sprintf("[%s → %s]\n", event.From, event.To))
		return
	}

	var update protocol.SessionUpdateBody
	if err := json.Unmarshal(event.Update, &update); err != nil {
		write("[invalid update] " + string(event.Update) + "\n")
		return
	}
	switch update.SessionUpdate {
	case protocol.UpdateAgentMessageChunk:
		if update.Content != nil {
			write(update.Content.Text)
		}
	case protocol.UpdateAgentThoughtChunk:
		if update.Content != nil {
			write("[thought] " + update.Content.Text)
		}
	case protocol.UpdateToolCall, protocol.UpdateToolCallUpdate:
		write(fmt.Sprintf("[tool %s %s %s]\n", update.Title, update.Status, update.ToolCallID))
	default:
		write(fmt.Sprintf("[%s] %s\n", update.SessionUpdate, event.Update))
	}
}

// handleApprove 审批通过：code 任务尝试 ff 合并（失败则提示走人工 complete），
// general 任务直接标记完成。
func (p *DispatcherPlugin) handleApprove(ctx *plugin.Context, id string) error {
	p.doApprove(ctx.Ctx, id, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) doApprove(execCtx context.Context, id string, write func(string)) {
	t, ok := p.store.Get(id)
	if !ok {
		write(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return
	}
	if t.Status != task.StatusAwaitingReview {
		write(fmt.Sprintf("✘ 任务 %s 状态为 %s，仅 awaiting_review 可审批\n", id, t.Status))
		return
	}
	p.audit("approve", t.ID, "")

	if t.Kind == task.KindGeneral {
		if err := t.Transition(task.StatusDone); err != nil {
			write(fmt.Sprintf("✘ %v\n", err))
			return
		}
		if err := p.updateTask(&t); err != nil {
			write(fmt.Sprintf("✘ %v\n", err))
			return
		}
		write(fmt.Sprintf("✔ 任务 %s 已完成\n", id))
		return
	}

	// code 任务：ff 合并进派发时的分支头
	if out, err := gitMergeFF(t.Repo, t.Branch); err != nil {
		write(fmt.Sprintf("✘ ff 合并失败（可能有分叉或冲突）：\n%s\n请人工合并后执行 /dispatch complete %s\n", strings.TrimSpace(out), id))
		return
	}
	if err := p.finishCodeTask(execCtx, &t, "approved"); err != nil {
		write(fmt.Sprintf("✘ %v\n", err))
		return
	}
	write(fmt.Sprintf("✔ 任务 %s 已合并并完成\n", id))
}

// handleComplete 人工合并后的收尾：清 worktree + 分支，标记 done。
func (p *DispatcherPlugin) handleComplete(ctx *plugin.Context, id string) error {
	p.doComplete(ctx.Ctx, id, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) doComplete(execCtx context.Context, id string, write func(string)) {
	snapshot, ok := p.store.Get(id)
	if !ok || snapshot.Kind != task.KindCode {
		write(fmt.Sprintf("✘ code 任务 %s 不存在\n", id))
		return
	}
	t := snapshot
	if t.Status != task.StatusAwaitingReview {
		write(fmt.Sprintf("✘ 任务 %s 状态为 %s，仅 awaiting_review 可收尾\n", id, t.Status))
		return
	}
	p.audit("complete", t.ID, "")
	if err := p.finishCodeTask(execCtx, &t, "merged-manually"); err != nil {
		write(fmt.Sprintf("✘ %v\n", err))
		return
	}
	write(fmt.Sprintf("✔ 任务 %s 已收尾完成\n", id))
}

func (p *DispatcherPlugin) finishCodeTask(execCtx context.Context, t *task.Task, via string) error {
	if err := t.Transition(task.StatusMerging); err != nil {
		return err
	}
	if err := p.updateTask(t); err != nil {
		return err
	}
	_ = dispatch.RemoveWorktree(execCtx, t.Repo, t.Worktree, t.Branch, true)
	if err := t.Transition(task.StatusDone); err != nil {
		return err
	}
	p.audit("done", t.ID, via)
	return p.updateTask(t)
}

// handleReject 拒绝任务：code 任务弃置 worktree 与分支。
func (p *DispatcherPlugin) handleReject(ctx *plugin.Context, id string) error {
	p.doReject(ctx.Ctx, id, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) doReject(execCtx context.Context, id string, write func(string)) {
	t, ok := p.store.Get(id)
	if !ok {
		write(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return
	}
	if t.Status != task.StatusAwaitingReview {
		write(fmt.Sprintf("✘ 任务 %s 状态为 %s，仅 awaiting_review 可拒绝\n", id, t.Status))
		return
	}
	if err := t.Transition(task.StatusRejected); err != nil {
		write(fmt.Sprintf("✘ %v\n", err))
		return
	}
	if err := p.updateTask(&t); err != nil {
		write(fmt.Sprintf("✘ %v\n", err))
		return
	}
	if t.Kind == task.KindCode && t.Worktree != "" {
		_ = dispatch.RemoveWorktree(execCtx, t.Repo, t.Worktree, t.Branch, true)
	}
	p.audit("reject", t.ID, "")
	write(fmt.Sprintf("✘ 任务 %s 已拒绝，产物已清理\n", id))
}

// handleCancel 取消任务：运行中的取消 ctx（orchestrator 落 cancelled），
// 排队/待审的直接流转。
func (p *DispatcherPlugin) handleCancel(ctx *plugin.Context, id string) error {
	p.doCancel(ctx.Ctx, id, ctx.Writer)
	return nil
}

func (p *DispatcherPlugin) doCancel(execCtx context.Context, id string, write func(string)) {
	t, ok := p.store.Get(id)
	if !ok {
		write(fmt.Sprintf("✘ 任务 %s 不存在\n", id))
		return
	}
	if t.Status.Terminal() {
		write(fmt.Sprintf("✘ 任务 %s 已是终态 %s\n", id, t.Status))
		return
	}

	p.mu.Lock()
	cancel, running := p.running[id]
	p.mu.Unlock()
	if running {
		p.audit("cancel", t.ID, "running")
		cancel()
		write(fmt.Sprintf("⏳ 任务 %s 取消中（worker 中断后落 cancelled）\n", id))
		return
	}
	if err := t.Transition(task.StatusCancelled); err != nil {
		write(fmt.Sprintf("✘ 任务 %s 无法取消: %v\n", id, err))
		return
	}
	if err := p.updateTask(&t); err != nil {
		write(fmt.Sprintf("✘ %v\n", err))
		return
	}
	if t.Kind == task.KindCode && t.Worktree != "" {
		_ = dispatch.RemoveWorktree(execCtx, t.Repo, t.Worktree, t.Branch, true)
	}
	p.audit("cancel", t.ID, "queued")
	write(fmt.Sprintf("✘ 任务 %s 已取消\n", id))
}

// ---- /workers ----

func (p *DispatcherPlugin) handleWorkers(ctx *plugin.Context) error {
	cfg := p.cfg.Dispatch
	ctx.Writer(fmt.Sprintf("dispatcher: enabled=%v max_parallel=%d\n", cfg.Enabled, orOne(cfg.MaxParallel)))
	defaultWorker, _ := cfg.ResolveDefaultWorker()
	for _, w := range cfg.Workers {
		marker := "  "
		if w.Name == defaultWorker {
			marker = "→ "
		}
		ctx.Writer(fmt.Sprintf("%s%-12s %s %s\n", marker, w.Name, w.Command, strings.Join(w.Args, " ")))
	}
	return nil
}

// ---- 审计 ----

// audit 追加一条 dispatcher 决策记录到 audit/dispatch.jsonl。
func (p *DispatcherPlugin) audit(action, taskID, detail string) {
	if p.paths.AuditDir == "" {
		return
	}
	if err := os.MkdirAll(p.paths.AuditDir, 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(p.paths.AuditDir, "dispatch.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	record, _ := json.Marshal(map[string]string{
		"ts": time.Now().UTC().Format(time.RFC3339), "action": action, "task": taskID, "detail": detail,
	})
	_, _ = f.Write(append(record, '\n'))
}

// ---- helpers ----

// gitMergeFF 在 repo 中 fast-forward 合并分支。
func gitMergeFF(repo, branch string) (string, error) {
	cmd := exec.Command("git", "-C", repo, "merge", "--ff-only", branch)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func truncate(s string, n int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}

func orOne(n int) int {
	if n <= 0 {
		return 1
	}
	return n
}

// statusError 生成 -32000 RPC 错误（ai-dispatch 的 protocol.Conn 会识别）。
func statusError(message string) error {
	return &protocol.RPCError{Code: -32000, Message: message}
}
