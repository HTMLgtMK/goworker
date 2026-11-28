package acp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	aisession "github.com/tinguo/goworker/ai-session"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Evaluator 执行一条用户输入（装配层通常注入 Engine.Eval）。
type Evaluator = func(ctx *model.Context, input string) error

// ProviderWriteAuthorizer decides whether one ACP connection may mutate the
// daemon-wide provider registry. It is independent of session control leases.
// The authorizer may be called concurrently for accepted connections. A nil
// authorizer denies provider writes.
type ProviderWriteAuthorizer func(connectionID string) bool

// FrontendHandler 实现 daemon 的 ACP 会话操作。每个连接使用独立 handler，
// 会话选择通过连接级 Binding 路由；BoundSessionSource 不可用时兼容单会话后端。
// SessionGate 由装配层创建并由所有 handler 共享，只保护会话管理操作；prompt
// 的同会话串行化由 runtime gate 负责。
type FrontendHandler struct {
	evaluate Evaluator
	commands func() []model.Command
	sessions model.SessionSource
	// gate serializes shared session-management mutations across all connections.
	gate *SessionGate

	// binding selects the session for this connection and is injected into the
	// evaluation context. Changing it does not mutate other connections or runs.
	binding agentcontrol.SessionBinding

	// binder 是连接级会话绑定登记面(工厂内对 sessions type-assert
	// model.ConnectionBinder 获得,RunControlSource 同款先例)。nil = 后端无
	// 生命周期登记能力(简化壳/测试桩):Set 照常、登记跳过,行为与单会话降级一致。
	binder model.ConnectionBinder

	// multi is optional: without it, session/new is unsupported and session/load uses the
	// guarded single-current-session fallback.
	multi model.BoundSessionSource
	// titleSource is optional so backends without fenced rename retain their existing surface.
	titleSource model.SessionTitleSource
	titleReader model.SessionTitleReader
	// titleGenerator is optional: only backends that can create a title advertise title/generate.
	titleGenerator model.SessionTitleGeneratorSource

	// connectionID 是本连接的稳定句柄(工厂内 crypto/rand 生成):x-goworker
	// control lease 的持有者寻址面,连接退出时经 Close 归还(订阅泵清理与能力
	// 解绑同钩子)。仅在本连接内自洽,跨连接无意义。
	connectionID string
	// providerWriteAllowed is granted once per connection by the transport's
	// injected authorizer. It never follows from session lease ownership/revision.
	providerWriteAllowed bool
	// runs 是 x-goworker 扩展协议的消费面(model.RunControlSource,工厂内
	// 对 sessions type-assert 获得)。nil = 后端无 run control 能力(纯
	// SessionSource 插件/测试桩):全部 x-goworker 方法诚实报错,与 loader/lister
	// 降级同型。
	runs model.RunControlSource
	// events 是 x-goworker 订阅泵的下行通知面:registerXGWMethods 注入本
	// 连接的 *aiacp.Server。nil = 无 RunControlSource，订阅方法不会注册。
	events xgwEventSink
	// xgwRegistry is the per-connection source of truth for registered extension
	// methods and notifications; capability discovery snapshots the same registry.
	xgwRegistry *xgwRegistry

	// subs 保存本连接每个会话的当前订阅。替换、detach 和 Close 都在 h.mu 下
	// 更新表；泵退出按句柄身份清理，避免旧泵删除新订阅。
	// 锁序：attach/detach/Close 可在持有 h.mu 时获取 source 的会话 guard，方向
	// 仅为 h.mu → session guard。source 不回调 handler、不获取 h.mu，避免锁环。
	subs map[string]*xgwSubscription

	// closed 是连接生命周期屏障:Close 在 h.mu 内置位,此后 attach 的提交
	// 段一律拒绝——Close 返回之后不会再发生新的订阅提交或 control lease 获取
	// (不复活订阅与 lease;Close/Attach 竞态:attach 的提交段与
	// Close 的「closed 置位 + 清表」同以 h.mu 串行,二者必有一个全序)。
	closed           bool
	closedForBinding atomic.Bool

	// Device tools are negotiated once, after initialize has been written. The
	// descriptor snapshot is immutable after toolsReady closes.
	toolsReady        chan struct{}
	toolsReadyOnce    sync.Once
	initializeOnce    sync.Once
	deviceCaller      model.DeviceCaller
	deviceDescriptors []acpworker.DeviceToolDescriptor
	deviceProbeCancel context.CancelFunc
	mobileClient      bool

	// selectionCommitHook 仅供测试，在管理门禁释放后、会话路由提交前注入并发交错。
	selectionCommitHook func(sessionID string)

	// preCommitHook 仅供测试，在订阅准备完成、提交锁获取前注入 Close/detach
	// 竞态。生产环境保持 nil；测试须在无并发 attach 时设置或清除。
	preCommitHook func()

	mu           sync.Mutex
	readOnly     map[string]struct{}      // 只读集合：归档 load 记入，prompt 诚实报错
	promptRoutes map[string]*sessionRoute // canonical session IDs with a live, promptable binding
	legacyID     string                   // non-multi backends may expose only one ACP session per connection
	active       string                   // latest declared canonical ID; diagnostics only, never a routing fallback
}

type sessionRoute struct{}

var (
	_ aiacp.SessionHandler       = (*FrontendHandler)(nil)
	_ aiacp.SessionCreator       = (*frontendSessionLifecycle)(nil)
	_ aiacp.SessionLoader        = (*frontendSessionLifecycle)(nil)
	_ aiacp.SessionLister        = (*FrontendHandler)(nil)
	_ aiacp.SessionModesProvider = (*FrontendHandler)(nil)
)

type frontendSessionLifecycle struct{ handler *FrontendHandler }

func (a *frontendSessionLifecycle) CreateSession(ctx context.Context, req protocol.NewSessionRequest, reporter aiacp.Reporter) (aisession.ID, error) {
	return a.handler.CreateManagedSession(ctx, req, reporter)
}

func (a *frontendSessionLifecycle) LoadSession(ctx context.Context, id aisession.ID, req protocol.LoadSessionRequest, reporter aiacp.Reporter) error {
	return a.handler.loadCanonicalSession(ctx, id, req, reporter)
}

// nil (session capabilities degrade honestly; command completion is omitted); evaluate and the
// shared SessionGate are required. Each handler gets a connection ID and optional RunControlSource
// capability; nil session sources skip the assertion. Provider writes are denied unless an
// authorizer is explicitly injected and grants that connection.
func NewFrontendHandlerFactory(evaluate Evaluator, commands func() []model.Command, sessions model.SessionSource, gate *SessionGate, authorizers ...ProviderWriteAuthorizer) HandlerFactory {
	var authorizeProviderWrites ProviderWriteAuthorizer
	if len(authorizers) > 1 {
		panic("acp: at most one provider write authorizer may be supplied")
	}
	if len(authorizers) == 1 {
		authorizeProviderWrites = authorizers[0]
	}
	return func() aiacp.SessionHandler {
		connectionID := newConnectionID()
		providerWriteAllowed := authorizeProviderWrites != nil && authorizeProviderWrites(connectionID)
		handler := &FrontendHandler{
			evaluate:             evaluate,
			commands:             commands,
			sessions:             sessions,
			gate:                 gate,
			connectionID:         connectionID,
			providerWriteAllowed: providerWriteAllowed,
			toolsReady:           make(chan struct{}),
		}
		if sessions != nil {
			// RunControlSource deliberately excludes host-only synchronous methods, so ACP
			// run/start and run/cancel cannot bypass the connection-lease fencing path.
			if runs, ok := sessions.(model.RunControlSource); ok {
				handler.runs = runs
			}
			// 连接级会话绑定登记面(可选能力,缺省 nil 时 CreateSession/LoadSession
			// 只做 Binding Set、不登记)。
			if binder, ok := sessions.(model.ConnectionBinder); ok {
				handler.binder = binder
			}
			// 会话面多会话化:session/new 真新建与 session/load 的 reload/重开/归档
			// 只读判定面(可选能力,缺省 nil 时落单活动降级路径)。
			if multi, ok := sessions.(model.BoundSessionSource); ok {
				handler.multi = multi
			}
			if reader, ok := sessions.(model.SessionTitleReader); ok {
				handler.titleReader = reader
			}
			if titles, ok := sessions.(model.SessionTitleSource); ok {
				handler.titleSource = titles
			}
			if generator, ok := sessions.(model.SessionTitleGeneratorSource); ok {
				handler.titleGenerator = generator
			}
		}
		return handler
	}
}

// InitializeReady negotiates x-device/tools exactly once after initialize's
// response has reached the transport. No authentication is implied by this state.
func (h *FrontendHandler) InitializeReady(ctx context.Context, reporter aiacp.Reporter) {
	if capable, ok := reporter.(interface {
		ClientCapabilities() protocol.ClientCapabilities
	}); ok {
		h.mu.Lock()
		h.mobileClient = isMobileClient(capable.ClientCapabilities())
		h.mu.Unlock()
	}
	h.initializeOnce.Do(func() { h.initializeDeviceTools(ctx, reporter) })
}

func (h *FrontendHandler) mobileClientEnabled() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mobileClient
}

func (h *FrontendHandler) initializeDeviceTools(ctx context.Context, reporter aiacp.Reporter) {
	probeCtx, cancelProbe := context.WithCancel(ctx)
	h.mu.Lock()
	if h.closed {
		h.toolsReadyOnce.Do(func() {
			if h.toolsReady != nil {
				close(h.toolsReady)
			}
		})
		h.mu.Unlock()
		cancelProbe()
		return
	}
	select {
	case <-h.toolsReady:
		h.mu.Unlock()
		cancelProbe()
		return
	default:
	}
	h.deviceProbeCancel = cancelProbe
	h.mu.Unlock()
	defer cancelProbe()

	capable, ok := reporter.(interface {
		ClientCapabilities() protocol.ClientCapabilities
	})
	caller, callerOK := reporter.(model.DeviceCaller)
	var descriptors []acpworker.DeviceToolDescriptor
	if ok && callerOK && capable.ClientCapabilities().Has(acpworker.CapabilityDeviceTools) {
		probeCtx, cancel := context.WithTimeout(probeCtx, acpworker.ProbeTimeout)
		descriptors = acpworker.ProbeDeviceTools(probeCtx, caller)
		cancel()
	}

	h.mu.Lock()
	h.deviceProbeCancel = nil
	select {
	case <-h.toolsReady:
		h.mu.Unlock()
		return
	default:
	}
	if !h.closed && len(descriptors) != 0 {
		h.deviceCaller = caller
		h.deviceDescriptors = append([]acpworker.DeviceToolDescriptor(nil), descriptors...)
	}
	h.toolsReadyOnce.Do(func() {
		if h.toolsReady != nil {
			close(h.toolsReady)
		}
	})
	h.mu.Unlock()
}

func (h *FrontendHandler) waitDeviceTools(ctx context.Context) bool {
	h.mu.Lock()
	ready := h.toolsReady
	h.mu.Unlock()
	if ready != nil {
		timer := time.NewTimer(acpworker.ProbeTimeout)
		defer timer.Stop()
		select {
		case <-ready:
		case <-timer.C:
			h.mu.Lock()
			var cancelProbe context.CancelFunc
			h.toolsReadyOnce.Do(func() {
				cancelProbe = h.deviceProbeCancel
				close(ready)
			})
			h.mu.Unlock()
			if cancelProbe != nil {
				cancelProbe()
			}
		case <-ctx.Done():
			return false
		}
	}
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	return !closed
}

func (h *FrontendHandler) bindDeviceTools(sessionID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bindDeviceToolsLocked(sessionID)
}

func (h *FrontendHandler) bindDeviceToolsLocked(sessionID string) {
	if sessionID == "" || h.closed || len(h.deviceDescriptors) == 0 || h.deviceCaller == nil {
		return
	}
	binder, ok := h.sessions.(model.NegotiatedClientToolBinder)
	if !ok {
		return
	}
	binder.BindClientToolSpecs(sessionID, h.connectionID, acpworker.DeviceToolSpecs(h.deviceDescriptors), h.deviceCaller)
}

// 只读集合。归档会话只读：load 成功记入只读集合，prompt 该 id 诚实报错而非掩盖
// 成未知会话。promptRoutes 只记录可写的 canonical session ID，不承担身份映射。
func (h *FrontendHandler) remember(sessionID string) {
	h.mu.Lock()
	if h.promptRoutes == nil {
		h.promptRoutes = make(map[string]*sessionRoute)
	}
	h.promptRoutes[sessionID] = &sessionRoute{}
	h.rememberLocked(sessionID)
	h.mu.Unlock()
}

func (h *FrontendHandler) rememberLocked(sessionID string) {
	delete(h.readOnly, sessionID)
	h.active = sessionID
}

func (h *FrontendHandler) routeBindingError(sessionID string) error {
	if h.multi == nil || h.binder == nil {
		return nil
	}
	if _, ok := h.sessions.(model.MultiSessionBinder); ok {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.sessions.(model.GuardedMultiSessionBinder); ok {
		return nil
	}
	for otherSessionID := range h.promptRoutes {
		if otherSessionID != sessionID {
			return &protocol.RPCError{Code: -32000, Message: "acp frontend: backend lifecycle binding does not support multiple sessions per connection"}
		}
	}
	return nil
}

// commitSessionSelection atomically publishes a canonical session ID and its lifecycle
// reference against Close. ACP transport IDs are the session IDs; there is no remapping layer.
func (h *FrontendHandler) commitSessionSelection(sessionID string, bindLifecycle, legacy bool) (bool, error) {
	if h.selectionCommitHook != nil {
		h.selectionCommitHook(sessionID)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		if bindLifecycle && sessionID != "" {
			if binder, ok := h.sessions.(model.GuardedMultiSessionBinder); ok {
				binder.BindConnectionSessionReferenceGuarded(h.connectionID, sessionID, func() bool { return false })
			} else if binder, ok := h.sessions.(model.GuardedConnectionBinder); ok {
				binder.BindConnectionSessionGuarded(h.connectionID, sessionID, func() bool { return false })
			}
		}
		return false, nil
	}
	if sessionID == "" {
		return false, nil
	}
	if legacy {
		if h.legacyID != "" && h.legacyID != sessionID {
			return false, &protocol.RPCError{Code: -32000, Message: "acp frontend: this backend supports one session per connection"}
		}
		for otherSessionID := range h.promptRoutes {
			if otherSessionID != sessionID {
				return false, &protocol.RPCError{Code: -32000, Message: "acp frontend: this backend supports one session per connection"}
			}
		}
	}
	if bindLifecycle && h.multi != nil {
		if binder, ok := h.sessions.(model.GuardedMultiSessionBinder); ok {
			if !binder.BindConnectionSessionReferenceGuarded(h.connectionID, sessionID, func() bool {
				return !h.closedForBinding.Load()
			}) {
				return false, nil
			}
		} else if binder, ok := h.sessions.(model.MultiSessionBinder); ok {
			binder.BindConnectionSessionReference(h.connectionID, sessionID)
		} else if h.binder != nil {
			// A replacement-only binder cannot represent two session IDs for one connection.
			for otherSessionID := range h.promptRoutes {
				if otherSessionID != sessionID {
					return false, &protocol.RPCError{Code: -32000, Message: "acp frontend: backend lifecycle binding does not support multiple sessions per connection"}
				}
			}
			if binder, ok := h.sessions.(model.GuardedConnectionBinder); ok {
				if !binder.BindConnectionSessionGuarded(h.connectionID, sessionID, func() bool {
					return !h.closedForBinding.Load()
				}) {
					return false, nil
				}
			} else {
				// Keep h.mu through legacy registration so Close either runs afterwards and
				// releases the reference or wins before this commit.
				h.binder.BindConnectionSession(h.connectionID, sessionID)
			}
		}
	} else if bindLifecycle && h.binder != nil {
		if binder, ok := h.sessions.(model.GuardedConnectionBinder); ok {
			if !binder.BindConnectionSessionGuarded(h.connectionID, sessionID, func() bool {
				return !h.closedForBinding.Load()
			}) {
				return false, nil
			}
		} else {
			// Keep h.mu through legacy registration so Close either runs afterwards and
			// releases the reference or wins before this commit.
			h.binder.BindConnectionSession(h.connectionID, sessionID)
		}
	}
	if h.promptRoutes == nil {
		h.promptRoutes = make(map[string]*sessionRoute)
	}
	h.promptRoutes[sessionID] = &sessionRoute{}
	h.rememberLocked(sessionID)
	if legacy {
		h.legacyID = sessionID
	}
	h.binding.Set(sessionID)
	return true, nil
}

func (h *FrontendHandler) routeFor(sessionID string) (*sessionRoute, bool, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.readOnly[sessionID]; ok {
		return nil, false, true
	}
	route, ok := h.promptRoutes[sessionID]
	return route, ok, false
}

// finishPromptRoute applies /new's request-local binding clear to this canonical session.
func (h *FrontendHandler) finishPromptRoute(sessionID string, route *sessionRoute, requestBinding *agentcontrol.SessionBinding) {
	if requestBinding.Get() != "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.promptRoutes[sessionID] != route {
		return
	}
	delete(h.promptRoutes, sessionID)
	if h.binding.Get() == sessionID {
		h.binding.Set("")
	}
	if h.closed {
		return // Close releases every connection reference after dropping h.mu.
	}
	if unbinder, ok := h.sessions.(interface{ UnbindConnectionSession(string, string) }); ok {
		unbinder.UnbindConnectionSession(h.connectionID, sessionID)
	}
}

func (h *FrontendHandler) rememberReadOnly(sessionID string) {
	h.mu.Lock()
	if h.readOnly == nil {
		h.readOnly = map[string]struct{}{}
	}
	h.readOnly[sessionID] = struct{}{}
	h.mu.Unlock()
}

// membership 返回 sessionID 的集合归属（可 prompt / 只读）。
func (h *FrontendHandler) membership(sessionID string) (promptable, readOnly bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, promptable = h.promptRoutes[sessionID]
	_, readOnly = h.readOnly[sessionID]
	return promptable, readOnly
}

// sessionBusyError reports that another short session-management operation holds the shared
// gate. Prompts use per-runtime execution gates and do not block session creation/loading;
// callers can retry after the management operation completes.
func sessionBusyError() *protocol.RPCError {
	return &protocol.RPCError{
		Code:    -32000,
		Message: "acp frontend: session busy: a prompt is in flight on the active session; cancel it or retry",
	}
}

// CreateManagedSession is the canonical session/new path for embedded adapters.
// The returned identity is the same stable ID used for persistence and routing.
func (h *FrontendHandler) CreateManagedSession(ctx context.Context, req protocol.NewSessionRequest, reporter aiacp.Reporter) (aisession.ID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !h.waitDeviceTools(ctx) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return "", protocol.ErrClosed
	}
	if h.multi == nil {
		return "", &protocol.RPCError{Code: -32000, Message: "acp frontend: session creation not supported"}
	}
	if !h.gate.tryAcquire() {
		return "", sessionBusyError()
	}
	defer h.gate.release()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var id string
	var rollback func() error
	var responseAbort func() error
	var err error
	if creator, ok := h.multi.(model.ConnectionScopedTransactionalBoundSessionCreator); ok {
		id, rollback, err = creator.CreateBoundSessionWithRollbackForConnection(req.Cwd, h.connectionID)
		responseAbort = rollback
	} else {
		creator, ok := h.multi.(model.TransactionalBoundSessionCreator)
		if !ok {
			return "", &protocol.RPCError{Code: -32000, Message: "acp frontend: backend does not support transactional session creation"}
		}
		id, rollback, err = creator.CreateBoundSessionWithRollback(req.Cwd)
	}
	if err != nil {
		return "", &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp frontend: create session: %v", err)}
	}
	rollbackOnError := func(cause error) error {
		if rollback == nil {
			return cause
		}
		if cleanupErr := rollback(); cleanupErr != nil {
			return errors.Join(cause, fmt.Errorf("acp frontend: rollback newly created session %q: %w", id, cleanupErr))
		}
		return cause
	}
	if err := ctx.Err(); err != nil {
		return "", rollbackOnError(err)
	}
	sessionID, err := aisession.ParseID(id)
	if err != nil {
		return "", rollbackOnError(fmt.Errorf("acp frontend: backend returned invalid session ID %q: %w", id, err))
	}
	previousActive, previousBinding := h.sessionSelectionSnapshot()
	committed, err := h.commitSessionSelection(id, true, false)
	if err != nil {
		return "", rollbackOnError(err)
	}
	if !committed {
		return "", rollbackOnError(protocol.ErrClosed)
	}
	createdRoute := h.sessionRoute(id)
	abortCreatedSelection := func() error {
		if err := responseAbort(); err != nil {
			return err
		}
		h.forgetCreatedSessionSelection(id, createdRoute, previousActive, previousBinding)
		return nil
	}
	if err := ctx.Err(); err != nil {
		if responseAbort != nil {
			if cleanupErr := abortCreatedSelection(); cleanupErr != nil {
				return "", errors.Join(err, fmt.Errorf("acp frontend: abort canceled session creation %q: %w", id, cleanupErr))
			}
		}
		return "", err
	}
	h.bindDeviceTools(id)
	if h.commands != nil {
		if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
			reporter.Update(id, body)
		}
	}
	if responseAbort != nil {
		if registrar, ok := reporter.(aiacp.SessionCreationAbortRegistrar); ok {
			registrar.SetSessionCreationAbort(abortCreatedSelection)
		}
	}
	return sessionID, nil
}

func (h *FrontendHandler) sessionSelectionSnapshot() (active, binding string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active, h.binding.Get()
}

func (h *FrontendHandler) sessionRoute(sessionID string) *sessionRoute {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.promptRoutes[sessionID]
}

func (h *FrontendHandler) forgetCreatedSessionSelection(sessionID string, route *sessionRoute, previousActive, previousBinding string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.promptRoutes[sessionID] == route {
		delete(h.promptRoutes, sessionID)
	}
	if h.closed {
		return
	}
	if h.active == sessionID {
		if _, ok := h.promptRoutes[previousActive]; ok {
			h.active = previousActive
		} else {
			h.active = ""
		}
	}
	if h.binding.Get() == sessionID {
		if _, ok := h.promptRoutes[previousBinding]; ok {
			h.binding.Set(previousBinding)
		} else {
			h.binding.Set("")
		}
	}
}

// deviceToolSource reuses the initialize-time snapshot. Prompt execution must not
// reverse-probe the client again; rebinding is session-scoped and schema-aware.
func (h *FrontendHandler) deviceToolSource(sessionID string) func(ctx context.Context) []core.Tool {
	h.mu.Lock()
	caller := h.deviceCaller
	descriptors := append([]acpworker.DeviceToolDescriptor(nil), h.deviceDescriptors...)
	h.mu.Unlock()
	if caller == nil || len(descriptors) == 0 {
		return nil
	}
	specs := acpworker.DeviceToolSpecs(descriptors)
	if binder, ok := h.sessions.(model.NegotiatedClientToolBinder); ok {
		return binder.ClientToolSourceForSpecs(sessionID, specs)
	}
	// Legacy binders only supply ClientToolBinder; keep their proxy/rebind path so
	// third-party backends do not silently fall through to the raw per-connection relay.
	if binder, ok := h.sessions.(model.ClientToolBinder); ok {
		return binder.ClientToolSource(h.connectionID, caller)
	}
	return func(context.Context) []core.Tool {
		return acpworker.RelayDeviceToolsForSession(caller, descriptors, sessionID)
	}
}

// Prompt validates the transport session and snapshots its runtime identity before
// evaluation. A later load/new on another transport cannot redirect this request.
func (h *FrontendHandler) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, reporter aiacp.Reporter) (string, error) {
	route, promptable, readOnly := h.routeFor(sessionID)
	if readOnly {
		return "", &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp frontend: archived session %q is read-only", sessionID)}
	}
	if !promptable || route == nil {
		return "", &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp frontend: unknown or invalidated session %q", sessionID)}
	}
	if source, ok := h.sessions.(interface{ SessionReadOnlyReason(string) error }); ok {
		if reason := source.SessionReadOnlyReason(sessionID); reason != nil {
			return "", &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp frontend: session %q is read-only: %v", sessionID, reason)}
		}
	}
	requestBinding := &agentcontrol.SessionBinding{}
	requestBinding.Set(sessionID)
	defer h.finishPromptRoute(sessionID, route, requestBinding)

	var input strings.Builder
	for _, block := range prompt {
		if block.Type != "text" {
			continue
		}
		if input.Len() != 0 {
			input.WriteByte('\n')
		}
		input.WriteString(block.Text)
	}

	output := &commandOutput{reporter: reporter, sessionID: sessionID}
	// /new must not let the plugin drop the connection-wide lifecycle reference before
	// we know whether another request-local selection was cleared. The unique request
	// identity is intentionally not registered; finishPromptRoute performs the exact
	// connection/session unbind after checking the route token under h.mu.
	requestClientID := h.connectionID + "\x00acp-session\x00" + sessionID
	commandContext := &model.Context{
		Ctx: agentcontrol.WithProviderWriteAuthorization(
			agentcontrol.WithClientID(agentcontrol.WithBinding(ctx, requestBinding), requestClientID),
			h.providerWriteAllowed,
		),
		FrontendContext: model.FrontendContext{
			Writer: output.Write,
			EmitToken: func(token core.Token) {
				reporter.Update(sessionID, TokenToUpdate(token))
			},
			Decide:      model.DecideViaACP(ctx, reporter, sessionID),
			ToolSource:  h.deviceToolSource(sessionID),
			ToolOptions: model.ToolCollectionOptions{DisableDefaultTools: h.mobileClientEnabled()},
		},
		Values: make(map[string]any),
	}
	evalErr := h.evaluate(commandContext, input.String())
	output.Close()
	if evalErr != nil {
		return "", evalErr
	}
	return protocol.StopEndTurn, nil
}

// LoadSession is retained for legacy in-package callers. ACP uses the strict typed
// adapter and validates the requested canonical ID before routing.
func (h *FrontendHandler) LoadSession(ctx context.Context, req protocol.LoadSessionRequest, reporter aiacp.Reporter) error {
	_, err := h.LoadSessionWithSessionID(ctx, req, reporter)
	return err
}

func (h *FrontendHandler) loadCanonicalSession(ctx context.Context, sessionID aisession.ID, req protocol.LoadSessionRequest, reporter aiacp.Reporter) error {
	if err := sessionID.Validate(); err != nil {
		return err
	}
	req.SessionID = sessionID.String()
	_, err := h.LoadSessionWithSessionID(ctx, req, reporter)
	return err
}

// LoadSessionWithSessionID returns the runtime selected by this exact session/load call.
// Archived sessions have no writable runtime and therefore return an empty identity.
func (h *FrontendHandler) LoadSessionWithSessionID(ctx context.Context, req protocol.LoadSessionRequest, reporter aiacp.Reporter) (string, error) {
	if !h.waitDeviceTools(ctx) {
		return "", protocol.ErrClosed
	}
	if req.SessionID == "" {
		return "", &protocol.RPCError{Code: -32000, Message: "acp frontend: session/load requires a session ID"}
	}
	if h.sessions == nil {
		return "", &protocol.RPCError{Code: -32000, Message: "acp frontend: session load not supported"}
	}
	backendID := req.SessionID
	if h.multi != nil {
		if h.multi.SessionArchived(backendID) {
			history, err := h.sessions.ArchivedSessionHistory(backendID)
			if err != nil {
				return "", fmt.Errorf("acp frontend: load archived session %q: %w", backendID, err)
			}
			h.rememberReadOnly(req.SessionID)
			if h.commands != nil {
				if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
					reporter.Update(req.SessionID, body)
				}
			}
			for _, body := range history {
				reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
			}
			return "", nil
		}
		if bindingErr := h.routeBindingError(backendID); bindingErr != nil {
			return "", bindingErr
		}
		if !h.gate.tryAcquire() {
			return "", sessionBusyError()
		}
		var (
			history         []model.SessionUpdate
			loadErr         error
			historyErr      error
			boundByLoader   bool
			bindingRejected bool
		)
		if loader, ok := h.multi.(model.GuardedBoundSessionLoader); ok {
			history, boundByLoader, loadErr = loader.LoadBoundSessionForConnectionGuarded(
				backendID, req.Cwd, h.connectionID,
				func() bool { return !h.closedForBinding.Load() },
			)
			bindingRejected = loadErr == nil && !boundByLoader
		} else if loader, ok := h.multi.(model.BoundSessionLoader); ok {
			h.mu.Lock()
			if h.closed {
				bindingRejected = true
			} else {
				history, loadErr = loader.LoadBoundSessionForConnection(backendID, req.Cwd, h.connectionID)
				boundByLoader = loadErr == nil
			}
			h.mu.Unlock()
		} else {
			loadErr = h.multi.LoadBoundSession(backendID, req.Cwd)
			if loadErr == nil {
				history, historyErr = h.multi.SessionHistory(backendID)
			}
		}
		h.gate.release()
		if loadErr != nil {
			if errors.Is(loadErr, model.ErrSessionUnknown) {
				return "", &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp frontend: unknown session %q", req.SessionID)}
			}
			return "", fmt.Errorf("acp frontend: load session %q: %w", backendID, loadErr)
		}
		if bindingRejected {
			return "", protocol.ErrClosed
		}
		if historyErr != nil {
			return "", fmt.Errorf("acp frontend: load session %q history: %w", backendID, historyErr)
		}
		// The loader may have added the lifecycle reference before a concurrent /new removed it.
		// Re-adding idempotently under h.mu makes route publication the final linearization point.
		committed, commitErr := h.commitSessionSelection(backendID, true, false)
		if commitErr != nil {
			return "", commitErr
		}
		if !committed {
			return "", protocol.ErrClosed
		}
		h.bindDeviceTools(backendID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range history {
			reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
		}
		return backendID, nil
	}

	// Read-only replay does not claim a writable route, so it must be resolved before
	// enforcing the legacy backend's one-session binding limit.
	if backendID != h.sessions.CurrentSessionID() {
		for _, info := range h.sessions.ListSessions() {
			if info.SessionID != backendID {
				continue
			}
			history, err := h.sessions.ArchivedSessionHistory(backendID)
			if err != nil {
				return "", fmt.Errorf("acp frontend: load archived session %q: %w", backendID, err)
			}
			h.rememberReadOnly(req.SessionID)
			if h.commands != nil {
				if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
					reporter.Update(req.SessionID, body)
				}
			}
			for _, body := range history {
				reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
			}
			return "", nil
		}
		return "", &protocol.RPCError{Code: -32000, Message: fmt.Sprintf("acp frontend: unknown session %q", req.SessionID)}
	}

	// Legacy sources must own the current-session check and snapshot as one atomic backend
	// operation. A handler-side recheck cannot exclude ABA between the check and history read.
	loader, ok := h.sessions.(model.CurrentSessionHistoryLoader)
	if !ok {
		return "", &protocol.RPCError{Code: -32000, Message: "acp frontend: backend cannot safely load the current session"}
	}
	h.mu.Lock()
	legacyID, legacyTransport := h.legacyID, h.active
	h.mu.Unlock()
	if legacyID != "" && legacyTransport != req.SessionID {
		return "", &protocol.RPCError{Code: -32000, Message: "acp frontend: this backend supports one session per connection"}
	}
	if !h.gate.tryAcquire() {
		return "", sessionBusyError()
	}
	history, err := loader.ReloadAndReadCurrentSession(backendID)
	h.gate.release()
	if err != nil {
		return "", fmt.Errorf("acp frontend: load current session %q: %w", backendID, err)
	}
	committed, commitErr := h.commitSessionSelection(backendID, true, true)
	if commitErr != nil {
		return "", commitErr
	}
	if !committed {
		return "", protocol.ErrClosed
	}
	h.bindDeviceTools(backendID)
	if h.commands != nil {
		if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
			reporter.Update(req.SessionID, body)
		}
	}
	for _, body := range history {
		reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
	}
	return backendID, nil
}

// ListSessions 实现 aiacp.SessionLister：转发给会话数据源；无数据源时返回空清单
// （非 nil，ACP 无 list 能力协商，空清单即「当前无可列会话」）。
func (h *FrontendHandler) ListSessions(_ context.Context, _ protocol.ListSessionsRequest) ([]protocol.SessionInfo, error) {
	if h.sessions == nil {
		return []protocol.SessionInfo{}, nil
	}
	summaries := h.sessions.ListSessions()
	infos := make([]protocol.SessionInfo, 0, len(summaries))
	for _, summary := range summaries {
		infos = append(infos, SessionInfoFromDomain(summary))
	}
	return infos, nil
}

// SessionModes 实现 aiacp.SessionModesProvider：转发给会话数据源（无数据源 =
// 无 modes，响应省略该字段）。
func (h *FrontendHandler) SessionModes() *protocol.SessionModeState {
	if h.sessions == nil {
		return nil
	}
	return SessionModeStateFromDomain(h.sessions.SessionModes())
}

// commandFence 是命令输出代码块的围栏。用 4 个反引号而非 3 个：命令输出会回显
// 用户内容（/history 回显对话、/rules 回显指令文件），其中出现 ``` 完全可能，
// 4 个反引号能让这些内容原样保留而不提前闭合代码块。
const commandFence = "````"

// commandOutput wraps preformatted command output in a Markdown code block so proportional-font
// renderers preserve terminal alignment. The unclosed fence remains valid during streaming, and
// empty output does not open a block. Writer/Close run on the command goroutine; token emission
// uses a separate reporter path.
type commandOutput struct {
	reporter  aiacp.Reporter
	sessionID string
	opened    bool
	trailing  byte // 已写出内容的最后一个字节，收尾时据此决定要不要补换行
}

// Write 实现 model.FrontendContext.Writer。
//
// 围栏开启前忽略纯空白：这条通道被 agent 路径共用（session.Run 每轮结束都会
// 无条件 Writer("\n")），不拦的话每次对话都会多出一个空代码块。
func (w *commandOutput) Write(text string) {
	if text == "" {
		return
	}
	if !w.opened {
		if strings.TrimSpace(text) == "" {
			return
		}
		w.reporter.MessageChunk(w.sessionID, commandFence+"\n")
		w.opened = true
	}
	w.reporter.MessageChunk(w.sessionID, text)
	w.trailing = text[len(text)-1]
}

// Close 闭合围栏；未写入过任何内容时不发，避免留下一个空代码块。
func (w *commandOutput) Close() {
	if !w.opened {
		return
	}
	if w.trailing != '\n' {
		w.reporter.MessageChunk(w.sessionID, "\n")
	}
	w.reporter.MessageChunk(w.sessionID, commandFence+"\n")
}
