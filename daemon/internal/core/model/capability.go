package model

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
)

const SessionSourceCapability = "session.source"

// SessionSummary is the protocol-neutral view of a persisted session. State is the active/idle/
// archived lifecycle value; Archived exposes the persisted terminal marker for load routing.
type SessionSummary struct {
	SessionID string
	Cwd       string
	Title     string
	UpdatedAt string
	// Archived 报告会话是否已落 archived 终态标记（与 State=archived
	// 同源——终态是互斥三态的一支，字段冗余是为 wire 之外的 domain 消费方
	// （load 分流）提供不解析 State 字符串的直读面）。
	Archived bool
	// State 是三态归属：active（连接绑定或 run 在飞）、
	// idle（无引用无在飞）、archived（终态标记）。空串 = 清单方未判定（仅降级
	// 形态可能产生）。
	State string
}

// 会话三态：wire 的 SessionInfo.state 直接取这三值。
const (
	SessionStateActive   = "active"
	SessionStateIdle     = "idle"
	SessionStateArchived = "archived"
)

// SessionMode is a protocol-neutral model/provider choice.
type SessionMode struct {
	ID          string
	Name        string
	Description string
}

// SessionModeState is the daemon domain representation of available modes.
type SessionModeState struct {
	CurrentModeID  string
	AvailableModes []SessionMode
}

// SessionContent is the protocol-neutral text content used by replay events.
type SessionContent struct {
	Type string
	Text string
}

// SessionUpdate is a typed port between the daemon session domain and a wire
// adapter. Raw carries extension fields without exposing map[string]any as a
// capability API.
type SessionUpdate struct {
	Kind       string
	Content    *SessionContent
	ToolCallID string
	Title      string
	Status     string
	Raw        json.RawMessage
}

const (
	SessionUpdateUserMessageChunk  = "user_message_chunk"
	SessionUpdateAgentMessageChunk = "agent_message_chunk"
	SessionUpdateAgentThoughtChunk = "agent_thought_chunk"
	SessionUpdateToolCall          = "tool_call"
	SessionUpdateToolCallUpdate    = "tool_call_update"
	SessionUpdateAvailableCommands = "available_commands_update"
)

// SessionSource is the consumer-facing capability exposed by the agent plugin.
// It deliberately uses daemon domain types; the VS Code/ACP adapter maps them
// to ai-acp/protocol values at the transport boundary.
type SessionSource interface {
	ListSessions() []SessionSummary
	CurrentSessionID() string
	// ActiveSessionID 返回最近一次由 ACP 连接声明的 transport 会话 id；它可能与
	// CurrentSessionID 返回的持久化 store id 不同。空值表示尚无连接声明。
	ActiveSessionID() string
	SetSession(sessionID, cwd string)
	ReloadCurrentSession() error
	CurrentSessionHistory() []SessionUpdate
	ArchivedSessionHistory(sessionID string) ([]SessionUpdate, error)
	SessionModes() *SessionModeState
}

// PendingApprovalInfo is a protocol-neutral summary of a pending approval.
type PendingApprovalInfo struct {
	RequestID string
	SessionID string
	RunID     string
	ToolName  string
	Command   string
	RiskLevel string
	ExpiresAt time.Time
}

// RunControlSource exposes per-session control leases and fenced asynchronous operations.
// Implementations atomically validate the lease with each control operation; connection close
// releases its leases. Synchronous host-only methods are intentionally excluded.
type RunControlSource interface {
	// StartRun submits a background run and returns its ID and initial state. Implementations
	// must atomically validate the session lease and submit under the same guard used by
	// AcquireControl. (sessionID, requestID) is the idempotency key: identical retries reuse
	// the run; a different input for that key returns a conflict.
	StartRun(sessionID, connectionID, input, requestID string) (runID, state string, err error)
	// CancelRun 取消一个后台 Run(connectionID fencing 同 StartRun:lease 校验与
	// 取消提交在同一受保护临界区内原子完成,接管后旧持有者一律拒绝)。
	CancelRun(sessionID, connectionID, runID string) error
	// RunStatus returns the current run state, the session's latest event sequence, and replay
	// integrity (complete, unknown, or incomplete). Unknown means prior history crosses a
	// process boundary; incomplete is sticky after a write/flush failure. Only complete
	// permits treating received events as a complete history; latestSeq is not an ack cursor.
	RunStatus(sessionID, runID string) (state string, latestSeq uint64, replayIntegrity string, err error)
	// PendingApprovals 返回会话未过期审批快照（daemon 侧扁平结构，避免
	// model→agent 类型泄漏）。未知会话/无审批返回空切片。
	PendingApprovals(sessionID string) ([]PendingApprovalInfo, error)
	// ResolveApproval atomically validates the connection's session lease and server-recorded
	// approval ownership before submitting the decision. The caller's session must match the
	// recorded owner; unknown, cross-session, stale-holder, late, and duplicate decisions fail.
	ResolveApproval(sessionID, connectionID, requestID string, decision hitl.Decision) error
	// SubscribeEvents returns the replay/live subscription and its cursor, integrity, active-run
	// snapshot, and close handle. It replays events after the acknowledged afterSeq; an invalid
	// cursor returns ErrCursorExpired. This is data-plane only: callers separately acquire a
	// control lease. Unknown sessions and sessions without an event hub return explicit errors.
	SubscribeEvents(sessionID string, afterSeq uint64) (agentcontrol.AttachResult, error)
	// ActiveRun returns the requested session's active run snapshot; no run or unavailable
	// runtime returns ok=false. It routes by sessionID rather than the plugin's current-session
	// pointer, so a non-current session still reports its own in-flight run.
	ActiveRun(sessionID string) (runID, state string, ok bool)
	// AcquireControl 获取 (sessionID, connectionID) 的 control lease：该会话
	// 空闲→占用并返回 false；他人持有→显式接管（返回 true，slog.Info 审计）；
	// 同持有者重复→false。每会话独立记账：连接 A 控制会话 A、连接 B
	// 控制会话 B 互不影响，接管只发生在同一会话上。
	AcquireControl(sessionID, connectionID string) (tookOver bool)
	// ReleaseControl 释放 (sessionID, connectionID) 的 control lease：仅该会话
	// 的持有者本人有效，他人调用为空操作。
	ReleaseControl(sessionID, connectionID string)
	// HoldControl 报告 connectionID 当前是否持有 sessionID 的 control lease。
	HoldControl(sessionID, connectionID string) bool
	// ReleaseConnection 释放 connectionID 持有的全部会话的 control lease
	// （断连收口——handler.Close 一次调用替代逐会话归还，审批不悬死在
	// 已死连接上）。连接不持有任何租约时幂等空操作。
	ReleaseConnection(connectionID string)
	// UnbindCapabilities 解绑某连接登记的全部客户端工具能力（
	// handler.Close 断连收口消费）。
	UnbindCapabilities(connectionID string)
	// CapabilityWaitTimeout 返回设备工具断连等待重绑的预算（默认 45s，
	// 硬约束 < ai-core 工具执行 60s）。
	CapabilityWaitTimeout() time.Duration
}

// DeviceCaller is the minimal reverse-call interface used for x-device discovery
// and tool execution. acpworker.DeviceCaller satisfies it without a package dependency.
type DeviceCaller interface {
	Call(ctx context.Context, method string, params, result any) error
}

// ClientToolBinder supplies stable, policy-aware proxies for client-declared tools.
// A nil source means no tools were declared or discovery failed; successful discovery
// may refresh bindings. Connection close removes all bindings for that connection.
type ClientToolBinder interface {
	ClientToolSource(connectionID string, caller DeviceCaller) func(ctx context.Context) []core.Tool
}

// NegotiatedClientToolBinder consumes the initialize-time descriptor snapshot.
// Its source must not probe again; execution resolves a schema/policy-compatible
// connection binding so reconnects can resume a waiting tool call safely.
type NegotiatedClientToolBinder interface {
	BindClientToolSpecs(sessionID, connectionID string, specs []runtimeagent.ClientToolSpec, caller DeviceCaller) bool
	ClientToolSourceForSpecs(sessionID string, specs []runtimeagent.ClientToolSpec) func(ctx context.Context) []core.Tool
}

// NegotiatedRunControlSource submits an idempotent background run with the
// connection's initialize-time device-tool snapshot. Implementations must capture
// the source only in the first submission closure; retries reuse the existing run.
type NegotiatedRunControlSource interface {
	StartRunWithTools(sessionID, connectionID, input, requestID string, toolSource func(context.Context) []core.Tool) (runID, state string, err error)
}

// ErrSessionUnknown is the protocol-neutral missing-session sentinel used by session/load
// to preserve the existing -32000 unknown-session wire error. ACP cannot import ai-runtime/agent,
// so implementations wrap this and any execution-layer sentinel for their respective consumers.
var ErrSessionUnknown = errors.New("session: unknown session")

// BoundSessionSource is an optional capability for connection-bound session
// creation, loading, history, and archival. Implementations without it retain the
// single-session fallback. ConnectionBinder separately records lifecycle references.
type BoundSessionSource interface {
	// CreateBoundSession creates and registers a new session, validating cwd against the sandbox
	// before creation. It returns the stable store ID without changing the caller's current
	// session; connection binding handles routing and lifecycle references. It intentionally has
	// no request context: a short create may finish after disconnect and be cleaned up on release.
	CreateBoundSession(cwd string) (string, error)
	// LoadBoundSession reloads a registered runtime or reopens and registers a persisted one,
	// restoring cwd from the request or workspace metadata. Missing sessions wrap
	// ErrSessionUnknown. An out-of-sandbox cwd does not block loading an existing session;
	// it is marked invalid so prompts fail honestly without making history unreachable.
	LoadBoundSession(sessionID, cwd string) error
	// SessionHistory 导出已注册会话的完整对话历史（session/load 响应前的重放
	// 面）。未知会话报错；纯内存（无持久化）返回空切片。
	SessionHistory(sessionID string) ([]SessionUpdate, error)
	// SessionArchived reports whether a session has the persisted archived marker.
	// Archived loads use read-only replay without binding or registering a runtime.
	// Unknown or non-persistent sessions return false; LoadBoundSession reports missing sessions.
	SessionArchived(sessionID string) bool
	// ArchiveSession records an irreversible, idempotent archived state. Implementations fence
	// it with the session's control lease and reject active runs; bound sessions remain bound
	// but become read-only, and lifecycle cleanup waits for references to drain. Persisted,
	// unregistered sessions may also be archived.
	ArchiveSession(sessionID, clientID string) error
}

// BoundSessionLoader lets a backend linearize load, history snapshot, and lifecycle binding.
type BoundSessionLoader interface {
	LoadBoundSessionForConnection(sessionID, cwd, connectionID string) ([]SessionUpdate, error)
}

// GuardedBoundSessionLoader binds only if the connection is still open at the
// loader's lifecycle commit point. The guard must be short and must not do I/O.
type GuardedBoundSessionLoader interface {
	LoadBoundSessionForConnectionGuarded(sessionID, cwd, connectionID string, allowBind func() bool) ([]SessionUpdate, bool, error)
}

// ConnectionBinder optionally records connection-to-session bindings for lifecycle reference
// counting and active-state reporting. SessionBinding remains the routing source; registration
// itself has no side effects. Implementations evaluate cleanup after rebinding, but must not
// delete sessions, cancel runs, or alter control leases as part of registration.
type ConnectionBinder interface {
	// BindConnectionSession 登记 clientID 当前绑定的会话（改绑即替换；空
	// sessionID = 解除绑定）。connectionID 与 RunControlSource 的连接句柄同源
	// （handler 工厂生成的 connectionID），断连收口经 ReleaseConnection 清账。
	BindConnectionSession(connectionID, sessionID string)
}

// GuardedConnectionBinder commits a session reference only while the connection
// remains open. allowBind must be a fast state check with no lifecycle lock acquisition.
type GuardedConnectionBinder interface {
	BindConnectionSessionGuarded(connectionID, sessionID string, allowBind func() bool) bool
}
