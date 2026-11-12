package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

// CapabilityRegistry 按 sessionID/connectionID 管理客户端工具通道；无绑定时等待重绑，
// 超时或取消后明确失败。TTL 回收过期绑定，锁外执行 Invoke 和等待。

const (
	// Internal key lets the supervisor identify proxy tools; Metadata is not exposed to the model.
	capabilityWaitMetaKey = "goworker.capability_wait"

	// Keep both wait budgets below ai-core's 60s tool context limit.
	defaultCapabilityWaitTimeout = 45 * time.Second

	// Clamp explicit settings to leave time for Invoke and preserve clear timeout results.
	maxCapabilityWaitTimeout = 55 * time.Second
)

// capabilityWaitObserverKey 是 onWait 观察者的 ctx 注入键(supervisor 观察者
// 包装面专用,同包共享;unexported 空结构体类型防跨包碰撞):代理 Execute 从
// toolCtx 读出观察者。未注入(RecordRun 同步路径直用代理)时为 nil——等待
// 照常发生(安全超时兜底)但 RunState 不迁移,waiting_capability 仅后台 Run
// 可观测(同步路径不包装 ToolSource)。
type capabilityWaitObserverKey struct{}

// capabilityWaitFunc 是能力等待的观察者回调(waiting=true 进入等待,false
// 等待收敛)。代理侧的配对纪律:waiting 标志 false→true 恰好调一次 true、
// true→false 恰好调一次 false——全部发生在执行 goroutine 上,顺序翻转即配对
// (出口矩阵:重绑成功/超时/ctx 取消三条路径各恰好一次
// onWait(false) 与结果返回)。
type capabilityWaitFunc func(waiting bool)

// withCapabilityWaitObserver 把观察者注入 ctx(supervisor 的 ToolSource 包装
// 面调用);capabilityWaitObserver 读出,nil 表示未注入。
func withCapabilityWaitObserver(ctx context.Context, fn capabilityWaitFunc) context.Context {
	return context.WithValue(ctx, capabilityWaitObserverKey{}, fn)
}

func capabilityWaitObserver(ctx context.Context) capabilityWaitFunc {
	fn, _ := ctx.Value(capabilityWaitObserverKey{}).(capabilityWaitFunc)
	return fn
}

// resolveCapabilityWait 归一等待预算:<=0 回落 defaultCapabilityWaitTimeout
// (45s 双保险);显式配置 > maxCapabilityWaitTimeout(55s)时钳到上限并
// slog.Warn(硬约束落地:45/55 < ai-core toolCtx 60s;超配时显式降级+告警,
// 避免静默语义劣化)。行为面由单元测试钉死,不做真实等待。
func resolveCapabilityWait(waitTimeout time.Duration) time.Duration {
	if waitTimeout <= 0 {
		return defaultCapabilityWaitTimeout
	}
	if waitTimeout > maxCapabilityWaitTimeout {
		slog.Warn("agent: capability wait timeout exceeds ai-core toolCtx budget, clamped",
			"configured", waitTimeout, "clamped_to", maxCapabilityWaitTimeout,
			"reason", "wait runs inside ai-core toolCtx (60s), budget must leave Invoke headroom")
		return maxCapabilityWaitTimeout
	}
	return waitTimeout
}

// ToolCapabilityBinding 是一笔能力绑定:某会话的某
// 连接声明可执行 ToolNames 里的客户端工具,有效期至 ExpiresAt。绑定关系归属
// (session, connection) 二元组——执行通道跟着连接走,不跟 Run/工具定义走。
type ToolCapabilityBinding struct {
	SessionID    string
	ConnectionID string
	ToolNames    []string
	ExpiresAt    time.Time
}

// ClientToolExecutor 是连接级执行通道(daemon 适配 x-device/call,即
// acpworker/sys_relay.go 的 DeviceCaller.Call 面的等价收窄;SDK 测试用 fake)。
// toolName 是客户端侧原名(不含 sys_ 前缀),argsJSON 是模型参数的 JSON 编码;
// Invoke 失败由调用方(proxy Execute)即时 Invalidate 该绑定。
type ClientToolExecutor interface {
	Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error)
}

// ClientToolSpec 是代理工具的声明(daemon 从 acpworker.DeviceToolDescriptor
// 转换)。Metadata 里的 risk_level/requires_permission/allow_always_key
// 原样透传到 core.Tool.Metadata——HITL 门控(hitl.go:113-122 resolveToolPolicy)
// 按 Metadata 裁决,声明即策略的单一事实源(sys_relay.go:118-120 同款边界)。
type ClientToolSpec struct {
	Name        string
	Description string
	Schema      string            // JSON Schema 字符串;非法/空回落 {"type":"object"}
	Metadata    map[string]string // 策略元数据原样透传
}

var clientToolPolicyMetadataKeys = [...]string{
	"risk_level",
	"requires_permission",
	"allow_always_key",
	"permission_policy",
}

// normalizeClientToolSchema mirrors the proxy's schema parsing and invalid-schema fallback.
// encoding/json sorts object keys, so the encoded representation is a stable identity.
func normalizeClientToolSchema(raw string) (map[string]any, string) {
	var schema map[string]any
	if err := json.Unmarshal([]byte(raw), &schema); err != nil || schema == nil {
		schema = map[string]any{"type": "object"}
	}
	encoded, _ := json.Marshal(schema) // values came from JSON and are always marshalable.
	return schema, string(encoded)
}

// clientToolSchemaFingerprint decodes only the compatibility identity with UseNumber,
// preserving integers that float64 cannot distinguish without changing tool Parameters.
// Invalid or trailing input retains normalizeClientToolSchema's object fallback semantics.
func clientToolSchemaFingerprint(raw string) string {
	const fallback = `{"type":"object"}`

	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var schema map[string]any
	if err := decoder.Decode(&schema); err != nil || schema == nil {
		return fallback
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fallback
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		return fallback
	}
	return string(encoded)
}

// clientToolSpecKey freezes schema and policy only. Descriptions and internal proxy metadata
// are deliberately excluded: neither affects the tool contract enforced by the agent runtime.
func clientToolSpecKey(spec ClientToolSpec) string {
	schema := clientToolSchemaFingerprint(spec.Schema)
	policy := make(map[string]string, len(clientToolPolicyMetadataKeys))
	for _, key := range clientToolPolicyMetadataKeys {
		if value, ok := spec.Metadata[key]; ok {
			policy[key] = value
		}
	}
	encodedPolicy, _ := json.Marshal(policy)
	return schema + "\x00" + string(encodedPolicy)
}

// capabilityBinding 是 registry 内部条目:导出值形态(ToolCapabilityBinding)
// + 执行通道。exec 不进导出结构——ToolCapabilityBinding 是导出的绑定快照,
// 执行通道是实现细节。
type capabilityBinding struct {
	ToolCapabilityBinding
	exec ClientToolExecutor
	// specKeys is nil for legacy Bind APIs, whose tool names carry no verified
	// schema/policy contract. Non-nil entries map names to accepted spec identities.
	specKeys map[string]map[string]struct{}
}

// CapabilityRegistry 是能力绑定的进程内注册表:sessionID → connectionID →
// binding。mu 只护 bindings/waiters 的读写;所有判定(工具名匹配、过期)都在
// 锁内取快照完成,无回调、无 I/O。
type CapabilityRegistry struct {
	mu       sync.Mutex
	bindings map[string]map[string]*capabilityBinding
	// waiters 是 (sessionID,toolName) → 独立通知 chan 集合。每个 Execute
	// 单独登记，Bind 对全体广播；容量 1 只用于合并重复唤醒，不会让并发等待者
	// 互相抢掉通知。Execute 出口移除自己的 chan，无泄漏。
	waiters map[string]map[chan struct{}]struct{}
}

// NewCapabilityRegistry 创建空注册表。
func NewCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{
		bindings: make(map[string]map[string]*capabilityBinding),
		waiters:  make(map[string]map[chan struct{}]struct{}),
	}
}

// capabilityWaitKey 构造等待者注册键:(sessionID, toolName) 二元组的扁平化。
// \x00 分隔——两段都可能含任意可见字符,\x00 不会出现在合法 id/工具名中。
func capabilityWaitKey(sessionID, toolName string) string {
	return sessionID + "\x00" + toolName
}

// Bind registers or replaces a connection's tool set and TTL. Tool names are copied;
// nil executors are rejected, and rebinding the same pair is an idempotent refresh.
func (r *CapabilityRegistry) Bind(sessionID, connectionID string, toolNames []string, ttl time.Duration, exec ClientToolExecutor) (refreshed bool) {
	return r.bind(sessionID, connectionID, toolNames, time.Now().Add(ttl), exec)
}

// BindConnection registers a connection-scoped name-only binding with no time-based expiry.
// It does not attest to a schema or policy contract; use BindConnectionSpecs for that.
func (r *CapabilityRegistry) BindConnection(sessionID, connectionID string, toolNames []string, exec ClientToolExecutor) (refreshed bool) {
	return r.bind(sessionID, connectionID, toolNames, time.Time{}, exec)
}

func (r *CapabilityRegistry) bind(sessionID, connectionID string, toolNames []string, expiresAt time.Time, exec ClientToolExecutor) (refreshed bool) {
	return r.bindWithSpecs(sessionID, connectionID, toolNames, expiresAt, nil, exec)
}

// BindConnectionSpecs registers a connection's fully described client tools without a TTL.
// Schema and policy metadata are snapshotted and normalized; description is intentionally
// excluded from compatibility. Duplicate names retain every advertised spec identity.
func (r *CapabilityRegistry) BindConnectionSpecs(sessionID, connectionID string, specs []ClientToolSpec, exec ClientToolExecutor) (refreshed bool) {
	if exec == nil {
		return false
	}
	names := make([]string, 0, len(specs))
	specKeys := make(map[string]map[string]struct{}, len(specs))
	for _, spec := range specs {
		key := clientToolSpecKey(spec)
		if _, exists := specKeys[spec.Name]; !exists {
			names = append(names, spec.Name)
			specKeys[spec.Name] = make(map[string]struct{})
		}
		specKeys[spec.Name][key] = struct{}{}
	}
	return r.bindWithSpecs(sessionID, connectionID, names, time.Time{}, specKeys, exec)
}

func (r *CapabilityRegistry) bindWithSpecs(sessionID, connectionID string, toolNames []string, expiresAt time.Time, specKeys map[string]map[string]struct{}, exec ClientToolExecutor) (refreshed bool) {
	if exec == nil {
		return false
	}
	names := append([]string(nil), toolNames...)
	binding := &capabilityBinding{
		ToolCapabilityBinding: ToolCapabilityBinding{
			SessionID:    sessionID,
			ConnectionID: connectionID,
			ToolNames:    names,
			ExpiresAt:    expiresAt,
		},
		exec:     exec,
		specKeys: specKeys,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	conns, ok := r.bindings[sessionID]
	if !ok {
		conns = make(map[string]*capabilityBinding)
		r.bindings[sessionID] = conns
	}
	_, refreshed = conns[connectionID]
	conns[connectionID] = binding
	for _, name := range names {
		for ch := range r.waiters[capabilityWaitKey(sessionID, name)] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	return refreshed
}

// UnbindConnection 撤销某连接在全部会话下的绑定(连接退出的收口,handler.Close
// 钩子调用)。连接不存在时为幂等空操作;撤销后清掉空会话桶。
func (r *CapabilityRegistry) UnbindConnection(connectionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for sessionID, conns := range r.bindings {
		if _, ok := conns[connectionID]; ok {
			delete(conns, connectionID)
		}
		if len(conns) == 0 {
			delete(r.bindings, sessionID)
		}
	}
}

// Invalidate 即时撤销某会话某连接的绑定(proxy Execute 里 Invoke 失败的出口
// 调用):连接声明过能力但执行失败 = 通道事实不可用,不等 TTL。不存在的
// 组合为幂等空操作。
func (r *CapabilityRegistry) Invalidate(sessionID, connectionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conns, ok := r.bindings[sessionID]
	if !ok {
		return
	}
	delete(conns, connectionID)
	if len(conns) == 0 {
		delete(r.bindings, sessionID)
	}
}

// invalidateIfCurrent prevents a stale Invoke failure from deleting a newer registration.
func (r *CapabilityRegistry) invalidateIfCurrent(sessionID, connectionID string, expected *capabilityBinding) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conns, ok := r.bindings[sessionID]
	if !ok || conns[connectionID] != expected {
		return
	}
	delete(conns, connectionID)
	if len(conns) == 0 {
		delete(r.bindings, sessionID)
	}
}

// Lookup returns an unexpired binding that explicitly advertises toolName. Expired
// bindings are removed lazily; duplicate matches use connectionID lexical order.
func (r *CapabilityRegistry) Lookup(sessionID, toolName string) (ClientToolExecutor, bool) {
	exec, _, ok := r.lookupResolved(sessionID, toolName)
	return exec, ok
}

// lookupResolved 是 Lookup 的内部扩展形(proxy Execute 使用):额外返回
// 绑定所属 connectionID——Invoke 失败的 Invalidate 需要精确撤销「该绑定」
// 而非盲删。判定语义与 Lookup 完全一致(过期读时逐出、确定性字典序选择、
// 无隐式路由),导出面保持既有形状不变。
func (r *CapabilityRegistry) lookupResolved(sessionID, toolName string) (ClientToolExecutor, string, bool) {
	exec, connectionID, _, ok := r.lookupResolvedBinding(sessionID, toolName, nil, true)
	return exec, connectionID, ok
}

// lookupResolvedBinding returns the selected binding pointer as a generation token. An old
// Invoke failure may invalidate only that exact instance, never a replacement under same IDs.
// With a frozen spec, verified registrations take precedence: an incompatible verified entry
// blocks legacy name-only fallback, while purely legacy registries retain their old behavior.
func (r *CapabilityRegistry) lookupResolvedBinding(sessionID, toolName string, expectedSpecKey *string, allowLegacy bool) (ClientToolExecutor, string, *capabilityBinding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conns, ok := r.bindings[sessionID]
	if !ok {
		return nil, "", nil, false
	}
	ids := make([]string, 0, len(conns))
	for id := range conns {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	now := time.Now()
	var legacy *capabilityBinding
	hasVerifiedCandidate := false
	for _, id := range ids {
		binding := conns[id]
		if !binding.ExpiresAt.IsZero() && !binding.ExpiresAt.After(now) {
			delete(conns, id)
			if len(conns) == 0 {
				delete(r.bindings, sessionID)
			}
			continue
		}
		if expectedSpecKey == nil {
			for _, name := range binding.ToolNames {
				if name == toolName {
					return binding.exec, binding.ConnectionID, binding, true
				}
			}
			continue
		}
		if binding.specKeys == nil {
			if allowLegacy && legacy == nil {
				for _, name := range binding.ToolNames {
					if name == toolName {
						legacy = binding
						break
					}
				}
			}
			continue // Legacy bindings cannot satisfy a strict proxy.
		}
		keys, advertisesName := binding.specKeys[toolName]
		if !advertisesName {
			continue
		}
		hasVerifiedCandidate = true
		if _, ok := keys[*expectedSpecKey]; ok {
			return binding.exec, binding.ConnectionID, binding, true
		}
	}
	if allowLegacy && !hasVerifiedCandidate && legacy != nil {
		return legacy.exec, legacy.ConnectionID, legacy, true
	}
	return nil, "", nil, false
}

// waitChan registers a private (sessionID,toolName) notification channel before lookup.
// Bind broadcasts while holding the same mutex, so no rebind can be lost between the
// unbound check and select. The Execute owner removes its channel on every exit.
func (r *CapabilityRegistry) waitChan(sessionID, toolName string) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := capabilityWaitKey(sessionID, toolName)
	waiters, ok := r.waiters[key]
	if !ok {
		waiters = make(map[chan struct{}]struct{})
		r.waiters[key] = waiters
	}
	ch := make(chan struct{}, 1)
	waiters[ch] = struct{}{}
	return ch
}

// dropWaitChan removes only this caller's subscription; concurrent waiters remain registered.
func (r *CapabilityRegistry) dropWaitChan(sessionID, toolName string, ch chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := capabilityWaitKey(sessionID, toolName)
	waiters, ok := r.waiters[key]
	if !ok {
		return
	}
	delete(waiters, ch)
	if len(waiters) == 0 {
		delete(r.waiters, key)
	}
}

// NewClientToolProxy preserves name-only fallback for purely legacy registrations. When a
// spec-aware registration advertises the same tool name, the frozen proxy spec is matched
// exactly and an incompatible verified binding blocks legacy fallback.
func NewClientToolProxy(reg *CapabilityRegistry, sessionID string, specs []ClientToolSpec, waitTimeout time.Duration) []core.Tool {
	return newClientToolProxy(reg, sessionID, specs, waitTimeout, false)
}

// NewSpecAwareClientToolProxy creates proxies that resolve only explicitly registered,
// schema-and-policy-compatible connection bindings. Specs are snapshotted at construction.
func NewSpecAwareClientToolProxy(reg *CapabilityRegistry, sessionID string, specs []ClientToolSpec, waitTimeout time.Duration) []core.Tool {
	return newClientToolProxy(reg, sessionID, specs, waitTimeout, true)
}

func newClientToolProxy(reg *CapabilityRegistry, sessionID string, specs []ClientToolSpec, waitTimeout time.Duration, specAware bool) []core.Tool {
	wait := resolveCapabilityWait(waitTimeout)
	tools := make([]core.Tool, 0, len(specs))
	for _, spec := range specs {
		spec := spec // Freeze scalar fields and compatibility identity for this Run.
		schema, _ := normalizeClientToolSchema(spec.Schema)
		metadata := make(map[string]string, len(spec.Metadata)+1)
		for k, v := range spec.Metadata {
			metadata[k] = v
		}
		metadata[capabilityWaitMetaKey] = "true"
		specKey := clientToolSpecKey(spec)

		tools = append(tools, core.Tool{
			Name:        spec.Name,
			Description: spec.Description,
			Parameters:  schema,
			Metadata:    metadata,
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				onWait := capabilityWaitObserver(ctx)
				if onWait == nil {
					onWait = func(bool) {}
				}
				argsJSON, err := json.Marshal(args)
				if err != nil {
					return "", fmt.Errorf("capability tool %s: encode args: %w", spec.Name, err)
				}
				return executeCapabilityToolForSpec(ctx, reg, sessionID, spec.Name, argsJSON, wait, onWait, &specKey, !specAware)
			},
		})
	}
	return tools
}

// executeCapabilityTool 先解析并调用当前绑定；无绑定或普通调用失败时等待重绑。
// 就绪通知仅用于唤醒，之后必须重查绑定；等待总预算不重置，超时/取消返回失败文本。
func executeCapabilityTool(ctx context.Context, reg *CapabilityRegistry, sessionID, toolName string, argsJSON []byte, wait time.Duration, onWait capabilityWaitFunc) (string, error) {
	return executeCapabilityToolForSpec(ctx, reg, sessionID, toolName, argsJSON, wait, onWait, nil, true)
}

func executeCapabilityToolForSpec(ctx context.Context, reg *CapabilityRegistry, sessionID, toolName string, argsJSON []byte, wait time.Duration, onWait capabilityWaitFunc, specKey *string, allowLegacy bool) (string, error) {
	// 就绪通知先注册后判定:Bind 落在「判定无绑定」与「select 挂起」之间的
	// 窗口也必然投递(非阻塞、容量 1),不漏唤醒。
	notify := reg.waitChan(sessionID, toolName)
	defer reg.dropWaitChan(sessionID, toolName, notify)

	var (
		timer     *time.Timer // 惰性:首次进入等待才起表
		timerC    <-chan time.Time
		waitStart time.Time
		waiting   bool
	)
	leaveWait := func() {
		if waiting {
			waiting = false
			onWait(false)
		}
	}
	defer leaveWait() // 兜底:任何 return 前收敛等待态(出口矩阵的安全网)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		exec, connID, binding, ok := reg.lookupResolvedBinding(sessionID, toolName, specKey, allowLegacy)
		if ok {
			// 绑定在(直接命中 / 等待中重绑成功):先收敛等待态再执行
			// (「→ onWait(false) → 重绑成功则 Invoke」)。
			leaveWait()
			result, err := exec.Invoke(ctx, toolName, argsJSON)
			if err == nil {
				return result, nil
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Cancellation or deadline does not imply the binding is unhealthy; invalidating
				// it would degrade healthy connections. Return directly through ai-core's tool
				// error channel instead.
				return result, err
			}
			// Invoke 失败 = 通道事实不可用:即时失效「该绑定」,转入等待——
			// 绝不静默吞掉失败后路由到其他连接。
			reg.invalidateIfCurrent(sessionID, connID, binding)
		}
		if !waiting {
			waiting = true
			onWait(true)
			if timer == nil {
				timer = time.NewTimer(wait)
				timerC = timer.C
				waitStart = time.Now()
			}
		}
		select {
		case <-notify:
			// 唤醒:回顶部重查 registry(通知可能来自不含本工具的重绑/TTL
			// 刷新,事实源是 registry 的 Bind;不匹配继续等)。
		case <-ctx.Done():
			leaveWait()
			return capabilityFailureText(toolName, time.Since(waitStart)), nil
		case <-timerC:
			leaveWait()
			return capabilityFailureText(toolName, time.Since(waitStart)), nil
		}
	}
}

// capabilityFailureText 组装等待出口的明确失败文本(「把明确的工具失败结果
// 交给 Agent,由 Agent 决定下一步」)。waited 取自等待真实
// 起点的墙钟差(毫秒取整),非配置预算的复述。
func capabilityFailureText(toolName string, waited time.Duration) string {
	return fmt.Sprintf("device tool %s unavailable: connection lost, waited %s", toolName, waited.Round(time.Millisecond))
}
