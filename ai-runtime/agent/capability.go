package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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

// capabilityBinding 是 registry 内部条目:导出值形态(ToolCapabilityBinding)
// + 执行通道。exec 不进导出结构——ToolCapabilityBinding 是导出的绑定快照,
// 执行通道是实现细节。
type capabilityBinding struct {
	ToolCapabilityBinding
	exec ClientToolExecutor
}

// CapabilityRegistry 是能力绑定的进程内注册表:sessionID → connectionID →
// binding。mu 只护 bindings/waiters 的读写;所有判定(工具名匹配、过期)都在
// 锁内取快照完成,无回调、无 I/O。
type CapabilityRegistry struct {
	mu       sync.Mutex
	bindings map[string]map[string]*capabilityBinding
	// waiters 是重绑就绪通知的注册表:(sessionID,toolName) → 容量 1
	// 的 chan。等待侧(proxy Execute)在判定无绑定之前经 waitChan 注册,Bind
	// 成功时对已注册的 chan 非阻塞投递(满则丢弃——通知只是唤醒,事实源是
	// bindings 表);Execute 出口经 dropWaitChan 撤销,无泄漏。
	waiters map[string]chan struct{}
}

// NewCapabilityRegistry 创建空注册表。
func NewCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{
		bindings: make(map[string]map[string]*capabilityBinding),
		waiters:  make(map[string]chan struct{}),
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
	if exec == nil {
		return false
	}
	names := make([]string, len(toolNames))
	copy(names, toolNames)
	binding := &capabilityBinding{
		ToolCapabilityBinding: ToolCapabilityBinding{
			SessionID:    sessionID,
			ConnectionID: connectionID,
			ToolNames:    names,
			ExpiresAt:    time.Now().Add(ttl),
		},
		exec: exec,
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
	// Bind 成功即对等待者非阻塞投递就绪信号:新声明/刷新的每个工具名
	// 各投一次。容量 1 满则丢弃——旧信号未消费说明等待者尚未回到 select,其
	// 醒来后按 bindings 现状重查,不会错过本次(或更新一次的)Bind。
	for _, name := range names {
		if ch, ok := r.waiters[capabilityWaitKey(sessionID, name)]; ok {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	conns, ok := r.bindings[sessionID]
	if !ok {
		return nil, "", false
	}
	ids := make([]string, 0, len(conns))
	for id := range conns {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	now := time.Now()
	for _, id := range ids {
		binding := conns[id]
		if !binding.ExpiresAt.After(now) {
			// 过期:视同无绑定,读时逐出(含空会话桶回收)。ids 是快照切片,
			// 迭代中删 map 安全。
			delete(conns, id)
			if len(conns) == 0 {
				delete(r.bindings, sessionID)
			}
			continue
		}
		for _, name := range binding.ToolNames {
			if name == toolName {
				return binding.exec, binding.ConnectionID, true
			}
		}
	}
	return nil, "", false
}

// waitChan 返回 (sessionID,toolName) 的重绑就绪通知 chan,不存在则创建(容量
// 1)。等待侧(proxy Execute)在判定无绑定**之前**注册——Bind 落在注册之后的
// 任何时刻都必然非阻塞投递,「判定无绑定」与「select 挂起」之间的窗口不漏
// 唤醒。chan 的生命周期归等待侧:Execute 出口经 dropWaitChan 撤销(进程内
// 存量,无泄漏;V1 无后台清扫)。
func (r *CapabilityRegistry) waitChan(sessionID, toolName string) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := capabilityWaitKey(sessionID, toolName)
	ch, ok := r.waiters[key]
	if !ok {
		ch = make(chan struct{}, 1)
		r.waiters[key] = ch
	}
	return ch
}

// dropWaitChan 撤销等待侧注册:仅当当前登记的正是本 chan 才删(防误删后来者
// 的重注册);不存在时幂等空操作。
func (r *CapabilityRegistry) dropWaitChan(sessionID, toolName string, ch chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := capabilityWaitKey(sessionID, toolName)
	if cur, ok := r.waiters[key]; ok && cur == ch {
		delete(r.waiters, key)
	}
}

// NewClientToolProxy 为每个声明创建稳定的 core.Tool，执行时动态解析绑定。
// Schema 无效时回退 object；Metadata 拷贝透传并附内部标记，等待预算归一化。
func NewClientToolProxy(reg *CapabilityRegistry, sessionID string, specs []ClientToolSpec, waitTimeout time.Duration) []core.Tool {
	wait := resolveCapabilityWait(waitTimeout)
	tools := make([]core.Tool, 0, len(specs))
	for _, spec := range specs {
		var schema map[string]any
		if err := json.Unmarshal([]byte(spec.Schema), &schema); err != nil || schema == nil {
			// 非法 schema 整体兜底,不放大为构造失败(探测面「方向必须是收缩
			// 不是放行」的镜像:坏声明只降级该工具的参数面,不注册不了了之)。
			schema = map[string]any{"type": "object"}
		}

		metadata := make(map[string]string, len(spec.Metadata)+1)
		for k, v := range spec.Metadata {
			metadata[k] = v
		}
		metadata[capabilityWaitMetaKey] = "true"

		tools = append(tools, core.Tool{
			Name:        spec.Name,
			Description: spec.Description,
			Parameters:  schema,
			Metadata:    metadata,
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				onWait := capabilityWaitObserver(ctx)
				if onWait == nil {
					// 未注入(RecordRun 同步路径直用代理):等待照常发生(安全
					// 超时兜底),仅 RunState 不迁移——空观察者即「仅执行面」。
					onWait = func(bool) {}
				}
				argsJSON, err := json.Marshal(args)
				if err != nil {
					return "", fmt.Errorf("capability tool %s: encode args: %w", spec.Name, err)
				}
				return executeCapabilityTool(ctx, reg, sessionID, spec.Name, argsJSON, wait, onWait)
			},
		})
	}
	return tools
}

// executeCapabilityTool 先解析并调用当前绑定；无绑定或普通调用失败时等待重绑。
// 就绪通知仅用于唤醒，之后必须重查绑定；等待总预算不重置，超时/取消返回失败文本。
func executeCapabilityTool(ctx context.Context, reg *CapabilityRegistry, sessionID, toolName string, argsJSON []byte, wait time.Duration, onWait capabilityWaitFunc) (string, error) {
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
		exec, connID, ok := reg.lookupResolved(sessionID, toolName)
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
			reg.Invalidate(sessionID, connID)
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
