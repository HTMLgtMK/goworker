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

// 本文件是客户端工具能力的 SDK 面(P4-3 构造面 + P4-4 等待/失效语义;总方案
// §8 的 ToolCapabilityBinding + 稳定 Tool Proxy):CapabilityRegistry 按
// sessionID→connectionID 登记连接级执行通道,代理工具在每次 Execute 时经
// Lookup 解析当前绑定,替换「ToolSource 只在 Run 开始收集一次」的结构限制
// (勘察结论 D:调用时机=每轮 prompt 工具收集,session.go:119-121;总方案 §8
// 对 ToolSource「只调一次」的批评)。
//
// 语义铁律(总方案 §8):
//   - 绑定只按 Bind 登记返回,不存在隐式路由面——绝不能把工具请求默默路由到
//     没有对应设备能力的连接(§8 :650);
//   - 断连 → 显式等待重绑 + 超时安全失败(§8 :668-677):无绑定 / Invoke 失败
//     即时失效后进入等待,onWait 观察者把 Run 迁移 waiting_capability;等待
//     具备相同工具能力的客户端显式重绑(就绪通知只是唤醒,事实源是 registry
//     的 Bind),重绑成功则重新 Invoke;超时/取消返回明确失败文本交 Agent
//     决定下一步——绝不跨设备静默执行;
//   - TTL 兜底「连接死而无人 Unbind」:ExpiresAt = now+ttl,过期判定读时进行
//     (lazy expiry,无后台 goroutine),过期条目视同无绑定;
//   - 幂等重绑:同 (session,conn) 每次 prompt 探测都会 Bind,重绑 = 刷新 TTL
//     + 工具集(不是新增一条绑定)。
//
// 并发与锁论证(plan P4-3/P4-4):r.mu 只护 bindings/waiters map 快照;Invoke
// 在锁外(执行 I/O 不持锁);等待发生在 proxy Execute 内(工具 goroutine 即
// run goroutine,ai-core toolCtx 60s 预算内,ai-core/agent/agent.go:147-153),
// waitChan 的注册/投递/撤销都只做 map 快照操作,不持锁等待;onWait 闭包调
// run.transition(run.mu)与 journalRunState(锁外 I/O)——与 supervisor 既有
// 锁序无环;慢等待不阻塞 AgentRun 之外的任何组件(等待本身就占着 run)。

const (
	// capabilityWaitMetaKey 是代理工具 Metadata 的内部标记键(P4-4 supervisor
	// 观察者注入的识别键):supervisor 的 ToolSource 包装面据此识别能力代理、
	// 注入 onWait 闭包(RunWaitingCapability 状态迁移)。值为任意非空串——
	// 消费面只判存在性(plan P4-4:Metadata[capabilityWaitMetaKey] != "")。
	// 内部约定:provider 序列化不输出 Metadata(core.Tool 契约),不会泄漏进
	// 模型可见的 schema。
	capabilityWaitMetaKey = "goworker.capability_wait"

	// defaultCapabilityWaitTimeout 是能力等待的默认预算(P4-4 proxy Execute
	// 的等待上限;openQuestions ③ 拍板 45s)。硬约束:必须 < ai-core toolCtx
	// 60s(ai-core/agent/agent.go:147-153)——等待发生在工具 Execute 内,预算
	// 留余量否则被 60s 掐断呈现为普通超时(勘察 risk)。NewClientToolProxy 对
	// <=0 回落此处(双保险,P2-7 同款),不依赖 yaml 载入路径。
	defaultCapabilityWaitTimeout = 45 * time.Second

	// maxCapabilityWaitTimeout 是显式配置等待预算的安全钳制上限(P4 评审
	// issue2:「必须 < ai-core toolCtx 60s」的硬约束从注释落为代码)。显式
	// 配置 >55s 时钳到该值并 slog.Warn——超配的等待必被 toolCtx 60s 掐断,
	// 「等待重绑→明确失败文本(capabilityFailureText)」的声明语义无声劣化为
	// 「莫名工具超时」,且失败文案不再是 capabilityFailureText,用户与模型
	// 无从分辨。55s 给 Invoke 留 5s 余量;45s 默认与 1~55s 显式区间原样放行。
	maxCapabilityWaitTimeout = 55 * time.Second
)

// capabilityWaitObserverKey 是 onWait 观察者的 ctx 注入键(supervisor 观察者
// 包装面专用,同包共享;unexported 空结构体类型防跨包碰撞):代理 Execute 从
// toolCtx 读出观察者。未注入(RecordRun 同步路径直用代理)时为 nil——等待
// 照常发生(安全超时兜底)但 RunState 不迁移,waiting_capability 仅后台 Run
// 可观测(plan P4-4 limitation:同步路径不包装 ToolSource)。
type capabilityWaitObserverKey struct{}

// capabilityWaitFunc 是能力等待的观察者回调(waiting=true 进入等待,false
// 等待收敛)。代理侧的配对纪律:waiting 标志 false→true 恰好调一次 true、
// true→false 恰好调一次 false——全部发生在执行 goroutine 上,顺序翻转即配对
// (plan §6.9 的出口矩阵:重绑成功/超时/ctx 取消三条路径各恰好一次
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
// (45s,双保险 P2-7 同款);显式配置 > maxCapabilityWaitTimeout(55s)时钳到
// 上限并 slog.Warn(P4 评审 issue2:45/55 < ai-core toolCtx 60s 的硬约束落地,
// 超配的静默语义劣化改为显式降级+告警)。行为面由单元测试钉死,不做真实等待。
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

// ToolCapabilityBinding 是一笔能力绑定(总方案 §8 :655-661 原形):某会话的某
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
// Invoke 失败由调用方(proxy Execute,P4-4)即时 Invalidate 该绑定。
type ClientToolExecutor interface {
	Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error)
}

// ClientToolSpec 是代理工具的声明(daemon 从 acpworker.DeviceToolDescriptor
// 转换,P5-9 接线)。Metadata 里的 risk_level/requires_permission/allow_always_key
// 原样透传到 core.Tool.Metadata——HITL 门控(hitl.go:113-122 resolveToolPolicy)
// 按 Metadata 裁决,声明即策略的单一事实源(sys_relay.go:118-120 同款边界)。
type ClientToolSpec struct {
	Name        string
	Description string
	Schema      string            // JSON Schema 字符串;非法/空回落 {"type":"object"}
	Metadata    map[string]string // 策略元数据原样透传
}

// capabilityBinding 是 registry 内部条目:导出值形态(ToolCapabilityBinding)
// + 执行通道。exec 不进导出结构——ToolCapabilityBinding 是总方案的绑定快照,
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
	// waiters 是重绑就绪通知的注册表(P4-4):(sessionID,toolName) → 容量 1
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

// Bind 登记/刷新一笔绑定:同 (sessionID, connectionID) 的既有绑定被整体替换
// (刷新 TTL + 工具集),否则新建。每次 prompt 的设备工具探测都会走到这里,
// 因此必须幂等——重绑不是错误,是刷新。ExpiresAt = now+ttl(TTL 兜底「连接死
// 而无人 Unbind」;ttl<=0 产生即刻过期条目,Lookup 视同无绑定)。
//
// 返回 refreshed=true 表示这是对既有绑定的重绑(刷新路径),false 表示新建
// 绑定;exec 为 nil 时拒绝登记(无执行通道的绑定无意义)并返回 false。
// toolNames 拷贝存储,与调用方切片脱钩(探测结果可能被上层复用/复写)。
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
	// Bind 成功即对等待者非阻塞投递就绪信号(P4-4):新声明/刷新的每个工具名
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
// 的 P5-7 钩子调用)。连接不存在时为幂等空操作;撤销后清掉空会话桶。
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
// 调用,P4-4):连接声明过能力但执行失败 = 通道事实不可用,不等 TTL。不存在的
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

// Lookup 解析某会话某工具的当前执行通道:只返回 Bind 登记过且未过期、且
// ToolNames 含 toolName 的绑定——不存在任何隐式路由(未登记的工具/会话/连接
// 一律 false,总方案 §8 :650)。过期条目视同无绑定,并读时逐出(lazy expiry,
// V1 无后台清扫 goroutine)。
//
// 同会话多连接都声明同名工具时,按 connectionID 字典序取首个(确定性选择,
// 不引入「最近绑定优先」之类的隐式策略;V1 常态是重绑前先 Unbind 旧连接,
// 多连接并存是瞬态)。
func (r *CapabilityRegistry) Lookup(sessionID, toolName string) (ClientToolExecutor, bool) {
	exec, _, ok := r.lookupResolved(sessionID, toolName)
	return exec, ok
}

// lookupResolved 是 Lookup 的内部扩展形(P4-4 proxy Execute 使用):额外返回
// 绑定所属 connectionID——Invoke 失败的 Invalidate 需要精确撤销「该绑定」
// 而非盲删。判定语义与 Lookup 完全一致(过期读时逐出、确定性字典序选择、
// 无隐式路由),导出面保持 P4-3 形状不变。
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

// NewClientToolProxy 按声明产出稳定的 core.Tool 代理(P4-3 构造面 + P4-4
// 执行语义):每个 ClientToolSpec 一个代理工具,工具定义(Name/Description/
// Parameters/Metadata)在构造期固化,执行端在每次 Execute 时经 reg.Lookup
// 解析当前绑定。
//
//   - Parameters:spec.Schema JSON 解析;非法/空回落 {"type":"object"}
//     (acpworker/sys_relay.go:125-129 同款兜底——"null" 反序列化也得 nil map);
//   - Metadata:spec.Metadata 原样透传(拷贝,不别名调用方 map),并增内部
//     标记 capabilityWaitMetaKey(goworker.capability_wait)——P4-4 supervisor
//     观察者注入的识别键。HITL 门控(hitl.go:113-122 resolveToolPolicy +
//     :201-249 checkSys)在每次调用照常触发:门控读的是固化在工具定义上的
//     Metadata,与执行端绑定无关,「危险操作重绑后仍需确认」由中间件 per-call
//     裁决天然保证(总方案 §6 :524);
//   - waitTimeout <=0 回落 defaultCapabilityWaitTimeout(45s,双保险 P2-7
//     同款);>55s 钳到 maxCapabilityWaitTimeout 并 slog.Warn(resolveCapa-
//     bilityWait,硬约束 < ai-core toolCtx 60s 从注释落为代码,见常量注释);
//   - Execute 语义(等待/失效/超时)见 executeCapabilityTool;onWait 观察者
//     经 ctx 注入(supervisor 的 ToolSource 包装面),未注入时等待照常发生、
//     仅 RunState 不迁移(waiting_capability 仅后台 Run 可观测的 limitation)。
//
// 行为变化声明:零——代理尚无 daemon 生产者(ACP 既有原始 relay 路径不变,
// P5-9 才接线)。
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

// executeCapabilityTool 是代理工具的执行语义(P4-4,总方案 §8 :668-677 的
// 「显式等待重新绑定,且超时安全失败」):
//
//  1. 绑定在(每轮 prompt 探测后连接未断开)→ Invoke,成功即返回;
//  2. 无绑定 / Invoke 失败(先分类再 Invalidate,P4 评审 issue5:错误为
//     context.Canceled/DeadlineExceeded——run 取消传播或 toolCtx 60s 到点
//     ——时跳过 Invalidate 直接上抛,健康绑定不得被误失效;其余失败才
//     Invalidate——精确到 connectionID,连接声明过能力但执行失败 = 通道
//     事实不可用,不等 TTL)→ 进入等待:
//     onWait(true)(supervisor 观察者把 Run 迁移 waiting_capability)→
//     select{就绪通知 / ctx.Done / waitTimer};
//  3. 就绪通知只是唤醒,事实源是 registry:唤醒后重查 lookupResolved,命中
//     (= registry 内出现了含本工具名的显式 Bind——「具备相同工具能力」由
//     Bind 的 ToolNames 校验、「获宿主授权」由 V1 本机信任模型承担,plan
//     拍板②)则 onWait(false) → 重新 Invoke;不匹配/虚假唤醒继续等。重绑
//     不改变门控:HITL 按固化在工具定义上的 Metadata 在每次调用裁决,重绑
//     后的再次调用照常走 per-call 确认(危险操作重绑后仍需确认,总方案 §6
//     :524);
//  4. 超时/ctx 取消 → onWait(false) → 返回明确失败文本(device tool X
//     unavailable: connection lost, waited Ns)交 Agent 决定下一步(§8
//     :677)——失败是工具结果不是执行异常(等待出口 error 恒 nil;唯一例外
//     是步骤 2 的取消/超时上抛,交 ai-core 错误通道),不中断 ReAct 循环,
//     模型可在下一轮重试(重试即全新 Execute,重新解析绑定与预算)。
//
// 出口矩阵(plan §6.9 review 重点):「重绑成功」「超时」「ctx 取消」三条
// 退出路径各恰好一次 onWait(false) 与一次结果返回——onWait 的配对由 waiting
// 标志翻转保证(false→true 恰调一次 true,true→false 恰调一次 false;全部
// 发生在执行 goroutine 上,无并发),defer leaveWait 兜底任何 return 路径;
// 未进入过等待的成功路径零次 onWait 调用。
//
// 预算:waitTimer 惰性起表(首次进入等待才计时,Invoke 耗时不占等待预算),
// 全程不复位——重绑后再失败回到等待时沿用剩余预算,总等待 <= wait,给
// ai-core toolCtx 60s(ai-core/agent/agent.go:147-153)留足余量(硬约束见
// defaultCapabilityWaitTimeout 注释)。
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
				// 取消/超时不是「通道事实不可用」(P4 评审 issue5):runCtx→
				// agentCtx→toolCtx 传播的取消,或 toolCtx 60s 到点——绑定本身
				// 健康,Invalidate 会误失效它(后者使同 run 下一次调用从直连
				// Invoke 劣化为「等待至超时→失败文本」),失败文本 connection
				// lost 在取消场景也名不副实。跳过 Invalidate 直接上抛,交
				// ai-core 的工具错误通道(agent.go:149-152 转为 error 文本工具
				// 结果,不中断执行面结构)。
				return result, err
			}
			// Invoke 失败 = 通道事实不可用:即时失效「该绑定」,转入等待——
			// 绝不静默吞掉失败后路由到其他连接(总方案 §8 :650)。
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

// capabilityFailureText 组装等待出口的明确失败文本(总方案 §8 :677「把明确
// 的工具失败结果交给 Agent,由 Agent 决定下一步」)。waited 取自等待真实
// 起点的墙钟差(毫秒取整),非配置预算的复述。
func capabilityFailureText(toolName string, waited time.Duration) string {
	return fmt.Sprintf("device tool %s unavailable: connection lost, waited %s", toolName, waited.Round(time.Millisecond))
}
