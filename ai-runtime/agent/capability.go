package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

// 本文件是客户端工具能力的 SDK 面(P4-3 构造面;总方案 §8 的
// ToolCapabilityBinding + 稳定 Tool Proxy):CapabilityRegistry 按
// sessionID→connectionID 登记连接级执行通道,代理工具(P4-4 补齐等待语义)
// 在每次 Execute 时经 Lookup 解析当前绑定,替换「ToolSource 只在 Run 开始
// 收集一次」的结构限制(勘察结论 D:调用时机=每轮 prompt 工具收集,
// session.go:119-121;总方案 §8 对 ToolSource「只调一次」的批评)。
//
// 语义铁律(总方案 §8):
//   - 绑定只按 Bind 登记返回,不存在隐式路由面——绝不能把工具请求默默路由到
//     没有对应设备能力的连接(§8 :650);
//   - TTL 兜底「连接死而无人 Unbind」:ExpiresAt = now+ttl,过期判定读时进行
//     (lazy expiry,无后台 goroutine),过期条目视同无绑定;
//   - 幂等重绑:同 (session,conn) 每次 prompt 探测都会 Bind,重绑 = 刷新 TTL
//     + 工具集(不是新增一条绑定)。
//
// 并发与锁论证(plan P4-3):r.mu 只护 bindings map 快照;Invoke 在锁外(执行
// I/O 不持锁,P4-4 的等待/调用全部发生在 proxy Execute 内,不在本文件);与
// manager.mu/rt.mu/gate/hub.mu 无嵌套。

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
)

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
// binding。mu 只护 bindings 的读写;所有判定(工具名匹配、过期)都在锁内取
// 快照完成,无回调、无 I/O。
type CapabilityRegistry struct {
	mu       sync.Mutex
	bindings map[string]map[string]*capabilityBinding
}

// NewCapabilityRegistry 创建空注册表。
func NewCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{bindings: make(map[string]map[string]*capabilityBinding)}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	conns, ok := r.bindings[sessionID]
	if !ok {
		return nil, false
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
				return binding.exec, true
			}
		}
	}
	return nil, false
}

// NewClientToolProxy 按声明产出稳定的 core.Tool 代理(P4-3 构造面):每个
// ClientToolSpec 一个代理工具,工具定义(Name/Description/Parameters/Metadata)
// 在构造期固化,执行端在每次 Execute 时经 reg.Lookup 解析(P4-4 补齐等待语义)。
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
//     同款;硬约束 < ai-core toolCtx 60s,见常量注释)。
//
// 本步只做构造面:Execute 返回占位错误(等待/绑定解析/失败失效语义在 P4-4
// 落码,届时复用本闭包捕获的 reg/sessionID/wait)。行为变化声明:零——代理
// 尚无 daemon 生产者(ACP 既有原始 relay 路径不变)。
func NewClientToolProxy(reg *CapabilityRegistry, sessionID string, specs []ClientToolSpec, waitTimeout time.Duration) []core.Tool {
	wait := waitTimeout
	if wait <= 0 {
		wait = defaultCapabilityWaitTimeout
	}
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
				// P4-3 占位:执行语义(①Lookup 命中 → Invoke;②无绑定/Invoke
				// 失败 → Invalidate + waiting_capability 等待重绑;③超时返回
				// 明确失败文本)在 P4-4 落码。占位错误带 wait 预算,方便上层
				// 辨认尚未接线的代理。
				return "", fmt.Errorf("capability tool %s: execute semantics not wired yet (P4-4), wait budget %s", spec.Name, wait)
			},
		})
	}
	return tools
}
