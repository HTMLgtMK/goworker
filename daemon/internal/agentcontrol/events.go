package agentcontrol

import (
	"context"
	"fmt"

	"github.com/tinguo/goworker/ai-runtime/session"
)

// events.go 是事件订阅契约(总方案 §七,PR3「收敛事件订阅接口」):EventHub
// 的具体实例(runtimeagent.EventHub)从 ACP 协议面隐藏——ACP 只面对
// SubscribeEvents 返回的 AttachResult 与 EventSubscription 订阅契约,负责通知
// 泵与 wire 转换;Replay/Live 原子切换、Seq 游标与 Journal 完整性归 EventHub
// (执行平面,§七「Re-Attach 的游标责任」),Application Control 只定位
// Session 并返回订阅句柄。
//
// 边界限制(§七「一项必要的边界限制」):SubscribeEvents 只做数据面订阅,绝不
// 顺便 AcquireControl——会话选择、事件订阅、控制权获取是三个独立操作(§6.2);
// attach(mode=control) 的两动作编排(先订阅成功、再取 lease、失败回滚不动
// lease)由 ACP handler 显式保证(第四轮评审 I4 语义)。这样只读观察者、网页
// 监控台、Session Debugger 等未来入口可复用订阅能力,不必获取控制权。

// EventSubscription 是一条会话事件订阅的回收契约(§七 草案类型,按工作树实态
// 落地:事件通道的元素类型是 session.SessionEvent,runtimeagent 无独立别名):
//   - Events 返回订阅的只读投递通道——hub 泵在 ctx 取消/慢订阅者断开/hub 停机
//     时关闭它,消费方 range 到关闭即知订阅终结,重连以已确认游标重新订阅;
//   - Close 取消底层 SDK 订阅(handler 作用域收口:detach/换代/断连统一回收,
//     订阅生命周期由消费方显式管理,不依赖 hub Close)。
//
// 实现方:本包内部的 hub 订阅适配(hubSubscription);接口即契约,宿主/测试
// 可自建同形实现(xgw 测试 shim 的 testSubscription 先例)。
type EventSubscription interface {
	Events() <-chan session.SessionEvent
	Close() error
}

// RunSnapshot 是活动 Run 的扁平快照(§七 AttachResult.CurrentRun 的形状):
// 取 AgentRun 按 wire 需要的最小面(RunID/State 字符串),不透传 runtime 句柄
// ——与 PendingApprovalInfo 的「扁平化不泄漏 agent 包结构」纪律同型(方向相反:
// 那处是 agent 结构不进 model,这处是 runtime 观测面收敛为订阅契约的一部分)。
type RunSnapshot struct {
	RunID string
	State string
}

// AttachResult 是一次事件订阅的完整结果(§七 草案类型,按工作树实态落地:
// ReplayIntegrity 是 runtimeagent 的三态字符串常量,无独立命名类型):
//   - LatestSeq:订阅注册后的会话游标快照——仅描述当前位置,客户端不得跳游标
//     (总方案 §7 :586-589);
//   - ReplayIntegrity:创建订阅**之后**读取的回放完整性三态(第四轮评审 §七
//     末段:不缓存旧状态——Subscribe 的回放路径可能把完整性降级为 Incomplete
//     (DE1-I1 的损坏批次跳过置位),先订阅后读才能反映本次订阅的真实完整性;
//     三态语义见 runtimeagent.ReplayIntegrity* 常量注释);
//   - CurrentRun:订阅时刻该会话活动 Run 的快照(rt.Active()),无活动 Run 或
//     无 runtime 为 nil(attach 响应的 currentRun=null 不是错误);
//   - Subscription:订阅回收契约,消费方 Close 即取消底层订阅。
type AttachResult struct {
	LatestSeq       uint64
	ReplayIntegrity string
	CurrentRun      *RunSnapshot
	Subscription    EventSubscription
}

// hubSubscription 是 EventSubscription 的 hub 订阅适配(§七:实现=hub 通道 +
// Close=cancel SDK 订阅):hub.Subscribe 返回的投递通道原样转交,订阅 ctx 的
// cancel 收口在 Close——调用方不再自持 cancel func(旧形态 handler 建_ctx 传
// 入 Subscribe 的两段式,收敛为契约内聚)。零新增 runtime 导出:EnsureHub/
// hub.Subscribe 均为既有导出面(第三轮评审 I3 的 EnsureHub 播种失败上抛语义
// 原样经本编排面上抛)。
type hubSubscription struct {
	ch     <-chan session.SessionEvent
	cancel context.CancelFunc
}

// 编译期断言:hub 通道适配满足订阅契约(签名漂移在编译期暴露,EventJournal
// 断言先例 event_hub.go:76)。
var _ EventSubscription = (*hubSubscription)(nil)

// Events 返回订阅的只读投递通道(hub.Subscribe 的返回原样转交)。
func (s *hubSubscription) Events() <-chan session.SessionEvent { return s.ch }

// Close 取消底层 SDK 订阅:hub 泵退出并关闭投递通道,订阅自注册表摘除
// (event_hub.go pumpEvents 的 defer 路径)。幂等(context.CancelFunc 幂等),
// 恒返回 nil(cancel 无错误面;hub 侧断开经通道关闭可观测,不经本方法返回)。
func (s *hubSubscription) Close() error {
	s.cancel()
	return nil
}

// SubscribeEvents 建立会话事件订阅(§七,RunControlSource.SubscribeEvents 的
// 编排面;方案草案签名带 ctx,落地按 PR3 拍板收敛为 (sessionID, afterSeq):
// 订阅必须活过单次请求——ai-acp 的 dispatchRequest 在响应返回时即 cancel 请求
// ctx,订阅 ctx 由本方法以 context.Background() 自建派生,经
// AttachResult.Subscription.Close 收口,直到 detach/换代/断连统一回收):
//
//  1. 定位 runtime(CR4 路由,SessionRouter 统一解析点);nil →
//     sessionNotFoundErr(诚实失败,不静默回落 current);
//  2. rt.EnsureHub()——遗留③「重启后 attach 时 hub 未建」的解法,attach 即建
//     (含 SeqRange 磁盘播种,游标续接既有历史;I3 语义:journal 播种读取失败
//     原样上抛,绝不返回看似健康的订阅);RunEvents 未接线/纯内存 → 诚实错误
//     (协议面映射 -32000,客户端指引 session/load);
//  3. hub.Subscribe(afterSeq)——Replay+Live 原子建立(event_hub.go 的 mu 内
//     一口气完成窗口校验/回放快照/注册;ErrCursorExpired 窗口失效原样上抛);
//  4. 创建订阅**之后**读取 LatestSeq 与 ReplayIntegrity(第四轮评审 §七 末段:
//     不缓存旧状态,回放期的完整性降级必须反映在本结果里);
//  5. rt.Active() 活动 Run 快照(与 RunStatus 的 active 分支同一观察点)。
//
// 不 AcquireControl(§七 边界):数据面与授权面拆分,control lease 的获取/
// 归还入口仍是 AcquireControl/ReleaseControl。失败路径零残留:步骤 1-3 任一
// 失败时订阅未建立(或 ctx 已收),不触碰既有订阅与 lease。
func (s *Service) SubscribeEvents(sessionID string, afterSeq uint64) (AttachResult, error) {
	rt := s.route(sessionID)
	if rt == nil {
		return AttachResult{}, sessionNotFoundErr(sessionID)
	}
	hub, err := rt.EnsureHub()
	if err != nil {
		return AttachResult{}, fmt.Errorf("agent: session %q: ensure event hub: %w", sessionID, err)
	}
	if hub == nil {
		return AttachResult{}, fmt.Errorf("agent: event stream not enabled for session %q (persistence disabled)", sessionID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := hub.Subscribe(ctx, afterSeq)
	if err != nil {
		cancel()
		return AttachResult{}, err
	}
	res := AttachResult{
		LatestSeq:       hub.LatestSeq(),
		ReplayIntegrity: hub.ReplayIntegrity(),
		Subscription:    &hubSubscription{ch: ch, cancel: cancel},
	}
	if run := rt.Active(); run != nil {
		res.CurrentRun = &RunSnapshot{RunID: run.ID, State: string(run.State())}
	}
	return res, nil
}
