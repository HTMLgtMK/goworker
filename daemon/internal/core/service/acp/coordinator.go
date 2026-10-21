package acp

import "context"

// SessionGate 是 ACP 前端层的会话执行协调器：daemon 是单活动会话模型，prompt
// 执行与会话切换（session/new、session/load-current）必须互斥。没有它，Prompt
// 的 superseded 守卫检查与 evaluate 之间存在 TOCTOU 空档——另一条连接的
// session/new 会在空档里把新 cwd 应用到正在执行的会话上（评审 P1）。
//
// 门禁由装配层创建、全 daemon 一份、跨所有连接共享；每连接一份的
// FrontendHandler 只是它的使用方，不是拥有方。
//
// 为什么是容量 1 的 channel 而非 sync.Mutex：Mutex 的获取无法被 context 取消，
// 连接断开（ctx canceled）后排队的 prompt 会永远悬在锁上；channel 让 acquire
// 在 select 里同时监听 ctx.Done()，连接一断立即以错误退出，错误路径保持诚实。
//
// 死锁分析（与 daemon/internal/plugins/agent/plugin.go 的 runGate 注释同一论证）：
// 门禁只被 FrontendHandler 的 ACP 入口获取。prompt 执行期间的嵌套命令执行
// （collectTools → hub.Eval 跑 /new、/compact 等）直接走 Engine，从不回到
// FrontendHandler，因此持有门禁的 prompt 不会递归再需要它。
type SessionGate struct{ ch chan struct{} }

// NewSessionGate 创建门禁；装配层在构造 ACP service 时创建一次，全 daemon 共享。
func NewSessionGate() *SessionGate {
	return &SessionGate{ch: make(chan struct{}, 1)}
}

// acquire 阻塞获取门禁；ctx 取消（连接断开）时立即失败返回，不无限排队。
func (g *SessionGate) acquire(ctx context.Context) error {
	select {
	case g.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryAcquire 非阻塞尝试获取；失败即返回 false，由调用方决定报 busy 还是排队。
func (g *SessionGate) tryAcquire() bool {
	select {
	case g.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// release 归还门禁；必须与成功的 acquire/tryAcquire 一一配对（调用方以 defer
// 持有），多还、少还都会破坏互斥。
func (g *SessionGate) release() {
	<-g.ch
}
