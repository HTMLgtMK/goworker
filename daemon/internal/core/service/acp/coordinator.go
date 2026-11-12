package acp

// SessionGate serializes session creation, load, and archival across connections; prompts use
// per-runtime gates. Its short critical sections change registry/repository state only and never
// wait on execution or connection I/O. A full one-slot channel makes tryAcquire return busy
// instead of queueing management operations.
type SessionGate struct{ ch chan struct{} }

// NewSessionGate 创建门禁；装配层在构造 ACP service 时创建一次，全 daemon 共享。
func NewSessionGate() *SessionGate {
	return &SessionGate{ch: make(chan struct{}, 1)}
}

// tryAcquire 非阻塞尝试获取管理门禁；失败即返回 false，由调用方对客户端报
// busy（sessionBusyError）——管理操作互斥的诚实拒绝，绝不排队：管理临界区
// 短（无 LLM 调用、无连接 I/O），busy 是瞬态错误。
func (g *SessionGate) tryAcquire() bool {
	select {
	case g.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// release 归还门禁；必须与成功的 tryAcquire 一一配对（调用方手动在临界区收窄
// 处调用——重放/下发是连接 I/O，不能攥住门禁），多还、少还都会破坏互斥。
func (g *SessionGate) release() {
	<-g.ch
}
