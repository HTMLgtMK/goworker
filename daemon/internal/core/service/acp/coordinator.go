package acp

// SessionGate 是 ACP 前端层的会话**管理**门禁（A2b2 起职责收窄——plan A2
// 「SessionGate 保留、职责收缩」）：只守会话管理操作（session/new、session/load
// 的可 prompt 分支；A3 落地的 session/archive 加入同一互斥面），短临界区互斥
// ——管理面不并发操纵 manager 注册表与 repo 缓存。prompt 已退出本门禁：执行
// 互斥下沉 per-runtime gate（插件 withSession 的 AcquireRun，容量 1）——同一
// session 的 prompt 串行、跨 session 并行；在飞 prompt 不再阻塞 new/load，
// 反之 new/load 也不再阻塞 prompt（busy 语义只发生在管理操作之间）。
//
// 历史（为什么曾经互斥一切）：daemon 曾是单活动会话模型，session/new 经
// SetSession 把新 cwd 应用到 current 正在执行的会话上——prompt 的 superseded
// 守卫检查与 evaluate 之间存在 TOCTOU 空档（评审 P1），需要门禁把 prompt 与
// 会话切换全序化。A2b1 后病灶消失：session/new 真新建（CreateBoundSession 只
// Create 新 store + 注册新 runtime，不动 current、不触碰任何在飞会话的状态）；
// cwd 是会话属性（runtime 级声明），声明只落新建/重开的目标会话。prompt 经
// binding → sessionRuntimeIn 解析 runtime 实例快照、执行全程持其 gate；管理
// 操作在全局临界区内只做状态突变（Create/注册/摘发起方 binding/引用减一），
// 不碰在飞 run 的实例——TOCTOU 论证的完整展开见 handler.go Prompt 函数头。
//
// 锁序（plan A2）：全局 gate 临界区从不等待 runtime gate（管理操作的临界区
// 只含注册表/repo 突变，无 LLM 调用、无 runtime gate 获取），两域无嵌套、
// 无环；这也是 new/load 不被在飞 prompt 卡死的前提。
//
// 门禁由装配层创建、全 daemon 一份、跨所有连接共享；每连接一份的
// FrontendHandler 只是它的使用方，不是拥有方。
//
// 为什么是容量 1 的 channel 而非 sync.Mutex：Mutex 的获取无法被 context 取消
// （历史 acquire 是阻塞获取方——prompt 排队面，A2b2 随 prompt 退出而退役，
// 方法删除）；管理操作只走 tryAcquire（满即 busy 拒绝，客户端稍后重试），
// channel 的非阻塞语义即 busy 的实现。
//
// 死锁分析（与 daemon/internal/plugins/agent/plugin.go 的 runGate 注释同一论证）：
// 门禁只被 FrontendHandler 的 ACP 管理入口获取。prompt 执行期间的嵌套命令执行
// （collectTools → hub.Eval 跑 /new、/compact 等）直接走 Engine，从不回到
// FrontendHandler，因此管理临界区不会被嵌套命令重入（prompt 链内的嵌套 /new
// 不取本门禁，它有自己的 withSession runtime gate 语义）。
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
