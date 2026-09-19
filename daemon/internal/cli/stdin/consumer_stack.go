package stdin

import "sync"

// Consumer 是责任链节点：处理 dispatch 路由的按键事件。
// Consume 返回 true 表示该事件已被本节点消费（终止链传递），false 放行给下一个节点。
// 每个节点只认自己职责内的事件，其余一律放行。
type Consumer interface {
	Consume(ev KeyEvent) bool
}

// ConsumerStack 以栈的形式组织消费者，替代多消费者抢同一个字节流的竞争模型。
//
// 事件从栈顶到栈底按责任链路由，第一个消费的节点终止传递；
// 无节点消费时静默丢弃（终端输入的兜底行为）。
//
// 典型栈态：
//
//	空闲       [keyWatcher, editor]            主输入循环，keyWatcher 常驻栈底
//	agent 运行 [keyWatcher, editor]            editor 无读行会话时放行，误敲丢弃
//	HITL       [keyWatcher, editor, HITLC]      HITL 会话在栈顶，编辑键归它
type ConsumerStack struct {
	mu    sync.Mutex
	items []Consumer
	buf   []Consumer // dispatch 快照缓冲（仅在 dispatch goroutine 内使用，无需锁）
}

func NewConsumerStack() *ConsumerStack {
	return &ConsumerStack{}
}

// Push 将消费者压栈。
func (s *ConsumerStack) Push(c Consumer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, c)
}

// Pop 弹出栈顶消费者并返回；空栈返回 nil。
func (s *ConsumerStack) Pop() Consumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) == 0 {
		return nil
	}
	top := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return top
}

// Top 返回栈顶消费者，空栈返回 nil。
func (s *ConsumerStack) Top() Consumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) == 0 {
		return nil
	}
	return s.items[len(s.items)-1]
}

// Len 返回栈内消费者数量。
func (s *ConsumerStack) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// dispatch 从栈顶到栈底路由一个按键事件，第一个消费的节点终止。
// 由 KeyDecoder goroutine 调用。
//
// 先在锁内快照消费者列表，再释放锁遍历——Consume 可能阻塞投递
// （keyCh 背压），持锁调用会把锁拖死，导致主循环的 Top/Push 全部卡死。
// 栈在遍历期间可能变化，但正在读行的消费者（active）只有一个且必然
// 出现在快照里，未在读行的消费者收到事件也会放行，语义不受影响。
func (s *ConsumerStack) dispatch(ev KeyEvent) {
	s.mu.Lock()
	n := len(s.items)
	if cap(s.buf) < n {
		s.buf = make([]Consumer, n)
	}
	s.buf = s.buf[:n]
	copy(s.buf, s.items)
	s.mu.Unlock()

	for i := n - 1; i >= 0; i-- {
		if s.buf[i].Consume(ev) {
			return
		}
	}
}
