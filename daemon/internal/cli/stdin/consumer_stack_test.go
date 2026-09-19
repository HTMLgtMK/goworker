package stdin

import "testing"

// fakeConsumer 记录 Consume 调用，并按预设返回消费结果。
type fakeConsumer struct {
	name      string
	consumed  map[KeyType]bool // 对某类型按键是否消费
	calls     []KeyEvent       // 收到的按键序列
	cancelAll bool             // 对一切按键消费
}

func (f *fakeConsumer) Consume(ev KeyEvent) bool {
	f.calls = append(f.calls, ev)
	if f.cancelAll {
		return true
	}
	return f.consumed[ev.Type]
}

func TestConsumerStackPushPop(t *testing.T) {
	top := &fakeConsumer{name: "top"}
	mid := &fakeConsumer{name: "mid"}
	base := &fakeConsumer{name: "base"}

	s := NewConsumerStack()
	if s.Top() != nil || s.Pop() != nil || s.Len() != 0 {
		t.Fatalf("empty stack: want nil top/pop, 0 len")
	}

	s.Push(base)
	s.Push(mid)
	s.Push(top)
	if s.Len() != 3 || s.Top() != top {
		t.Fatalf("after push: want len 3 top=%v, got len %d top=%v", top, s.Len(), s.Top())
	}
	if got := s.Pop(); got != top {
		t.Fatalf("pop: want %v, got %v", top, got)
	}
	if got := s.Pop(); got != mid {
		t.Fatalf("pop: want %v, got %v", mid, got)
	}
	if got := s.Pop(); got != base {
		t.Fatalf("pop: want %v, got %v", base, got)
	}
}

func TestConsumerStackDispatch(t *testing.T) {
	// 栈底 editor：消费编辑键；栈顶 keyWatcher：消费取消键，放行编辑键
	editor := &fakeConsumer{consumed: map[KeyType]bool{KeyChar: true}}
	kw := &fakeConsumer{consumed: map[KeyType]bool{KeyEsc: true}}

	s := NewConsumerStack()
	s.Push(editor)
	s.Push(kw)

	charEv := KeyEvent{Type: KeyChar, Rune: 'a'}

	// 编辑键不被 keyWatcher 消费，放行到 editor 被消费
	s.dispatch(charEv)
	if len(kw.calls) != 1 || kw.calls[0] != charEv {
		t.Fatalf("keyWatcher should receive char, got %v", kw.calls)
	}
	if len(editor.calls) != 1 || editor.calls[0] != charEv {
		t.Fatalf("editor should receive char, got %v", editor.calls)
	}

	// 取消键被栈顶 keyWatcher 消费，editor 收不到
	s.dispatch(key(KeyEsc))
	if len(kw.calls) != 2 || len(editor.calls) != 1 {
		t.Fatalf("Esc should stop at keyWatcher: kw=%v editor=%v", kw.calls, editor.calls)
	}
}

func TestConsumerStackDispatchDrop(t *testing.T) {
	// 无节点消费：事件静默丢弃，不 panic
	s := NewConsumerStack()
	s.dispatch(key(KeyChar))
	if s.Len() != 0 {
		t.Fatalf("len want 0, got %d", s.Len())
	}

	// 栈顶消费后终止，底层不收到
	top := &fakeConsumer{cancelAll: true}
	base := &fakeConsumer{consumed: map[KeyType]bool{KeyChar: true}}
	s.Push(base)
	s.Push(top)
	s.dispatch(key(KeyChar))
	if len(base.calls) != 0 {
		t.Fatalf("base should not receive after top consumed, got %v", base.calls)
	}
}
