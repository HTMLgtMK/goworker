package stdin

// 测试共用的按键构造与投递工具。

func key(t KeyType) KeyEvent { return KeyEvent{Type: t} }

func char(r rune) KeyEvent { return KeyEvent{Type: KeyChar, Rune: r} }

// typeKey 向任意责任链消费者投递一个按键事件（模拟 dispatch 路由）。
func typeKey(c Consumer, ev KeyEvent) { c.Consume(ev) }

// typeString 逐个字符投递 KeyChar 事件。
func typeString(c Consumer, s string) {
	for _, r := range s {
		c.Consume(char(r))
	}
}
