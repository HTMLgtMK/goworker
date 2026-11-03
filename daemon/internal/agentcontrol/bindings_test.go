package agentcontrol

import (
	"reflect"
	"testing"
)

// bindings_test.go 是 BindingRegistry 的单元测试(Plan A2 生命周期半边,对齐
// lease_test.go 的注册表层风格):登记/改绑替换、断连清账、计数取数与空 clientID
// 语义。全链路(登记 → 引用归零判定 → CloseSession/Remove 收口)的用例由
// plugin 包的 lifecycle_test.go 覆盖,本文件锁死注册表自身的契约。

// TestBindingRegistryBindReplaceAndCount:登记/改绑/解绑对「绑定集合」的影响
// ——改绑即替换(旧会话失去引用)、重复绑定同一会话幂等、Bindings 返回升序
// 连接集合拷贝。
func TestBindingRegistryBindReplaceAndCount(t *testing.T) {
	r := NewBindingRegistry()

	if prev := r.Bind("conn-A", "sess-1"); prev != "" {
		t.Fatalf("first bind prev = %q, want empty", prev)
	}
	if prev := r.Bind("conn-B", "sess-1"); prev != "" {
		t.Fatalf("second connection bind prev = %q, want empty", prev)
	}
	if got := r.Bindings("sess-1"); !reflect.DeepEqual(got, []string{"conn-A", "conn-B"}) {
		t.Fatalf("Bindings(sess-1) = %v, want [conn-A conn-B] (升序集合)", got)
	}
	if got := r.Bindings("sess-404"); len(got) != 0 || got == nil {
		t.Fatalf("Bindings(unknown) = %v, want non-nil empty", got)
	}

	// 改绑:conn-A 从 sess-1 换到 sess-2 → 旧会话失去该引用,新会话获得。
	if prev := r.Bind("conn-A", "sess-2"); prev != "sess-1" {
		t.Fatalf("rebind prev = %q, want sess-1", prev)
	}
	if got := r.Bindings("sess-1"); !reflect.DeepEqual(got, []string{"conn-B"}) {
		t.Fatalf("Bindings(sess-1) after rebind = %v, want [conn-B]", got)
	}
	if got := r.Bindings("sess-2"); !reflect.DeepEqual(got, []string{"conn-A"}) {
		t.Fatalf("Bindings(sess-2) after rebind = %v, want [conn-A]", got)
	}

	// 重复绑定同一会话幂等(prev == sessionID,引用计数不变)。
	if prev := r.Bind("conn-B", "sess-1"); prev != "sess-1" {
		t.Fatalf("idempotent rebind prev = %q, want sess-1", prev)
	}
	if got := r.Bindings("sess-1"); !reflect.DeepEqual(got, []string{"conn-B"}) {
		t.Fatalf("Bindings(sess-1) after idempotent rebind = %v, want [conn-B]", got)
	}

	// Bind 空会话 id = 解除绑定(/new 收口 b.Set("") 的登记侧同形)。
	if prev := r.Bind("conn-B", ""); prev != "sess-1" {
		t.Fatalf("bind-empty prev = %q, want sess-1", prev)
	}
	if got := r.Bindings("sess-1"); len(got) != 0 {
		t.Fatalf("Bindings(sess-1) after bind-empty = %v, want empty", got)
	}

	// Unbind:整行清除,返回被解除的会话;未登记者幂等空操作。
	if prev := r.Unbind("conn-A"); prev != "sess-2" {
		t.Fatalf("unbind prev = %q, want sess-2", prev)
	}
	if prev := r.Unbind("conn-A"); prev != "" {
		t.Fatalf("second unbind prev = %q, want empty (idempotent)", prev)
	}
	if got := r.Bindings("sess-2"); len(got) != 0 {
		t.Fatalf("Bindings(sess-2) after unbind = %v, want empty", got)
	}

	// 空 clientID:Bind/Unbind 一律无操作。
	if prev := r.Bind("", "sess-1"); prev != "" {
		t.Fatalf("empty clientID bind prev = %q, want empty (rejected)", prev)
	}
	if prev := r.Unbind(""); prev != "" {
		t.Fatalf("empty clientID unbind prev = %q, want empty (no-op)", prev)
	}
	r.Bind("conn-C", "sess-3")
	if got := r.Bindings("sess-3"); !reflect.DeepEqual(got, []string{"conn-C"}) {
		t.Fatalf("empty clientID calls must not disturb registrations: %v", got)
	}
}

// TestServiceReleaseConnectionClearsBinding(Plan A2:断连清账与 lease 同构):
// Service.ReleaseConnection 在清全部 control lease 的同时清除该连接的绑定登记,
// 并返回被清除登记指向的会话 id;其他连接的租约与登记不受牵连。
func TestServiceReleaseConnectionClearsBinding(t *testing.T) {
	s := NewService(nil, &fakeRouter{}, nil)

	s.BindSession("conn-A", "sess-1")
	s.BindSession("conn-B", "sess-1")
	s.BindSession("conn-C", "sess-2")
	s.AcquireControl("sess-1", "conn-B")
	s.AcquireControl("sess-2", "conn-C")

	if prev := s.ReleaseConnection("conn-B"); prev != "sess-1" {
		t.Fatalf("ReleaseConnection prev = %q, want sess-1", prev)
	}
	if got := s.SessionBindings("sess-1"); !reflect.DeepEqual(got, []string{"conn-A"}) {
		t.Fatalf("SessionBindings(sess-1) after disconnect = %v, want [conn-A]", got)
	}
	if s.HoldControl("sess-1", "conn-B") {
		t.Fatal("ReleaseConnection must clear the connection's control leases too")
	}
	if !s.HoldControl("sess-2", "conn-C") || !reflect.DeepEqual(s.SessionBindings("sess-2"), []string{"conn-C"}) {
		t.Fatal("other connections' leases and bindings must be untouched")
	}

	// 未登记连接的断连:幂等空操作,prev 为空。
	if prev := s.ReleaseConnection("conn-404"); prev != "" {
		t.Fatalf("ReleaseConnection(unknown) prev = %q, want empty", prev)
	}
}

// TestServiceSessionBindingsCount(生命周期/A3 判定的取数契约):多连接同绑一
// 会话时计数齐全;解绑后归零;SessionBindings 是「绑定集合空 && 无在飞 run」
// 归零判定的绑定半边事实源。
func TestServiceSessionBindingsCount(t *testing.T) {
	s := NewService(nil, &fakeRouter{}, nil)

	for _, c := range []string{"conn-1", "conn-2", "conn-3"} {
		s.BindSession(c, "sess-X")
	}
	if got := len(s.SessionBindings("sess-X")); got != 3 {
		t.Fatalf("bindings count = %d, want 3", got)
	}

	// 切换触发点:conn-1 经 BindSession 改绑(返回旧会话,供调用方评估收口)。
	if prev := s.BindSession("conn-1", "sess-Y"); prev != "sess-X" {
		t.Fatalf("switch prev = %q, want sess-X", prev)
	}
	if got := len(s.SessionBindings("sess-X")); got != 2 {
		t.Fatalf("bindings count after switch = %d, want 2", got)
	}

	// /new 触发点:发起方 UnbindSession 返回被摘除的会话。
	if prev := s.UnbindSession("conn-2"); prev != "sess-X" {
		t.Fatalf("unbind prev = %q, want sess-X", prev)
	}
	if got := len(s.SessionBindings("sess-X")); got != 1 {
		t.Fatalf("bindings count after unbind = %d, want 1", got)
	}
}
