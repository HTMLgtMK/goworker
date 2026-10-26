package agent

import (
	"context"
	"strings"
	"testing"
	"time"
)

// 本文件是 P4-3 CapabilityRegistry / ClientToolProxy 构造面的验收测试(纯单元
// 面,零 Provider、零 LLM):Bind/Lookup 的幂等往返与 TTL 过期、重绑刷新、跨
// connection 隔离(不存在隐式路由面)、代理构造断言(Name/Description/Schema/
// Metadata 透传 + capability_wait 标记)。语义铁律见 capability.go 头注释
// (总方案 §8);Execute 的等待/失效语义属 P4-4,本文件只钉构造面与占位错误。

// fakeClientExecutor 是 ClientToolExecutor 的测试桩:记录调用次数,返回预置
// 结果。各绑定用独立实例,靠指针相等断言 Lookup 返回的正是登记的执行通道。
type fakeClientExecutor struct {
	calls  int
	result string
	err    error
}

func (f *fakeClientExecutor) Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error) {
	f.calls++
	return f.result, f.err
}

// TestCapabilityRegistryBindLookupRoundtrip:Bind 新建 → refreshed=false,
// Lookup 按登记的 ToolNames/executor 返回;未登记的会话/工具一律 false——
// Lookup 只按 Bind 登记返回,不存在隐式路由面(总方案 §8 :650)。
func TestCapabilityRegistryBindLookupRoundtrip(t *testing.T) {
	r := NewCapabilityRegistry()
	execA := &fakeClientExecutor{result: "from-A"}

	if refreshed := r.Bind("sess-1", "conn-A", []string{"sys_x", "sys_y"}, time.Minute, execA); refreshed {
		t.Fatalf("first Bind must report refreshed=false")
	}
	got, ok := r.Lookup("sess-1", "sys_x")
	if !ok || got != execA {
		t.Fatalf("Lookup(sess-1, sys_x) = (%v, %v), want (execA, true)", got, ok)
	}
	if got, ok := r.Lookup("sess-1", "sys_y"); !ok || got != execA {
		t.Fatalf("Lookup(sess-1, sys_y) = (%v, %v), want (execA, true)", got, ok)
	}
	// 未登记的组合一律 false:未知工具、未知会话、未声明该工具的绑定。
	if _, ok := r.Lookup("sess-1", "sys_z"); ok {
		t.Fatalf("Lookup for unbound tool must be false")
	}
	if _, ok := r.Lookup("sess-other", "sys_x"); ok {
		t.Fatalf("Lookup for unknown session must be false")
	}
	// exec 为 nil 的绑定被拒绝(无执行通道的绑定无意义)。
	if refreshed := r.Bind("sess-1", "conn-N", []string{"sys_x"}, time.Minute, nil); refreshed {
		t.Fatalf("Bind with nil executor must be rejected (refreshed=false)")
	}
	if _, ok := r.Lookup("sess-1", "sys_x"); !ok {
		t.Fatalf("rejected Bind must not disturb existing binding")
	}
}

// TestCapabilityRegistryRebindRefreshes:同 (session,conn) 重绑 = 刷新 TTL +
// 工具集——refreshed=true;重绑后活过原 TTL 的到点(未刷新此刻必已过期);
// 工具集整体替换(旧集合中被剔除的工具不再可见)。每次 prompt 探测都会 Bind,
// 幂等是硬要求。
func TestCapabilityRegistryRebindRefreshes(t *testing.T) {
	r := NewCapabilityRegistry()
	execA := &fakeClientExecutor{}

	if refreshed := r.Bind("sess-1", "conn-A", []string{"sys_x", "sys_y"}, 200*time.Millisecond, execA); refreshed {
		t.Fatalf("first Bind must report refreshed=false")
	}
	time.Sleep(100 * time.Millisecond) // 原绑定走完一半寿命
	execA2 := &fakeClientExecutor{}
	if refreshed := r.Bind("sess-1", "conn-A", []string{"sys_x"}, 200*time.Millisecond, execA2); !refreshed {
		t.Fatalf("rebind of existing (session,conn) must report refreshed=true")
	}
	// 再走 120ms:距首次 Bind 已 220ms > 原 TTL 200ms——若 TTL 未被刷新,此处
	// 必已过期;Lookup 仍命中即钉死「重绑刷新 TTL」。
	time.Sleep(120 * time.Millisecond)
	if got, ok := r.Lookup("sess-1", "sys_x"); !ok || got != execA2 {
		t.Fatalf("binding must outlive the original TTL after rebind, got (%v, %v)", got, ok)
	}
	// 工具集是整体替换:重绑声明只有 sys_x,sys_y 不再可见。
	if _, ok := r.Lookup("sess-1", "sys_y"); ok {
		t.Fatalf("dropped tool must not stay visible after tool-set refresh")
	}
}

// TestCapabilityRegistryTTLExpiry:过期条目视同无绑定(Lazy expiry,读时逐出)
// ——过期后 Lookup=false,且条目已被逐出(再次 Bind 报 refreshed=false 即新建)。
// ttl<=0 产生即刻过期条目,Bind 照常登记但 Lookup 视同无绑定。
func TestCapabilityRegistryTTLExpiry(t *testing.T) {
	r := NewCapabilityRegistry()
	exec := &fakeClientExecutor{}

	r.Bind("sess-1", "conn-A", []string{"sys_x"}, 80*time.Millisecond, exec)
	if _, ok := r.Lookup("sess-1", "sys_x"); !ok {
		t.Fatalf("binding must be live before expiry")
	}
	time.Sleep(150 * time.Millisecond)
	if _, ok := r.Lookup("sess-1", "sys_x"); ok {
		t.Fatalf("expired binding must be treated as unbound")
	}
	// 读时逐出的可观测证据:过期后再 Bind 是新建(refreshed=false),而非对
	// 残留条目的刷新。
	if refreshed := r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, exec); refreshed {
		t.Fatalf("Bind after lazy eviction must be a fresh binding (refreshed=false)")
	}
	// ttl<=0:即刻过期,登记不报错但 Lookup 视同无绑定。
	r.Bind("sess-2", "conn-B", []string{"sys_x"}, 0, exec)
	if _, ok := r.Lookup("sess-2", "sys_x"); ok {
		t.Fatalf("binding with non-positive ttl must be immediately expired")
	}
}

// TestCapabilityRegistryUnbindConnection:UnbindConnection 撤销该连接在全部
// 会话下的绑定(连接退出收口);未知连接幂等空操作;不影响其他连接。
func TestCapabilityRegistryUnbindConnection(t *testing.T) {
	r := NewCapabilityRegistry()
	execA, execB := &fakeClientExecutor{}, &fakeClientExecutor{}

	r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, execA)
	r.Bind("sess-2", "conn-A", []string{"sys_x"}, time.Minute, execA)
	r.Bind("sess-1", "conn-B", []string{"sys_y"}, time.Minute, execB)

	r.UnbindConnection("conn-A")
	if _, ok := r.Lookup("sess-1", "sys_x"); ok {
		t.Fatalf("conn-A binding in sess-1 must be gone after UnbindConnection")
	}
	if _, ok := r.Lookup("sess-2", "sys_x"); ok {
		t.Fatalf("conn-A binding in sess-2 must be gone after UnbindConnection")
	}
	if got, ok := r.Lookup("sess-1", "sys_y"); !ok || got != execB {
		t.Fatalf("other connections must survive UnbindConnection, got (%v, %v)", got, ok)
	}
	// 幂等:重复 Unbind 与未知连接都是空操作(无 panic、无副作用)。
	r.UnbindConnection("conn-A")
	r.UnbindConnection("conn-never-bound")
	if got, ok := r.Lookup("sess-1", "sys_y"); !ok || got != execB {
		t.Fatalf("idempotent UnbindConnection must not disturb others, got (%v, %v)", got, ok)
	}
}

// TestCapabilityRegistryInvalidate:Invalidate 即时撤销指定 (session,conn)
// 绑定(Invoke 失败出口);幂等;只动该组合,不动同会话其他连接。
func TestCapabilityRegistryInvalidate(t *testing.T) {
	r := NewCapabilityRegistry()
	execA, execB := &fakeClientExecutor{}, &fakeClientExecutor{}

	r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, execA)
	r.Bind("sess-2", "conn-A", []string{"sys_x"}, time.Minute, execA)
	r.Bind("sess-1", "conn-B", []string{"sys_x"}, time.Minute, execB)

	r.Invalidate("sess-1", "conn-A")
	if _, ok := r.Lookup("sess-1", "sys_x"); !ok {
		t.Fatalf("sess-1/conn-A gone but conn-B still declares sys_x")
	}
	if got, ok := r.Lookup("sess-1", "sys_x"); !ok || got != execB {
		t.Fatalf("Lookup must fall through to conn-B's own binding, got (%v, %v)", got, ok)
	}
	if got, ok := r.Lookup("sess-2", "sys_x"); !ok || got != execA {
		t.Fatalf("Invalidate is scoped to (session,conn): sess-2/conn-A must survive, got (%v, %v)", got, ok)
	}
	// 幂等:重复 Invalidate 与未知组合都是空操作。
	r.Invalidate("sess-1", "conn-A")
	r.Invalidate("sess-x", "conn-x")
	if got, ok := r.Lookup("sess-2", "sys_x"); !ok || got != execA {
		t.Fatalf("idempotent Invalidate must not disturb others, got (%v, %v)", got, ok)
	}
}

// TestCapabilityRegistryCrossConnectionIsolation:绑定按 (session,connection)
// 隔离——A 的绑定对别的会话不可见,反向亦然;同会话两个连接都声明同名工具时,
// Lookup 按 connectionID 字典序确定性取首个(无隐式路由策略)。
func TestCapabilityRegistryCrossConnectionIsolation(t *testing.T) {
	r := NewCapabilityRegistry()
	execA, execB := &fakeClientExecutor{}, &fakeClientExecutor{}

	// 会话 1 挂 conn-A,会话 2 挂 conn-B:各自只声明自己的工具。
	r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, execA)
	r.Bind("sess-2", "conn-B", []string{"sys_y"}, time.Minute, execB)

	if _, ok := r.Lookup("sess-2", "sys_x"); ok {
		t.Fatalf("conn-A's tool must be invisible to sess-2 (no implicit routing)")
	}
	if _, ok := r.Lookup("sess-1", "sys_y"); ok {
		t.Fatalf("conn-B's tool must be invisible to sess-1 (no implicit routing)")
	}
	if got, ok := r.Lookup("sess-1", "sys_x"); !ok || got != execA {
		t.Fatalf("own-session binding must resolve to its own executor, got (%v, %v)", got, ok)
	}

	// 同会话两连接并存且都声明 sys_x(重绑前的瞬态):确定性按 connectionID
	// 字典序取首个——conn-A < conn-B,恒返回 execA。
	r.Bind("sess-1", "conn-B", []string{"sys_x"}, time.Minute, execB)
	for i := 0; i < 20; i++ {
		got, ok := r.Lookup("sess-1", "sys_x")
		if !ok || got != execA {
			t.Fatalf("Lookup must pick conn-A deterministically (sorted connectionID), got (%v, %v) at iter %d", got, ok, i)
		}
	}
	// 断开 conn-A 后解析面收敛到 conn-B:Bind 登记了什么就返回什么。
	r.UnbindConnection("conn-A")
	if got, ok := r.Lookup("sess-1", "sys_x"); !ok || got != execB {
		t.Fatalf("after UnbindConnection(conn-A) Lookup must return conn-B's executor, got (%v, %v)", got, ok)
	}
}

// TestClientToolProxyConstruction:代理构造面——每个 spec 一个 core.Tool,
// Name/Description 透传、Schema JSON 解析(非法回落 {"type":"object"})、
// Metadata 原样透传(risk_level/requires_permission/allow_always_key)+ 内部
// 标记 goworker.capability_wait;标记加在工具副本上,不污染调用方 spec。
// HITL 门控按 Metadata 在每次调用照常触发,透传即门控事实源。
func TestClientToolProxyConstruction(t *testing.T) {
	r := NewCapabilityRegistry()
	specs := []ClientToolSpec{
		{
			Name:        "sys_camera",
			Description: "take a photo",
			Schema:      `{"type":"object","properties":{"resolution":{"type":"string"}},"required":["resolution"]}`,
			Metadata: map[string]string{
				"risk_level":          "critical",
				"requires_permission": "true",
				"allow_always_key":    "sys_camera",
			},
		},
		{
			Name:        "sys_broken",
			Description: "bad schema",
			Schema:      `{not-json`,
		},
	}
	tools := NewClientToolProxy(r, "sess-1", specs, 2*time.Second)
	if len(tools) != len(specs) {
		t.Fatalf("proxy must produce one tool per spec, got %d want %d", len(tools), len(specs))
	}

	cam := tools[0]
	if cam.Name != "sys_camera" || cam.Description != "take a photo" {
		t.Fatalf("Name/Description must pass through, got %q / %q", cam.Name, cam.Description)
	}
	props, _ := cam.Parameters["properties"].(map[string]any)
	if props == nil || props["resolution"] == nil {
		t.Fatalf("valid schema must be parsed into Parameters, got %v", cam.Parameters)
	}
	if cam.Metadata["risk_level"] != "critical" ||
		cam.Metadata["requires_permission"] != "true" ||
		cam.Metadata["allow_always_key"] != "sys_camera" {
		t.Fatalf("policy metadata must pass through verbatim, got %v", cam.Metadata)
	}
	if cam.Metadata[capabilityWaitMetaKey] == "" {
		t.Fatalf("proxy tool must carry the %s marker for the P4-4 observer", capabilityWaitMetaKey)
	}
	if capabilityWaitMetaKey != "goworker.capability_wait" {
		t.Fatalf("capabilityWaitMetaKey value is a cross-step contract (P4-4 observer), got %q", capabilityWaitMetaKey)
	}
	// 透传是拷贝:调用方 spec.Metadata 不得被内部标记污染。
	if _, leaked := specs[0].Metadata[capabilityWaitMetaKey]; leaked {
		t.Fatalf("caller's spec.Metadata must not be mutated by the proxy marker")
	}

	broken := tools[1]
	if broken.Name != "sys_broken" {
		t.Fatalf("second tool name mismatch: %q", broken.Name)
	}
	if broken.Parameters["type"] != "object" || len(broken.Parameters) != 1 {
		t.Fatalf("invalid schema must fall back to {type:object}, got %v", broken.Parameters)
	}

	// 空 spec 列表 → 空代理面。
	if got := NewClientToolProxy(r, "sess-1", nil, time.Second); len(got) != 0 {
		t.Fatalf("no specs must yield no tools, got %d", len(got))
	}
}

// TestClientToolProxyExecutePlaceholder:本步只做构造面——Execute 返回占位
// 错误(等待/绑定解析/失败失效语义在 P4-4 落码);waitTimeout<=0 回落
// defaultCapabilityWaitTimeout=45s(双保险,硬约束 < ai-core toolCtx 60s),
// 占位错误文本带预算以便上层辨认。
func TestClientToolProxyExecutePlaceholder(t *testing.T) {
	r := NewCapabilityRegistry()

	custom := NewClientToolProxy(r, "sess-1", []ClientToolSpec{{Name: "sys_x", Schema: "{}"}}, 2*time.Second)
	if _, err := custom[0].Execute(context.Background(), map[string]any{}); err == nil ||
		!strings.Contains(err.Error(), "P4-4") || !strings.Contains(err.Error(), "2s") {
		t.Fatalf("placeholder execute must fail with budget echoed, got %v", err)
	}

	fallback := NewClientToolProxy(r, "sess-1", []ClientToolSpec{{Name: "sys_x", Schema: "{}"}}, 0)
	_, err := fallback[0].Execute(context.Background(), map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "45s") {
		t.Fatalf("non-positive waitTimeout must fall back to %s, got %v", defaultCapabilityWaitTimeout, err)
	}
	if defaultCapabilityWaitTimeout != 45*time.Second {
		t.Fatalf("defaultCapabilityWaitTimeout must stay 45s (< ai-core toolCtx 60s), got %v", defaultCapabilityWaitTimeout)
	}
}
