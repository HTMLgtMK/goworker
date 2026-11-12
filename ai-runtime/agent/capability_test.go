package agent

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/ai-sandbox"
)

// 本文件验收 CapabilityRegistry/ClientToolProxy 的绑定隔离、TTL、代理构造及执行等待语义，
// 并覆盖等待/失效状态和 StartRun 全链路。所有用例使用模拟 Provider；语义契约见 capability.go。

// fakeClientExecutor 是 ClientToolExecutor 的测试桩:记录调用次数,返回预置
// 结果。各绑定用独立实例,靠指针相等断言 Lookup 返回的正是登记的执行通道。
type fakeClientExecutor struct {
	mu     sync.Mutex
	calls  int
	result string
	err    error
}

func (f *fakeClientExecutor) Invoke(ctx context.Context, toolName string, argsJSON []byte) (string, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.result, f.err
}

// TestCapabilityRegistryBindConnectionLifecycle:连接绑定不依赖 TTL,跨时间仍有效;
// UnbindConnection 是唯一连接生命周期收口,调用后 Lookup 不再命中。
func TestCapabilityRegistryBindConnectionLifecycle(t *testing.T) {
	r := NewCapabilityRegistry()
	exec := &fakeClientExecutor{result: "connection-live"}

	if refreshed := r.BindConnection("sess-connection", "conn-A", []string{"sys_x"}, exec); refreshed {
		t.Fatal("first BindConnection must report refreshed=false")
	}
	time.Sleep(30 * time.Millisecond)
	if got, ok := r.Lookup("sess-connection", "sys_x"); !ok || got != exec {
		t.Fatalf("connection binding expired without disconnect: Lookup = (%v, %v)", got, ok)
	}

	r.UnbindConnection("conn-A")
	if _, ok := r.Lookup("sess-connection", "sys_x"); ok {
		t.Fatal("connection binding must be unavailable after UnbindConnection")
	}
}

func TestCapabilityRegistryBindConnectionWakesWaiter(t *testing.T) {
	r := NewCapabilityRegistry()
	tools := NewClientToolProxy(r, "sess-wait", []ClientToolSpec{{Name: "sys_x", Schema: "{}"}}, time.Second)
	waitState := make(chan bool, 2)
	ctx := withCapabilityWaitObserver(context.Background(), func(waiting bool) {
		waitState <- waiting
	})
	result := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		got, err := tools[0].Execute(ctx, map[string]any{})
		result <- got
		errCh <- err
	}()

	select {
	case waiting := <-waitState:
		if !waiting {
			t.Fatal("first wait notification must enter waiting state")
		}
	case <-time.After(time.Second):
		t.Fatal("tool did not enter waiting state")
	}

	exec := &fakeClientExecutor{result: "rebound"}
	if refreshed := r.BindConnection("sess-wait", "conn-A", []string{"sys_x"}, exec); refreshed {
		t.Fatal("first BindConnection must report refreshed=false")
	}
	select {
	case got := <-result:
		if got != "rebound" {
			t.Fatalf("Execute result = %q, want rebound", got)
		}
	case <-time.After(time.Second):
		t.Fatal("BindConnection did not wake the waiting tool")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("executor invoked %d times, want 1", exec.calls)
	}
	select {
	case waiting := <-waitState:
		if waiting {
			t.Fatal("waiter must leave waiting state after successful rebind")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not report leaving waiting state")
	}
}

// TestSpecAwareProxyMatchesFrozenSchemaAndPolicy verifies that a proxy created for an older
// Run cannot invoke a connection advertising a changed schema/policy, while a proxy created
// for the newer Run can. Description and internal/non-policy metadata are not compatibility keys.
func TestSpecAwareProxyMatchesFrozenSchemaAndPolicy(t *testing.T) {
	r := NewCapabilityRegistry()
	oldSpec := ClientToolSpec{
		Name:        "sys_camera",
		Description: "old description",
		Schema:      `{"type":"object","properties":{"resolution":{"type":"string"}}}`,
		Metadata: map[string]string{
			"risk_level":               "normal",
			"requires_permission":      "false",
			"goworker.capability_wait": "caller-value-is-not-policy",
			"display_hint":             "old",
		},
	}
	newSpec := ClientToolSpec{
		Name:        oldSpec.Name,
		Description: "new description",
		Schema:      `{"type":"object","properties":{"resolution":{"type":"string"},"flash":{"type":"boolean"}}}`,
		Metadata:    map[string]string{"risk_level": "critical", "requires_permission": "true"},
	}
	newExec := &fakeClientExecutor{result: "new-contract"}
	if r.BindConnectionSpecs("sess-spec", "conn-A", []ClientToolSpec{newSpec}, newExec) {
		t.Fatal("first spec binding must report refreshed=false")
	}

	oldProxy := NewSpecAwareClientToolProxy(r, "sess-spec", []ClientToolSpec{oldSpec}, 40*time.Millisecond)
	got, err := oldProxy[0].Execute(context.Background(), map[string]any{})
	if err != nil || !strings.Contains(got, "unavailable") {
		t.Fatalf("old proxy result = (%q, %v), want compatibility wait timeout", got, err)
	}
	if newExec.calls != 0 {
		t.Fatalf("new-contract executor invoked %d times by old proxy, want 0", newExec.calls)
	}

	newProxy := NewSpecAwareClientToolProxy(r, "sess-spec", []ClientToolSpec{newSpec}, time.Second)
	if got, err := newProxy[0].Execute(context.Background(), map[string]any{}); err != nil || got != "new-contract" {
		t.Fatalf("new proxy result = (%q, %v), want new-contract", got, err)
	}

	// Register the old contract using equivalent JSON formatting and a changed description.
	oldExec := &fakeClientExecutor{result: "old-contract"}
	oldAdvertised := oldSpec
	oldAdvertised.Description = "description does not participate"
	oldAdvertised.Schema = ` { "properties" : { "resolution" : { "type" : "string" } }, "type" : "object" } `
	oldAdvertised.Metadata = map[string]string{
		"risk_level":               "normal",
		"requires_permission":      "false",
		"goworker.capability_wait": "different-internal-value",
		"display_hint":             "new-non-policy-value",
	}
	if r.BindConnectionSpecs("sess-spec", "conn-B", []ClientToolSpec{oldAdvertised}, oldExec) {
		t.Fatal("first conn-B binding must report refreshed=false")
	}
	if got, err := oldProxy[0].Execute(context.Background(), map[string]any{}); err != nil || got != "old-contract" {
		t.Fatalf("old proxy must match canonical schema and policy despite description change: (%q, %v)", got, err)
	}
}

func TestSpecAwareProxyDistinguishesLargeSchemaIntegers(t *testing.T) {
	const expectedSchema = `{"type":"object","properties":{"x":{"const":9007199254740992}}}`
	const advertisedSchema = `{"type":"object","properties":{"x":{"const":9007199254740993}}}`

	r := NewCapabilityRegistry()
	exec := &fakeClientExecutor{result: "must-not-run"}
	r.BindConnectionSpecs("sess-large-integer", "conn-A", []ClientToolSpec{{Name: "sys_x", Schema: advertisedSchema}}, exec)
	proxy := NewSpecAwareClientToolProxy(r, "sess-large-integer", []ClientToolSpec{{Name: "sys_x", Schema: expectedSchema}}, 25*time.Millisecond)

	got, err := proxy[0].Execute(context.Background(), map[string]any{})
	if err != nil || !strings.Contains(got, "unavailable") {
		t.Fatalf("proxy result = (%q, %v), want compatibility timeout for distinct integers above 2^53", got, err)
	}
	if exec.calls != 0 {
		t.Fatalf("incompatible executor invoked %d times, want 0", exec.calls)
	}
}

func TestSpecAwareProxyFingerprintPreservesSchemaFallbackAndParameters(t *testing.T) {
	r := NewCapabilityRegistry()
	fallbackExec := &fakeClientExecutor{result: "fallback-matched"}
	r.BindConnectionSpecs("sess-schema-fallback", "conn-fallback", []ClientToolSpec{{Name: "sys_fallback", Schema: `{} trailing`}}, fallbackExec)
	validLargeSchema := `{"type":"object","properties":{"x":{"const":9007199254740993}}}`
	largeExec := &fakeClientExecutor{result: "valid-schema-matched"}
	r.BindConnectionSpecs("sess-schema-fallback", "conn-large", []ClientToolSpec{{Name: "sys_large", Schema: validLargeSchema}}, largeExec)

	proxy := NewSpecAwareClientToolProxy(r, "sess-schema-fallback", []ClientToolSpec{
		{Name: "sys_fallback", Schema: `{"type":"object"}`},
		{Name: "sys_large", Schema: validLargeSchema},
	}, time.Second)
	if got, err := proxy[0].Execute(context.Background(), map[string]any{}); err != nil || got != "fallback-matched" {
		t.Fatalf("trailing-invalid schema fallback result = (%q, %v), want fallback-matched", got, err)
	}
	if got, err := proxy[1].Execute(context.Background(), map[string]any{}); err != nil || got != "valid-schema-matched" {
		t.Fatalf("valid large-number schema result = (%q, %v), want valid-schema-matched", got, err)
	}

	parameters := proxy[1].Parameters["properties"].(map[string]any)["x"].(map[string]any)
	if value, ok := parameters["const"].(float64); !ok || value != float64(9007199254740993) {
		t.Fatalf("core.Tool.Parameters const = (%T) %v, want existing float64 decoding behavior", parameters["const"], parameters["const"])
	}
	fallbackParameters := proxy[0].Parameters
	if len(fallbackParameters) != 1 || fallbackParameters["type"] != "object" {
		t.Fatalf("trailing-invalid schema Parameters = %v, want object fallback", fallbackParameters)
	}
}

func TestSpecAwareProxyRejectsSchemaOrPolicyDriftIndependently(t *testing.T) {
	base := ClientToolSpec{
		Name:     "sys_x",
		Schema:   `{"type":"object","properties":{"x":{"type":"string"}}}`,
		Metadata: map[string]string{"risk_level": "normal", "requires_permission": "false"},
	}
	alwaysHITL := base
	alwaysHITL.Metadata = map[string]string{"risk_level": "normal", "requires_permission": "false", "permission_policy": "always_hitl"}
	tests := []struct {
		name       string
		expected   ClientToolSpec
		advertised ClientToolSpec
	}{
		{
			name:     "schema drift",
			expected: base,
			advertised: ClientToolSpec{
				Name: base.Name, Schema: `{"type":"object","properties":{"y":{"type":"string"}}}`,
				Metadata: map[string]string{"risk_level": "normal", "requires_permission": "false"},
			},
		},
		{
			name:       "risk policy drift",
			expected:   base,
			advertised: ClientToolSpec{Name: base.Name, Schema: base.Schema, Metadata: map[string]string{"risk_level": "critical", "requires_permission": "false"}},
		},
		{
			name:       "permission policy added",
			expected:   base,
			advertised: alwaysHITL,
		},
		{
			name:       "permission policy removed",
			expected:   alwaysHITL,
			advertised: base,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewCapabilityRegistry()
			exec := &fakeClientExecutor{result: "must-not-run"}
			r.BindConnectionSpecs("sess-drift", "conn-A", []ClientToolSpec{tt.advertised}, exec)
			proxy := NewSpecAwareClientToolProxy(r, "sess-drift", []ClientToolSpec{tt.expected}, 25*time.Millisecond)
			got, err := proxy[0].Execute(context.Background(), map[string]any{})
			if err != nil || !strings.Contains(got, "unavailable") {
				t.Fatalf("proxy result = (%q, %v), want compatibility timeout", got, err)
			}
			if exec.calls != 0 {
				t.Fatalf("incompatible executor invoked %d times, want 0", exec.calls)
			}
		})
	}
}

func TestLegacyProxyRejectsMismatchedSpecAwareBinding(t *testing.T) {
	r := NewCapabilityRegistry()
	oldSpec := ClientToolSpec{
		Name:     "sys_mode_off",
		Schema:   `{"type":"object"}`,
		Metadata: map[string]string{"risk_level": "never"},
	}
	newConnectionSpec := ClientToolSpec{
		Name:        oldSpec.Name,
		Description: "forces HITL even in mode off",
		Schema:      oldSpec.Schema,
		Metadata: map[string]string{
			"risk_level":        "never",
			"permission_policy": "always_hitl",
		},
	}
	legacyExec := &fakeClientExecutor{result: "must-not-fallback-to-unverified-legacy"}
	r.BindConnection("sess-legacy-proxy", "conn-legacy", []string{oldSpec.Name}, legacyExec)
	newExec := &fakeClientExecutor{result: "must-not-run-with-old-policy"}
	r.BindConnectionSpecs("sess-legacy-proxy", "conn-new", []ClientToolSpec{newConnectionSpec}, newExec)

	// The pre-existing proxy API must freeze the old Run's policy and refuse a new
	// spec-aware connection whose always_hitl behavior is not present in that Run.
	// A same-name legacy binding cannot bypass the incompatible verified candidate.
	oldProxy := NewClientToolProxy(r, "sess-legacy-proxy", []ClientToolSpec{oldSpec}, 25*time.Millisecond)
	got, err := oldProxy[0].Execute(context.Background(), map[string]any{})
	if err != nil || !strings.Contains(got, "unavailable") {
		t.Fatalf("old proxy result = (%q, %v), want timeout for mismatched always_hitl registration", got, err)
	}
	if newExec.calls != 0 {
		t.Fatalf("new always_hitl executor invoked %d times by old proxy, want 0", newExec.calls)
	}
	if legacyExec.calls != 0 {
		t.Fatalf("unverified legacy executor invoked %d times despite incompatible verified registration, want 0", legacyExec.calls)
	}
}

func TestSpecAwareProxyFreezesMetadataAndSharesSchemaFallback(t *testing.T) {
	r := NewCapabilityRegistry()
	registered := ClientToolSpec{
		Name:     "sys_x",
		Schema:   `{not-json`,
		Metadata: map[string]string{"risk_level": "normal"},
	}
	exec := &fakeClientExecutor{result: "fallback-matched"}
	if r.BindConnectionSpecs("sess-frozen", "conn-A", []ClientToolSpec{registered}, exec) {
		t.Fatal("first binding must report refreshed=false")
	}
	registered.Metadata["risk_level"] = "critical"

	proxySpec := ClientToolSpec{
		Name:     "sys_x",
		Schema:   `{"type":"object"}`,
		Metadata: map[string]string{"risk_level": "normal"},
	}
	proxy := NewSpecAwareClientToolProxy(r, "sess-frozen", []ClientToolSpec{proxySpec}, time.Second)
	proxySpec.Metadata["risk_level"] = "critical"
	proxySpec.Schema = `{"type":"array"}`

	if got, err := proxy[0].Execute(context.Background(), map[string]any{}); err != nil || got != "fallback-matched" {
		t.Fatalf("proxy did not preserve its snapshotted contract: (%q, %v)", got, err)
	}
}

func TestSpecAwareProxyRejectsLegacyUnverifiedBinding(t *testing.T) {
	r := NewCapabilityRegistry()
	legacyExec := &fakeClientExecutor{result: "unverified"}
	r.BindConnection("sess-legacy", "conn-A", []string{"sys_x"}, legacyExec)
	proxy := NewSpecAwareClientToolProxy(r, "sess-legacy", []ClientToolSpec{{Name: "sys_x", Schema: `{"type":"object"}`}}, 30*time.Millisecond)

	got, err := proxy[0].Execute(context.Background(), map[string]any{})
	if err != nil || !strings.Contains(got, "unavailable") {
		t.Fatalf("strict proxy result = (%q, %v), want timeout for unverified legacy binding", got, err)
	}
	if legacyExec.calls != 0 {
		t.Fatalf("legacy executor invoked %d times by strict proxy, want 0", legacyExec.calls)
	}
}

func TestSpecAwareWaitersAllWakeAndCleanUp(t *testing.T) {
	r := NewCapabilityRegistry()
	const waiterCount = 4
	spec := ClientToolSpec{Name: "sys_x", Schema: `{ "type" : "object" }`}
	proxy := NewSpecAwareClientToolProxy(r, "sess-many-waiters", []ClientToolSpec{spec}, 2*time.Second)
	ready := make(chan struct{}, waiterCount)
	results := make(chan string, waiterCount)
	for i := 0; i < waiterCount; i++ {
		go func() {
			ctx := withCapabilityWaitObserver(context.Background(), func(waiting bool) {
				if waiting {
					ready <- struct{}{}
				}
			})
			got, err := proxy[0].Execute(ctx, map[string]any{})
			if err != nil {
				results <- "error: " + err.Error()
				return
			}
			results <- got
		}()
	}
	for i := 0; i < waiterCount; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("not all concurrent calls entered capability wait")
		}
	}

	exec := &fakeClientExecutor{result: "woken"}
	if r.BindConnectionSpecs("sess-many-waiters", "conn-A", []ClientToolSpec{spec}, exec) {
		t.Fatal("first spec binding must report refreshed=false")
	}
	for i := 0; i < waiterCount; i++ {
		select {
		case got := <-results:
			if got != "woken" {
				t.Fatalf("waiter result = %q, want woken", got)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter %d was not woken by binding", i)
		}
	}
	if exec.calls != waiterCount {
		t.Fatalf("executor invoked %d times, want %d", exec.calls, waiterCount)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.waiters[capabilityWaitKey("sess-many-waiters", "sys_x")]; ok {
		t.Fatal("completed Execute calls left waiter registrations behind")
	}
}

type blockingClientExecutor struct {
	started chan struct{}
	release chan struct{}
}

func (e *blockingClientExecutor) Invoke(context.Context, string, []byte) (string, error) {
	close(e.started)
	<-e.release
	return "stale failure", errors.New("old channel failed")
}

func TestSpecAwareInvokeFailureDoesNotInvalidateReplacement(t *testing.T) {
	r := NewCapabilityRegistry()
	spec := ClientToolSpec{Name: "sys_x", Schema: `{"type":"object"}`}
	stale := &blockingClientExecutor{started: make(chan struct{}), release: make(chan struct{})}
	if r.BindConnectionSpecs("sess-rebind", "conn-A", []ClientToolSpec{spec}, stale) {
		t.Fatal("first binding must report refreshed=false")
	}
	proxy := NewSpecAwareClientToolProxy(r, "sess-rebind", []ClientToolSpec{spec}, time.Second)
	result := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		got, err := proxy[0].Execute(context.Background(), map[string]any{})
		result <- got
		errCh <- err
	}()
	select {
	case <-stale.started:
	case <-time.After(time.Second):
		t.Fatal("stale executor was not invoked")
	}

	replacement := &fakeClientExecutor{result: "replacement"}
	if !r.BindConnectionSpecs("sess-rebind", "conn-A", []ClientToolSpec{spec}, replacement) {
		t.Fatal("replacement binding must report refreshed=true")
	}
	close(stale.release)
	select {
	case got := <-result:
		if got != "replacement" {
			t.Fatalf("Execute result = %q, want replacement after stale failure", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement binding was removed by stale Invoke failure")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if replacement.calls != 1 {
		t.Fatalf("replacement executor invoked %d times, want 1", replacement.calls)
	}
}

// TestCapabilityRegistryBindLookupRoundtrip verifies the legacy TTL Bind lookup contract.
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

// TestClientToolProxyExecuteInvokeAndTimeoutFail(P4-4):Execute 真实语义的
// 单元面——绑定在 → Invoke 结果直通;无绑定 → 等待满短预算 → 明确失败文本
// (device tool X unavailable: connection lost, waited Ns)。失败是工具结果
// 不是执行异常(error 恒 nil),交 Agent 决定下一步(总方案 §8 :677)。
func TestClientToolProxyExecuteInvokeAndTimeoutFail(t *testing.T) {
	r := NewCapabilityRegistry()
	specs := []ClientToolSpec{{Name: "sys_x", Schema: "{}"}}

	// 绑定在:Invoke 结果直通,零等待(onWait 零调用面无观察者可断,结果即证)。
	exec := &fakeClientExecutor{result: "invoked-ok"}
	r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, exec)
	tools := NewClientToolProxy(r, "sess-1", specs, time.Second)
	got, err := tools[0].Execute(context.Background(), map[string]any{"k": "v"})
	if err != nil || got != "invoked-ok" {
		t.Fatalf("Execute with live binding = (%q, %v), want (invoked-ok, nil)", got, err)
	}
	if exec.calls != 1 {
		t.Fatalf("executor invoked %d time(s), want 1", exec.calls)
	}

	// 无绑定:等待满短预算 → 明确失败文本(error 恒 nil,不 panic、不中断)。
	empty := NewClientToolProxy(NewCapabilityRegistry(), "sess-1", specs, 80*time.Millisecond)
	start := time.Now()
	res, err := empty[0].Execute(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("timeout exit must be a tool result, not an error: %v", err)
	}
	if !strings.Contains(res, "device tool sys_x unavailable: connection lost") || !strings.Contains(res, "waited") {
		t.Fatalf("timeout result = %q, want explicit failure text", res)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("Execute returned after %v, want it to hold the wait window", elapsed)
	}
}

// TestCapabilityWaitTimeoutFallback:waitTimeout<=0 回落 defaultCapabilityWait
// Timeout(45s,双保险 P2-7 同款;硬约束 < ai-core toolCtx 60s)。行为面经
// resolveCapabilityWait 单元钉死,45s 不做真实等待。
func TestCapabilityWaitTimeoutFallback(t *testing.T) {
	if got := resolveCapabilityWait(0); got != defaultCapabilityWaitTimeout {
		t.Fatalf("resolveCapabilityWait(0) = %v, want %v", got, defaultCapabilityWaitTimeout)
	}
	if got := resolveCapabilityWait(-time.Second); got != defaultCapabilityWaitTimeout {
		t.Fatalf("resolveCapabilityWait(-1s) = %v, want %v", got, defaultCapabilityWaitTimeout)
	}
	if got := resolveCapabilityWait(3 * time.Second); got != 3*time.Second {
		t.Fatalf("resolveCapabilityWait(3s) = %v, want 3s(显式预算不被覆盖)", got)
	}
	if defaultCapabilityWaitTimeout != 45*time.Second {
		t.Fatalf("defaultCapabilityWaitTimeout must stay 45s (< ai-core toolCtx 60s), got %v", defaultCapabilityWaitTimeout)
	}
}

// TestCapabilityWaitTimeoutClampedToToolCtxBudget(P4 评审 issue2):显式配置
// >=60s 被钳到 maxCapabilityWaitTimeout(55s)并 slog.Warn——45/55 < ai-core
// toolCtx 60s 的硬约束从注释落为代码,超配的「静默语义劣化」改为显式降级+
// 告警;<=0 回落 45s、1~55s 显式区间原样放行(上限值本身不钳)。
func TestCapabilityWaitTimeoutClampedToToolCtxBudget(t *testing.T) {
	// 捕获 slog 默认 logger 输出断言 warn 面(本包测试不并行,SetDefault 安全;
	// defer 还原,不污染其他用例)。
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	if got := resolveCapabilityWait(60 * time.Second); got != 55*time.Second {
		t.Fatalf("resolveCapabilityWait(60s) = %v, want 55s(超配被钳制)", got)
	}
	if got := resolveCapabilityWait(90 * time.Second); got != maxCapabilityWaitTimeout {
		t.Fatalf("resolveCapabilityWait(90s) = %v, want %v", got, maxCapabilityWaitTimeout)
	}
	// 钳制边界内原样放行:55s 恰为上限,不得自钳;1s 显式预算不被覆盖。
	if got := resolveCapabilityWait(55 * time.Second); got != 55*time.Second {
		t.Fatalf("resolveCapabilityWait(55s) = %v, want 55s(上限值原样)", got)
	}
	if got := resolveCapabilityWait(time.Second); got != time.Second {
		t.Fatalf("resolveCapabilityWait(1s) = %v, want 1s(区间内不被钳)", got)
	}
	if !strings.Contains(buf.String(), "clamped") {
		t.Fatalf("clamp warning missing from log output: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "configured=1m30s") {
		t.Fatalf("clamp warning missing configured value: %q", buf.String())
	}
}

// TestCapabilityInvokeFailureClassification(P4 评审 issue5):Invoke 失败按
// 错误类别分流——context.Canceled/DeadlineExceeded(run 取消传播 / toolCtx
// 60s 到点)不是「通道事实不可用」,跳过 Invalidate 直接上抛(健康绑定不得
// 误失效;Invalidate 无计数面,以「绑定仍可 Lookup 命中」为等价观测);其余
// 失败仍即时失效并转入等待,超时返回明确失败文本。
func TestCapabilityInvokeFailureClassification(t *testing.T) {
	specs := []ClientToolSpec{{Name: "sys_x", Schema: "{}"}}

	t.Run("ctx cancel keeps binding", func(t *testing.T) {
		r := NewCapabilityRegistry()
		exec := &fakeClientExecutor{err: context.Canceled}
		r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, exec)
		tools := NewClientToolProxy(r, "sess-1", specs, time.Second)

		_, err := tools[0].Execute(context.Background(), map[string]any{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute = %v, want context.Canceled rethrown", err)
		}
		if exec.calls != 1 {
			t.Fatalf("executor invoked %d time(s), want 1", exec.calls)
		}
		if _, ok := r.Lookup("sess-1", "sys_x"); !ok {
			t.Fatal("binding invalidated on cancel(取消不得误失效健康绑定)")
		}
	})

	t.Run("tool deadline keeps binding", func(t *testing.T) {
		r := NewCapabilityRegistry()
		exec := &fakeClientExecutor{err: context.DeadlineExceeded}
		r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, exec)
		tools := NewClientToolProxy(r, "sess-1", specs, time.Second)

		_, err := tools[0].Execute(context.Background(), map[string]any{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Execute = %v, want context.DeadlineExceeded rethrown", err)
		}
		if _, ok := r.Lookup("sess-1", "sys_x"); !ok {
			t.Fatal("binding invalidated on deadline(60s 超时不得误失效健康绑定)")
		}
	})

	t.Run("plain error invalidates and waits", func(t *testing.T) {
		r := NewCapabilityRegistry()
		exec := &fakeClientExecutor{err: errors.New("connection dead")}
		r.Bind("sess-1", "conn-A", []string{"sys_x"}, time.Minute, exec)
		tools := NewClientToolProxy(r, "sess-1", specs, 80*time.Millisecond)

		res, err := tools[0].Execute(context.Background(), map[string]any{})
		if err != nil {
			t.Fatalf("wait-exit must be a tool result, not an error: %v", err)
		}
		if !strings.Contains(res, "device tool sys_x unavailable: connection lost") {
			t.Fatalf("result = %q, want explicit failure text", res)
		}
		if _, ok := r.Lookup("sess-1", "sys_x"); ok {
			t.Fatal("binding kept on plain failure(通道事实不可用必须即时失效)")
		}
	})
}

// P4-4 验收等待/超时/失效、waiting_capability 与观察者注入。
// 断连测试用 tool_call token 的 EmitToken 回调触发解绑；调用面未保证回调先于 Execute，
// 因此需要确定性先后关系的断连场景必须在 StartRun 前完成解绑。

// capToolName 是能力测试的代理工具名:sys_ 前缀命中 HITL 的 checkSys 面
// (hitl.go:201-249),metadata risk_level 声明即策略(never=normal 模式直接
// 放行,mode=normal 模式询问)。
const capToolName = "sys_camera"

// capToolCallRound 构造一轮对代理工具的 tool call 消息(scriptedChatProvider 脚本)。
func capToolCallRound(id string) core.Message {
	return core.Message{Role: "assistant", ToolCalls: []core.ToolCall{{
		ID:       id,
		Type:     "function",
		Function: core.ToolCallFunction{Name: capToolName, Arguments: `{"resolution":"4k"}`},
	}}}
}

// capToolSpecs 构造代理声明;riskLevel 透传进 Metadata(HITL 门控的事实源)。
func capToolSpecs(riskLevel string) []ClientToolSpec {
	return []ClientToolSpec{{
		Name:        capToolName,
		Description: "take a photo",
		Schema:      `{"type":"object"}`,
		Metadata:    map[string]string{"risk_level": riskLevel},
	}}
}

// newCapabilityRunRuntime 构造「仅 ToolSource 工具面」的后台 Run 测试装置:
// CollectTools 返回 nil(代理工具只经 RunRequest.ToolSource 进入),沙箱用
// 默认配置(normal)。RunJournal 仅非 nil 时装配(typed-nil 纪律同
// newHitlRunRuntime,approval_test.go)。
func newCapabilityRunRuntime(t *testing.T, m *SessionManager, id string, pv core.Provider, jr *stubRunJournal) string {
	t.Helper()
	deps := SessionDeps{
		Config:       runtimeconfig.Default(),
		CollectTools: func(*sandbox.Config) []core.Tool { return nil },
		NewProvider: func(*runtimeconfig.Config) (core.Provider, error) {
			return pv, nil
		},
		Store: &fakeConversationStore{},
	}
	if jr != nil {
		deps.RunJournal = jr
	}
	s := NewSession(deps)
	rt := NewSessionRuntime(id, s)
	if err := m.Register(rt); err != nil {
		t.Fatalf("register: %v", err)
	}
	return id
}

// waitRunState 轮询直至句柄到达 want 状态(onWait(true) 的迁移与观察者注入
// 都在执行 goroutine 上,观察点异步可达,有界等待防挂死)。
func waitRunState(t *testing.T, h RunHandle, want RunState) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for h.State() != want {
		select {
		case <-deadline:
			t.Fatalf("state never reached %q (now %q)", want, h.State())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// waitToolCallUnbound 收集 tool_result，并在目标 tool_call 到达时执行 disconnect。
// EmitToken 的通道交接完成后 Execute 可能先于回调运行，因此回调不能承担严格的先行编排；
// 需要先于 Execute 的断连必须预先完成。该钩子只用于无时序要求的通知和收集。
func waitToolCallUnbound(disconnect func(), toolResults *[]string) (cb RunCallbacks, done chan struct{}) {
	done = make(chan struct{})
	fired := false // 仅执行 goroutine 读写(EmitToken 单调用面)
	cb = RunCallbacks{
		EmitToken: func(tok core.Token) {
			if tok.Type == core.TokenTypeToolCall && tok.ToolCall.Function.Name == capToolName && !fired {
				fired = true
				disconnect()
				close(done)
			}
			if tok.Type == core.TokenTypeToolResult {
				*toolResults = append(*toolResults, tok.Content)
			}
		},
	}
	return cb, done
}

// TestToolBindingDisconnect 验证解绑后 run 等待显式重绑，不会静默路由到旧连接；
// 断连在 StartRun 前确定性完成。重绑后仅新 executor 被调用；在途 Invoke 失败后的
// 即时失效与等待由 TestCapabilityInvokeFailureClassification 覆盖。
func TestToolBindingDisconnect(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	reg := m.Capabilities()

	dead := &fakeClientExecutor{err: errors.New("connection dead")}
	execB := &fakeClientExecutor{result: "photo-bytes-from-B"}
	reg.Bind("cap-disc", "conn-A", []string{capToolName}, time.Minute, dead)

	pv := &scriptedChatProvider{msgs: []core.Message{
		capToolCallRound("call-cap-1"),
		{Role: "assistant", Content: "photo taken"},
	}}
	sid := newCapabilityRunRuntime(t, m, "cap-disc", pv, nil)
	tools := NewClientToolProxy(reg, sid, capToolSpecs("never"), 5*time.Second)

	// 断连编排:先于 StartRun(确定性),handler.Close 断连收口的等价物。
	reg.UnbindConnection("conn-A")

	var toolResults []string
	cb, _ := waitToolCallUnbound(func() {}, &toolResults) // 只收集 tool_result

	h, err := m.StartRun(sid, RunRequest{Input: "take a photo", ToolSource: func(context.Context) []core.Tool { return tools }}, cb)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	waitRunState(t, h, RunWaitingCapability) // 断连 → waiting_capability(与 waiting_input 可区分)

	reg.Bind(sid, "conn-B", []string{capToolName}, time.Minute, execB) // 显式重绑:含同名工具

	res := waitHandle(t, h) // 工具在 conn-B 执行完成 → RunCompleted+Persisted
	if dead.calls != 0 {
		t.Fatalf("dead executor invoked %d time(s), want 0(绝不静默路由到已断开连接)", dead.calls)
	}
	if execB.calls != 1 {
		t.Fatalf("conn-B executor invoked %d time(s), want 1(重绑后恰执行一次)", execB.calls)
	}
	if len(toolResults) != 1 || toolResults[0] != "photo-bytes-from-B" {
		t.Fatalf("tool results = %v, want [photo-bytes-from-B](结果来自 conn-B)", toolResults)
	}
	if res.State != RunCompleted {
		t.Fatalf("state = %q, want RunCompleted", res.State)
	}
}

// TestToolBindingTimeoutFails:无重绑 → 短窗等待 → 超时返回明确失败文本交
// Agent → run 正常完成(失败是工具结果不是执行异常:journal 终态 completed
// 但工具结果可辨,总方案 §8 :677)。
func TestToolBindingTimeoutFails(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	reg := m.Capabilities() // 从不绑定:等待预算耗尽是唯一出口

	jr := &stubRunJournal{}
	pv := &scriptedChatProvider{msgs: []core.Message{
		capToolCallRound("call-cap-t"),
		{Role: "assistant", Content: "handled unavailable tool"},
	}}
	sid := newCapabilityRunRuntime(t, m, "cap-timeout", pv, jr)
	tools := NewClientToolProxy(reg, sid, capToolSpecs("never"), 200*time.Millisecond)

	var toolResults []string
	cb, _ := waitToolCallUnbound(func() {}, &toolResults) // 只收集 tool_result

	h, err := m.StartRun(sid, RunRequest{Input: "take a photo", ToolSource: func(context.Context) []core.Tool { return tools }}, cb)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	res := waitHandle(t, h) // 超时不挂死:run 正常 RunCompleted

	if len(toolResults) != 1 {
		t.Fatalf("tool results = %v, want exactly 1", toolResults)
	}
	if !strings.Contains(toolResults[0], "device tool "+capToolName+" unavailable: connection lost") ||
		!strings.Contains(toolResults[0], "waited") {
		t.Fatalf("tool result %q missing explicit failure text", toolResults[0])
	}
	if res.State != RunCompleted {
		t.Fatalf("state = %q, want RunCompleted(失败结果交给 Agent,run 不失败)", res.State)
	}

	// journal 观测面:queued → running → waiting_capability → completed,
	// waiting_capability 先于终态(durable-first;isTerminalRunState 不含它,
	// 恢复扫描把卡在此态的 run 标 interrupted)。
	recs := jr.snapshot()
	wantStates := []string{string(RunQueued), string(RunRunning), string(RunWaitingCapability), string(RunCompleted)}
	if len(recs) != len(wantStates) {
		var got []string
		for _, rec := range recs {
			got = append(got, rec.Kind+":"+rec.State)
		}
		t.Fatalf("journal records = %v, want states %v", got, wantStates)
	}
	for i, rec := range recs {
		if rec.RunID != h.RunID || rec.State != wantStates[i] {
			t.Fatalf("record %d = {run:%q state:%q}, want run=%q state=%q", i, rec.RunID, rec.State, h.RunID, wantStates[i])
		}
	}
}

// TestRebindRequiresCapabilityMatch:重绑连接的 ToolNames 不含该工具 → 等待
// 不解除直至超时。就绪通知按 (sessionID, toolName) 寻址:mismatch 的 Bind 根
// 本不会唤醒 sys_camera 的等待者;且唤醒后的事实源仍是 registry 重查(通知
// 只是唤醒,不匹配继续等)——双重不依赖通知面的「善意」。
func TestRebindRequiresCapabilityMatch(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()
	reg := m.Capabilities()

	execA := &fakeClientExecutor{result: "from-A"}
	execOther := &fakeClientExecutor{result: "from-other"}
	reg.Bind("cap-mismatch", "conn-A", []string{capToolName}, time.Minute, execA)

	pv := &scriptedChatProvider{msgs: []core.Message{
		capToolCallRound("call-cap-m"),
		{Role: "assistant", Content: "gave up waiting"},
	}}
	sid := newCapabilityRunRuntime(t, m, "cap-mismatch", pv, nil)
	tools := NewClientToolProxy(reg, sid, capToolSpecs("never"), 600*time.Millisecond)

	// 断连编排:先于 StartRun 的确定性 UnbindConnection(时序竞态与修正理由见
	// waitToolCallUnbound 的警示——钩子式断连满载下可晚于 Invoke,健康执行器
	// 会先成功返回把 run 直送 completed)。
	reg.UnbindConnection("conn-A")

	var toolResults []string
	cb, _ := waitToolCallUnbound(func() {}, &toolResults) // 只收集 tool_result

	h, err := m.StartRun(sid, RunRequest{Input: "take a photo", ToolSource: func(context.Context) []core.Tool { return tools }}, cb)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	waitRunState(t, h, RunWaitingCapability)

	// mismatch 重绑:conn-B 只声明 sys_other,不含 sys_camera。
	reg.Bind(sid, "conn-B", []string{"sys_other"}, time.Minute, execOther)

	// 等待不得被 mismatch 解除:mismatch 后 300ms(< 600ms 预算)run 仍未
	// 完成、状态仍停在 waiting_capability。
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, waitErr := h.Wait(waitCtx)
	cancelWait()
	if waitErr == nil {
		t.Fatalf("run finished before the wait budget despite mismatched rebind(等待被 mismatch 解除)")
	}
	if got := h.State(); got != RunWaitingCapability {
		t.Fatalf("state after mismatched-rebind window = %q, want still %q", got, RunWaitingCapability)
	}

	waitHandle(t, h) // 超时出口:明确失败文本,run 正常完成
	if execOther.calls != 0 {
		t.Fatalf("mismatched executor invoked %d time(s), want 0(不匹配的重绑不得承接执行)", execOther.calls)
	}
	if execA.calls != 0 {
		t.Fatalf("dead conn-A executor invoked %d time(s), want 0", execA.calls)
	}
	if len(toolResults) != 1 || !strings.Contains(toolResults[0], "device tool "+capToolName+" unavailable: connection lost") {
		t.Fatalf("tool results = %v, want explicit failure text", toolResults)
	}
}

// TestRebindReconfirmViaHITL 验证风险 metadata 在重绑后仍触发审批。
// 首次 Invoke 失败后失效并等待 conn-B 重绑；重试调用再次请求确认，确保危险操作
// 不会因连接重绑绕过 HITL。
func TestRebindReconfirmViaHITL(t *testing.T) {
	m := NewSessionManager()
	defer m.Close()

	pv := &scriptedChatProvider{msgs: []core.Message{
		capToolCallRound("call-cap-h1"),
		capToolCallRound("call-cap-h2"),
		{Role: "assistant", Content: "device work done"},
	}}
	fx := newEventRunRuntime(t, m, pv) // RunEvents=真 RunJournalStore:permission.requested 可回放断言
	sid := fx.sessionID
	reg := m.Capabilities()

	dead := &fakeClientExecutor{err: errors.New("connection dead")}
	execB := &fakeClientExecutor{result: "photo-bytes-from-B"}
	reg.Bind(sid, "conn-A", []string{capToolName}, time.Minute, dead)

	tools := NewClientToolProxy(reg, sid, capToolSpecs("mode"), 5*time.Second) // normal 模式 → hitl

	var decideMu sync.Mutex
	decides := 0
	cb := RunCallbacks{
		EmitToken: func(core.Token) {},
		Decide: func(req *hitl.InterruptRequest) hitl.Decision {
			decideMu.Lock()
			decides++
			decideMu.Unlock()
			// 决策回执 InterruptRequest.ID(GetDecision 仅消费 InterruptID
			// 匹配的决策,P4 评审 issue1):生产调用方(DecideViaACP/
			// hitl_consumer)均按 ID 回执。
			return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionApprove}
		},
	}

	h, err := m.StartRun(sid, RunRequest{Input: "take a photo", ToolSource: func(context.Context) []core.Tool { return tools }}, cb)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// 第 1 次调用:approve 后 conn-A Invoke 失败 → Invalidate → 等待重绑。
	waitRunState(t, h, RunWaitingCapability)
	reg.Bind(sid, "conn-B", []string{capToolName}, time.Minute, execB)

	res := waitHandle(t, h)

	decideMu.Lock()
	gotDecides := decides
	decideMu.Unlock()
	if gotDecides != 2 {
		t.Fatalf("interrupts = %d, want 2(重绑后的执行再次触发确认)", gotDecides)
	}
	if dead.calls != 1 {
		t.Fatalf("dead executor invoked %d time(s), want 1(第 1 次调用真实尝试过 conn-A 后失效)", dead.calls)
	}
	if execB.calls != 2 {
		t.Fatalf("conn-B executor invoked %d time(s), want 2(重绑收敛执行 + 第 2 次调用)", execB.calls)
	}
	if res.State != RunCompleted {
		t.Fatalf("state = %q, want RunCompleted", res.State)
	}

	// 事件面(tee 包 Decide → permission.requested/resolved 事件对):第二次
	// permission.requested 在事件流可回放。
	evs, err := fx.journal.ReadEvents(0)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	requested, resolved := 0, 0
	for _, ev := range evs {
		switch ev.Type {
		case session.EventPermissionRequested:
			requested++
		case session.EventPermissionResolved:
			resolved++
		}
	}
	if requested != 2 || resolved != 2 {
		t.Fatalf("permission events requested=%d resolved=%d, want 2/2(第二次 permission.requested)", requested, resolved)
	}
}
