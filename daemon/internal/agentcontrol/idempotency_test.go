package agentcontrol

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// idempotency_test.go 是 RequestRegistry 的单元测试(per-key single-flight 的
// §4.3 验收):同 Key 并发提交只有一个底层 StartRun、等待同一结果;不同 Key 不
// 互相阻塞;Input 指纹冲突;失败不入表;环形逐出。start 闭包返回手工构造的
// RunHandle(仅 RunID/SessionID 字段被本注册表消费——Submit/byRun 不做 State
// 快照,State() 由 Service 从真实句柄调用),无需真实 runtime 栈。

// handle 构造测试用 RunHandle(本注册表只读导出字段)。
func testHandle(sessionID, runID string) runtimeagent.RunHandle {
	return runtimeagent.RunHandle{SessionID: sessionID, RunID: runID}
}

// TestRequestRegistryIdempotentRetry(I5 语义):同键同 Input 重试返回原句柄且
// 不重复触发底层提交;同键不同 Input 显式冲突,原记账不受影响。
func TestRequestRegistryIdempotentRetry(t *testing.T) {
	r := NewRequestRegistry()
	key := RequestKey{SessionID: "sess", RequestID: "req-1"}

	var starts atomic.Int32
	h1, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
		starts.Add(1)
		return testHandle("sess", "run-1"), nil
	})
	if err != nil {
		t.Fatalf("Submit #1: %v", err)
	}
	if h1.RunID != "run-1" {
		t.Fatalf("Submit #1 runID = %q, want run-1", h1.RunID)
	}

	h2, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
		starts.Add(1)
		return testHandle("sess", "run-2"), nil
	})
	if err != nil {
		t.Fatalf("Submit retry: %v", err)
	}
	if h2.RunID != "run-1" {
		t.Fatalf("retry runID = %q, want original run-1 (幂等短路)", h2.RunID)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("underlying start called %d times, want 1", got)
	}

	// 同键不同 Input → 显式冲突,原记账不被覆盖。
	if _, _, err := r.Submit(key, "payload-changed", func() (runtimeagent.RunHandle, error) {
		starts.Add(1)
		return testHandle("sess", "run-3"), nil
	}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("different-input retry err = %v, want ErrRequestConflict", err)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("conflicting retry must not trigger a start, got %d", got)
	}
	if h3, ok := r.Tracked("sess", "req-1"); !ok || h3.RunID != "run-1" {
		t.Fatalf("Tracked after conflict = (%q, %v), want original run-1 preserved", h3.RunID, ok)
	}
}

// TestRequestRegistrySingleFlightConcurrent(§4.3 硬性要求):同 Key 并发提交
// 恰好一次底层 StartRun,其余等待同一结果(同 runID,零错误);不同 Input 的
// 并发重试不等 leader 完成即被拒。
func TestRequestRegistrySingleFlightConcurrent(t *testing.T) {
	r := NewRequestRegistry()
	key := RequestKey{SessionID: "sess", RequestID: "req-flight"}

	// leader 的 start 挂在 release 上,制造真实的并发 join 窗口。
	var starts atomic.Int32
	release := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond) // 确保等待者先到齐再放行
		close(release)
	}()

	const n = 32
	var wg sync.WaitGroup
	results := make([]runtimeagent.RunHandle, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
				starts.Add(1)
				<-release
				return testHandle("sess", "run-flight"), nil
			})
			results[i], errs[i] = h, err
		}(i)
	}
	wg.Wait()

	if got := starts.Load(); got != 1 {
		t.Fatalf("underlying start called %d times, want exactly 1 (single-flight)", got)
	}
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("joiner #%d err = %v, want nil (等待同一结果)", i, errs[i])
		}
		if results[i].RunID != "run-flight" {
			t.Fatalf("joiner #%d runID = %q, want leader's run-flight", i, results[i].RunID)
		}
	}
	if h, ok := r.Tracked("sess", "req-flight"); !ok || h.RunID != "run-flight" {
		t.Fatalf("Tracked after single-flight = (%q, %v), want leader's run recorded", h.RunID, ok)
	}
}

// TestRequestRegistryConcurrentDifferentInputConflict:joiner 与 leader 的 Input
// 指纹不一致时立即冲突(leader 指纹在 flight 创建时定稿,joiner 不等待)。
func TestRequestRegistryConcurrentDifferentInputConflict(t *testing.T) {
	r := NewRequestRegistry()
	key := RequestKey{SessionID: "sess", RequestID: "req-conflict"}

	release := make(chan struct{})
	entered := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = r.Submit(key, "leader-input", func() (runtimeagent.RunHandle, error) {
			close(entered) // flight 已登记,leader 已进入提交段
			<-release
			return testHandle("sess", "run-leader"), nil
		})
	}()
	// 等 leader 真正登记 flight(entered 关闭 ⇒ flight 注册早于 start() 调用,
	// joiner 的检查必见在途 flight):joiner 的冲突必须在 leader 完成之前返回,
	// 否则本测试的时序前提失败。
	<-entered

	start := time.Now()
	_, _, err := r.Submit(key, "joiner-input", func() (runtimeagent.RunHandle, error) {
		t.Error("conflicting joiner must not trigger a start")
		return testHandle("sess", "run-joiner"), nil
	})
	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("concurrent different-input err = %v, want ErrRequestConflict", err)
	}
	close(release)
	<-leaderDone
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("joiner conflict waited %v — must fail fast before the leader completes", elapsed)
	}
}

// TestRequestRegistryDifferentKeysIndependent(§4.3:不同 Key 不互相阻塞):
// Key1 的 leader 挂起期间,Key2 的提交照常完成(底层提交在 registry.mu 之外);
// Key1 放行后各自记账。
func TestRequestRegistryDifferentKeysIndependent(t *testing.T) {
	r := NewRequestRegistry()
	key1 := RequestKey{SessionID: "sess", RequestID: "req-slow"}
	key2 := RequestKey{SessionID: "sess", RequestID: "req-fast"}

	release := make(chan struct{})
	entered := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = r.Submit(key1, "slow", func() (runtimeagent.RunHandle, error) {
			close(entered) // flight 已登记,leader 已进入提交段
			<-release
			return testHandle("sess", "run-slow"), nil
		})
	}()
	<-entered // 等 key1 的 leader 登记 flight 并挂起

	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		h, _, err := r.Submit(key2, "fast", func() (runtimeagent.RunHandle, error) {
			return testHandle("sess", "run-fast"), nil
		})
		if err != nil || h.RunID != "run-fast" {
			t.Errorf("Submit(key2) while key1's start is stuck = (%q, %v), want run-fast/nil (跨 Key 提交被串行化)", h.RunID, err)
		}
	}()
	select {
	case <-fastDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit(key2) blocked while key1's start was stuck (§4.3 回归)")
	}

	close(release)
	<-leaderDone
	if h, ok := r.Tracked("sess", "req-slow"); !ok || h.RunID != "run-slow" {
		t.Fatalf("Tracked(req-slow) = (%q, %v), want run-slow recorded after release", h.RunID, ok)
	}
}

// TestRequestRegistryErrorNotRecorded:底层提交失败 → 错误上抛、不入表(拒绝
// 路径不得入表);重试重新触发提交(新 flight);并发 joiner 拿到同一错误。
func TestRequestRegistryErrorNotRecorded(t *testing.T) {
	r := NewRequestRegistry()
	key := RequestKey{SessionID: "sess", RequestID: "req-err"}

	cause := errors.New("manager rejected")
	var starts atomic.Int32
	_, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
		starts.Add(1)
		return runtimeagent.RunHandle{}, cause
	})
	if !errors.Is(err, cause) {
		t.Fatalf("Submit err = %v, want the start's own error", err)
	}
	if _, tracked := r.Tracked("sess", "req-err"); tracked {
		t.Fatal("failed submission recorded an idempotency entry (拒绝路径不得入表)")
	}

	// 重试:重新成为 leader(新 flight),第二次提交成功。
	h, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
		starts.Add(1)
		return testHandle("sess", "run-retry"), nil
	})
	if err != nil || h.RunID != "run-retry" {
		t.Fatalf("retry after failure = (%q, %v), want run-retry/nil", h.RunID, err)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("start called %d times across failure+retry, want 2", got)
	}
}

// TestRequestRegistryJoinerGetsError:leader 失败时并发 joiner 拿到同一错误
// (等待同一结果包含失败结果,joiner 不重试、不挂死)。在途 flight 由测试直接
// 注入(in-package 白盒,等价于「leader 已登记、提交在途」的注册表状态):
// 失败提交不入表(设计使然,迟到的重试必须重新起跑),join-on-error 的等待
// 路径因此无法靠调度时序钉住——直接构造该状态,断言零时序依赖。leader 侧的
// 失败路径由 TestRequestRegistryErrorNotRecorded 覆盖。
func TestRequestRegistryJoinerGetsError(t *testing.T) {
	r := NewRequestRegistry()
	key := RequestKey{SessionID: "sess", RequestID: "req-join-err"}

	f := &flight{inputHash: inputFingerprint("payload"), done: make(chan struct{}), err: errFlightAborted}
	r.mu.Lock()
	r.flights[key] = f
	r.mu.Unlock()

	// 模拟 leader 提交失败收场:先写结果后关 done(与 leader 正常路径同序)。
	go func() {
		f.err = errors.New("leader failed")
		close(f.done)
	}()

	h, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
		t.Error("joiner must not trigger its own start")
		return testHandle("sess", "run-x"), nil
	})
	if !errors.Is(err, f.err) {
		t.Fatalf("joiner err = %v, want the leader's error", err)
	}
	if h.RunID != "" {
		t.Fatalf("joiner handle = %q, want zero handle on error", h.RunID)
	}
	if _, tracked := r.Tracked("sess", "req-join-err"); tracked {
		t.Fatal("failed submission must leave no idempotency entry")
	}
	// leader 完成路径会清 flight:注入态由测试收尾清理,验证后续提交重新起跑。
	r.mu.Lock()
	delete(r.flights, key)
	r.mu.Unlock()
	h2, _, err := r.Submit(key, "payload", func() (runtimeagent.RunHandle, error) {
		return testHandle("sess", "run-retry"), nil
	})
	if err != nil || h2.RunID != "run-retry" {
		t.Fatalf("retry after joined failure = (%q, %v), want run-retry/nil", h2.RunID, err)
	}
}

// TestRequestRegistryRingEviction(4096 环形逐出):容量上限 4096,第 4097 笔
// 插入逐出最老条目;同键重复记录不重复入队(刷新句柄语义)。
func TestRequestRegistryRingEviction(t *testing.T) {
	r := NewRequestRegistry()
	start := func(i int) func() (runtimeagent.RunHandle, error) {
		return func() (runtimeagent.RunHandle, error) {
			return testHandle("sess", fmt.Sprintf("run-%d", i)), nil
		}
	}
	for i := 0; i < maxTrackedRequestIDs; i++ {
		key := RequestKey{SessionID: "sess", RequestID: fmt.Sprintf("req-%d", i)}
		if _, _, err := r.Submit(key, "payload", start(i)); err != nil {
			t.Fatalf("Submit #%d: %v", i, err)
		}
	}
	// 容量边界:最老与最新都在册。
	if _, ok := r.Tracked("sess", "req-0"); !ok {
		t.Fatal("req-0 should be tracked at capacity")
	}
	// 第 4097 笔:逐出 req-0。
	if _, _, err := r.Submit(RequestKey{SessionID: "sess", RequestID: "req-4096"}, "payload", start(4096)); err != nil {
		t.Fatalf("Submit overflow: %v", err)
	}
	if _, ok := r.Tracked("sess", "req-0"); ok {
		t.Fatal("oldest entry must be evicted beyond capacity")
	}
	if h, ok := r.Tracked("sess", "req-4096"); !ok || h.RunID != "run-4096" {
		t.Fatalf("newest entry = (%q, %v), want run-4096 tracked", h.RunID, ok)
	}
	// 同键重复提交:幂等短路返回原句柄(表命中路径,不触发底层提交、不重复
	// 入队);recordLocked 的同键刷新分支是保守自愈,直接在锁内驱动验证。
	if h2, _, err := r.Submit(RequestKey{SessionID: "sess", RequestID: "req-4096"}, "payload", func() (runtimeagent.RunHandle, error) {
		t.Error("same-key resubmission must short-circuit on the table hit")
		return testHandle("sess", "run-refresh"), nil
	}); err != nil || h2.RunID != "run-4096" {
		t.Fatalf("same-key resubmission = (%q, %v), want original run-4096", h2.RunID, err)
	}
	r.mu.Lock()
	r.recordLocked(RequestKey{SessionID: "sess", RequestID: "req-4096"}, testHandle("sess", "run-refresh"), inputFingerprint("payload"))
	r.mu.Unlock()
	if h, ok := r.Tracked("sess", "req-4096"); !ok || h.RunID != "run-refresh" {
		t.Fatalf("same-key re-record must refresh the handle, got (%q, %v)", h.RunID, ok)
	}
	if _, ok := r.Tracked("sess", "req-1"); !ok {
		t.Fatal("re-recording an existing key must not evict additional entries")
	}
}

// TestRequestRegistryTrackedAndByRun(观测面):Tracked 按键直查(空 requestID
// 恒 false);byRun 按 (sessionID, runID) 反查且按会话过滤(防跨会话 runID 误配)。
func TestRequestRegistryTrackedAndByRun(t *testing.T) {
	r := NewRequestRegistry()
	if _, tracked := r.Tracked("sess", ""); tracked {
		t.Fatal("empty requestID must never be tracked (空键不记账)")
	}
	if _, tracked := r.Tracked("sess", "missing"); tracked {
		t.Fatal("unknown key must not be tracked")
	}

	h, _, err := r.Submit(RequestKey{SessionID: "sess-A", RequestID: "req-a"}, "payload", func() (runtimeagent.RunHandle, error) {
		return testHandle("sess-A", "run-a"), nil
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got, ok := r.Tracked("sess-A", "req-a"); !ok || got.RunID != h.RunID {
		t.Fatalf("Tracked = (%q, %v), want run-a", got.RunID, ok)
	}
	if got, ok := r.byRun("sess-A", "run-a"); !ok || got.RunID != "run-a" {
		t.Fatalf("byRun = (%q, %v), want run-a", got.RunID, ok)
	}
	// 会话过滤:B 会话查 A 的 runID 必落空。
	if _, ok := r.byRun("sess-B", "run-a"); ok {
		t.Fatal("byRun must not match across sessions (会话过滤)")
	}
	if _, ok := r.byRun("sess-A", "run-other"); ok {
		t.Fatal("byRun must not match unknown runIDs")
	}
}
