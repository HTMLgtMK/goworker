package agentcontrol

import (
	"errors"
	"testing"
	"time"
)

// lease_test.go 是 LeaseRegistry 的单元测试(fencing/幂等/跨会话矩阵的注册表层
// 面):语义与迁移前 plugin runcontrol_test.go 的 lease 用例逐条对齐——接管、
// per-session 独立、断连收口、空 clientID 拒绝、WithControl 的 fencing 与提交
// 临界区互斥。全链路(plugin → Service → runtime)的等价用例由 plugin 包的
// runcontrol_test.go 适配保留,本文件覆盖注册表自身的契约。

// TestLeaseRegistryTakeover(P5-6 拍板②,I3 后按会话记账):A 占会话 → B 显式
// 接管(tookOver=true)→ A 的 Hold 失效;非持有者 Release 无操作;持有者
// Release 后归还空闲。
func TestLeaseRegistryTakeover(t *testing.T) {
	r := NewLeaseRegistry()

	if r.Acquire("sess", "conn-A") {
		t.Fatal("first acquire on an idle lease must not report takeover")
	}
	if !r.Hold("sess", "conn-A") {
		t.Fatal("conn-A should hold the lease after acquiring")
	}
	if r.Acquire("sess", "conn-A") {
		t.Fatal("re-acquire by the same holder must not report takeover")
	}
	if !r.Acquire("sess", "conn-B") {
		t.Fatal("second holder must explicitly take over (tookOver=true)")
	}
	if r.Hold("sess", "conn-A") {
		t.Fatal("previous holder must lose the lease after takeover")
	}
	if !r.Hold("sess", "conn-B") {
		t.Fatal("new holder should hold the lease after takeover")
	}
	r.Release("sess", "conn-A") // 非持有者:无操作
	if !r.Hold("sess", "conn-B") {
		t.Fatal("release by a non-holder must not clear the lease")
	}
	r.Release("sess", "conn-B")
	if r.Hold("sess", "conn-B") {
		t.Fatal("lease should be idle after the holder releases it")
	}
}

// TestLeaseRegistryPerSession(I3 修复验收):两连接分别控制两会话互不影响;B
// 接管 A 的会话 X 后 A 的 Hold(X)==false 且 B 在其他会话的持有不受牵连;
// ReleaseConnection 只清该连接自己的全部租约;空 clientID 一律拒绝或无操作。
func TestLeaseRegistryPerSession(t *testing.T) {
	r := NewLeaseRegistry()

	if r.Acquire("sess-A", "conn-A") || r.Acquire("sess-B", "conn-B") {
		t.Fatal("acquire on idle sessions must not report takeover (各控各会话)")
	}
	if !r.Hold("sess-A", "conn-A") || !r.Hold("sess-B", "conn-B") {
		t.Fatal("each connection must hold its own session's lease")
	}
	if r.Hold("sess-A", "conn-B") || r.Hold("sess-B", "conn-A") {
		t.Fatal("per-session lease must not leak across sessions")
	}

	// B 接管 A 的会话 X=sess-A:sess-B 的持有不受牵连。
	if !r.Acquire("sess-A", "conn-B") {
		t.Fatal("takeover of a held lease must report tookOver=true")
	}
	if r.Hold("sess-A", "conn-A") || !r.Hold("sess-A", "conn-B") {
		t.Fatal("takeover must move the lease from conn-A to conn-B on sess-A only")
	}
	if !r.Hold("sess-B", "conn-B") {
		t.Fatal("takeover on session A must not disturb the taker's lease on session B")
	}

	// 释放语义:非持有者无操作;持有者按会话释放只清该会话。
	r.Release("sess-A", "conn-A")
	if !r.Hold("sess-A", "conn-B") {
		t.Fatal("release by the evicted holder must not clear the new holder's lease")
	}
	r.Release("sess-B", "conn-B")
	if r.Hold("sess-B", "conn-B") || r.Hold("sess-B", "conn-A") {
		t.Fatal("holder's per-session release must clear exactly that session")
	}
	if !r.Hold("sess-A", "conn-B") {
		t.Fatal("per-session release on B must not disturb the lease on A")
	}

	// ReleaseConnection:conn-B 重新占住 sess-B 后,一次调用清掉 B 的全部租约。
	if r.Acquire("sess-B", "conn-B") {
		t.Fatal("re-acquire on released session B must be a fresh acquire")
	}
	r.ReleaseConnection("conn-B")
	if r.Hold("sess-A", "conn-B") || r.Hold("sess-B", "conn-B") {
		t.Fatal("ReleaseConnection must clear every lease held by the connection")
	}
	if r.Acquire("sess-A", "conn-A") {
		t.Fatal("session A must be idle again after conn-B's leases were released")
	}

	// 空 clientID:Acquire/Hold/Release/ReleaseConnection 一律拒绝或无操作。
	if r.Acquire("sess-A", "") {
		t.Fatal("empty clientID acquire must be rejected")
	}
	if r.Hold("sess-A", "") {
		t.Fatal("empty clientID must never hold a lease")
	}
	r.Release("sess-A", "")
	r.ReleaseConnection("")
	if !r.Hold("sess-A", "conn-A") {
		t.Fatal("empty clientID calls must not disturb existing leases")
	}
}

// TestLeaseRegistryWithControlFencing(WithControl = fencing 提交入口):持有者
// 在临界区内执行 fn(错误原样上抛);失权的旧持有者(从未持有/已被接管)被
// 拒且 fn 绝不执行——校验先于一切提交判定。
func TestLeaseRegistryWithControlFencing(t *testing.T) {
	r := NewLeaseRegistry()

	// 未持有 → ErrControlLeaseLost,fn 不执行。
	fnRan := false
	err := r.WithControl("sess", "conn-A", func() error {
		fnRan = true
		return nil
	})
	if !errors.Is(err, ErrControlLeaseLost) {
		t.Fatalf("WithControl without lease err = %v, want ErrControlLeaseLost", err)
	}
	if fnRan {
		t.Fatal("fenced fn must not run for a non-holder")
	}

	// 持有 → fn 执行,fn 的错误原样上抛(提交失败语义不吞)。
	r.Acquire("sess", "conn-A")
	sentinel := errors.New("commit failed")
	err = r.WithControl("sess", "conn-A", func() error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithControl fn error = %v, want the fn's own error", err)
	}

	// 接管后旧持有者被拒(Acquire 与 WithControl 同锁线性化)。
	r.Acquire("sess", "conn-B")
	fnRan = false
	err = r.WithControl("sess", "conn-A", func() error {
		fnRan = true
		return nil
	})
	if !errors.Is(err, ErrControlLeaseLost) {
		t.Fatalf("WithControl after takeover err = %v, want ErrControlLeaseLost", err)
	}
	if fnRan {
		t.Fatal("fenced fn must not run after the holder was taken over")
	}

	// 新持有者照常提交。
	if err := r.WithControl("sess", "conn-B", func() error { return nil }); err != nil {
		t.Fatalf("WithControl by the new holder: %v", err)
	}
}

// TestLeaseRegistryWithControlSerializesCommit(提交临界区互斥的确定性断言):
// 持有者的 fn 在 lease.mu 内执行期间,他人的 Acquire 无法插入(接管被挡在提交
// 临界区外——若完成即 fencing 破洞);fn 返回后接管照常完成。与迁移前
// TestCrossSessionStartRunNotBlockedByFsync 的 ② 号断言同型,在注册表层用纯
// 顺序时序钉死,不依赖调度。
func TestLeaseRegistryWithControlSerializesCommit(t *testing.T) {
	r := NewLeaseRegistry()
	if r.Acquire("sess", "conn-A") {
		t.Fatal("test setup: fresh acquire must not report takeover")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	fnDone := make(chan error, 1)
	go func() {
		fnDone <- r.WithControl("sess", "conn-A", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("holder's WithControl never entered the commit section")
	}

	takeover := make(chan bool, 1)
	go func() { takeover <- r.Acquire("sess", "conn-B") }()
	select {
	case <-takeover:
		t.Fatal("takeover completed while conn-A's fenced commit is still in flight (校验与提交不在同一临界区)")
	case <-time.After(100 * time.Millisecond):
		// 仍在等待 lease.mu:线性化语义成立(接管被挡在提交临界区外)。
	}

	close(release)
	select {
	case err := <-fnDone:
		if err != nil {
			t.Fatalf("holder's WithControl returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("holder's WithControl did not return after the release")
	}
	select {
	case took := <-takeover:
		if !took {
			t.Fatal("takeover after conn-A's commit completed must report tookOver=true")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("takeover did not complete after the fenced commit finished")
	}
}
