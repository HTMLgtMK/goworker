package service

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
)

// stop_test.go 是 PR5 Stop 收口（总方案 §十测试矩阵「Plugin Stop:不遗留失控
// goroutine、连接订阅或 Store 句柄」；P4 报备「P5 再接线」）的插件侧验收。
//
// TestPluginStopClosesManager：在飞 Run → 插件 Stop →
//   - run 收敛取消终态（journal 末条 cancelled，取消不是失败，P2-1 归一）；
//   - manager 停机（注册表清空 + Register/StartRun 一律 ErrManagerClosed）；
//   - 事件订阅随 hub 收口关闭（订阅通道关闭可观测，无失控订阅面）；
//   - Stop 返回后可再入（幂等：第二次 Stop 不 panic、不悬挂）。
//
// Store 句柄释放面（flock 已释放）在 daemon 根 e2e 以包外视角断言
// （e2e_plugin_stop_test.go：session.Open 同目录重开成功 ⇒ 锁已释放）；
// 本包内 currentStore 经 repo 缓存取数，锁释放不构成可观测差异。
//
// goroutine 收敛不直接用 runtime.NumGoroutine 断言（并行测试下基线不可靠）：
// 在飞 Run 的执行 goroutine 以「journal 终态 + active 清空」收敛为证，订阅
// goroutine 以「订阅通道关闭」为证——两者正是失控 goroutine 的全部来源面。

// TestPluginStopClosesManager:在途 run → Stop → run 终态、manager closed。
func TestPluginStopClosesManager(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()

	// 在飞后台 Run：提交后等真实开工（started 关闭 ⇒ run.started 已发布），
	// 不释放闸门——Stop 的 manager.Close 依赖 runCtx 取消收敛它。
	p.AcquireControl(sid, "conn-stop")
	runID, _, err := p.StartRun(sid, "conn-stop", "hello", "req-stop")
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background run did not start in time")
	}

	// 在飞订阅：Stop 的 hub 收口应把它断开（通道关闭可观测）。
	res, err := p.SubscribeEvents(sid, 0)
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	if res.Subscription == nil {
		t.Fatal("SubscribeEvents returned nil subscription")
	}

	// Stop 在独立 goroutine 执行 + 超时兜底：manager.Close 的等待面若因缺陷
	// 卡死，放行 provider 闸门让失败可见，而不是拖垮整个测试进程。release 的
	// close 用 Once 收口（超时分支与 defer 都可能触达，双重 close 即 panic）。
	releaseGate := sync.OnceFunc(func() { close(gated.release) })
	stopped := make(chan struct{})
	go func() { _ = p.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		releaseGate() // 兜底放行
		<-stopped
		t.Fatal("Stop did not return in time (manager.Close wait stuck?)")
	}
	// 闸门所有权归本测试：正常路径下从未释放，此处兜底关闭（provider 的
	// select 以 ctx.Done 先行命中，release 关闭只为防泄漏）。
	defer releaseGate()

	// ① run 终态：manager.Close 取消 runCtx → journal 末条 cancelled。
	// Close 返回 ⇒ 等待面已见 done，而终态记录先于 done 关闭落盘
	//（finalizeRun 次序不变量），此处直读必然可见。
	states := readStateRecords(t, filepath.Join(p.cfg.Session.Dir, sid, "runs", runID+".jsonl"))
	if len(states) == 0 {
		t.Fatal("run journal has no state records")
	}
	if last := states[len(states)-1].State; last != string(runtimeagent.RunCancelled) {
		t.Fatalf("last journal state = %q, want %q (in-flight run must converge to cancelled on Stop)", last, runtimeagent.RunCancelled)
	}

	// ② manager closed：注册表清空 + 停机拒绝语义（closed 检查先于会话查找，
	// supervisor.go StartRun ①；Register 同拒）。
	if rt := p.manager.Get(sid); rt != nil {
		t.Fatalf("manager.Get(%q) after Stop = %v, want nil (registry cleared)", sid, rt)
	}
	if _, err := p.manager.StartRun(sid, runtimeagent.RunRequest{Input: "x"}, runtimeagent.RunCallbacks{}); !errors.Is(err, runtimeagent.ErrManagerClosed) {
		t.Fatalf("manager.StartRun after Stop = %v, want ErrManagerClosed", err)
	}
	if err := p.manager.Register(runtimeagent.NewSessionRuntime("post-stop", nil)); !errors.Is(err, runtimeagent.ErrManagerClosed) {
		t.Fatalf("manager.Register after Stop = %v, want ErrManagerClosed", err)
	}
	// 协议面同一收敛：Service 委托链终点的 manager.StartRun 以 ErrManagerClosed
	// 拒绝（route 的「current 优先」在停机后仍可解析——p.current 不随注册表
	// 清空，拒绝语义由 manager 的 closed 检查给出，诚实且不含糊）。停机后无
	// 「看似健康」的提交面。
	if _, _, err := p.StartRun(sid, "conn-stop", "x", "req-post-stop"); !errors.Is(err, runtimeagent.ErrManagerClosed) {
		t.Fatalf("StartRun after Stop = %v, want ErrManagerClosed", err)
	}

	// ③ 订阅不遗留：hub.Close 断开全部订阅者 → pump defer close(ch) →
	// 订阅通道最终关闭（缓冲中的 replay 事件排干后读到 ok=false）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case _, ok := <-res.Subscription.Events():
			if !ok {
				return // 通道已关闭：订阅面随 Stop 收口，无失控订阅
			}
		default:
			if time.Now().After(deadline) {
				t.Fatal("subscription channel not closed after Stop (leaked subscription)")
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// TestStopIdempotentAfterClose:Stop 幂等——连续两次 Stop 不悬挂、不 panic
// （manager.Close 幂等；memory/store/repo 收尾对 nil/已关闭实例幂等）。停机
// 路径（Engine.StopAll 与失败回滚）可能多次触达 Stop，语义必须可重入。
func TestStopIdempotentAfterClose(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "stop-idem"})
	sid := p.CurrentSessionID()
	if err := p.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	done := make(chan struct{})
	go func() { _ = p.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("second Stop did not return in time")
	}
	// 第二次 Stop 后执行平面保持停机态：注册表仍为空、拒绝语义仍在。
	// （断言不取 CurrentSessionID/currentStore——停机后该查询会经 repo.Open
	// 重开 store 实例，本测试不再持有其收尾路径。）
	if rt := p.manager.Get(sid); rt != nil {
		t.Fatalf("manager.Get(%q) after second Stop = %v, want nil", sid, rt)
	}
	if _, err := p.manager.StartRun("any", runtimeagent.RunRequest{Input: "x"}, runtimeagent.RunCallbacks{}); !errors.Is(err, runtimeagent.ErrManagerClosed) {
		t.Fatalf("manager.StartRun after second Stop = %v, want ErrManagerClosed", err)
	}
}
