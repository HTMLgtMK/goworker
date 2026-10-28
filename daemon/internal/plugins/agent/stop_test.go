package service

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
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
// TestStopWaitsForInFlightMaintenance 是 S1（第四轮评审）的插件侧验收：
// Stop 的等待面除 manager 的 AgentRun 外，还必须覆盖 withSession 维护闭包
// （/new /compact 等经 Session Gate 但不在 Manager 活跃集的执行面）——
// store.Close 不得早于在飞维护操作完成。
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

// shutdownBarrierProvider 让 manager.Close 已经发出取消但仍停在 run.done 等待面，
// 用于验证 Stop 必须先关闭维护入口，而不是等 manager.Close 返回后才关入口。
type shutdownBarrierProvider struct {
	started        chan struct{}
	cancelObserved chan struct{}
	release        chan struct{}
	startOnce      sync.Once
	cancelOnce     sync.Once
}

func (p *shutdownBarrierProvider) Name() string  { return "shutdown-barrier" }
func (p *shutdownBarrierProvider) Model() string { return "m" }

func (p *shutdownBarrierProvider) Chat(ctx context.Context, _ *core.ChatRequest) (*core.ChatResponse, error) {
	p.startOnce.Do(func() { close(p.started) })
	go func() {
		<-ctx.Done()
		p.cancelOnce.Do(func() { close(p.cancelObserved) })
	}()
	<-p.release // 故意忽略取消，保持 manager.Close 的等待面可观测
	return &core.ChatResponse{Choices: []core.ResponseChoice{{Message: core.Message{Role: "assistant", Content: "shutdown-barrier"}}}}, nil
}

func (p *shutdownBarrierProvider) ChatStream(context.Context, *core.ChatRequest) (<-chan core.Token, error) {
	return nil, errors.New("not implemented")
}

// TestStopClosesMaintenanceAdmissionBeforeManagerClose 验证 Stop 在 manager.Close
// 仍等待 Run 时已经拒绝新的维护请求；否则请求会卡在同一个 Session Gate，无法
// 证明「Stop 开始后不再接受」这一入口契约。
func TestStopClosesMaintenanceAdmissionBeforeManagerClose(t *testing.T) {
	provider := &shutdownBarrierProvider{
		started:        make(chan struct{}),
		cancelObserved: make(chan struct{}),
		release:        make(chan struct{}),
	}
	p := newPersistedPlugin(t, provider)
	releaseRun := sync.OnceFunc(func() { close(provider.release) })
	t.Cleanup(releaseRun)
	sid := p.CurrentSessionID()
	p.AcquireControl(sid, "conn-stop-admission")
	if _, _, err := p.StartRun(sid, "conn-stop-admission", "hello", "req-stop-admission"); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background run did not start in time")
	}

	stopped := make(chan struct{})
	go func() { _ = p.Stop(); close(stopped) }()
	defer func() {
		releaseRun()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("Stop did not return in cleanup")
		}
	}()

	// manager.Close 已调用宿主 cancel，但 provider 仍阻塞，所以 Stop 还没有
	// 走完；这是一个确定的窗口，不靠调度时序猜测。
	select {
	case <-provider.cancelObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("manager.Close did not cancel the in-flight run")
	}

	maintenanceDone := make(chan error, 1)
	go func() {
		maintenanceDone <- p.withSession(context.Background(), func(context.Context) error { return nil })
	}()
	var maintenanceErr error
	select {
	case maintenanceErr = <-maintenanceDone:
		if !errors.Is(maintenanceErr, errAgentStopping) {
			t.Fatalf("maintenance request during manager.Close = %v, want errAgentStopping", maintenanceErr)
		}
	case <-stopped:
		t.Fatal("Stop returned before the shutdown barrier was released")
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance request was not rejected while manager.Close was waiting")
	}
	releaseRun()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return after releasing the run")
	}
}

// gateAdmissionContext 在 AcquireRun 读取 Done 时发出屏障信号。withSession 的
// 新顺序保证此刻请求已经 enter；测试借此证明请求虽尚未执行闭包，Stop 仍需排空。
type gateAdmissionContext struct {
	context.Context
	admitted chan struct{}
	once     sync.Once
}

func (c *gateAdmissionContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.admitted) })
	return c.Context.Done()
}

// TestStopDrainsMaintenanceWaitingBehindRunGate 验证已在插件入口登记、但仍在
// Run 持有的 Session Gate 前排队的维护请求：Run 收敛后它必须完成真实 store 写，
// Stop 才能关闭持久化资源。
func TestStopDrainsMaintenanceWaitingBehindRunGate(t *testing.T) {
	gated := &gatedProvider{started: make(chan struct{}), release: make(chan struct{})}
	p := newPersistedPlugin(t, gated)
	sid := p.CurrentSessionID()
	p.AcquireControl(sid, "conn-gate-drain")
	if _, _, err := p.StartRun(sid, "conn-gate-drain", "hello", "req-gate-drain"); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	select {
	case <-gated.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background run did not start in time")
	}

	ctx := &gateAdmissionContext{Context: context.Background(), admitted: make(chan struct{})}
	maintenanceEntered := make(chan struct{})
	maintenanceDone := make(chan struct{})
	maintenanceRelease := make(chan struct{})
	releaseMaintenance := sync.OnceFunc(func() { close(maintenanceRelease) })
	t.Cleanup(releaseMaintenance)
	var storeOpErr error
	go func() {
		_ = p.withSession(ctx, func(runCtx context.Context) error {
			defer close(maintenanceDone)
			close(maintenanceEntered)
			<-maintenanceRelease
			st := p.currentStoreIn(runCtx)
			if st == nil {
				storeOpErr = errors.New("maintenance closure: store resolved nil")
				return storeOpErr
			}
			if _, err := st.Commit([]core.Message{{Role: "user", MsgID: "gate-drain-1", Content: "maintenance write behind run gate"}}); err != nil {
				storeOpErr = err
				return err
			}
			return nil
		})
	}()

	select {
	case <-ctx.admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance request did not reach the Session Gate")
	}
	stopped := make(chan struct{})
	go func() { _ = p.Stop(); close(stopped) }()

	select {
	case <-maintenanceEntered:
	case <-stopped:
		t.Fatal("Stop returned before the admitted maintenance closure ran")
	case <-time.After(10 * time.Second):
		t.Fatal("admitted maintenance request was not released after Run shutdown")
	}
	releaseMaintenance()
	select {
	case <-maintenanceDone:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance closure did not finish after release")
	}
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return after draining admitted maintenance")
	}
	if storeOpErr != nil {
		t.Fatalf("store write behind Session Gate failed during Stop: %v", storeOpErr)
	}
}

// TestStopWaitsForInFlightMaintenance(S1 验收,第四轮评审):withSession 维护闭包
// (/compact 等命令的执行面)持有 store 操作期间触发 Stop → store.Close 不早于
// 维护操作完成。确定性交错(生产可达面:CLI stdin 信号路径在 Eval 在飞时即可
// 进入 appl.Stop → Engine.StopAll → Plugin.Stop,Engine 层无排空屏障):
//  1. 请求在 withSession 入口登记后，闭包在 gate 内挂起(close(entered) 钩子);
//  2. Stop 并发启动(此刻起 inFlight admission closed,新维护回合被拒绝);
//  3. 放行闭包 → 闭包对 store 真实 Commit——若 Stop 先于闭包关掉 store,
//     Commit 以错误可观测(Store 关闭后 appendRecord 落在已关句柄上);
//  4. Stop 返回后断言:闭包内部的生产操作已完成(happens-before 经
//     inFlight.wait 的 WaitGroup 建立:闭包完成 → exit/Done → wait 返回
//     → Stop 返回 → stopped 关闭),store 写成功,且重启文件视图可见写入。
func TestStopWaitsForInFlightMaintenance(t *testing.T) {
	p := newPersistedPlugin(t, fixedProvider{content: "maint-stop"})
	sid := p.CurrentSessionID()

	entered := make(chan struct{})
	release := make(chan struct{})
	closeRelease := sync.OnceFunc(func() { close(release) })
	t.Cleanup(closeRelease)
	closureDone := make(chan struct{})
	var storeOpErr error

	// 模拟 /compact 的执行面:withSession 在追踪的维护闭包内挂起，放行后做
	// 与 /compact 同型的真实 store 写。完成信号位于闭包内部，准确对应
	// inFlight.exit 前的生产工作，而不是外围调用 goroutine 的调度。
	go func() {
		_ = p.withSession(context.Background(), func(runCtx context.Context) error {
			defer close(closureDone)
			close(entered)
			<-release
			st := p.currentStoreIn(runCtx)
			if st == nil {
				storeOpErr = errors.New("maintenance closure: store resolved nil (repo finalized under the closure?)")
				return storeOpErr
			}
			if _, err := st.Commit([]core.Message{{Role: "user", MsgID: "maint-1", Content: "maintenance write during stop"}}); err != nil {
				storeOpErr = err
				return err
			}
			return nil
		})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance closure did not enter withSession in time")
	}

	stopped := make(chan struct{})
	go func() { _ = p.Stop(); close(stopped) }()
	select {
	case <-stopped:
		closeRelease() // 兜底:Stop 不该先于闭包返回;放行防泄漏
		<-closureDone
		t.Fatal("Stop returned before the in-flight maintenance closure completed (inFlight wait missing?)")
	case <-time.After(50 * time.Millisecond):
		// Stop 尚未返回 = 修复的等待面在工作(非判定性早退检查,真正的判定
		// 断言在下方 happens-before 检查)。
	}
	closeRelease() // 放行闭包:它与 Stop 的收尾步骤从此刻起真实并发
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return in time (inFlight wait stuck?)")
	}

	// ① 次序:Stop 返回 ⇒ inFlight.wait 返回 ⇒ 闭包已返回 ⇒ closureDone 已关。
	select {
	case <-closureDone:
	default:
		t.Fatal("Stop returned before the maintenance closure completed (store.Close would precede the maintenance op)")
	}
	// ② 闭包内的 store 写在 Stop 收尾期间成功(无「写已关闭 store」错误)。
	if storeOpErr != nil {
		t.Fatalf("store write inside maintenance closure failed while Stop was winding down: %v", storeOpErr)
	}
	// ③ 写入已落盘:Close 的 Sync 之后从磁盘重读,消息在活跃视图中。
	view, err := session.OpenArchiveView(filepath.Join(p.cfg.Session.Dir, sid, "current.jsonl"))
	if err != nil {
		t.Fatalf("reopen persisted store file after Stop: %v", err)
	}
	found := false
	for _, m := range view.ActiveView() {
		if m.MsgID == "maint-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("maintenance write missing from the persisted active view after Stop (store closed before the op landed)")
	}
	// ④ 收口开始后的新维护回合诚实拒绝(S1 拒绝语义,与 ErrManagerClosed 同族):
	// withSession 在 AcquireRun 成功后被 inFlightTracker 拒绝,不触碰任何持久化面。
	if err := p.withSession(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, errAgentStopping) {
		t.Fatalf("withSession after Stop = %v, want errAgentStopping", err)
	}
}
