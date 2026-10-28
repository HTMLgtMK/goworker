package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	agentplugin "github.com/tinguo/goworker/daemon/internal/plugins/agent"
)

// e2e_plugin_stop_test.go 补齐重构方案 §十测试矩阵的最后一行（PR5）：
//
//	场景          | 验收条件
//	--------------+--------------------------------------------------------
//	Plugin Stop   | 不遗留失控 goroutine、连接订阅或 Store 句柄
//
// 其余矩阵行由既有测试钉死（引用，不在本文件重复）：
//   - 多客户端多会话（A/B 各自创建/启动/取消互不影响，PR2）：
//     daemon/internal/plugins/agent/binding_test.go（TestBindingRoutesPromptToBoundSession、
//     TestHandleNewDetachesOnlyInitiatorBinding）、runcontrol_test.go
//     （TestCrossSessionRunControl、TestCrossSessionRequestIdempotency）；
//   - Lease 接管（新持有者先提交后旧操作拒绝）：
//     daemon/internal/agentcontrol/lease_test.go（TestLeaseRegistryTakeover、
//     TestLeaseRegistryWithControlFencing）、service_test.go（TestServiceStartRunFencing）；
//   - 后台运行/Replay-Live/审批隔离/幂等重试：acp 包 xgw_test.go 与
//     xgw_integration_test.go、agentcontrol/idempotency_test.go、
//     ai-runtime/agent 的 run_lifecycle/event_hub 测试族。
//
// 本文件用真实插件装配链（NewPlugin+Init → 真实 provider 工厂 → mock LLM HTTP
// 服务）跑端到端 Stop：在飞后台 Run → 插件 Stop →
//   ① run 收敛取消终态（journal 末条 cancelled，磁盘可证）；
//   ② manager 停机（提交面拒绝，注册表清空）；
//   ③ 事件订阅随 hub 收口关闭（订阅通道关闭可观测）；
//   ④ Store 句柄释放（包外视角：对会话目录 session.Open 重开成功 ⇒ 插件的
//     flock 已释放；若 Stop 泄漏句柄，同进程第二次持锁被 ErrStoreLocked 拒绝）。
// 失控 goroutine 的两个来源面（在飞 Run 执行体、订阅泵）分别由 ①的终态收敛与
// ③的通道关闭覆盖；不直接断言 runtime.NumGoroutine（并行测试基线不可靠）。

// TestE2E_PluginStopConvergesInFlightRunAndReleasesHandles:在飞 run → Stop →
// run 终态、manager closed、订阅关闭、Store 句柄释放（无泄漏）。
func TestE2E_PluginStopConvergesInFlightRunAndReleasesHandles(t *testing.T) {
	// mock LLM：挂在 release 上直到客户端 ctx 取消（runCtx 取消 → HTTP 请求
	// 中止 → run 收敛取消）。release 是防御性兜底：若取消链失效，放行使 run
	// 跑完，让「末条非 cancelled」的断言失败可见，而不是拖垮测试进程。
	var releaseOnce sync.Once
	release := make(chan struct{})
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(core.ChatResponse{
				Choices: []core.ResponseChoice{{
					Message: core.Message{Role: "assistant", Content: "e2e stop reply."},
				}},
			})
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	cfg := config.Default()
	provider := cfg.LLM.Providers[cfg.LLM.DefaultProvider]
	provider.Endpoint = srv.URL
	cfg.LLM.Providers[cfg.LLM.DefaultProvider] = provider
	cfg.Session.Enabled = true
	cfg.Session.Dir = filepath.Join(dir, "sessions")
	cfg.Memory.Enabled = false

	// 真实插件装配链：NewPlugin + Init（startSession 建 repo/首会话，provider
	// 经 acpworker.ProviderFactory 从 cfg 解析到 mock 服务）。
	hub := &model.Hub{
		Config:             cfg,
		RegisterCommand:    func(model.Command) error { return nil },
		SetFallbackHandler: func(func(*model.Context) error) {},
		Tools:              func() []model.Tool { return nil },
	}
	p := agentplugin.NewPlugin(cfg.ToRuntime(), runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	sid := p.CurrentSessionID()
	if sid == "" {
		t.Fatal("no current session id after Init")
	}

	// 在飞后台 Run：持 lease 提交（fencing），等 run 真实开工（RunStatus 的
	// active 分支报 running）。
	p.AcquireControl(sid, "conn-e2e")
	runID, _, err := p.StartRun(sid, "conn-e2e", "hello", "req-e2e-stop")
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	waitRunning(t, p, sid, runID)

	// 在飞订阅：Stop 的 hub 收口应把它断开。
	res, err := p.SubscribeEvents(sid, 0)
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}

	// 停机：独立 goroutine + 超时兜底（等待面卡死时先放行 provider 再失败）。
	stopped := make(chan struct{})
	go func() { _ = p.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		releaseGate()
		<-stopped
		t.Fatal("Stop did not return in time (manager.Close wait stuck?)")
	}

	// ① run 终态（磁盘可证）：journal 末条 cancelled。Close 返回 ⇒ 等待面已见
	// done，终态记录先于 done 落盘（finalizeRun 次序不变量）。
	states := e2eStateRecords(t, filepath.Join(cfg.Session.Dir, sid, "runs", runID+".jsonl"))
	if len(states) == 0 {
		t.Fatal("run journal has no state records")
	}
	if last := states[len(states)-1].State; last != string(runtimeagent.RunCancelled) {
		t.Fatalf("last journal state = %q, want %q (in-flight run must converge to cancelled on Stop)", last, runtimeagent.RunCancelled)
	}

	// ② manager 停机：提交面拒绝（经 Service 委托链的 closed 检查——route 的
	// 「current 优先」停机后仍可解析，拒绝语义由 manager 的 closed 检查给出）。
	if _, _, err := p.StartRun(sid, "conn-e2e", "x", "req-post-stop"); !errors.Is(err, runtimeagent.ErrManagerClosed) {
		t.Fatalf("StartRun after Stop = %v, want ErrManagerClosed", err)
	}
	// 读侧观测面在停机后仍可用：RunStatus 经 journal 权威分支（磁盘事实源）
	// 继续报告该 run 的终态——停机不丢观测，也不发明第二事实源。
	state, latest, _, err := p.RunStatus(sid, runID)
	if err != nil || state != string(runtimeagent.RunCancelled) || latest == 0 {
		t.Fatalf("RunStatus after Stop = (%q, %d, %v), want (cancelled, >0, nil) via journal authority", state, latest, err)
	}

	// ③ 订阅不遗留：hub.Close 断开全部订阅者 → 订阅通道关闭（replay 缓冲排干
	// 后读到 ok=false）。
	subDeadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case _, ok := <-res.Subscription.Events():
			if !ok {
				// 通道已关闭，进入 ④
				e2eAssertStoreHandleReleased(t, cfg.Session.Dir, sid)
				return
			}
		default:
			if time.Now().After(subDeadline) {
				t.Fatal("subscription channel not closed after Stop (leaked subscription)")
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// e2eAssertStoreHandleReleased 从包外视角断言 Store 句柄已释放：对插件挂载的
// 会话目录（<Session.Dir>/<sid>/）session.Open 重开成功 ⇒ 插件 Stop 已 Close
// store 并释放跨进程文件锁；若句柄泄漏（flock 仍被插件实例持有），同进程的
// 第二次 LOCK_EX|LOCK_NB 被拒绝（ErrStoreLocked，store.go「宁拒勿双写」）。
// 重开实例随即 Close，不留新句柄。
func e2eAssertStoreHandleReleased(t *testing.T, root, sid string) {
	t.Helper()
	st, err := session.Open(filepath.Join(root, sid))
	if err != nil {
		t.Fatalf("reopen session store after Stop: %v (store handle leaked? lock still held?)", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

// waitRunning 轮询 RunStatus 直到 run 报告 running（active 分支，即 run 真实
// 开工——queued 的 journal 登记先于 execute 获得 gate，queued 不算开工）。
func waitRunning(t *testing.T, p *agentplugin.AgentPlugin, sid, runID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, _, _, err := p.RunStatus(sid, runID)
		if err == nil && state == string(runtimeagent.RunRunning) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reached running (last state=%q err=%v)", runID, state, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// e2eStateRecords 读一个 run journal 文件并只保留 kind=="state" 的记录（与
// plugins/agent 的 readStateRecords 同型；e2e 包无法复用包内测试 helper）。
func e2eStateRecords(t *testing.T, path string) []session.RunStateRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read run journal %s: %v", path, err)
	}
	var states []session.RunStateRecord
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec session.RunStateRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parse %s line %q: %v", path, line, err)
		}
		if rec.Kind == "state" {
			states = append(states, rec)
		}
	}
	return states
}
