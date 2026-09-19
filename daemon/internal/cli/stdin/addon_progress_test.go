package stdin

import (
	"strings"
	"testing"
	"time"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/cli/statusbar"
)

// newProgressFixture 构造 Bar + ProgressAddon（经 Use 触发 OnRegister 订阅）。
func newProgressFixture() (*statusbar.Bar, *ProgressAddon) {
	bar := statusbar.New()
	a := NewProgressAddon()
	bar.Use(a)
	return bar, a
}

func phase(kind runtimeconfig.PhaseKind, label string) runtimeconfig.PhaseEvent {
	return runtimeconfig.PhaseEvent{Kind: kind, Label: label}
}

// TestProgressAddon_PhaseLifecycle begin 激活 → stage 切阶段 → end 停用，全程驱动 Bar。
// 测试内无 Run 刷新循环，Stop 处于未 Tick 状态走静默路径，不污染 stderr。
func TestProgressAddon_PhaseLifecycle(t *testing.T) {
	bar, a := newProgressFixture()

	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseBegin, "固化记忆中"))
	if !bar.Active() {
		t.Fatal("begin: bar not active")
	}
	if !strings.Contains(a.Render(), "固化记忆中") {
		t.Errorf("begin render = %q, want stage label", a.Render())
	}
	// begin 触发 Start → Reset：spinner 归零
	if a.idx != 0 {
		t.Errorf("begin: spinner idx = %d, want 0 (reset)", a.idx)
	}

	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseStage, "压缩会话历史中"))
	if !strings.Contains(a.Render(), "压缩会话历史中") {
		t.Errorf("stage render = %q, want updated stage label", a.Render())
	}

	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseEnd, ""))
	if bar.Active() {
		t.Error("end: bar still active")
	}
}

// TestProgressAddon_BeginResetsElapsed end 后再次 begin（新命令）重置计时与阶段；
// 活跃期间重复 begin 被防重入拦截，不改 label、不重置计时。
func TestProgressAddon_BeginResetsElapsed(t *testing.T) {
	bar, a := newProgressFixture()

	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseBegin, "固化记忆中"))
	a.start = a.start.Add(-2 * time.Minute) // 模拟已运行一段时间

	// 活跃期间重复 begin：拦截，label 不变、计时不重置
	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseBegin, "压缩会话历史中"))
	if a.label != "固化记忆中" {
		t.Errorf("label = %q, want unchanged (reentrant begin blocked)", a.label)
	}
	if elapsed := time.Since(a.start); elapsed < time.Minute {
		t.Errorf("start reset by reentrant begin, elapsed = %v", elapsed)
	}

	// end 后新命令 begin：重置生效
	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseEnd, ""))
	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseBegin, "压缩会话历史中"))
	if a.label != "压缩会话历史中" {
		t.Errorf("label = %q, want overwritten by new begin", a.label)
	}
	if elapsed := time.Since(a.start); elapsed > time.Second {
		t.Errorf("start not reset, elapsed = %v", elapsed)
	}
}

// TestProgressAddon_EventsWithoutBegin 未 begin 先收 stage/end 必须无副作用。
func TestProgressAddon_EventsWithoutBegin(t *testing.T) {
	bar, a := newProgressFixture()

	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseStage, "固化记忆中"))
	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseEnd, ""))
	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseBegin, ""))
	bar.Publish(runtimeconfig.EventPhase, phase(runtimeconfig.PhaseBegin, "")) // 重复 begin 防重入

	if !bar.Active() {
		t.Error("bar should be active after single begin")
	}
	if a.label != "" {
		t.Errorf("label = %q, want empty (begin without label)", a.label)
	}
}

// TestProgressAddon_RenderLabel 渲染形态：无 label 维持旧格式，有 label 插入阶段文本。
func TestProgressAddon_RenderLabel(t *testing.T) {
	a := NewProgressAddon()
	if got := a.Render(); !strings.HasPrefix(got, "⠋ ⏱ ") {
		t.Errorf("no-label render = %q, want spinner+elapsed", got)
	}

	a.label = "压缩会话历史中"
	got := a.Render()
	if !strings.Contains(got, "⠋ 压缩会话历史中 ⏱ ") {
		t.Errorf("label render = %q, want spinner+label+elapsed", got)
	}
}

// TestProgressAddon_OnStopCommit end 定格为 ✓（Stopper 契约）。
func TestProgressAddon_OnStopCommit(t *testing.T) {
	a := NewProgressAddon()
	a.OnStop()
	if got := a.Render(); !strings.HasPrefix(got, "✓") {
		t.Errorf("stopped render = %q, want ✓ prefix", got)
	}
}

// TestProgressAddon_IgnoreForeignPayload 非 PhaseEvent 载荷忽略不 panic。
func TestProgressAddon_IgnoreForeignPayload(t *testing.T) {
	bar, _ := newProgressFixture()
	bar.Publish(runtimeconfig.EventPhase, "junk")
	bar.Publish(runtimeconfig.EventPhase, nil)
	if bar.Active() {
		t.Error("bar should stay inactive")
	}
}
