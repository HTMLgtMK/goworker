package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// newGatedPlugin 构造仅用于 gate 测试的插件：NewPlugin 不触碰 cfg/paths、不创建会话，
// acquire/release 只操作 runGate 信号量，全程无 LLM 网络调用。
func newGatedPlugin() *AgentPlugin {
	return NewPlugin(&runtimeconfig.Config{}, runtimeconfig.Paths{})
}

func TestWithSession_ReentrantContextDoesNotBlock(t *testing.T) {
	p := newGatedPlugin()
	completed := make(chan error, 1)

	go func() {
		completed <- p.withSession(context.Background(), func(ctx context.Context) error {
			if owner, _ := ctx.Value(sessionGateKey{}).(*AgentPlugin); owner != p {
				return errors.New("session owner missing from context")
			}
			return p.withSession(ctx, func(context.Context) error { return nil })
		})
	}()

	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("withSession: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reentrant withSession blocked")
	}
}

func TestWithSession_WaitsForOtherOwnerAndRespectsCancellation(t *testing.T) {
	p := newGatedPlugin()
	release := make(chan struct{})
	entered := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- p.withSession(context.Background(), func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- p.withSession(ctx, func(context.Context) error {
			return errors.New("second owner entered while gate was held")
		})
	}()

	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued withSession error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled withSession did not return")
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first withSession: %v", err)
	}
}

// 第一个持有者未释放时，第二个 acquire 必须阻塞；release 后阻塞者立即进入。
func TestRunGate_SecondAcquireBlocksUntilRelease(t *testing.T) {
	p := newGatedPlugin()
	ctx := context.Background()

	if err := p.acquireRun(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	acquired := make(chan error, 1)
	go func() { acquired <- p.acquireRun(ctx) }()

	select {
	case err := <-acquired:
		t.Fatalf("second acquire returned %v while gate held, want blocked", err)
	case <-time.After(50 * time.Millisecond):
		// 预期：仍阻塞在 gate 上
	}

	p.releaseRun()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("second acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked acquirer did not enter after release")
	}
	p.releaseRun()
}

// gate 被持有且不释放时，排队者 ctx 取消后必须立即返回 ctx.Err()。
func TestRunGate_AcquireReturnsContextErrorOnCancel(t *testing.T) {
	p := newGatedPlugin()
	if err := p.acquireRun(context.Background()); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer p.releaseRun()

	ctx, cancel := context.WithCancel(context.Background())
	acquired := make(chan error, 1)
	go func() { acquired <- p.acquireRun(ctx) }()
	time.Sleep(20 * time.Millisecond) // 给排队者时间进入阻塞
	cancel()

	select {
	case err := <-acquired:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("acquire error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled acquirer did not return")
	}
}
