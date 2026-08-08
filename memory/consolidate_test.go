package memory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// 固化统一入口测试：串行化（无 lost update）、ctx 打断、配置旋钮、空输入。

// gatedSeqLLM 按调用次序返回不同 summary_delta，前两次调用阻塞在 release 上。
// 用门闩制造两个固化同时"卡在 LLM 阶段"的交错 —— 若无串行化，两个固化
// 会基于同一份过期快照各自 apply，后写覆盖前写；串行化则必然顺序执行。
type gatedSeqLLM struct {
	mu      sync.Mutex
	calls   int
	id      string
	entered chan struct{} // 第一次进入 Chat 时关闭（该固化已持锁并读过快照）
	release chan struct{} // 测试放行
}

func (g *gatedSeqLLM) Model() string { return "stub" }

func (g *gatedSeqLLM) Chat(_ context.Context, _ *ChatRequest) (*ChatResponse, error) {
	g.mu.Lock()
	n := g.calls
	g.calls++
	g.mu.Unlock()
	if n == 0 {
		close(g.entered)
	}
	if n < 2 {
		<-g.release
	}
	delta := "first"
	if n == 1 {
		delta = "second"
	}
	resp := fmt.Sprintf(`{"tasks":[{"id":"%s","summary_delta":"%s"}],"decisions":[]}`, g.id, delta)
	return &ChatResponse{Choices: []ResponseChoice{{Message: Message{Role: "assistant", Content: resp}}}}, nil
}

func TestClient_CheckpointConcurrentNoLostUpdate(t *testing.T) {
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	c.UpsertTask(&Task{Title: "task A", Status: "open", Summary: "base"})
	openTasks, _ := c.OpenTasks(10)
	id := openTasks[0].ID

	llm := &gatedSeqLLM{id: id, entered: make(chan struct{}), release: make(chan struct{})}
	opts := CheckpointOptions{LLM: llm, LtmExtract: true, CWD: "/work"}
	conv := []Message{{Role: "user", Content: "q"}, {Role: "assistant", Content: "a"}}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.Checkpoint(context.Background(), conv, opts) }()
	go func() { defer wg.Done(); c.Checkpoint(context.Background(), conv, opts) }()

	<-llm.entered // 第一个固化已读快照并进入 LLM（阻塞在 release，持锁）
	// 给第二个固化调度机会：无串行化时它也会读到同一份过期快照并阻塞在 release。
	// 串行化时它被信号量挡住，读不到快照 —— 结果不受这段等待影响。
	time.Sleep(100 * time.Millisecond)
	close(llm.release)
	wg.Wait()

	all, _ := c.ListTasks(10)
	if len(all) != 1 {
		t.Fatalf("tasks = %d, want 1", len(all))
	}
	// 两个固化都落库才算赢：串行化下第二个固化基于第一个的写入做 merge，
	// summary 应同时含 first 与 second；双锁回归则后写覆盖前写，丢一条。
	if !strings.Contains(all[0].Summary, "first") || !strings.Contains(all[0].Summary, "second") {
		t.Errorf("lost update: summary = %q, want both first and second deltas", all[0].Summary)
	}
}

func TestClient_CheckpointSerializesWaiters(t *testing.T) {
	// 串行化机制直测：第一个固化持锁进入 LLM 时，第二个固化必须阻塞，
	// 不能进入 LLM（否则 read→LLM→write 周期可交错）。
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	c.UpsertTask(&Task{Title: "t", Status: "open"})
	openTasks, _ := c.OpenTasks(10)
	id := openTasks[0].ID

	started := make(chan struct{})
	release := make(chan struct{})
	llm := &blockingLLM{id: id, started: started, release: release}
	opts := CheckpointOptions{LLM: llm, CWD: "/work"}
	conv := []Message{{Role: "user", Content: "q"}}

	firstDone := make(chan struct{})
	go func() { c.Checkpoint(context.Background(), conv, opts); close(firstDone) }()
	<-started // 第一个固化持锁进入 LLM

	secondEntered := make(chan struct{})
	go func() { c.Checkpoint(context.Background(), conv, opts); close(secondEntered) }()
	select {
	case <-secondEntered:
		t.Fatal("second checkpoint entered LLM while first holds the lock — not serialized")
	case <-time.After(100 * time.Millisecond):
		// 正确：第二个固化被信号量挡住
	}
	close(release)
	<-firstDone
	<-secondEntered
}

// blockingLLM 进入 Chat 后先关 started、等 release 才返回 —— 模拟一个被拖住的固化。
// started 用 Once 关：等锁的第二个固化会在 release 后进入 Chat，不能重复 close。
type blockingLLM struct {
	id      string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingLLM) Model() string { return "stub" }

func (b *blockingLLM) Chat(_ context.Context, _ *ChatRequest) (*ChatResponse, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	resp := fmt.Sprintf(`{"tasks":[{"id":"%s","summary_delta":"x"}],"decisions":[]}`, b.id)
	return &ChatResponse{Choices: []ResponseChoice{{Message: Message{Role: "assistant", Content: resp}}}}, nil
}

func TestClient_CheckpointContextTimeout(t *testing.T) {
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	c.checkpointCh <- struct{}{} // 占住信号量，模拟固化进行中
	defer func() { <-c.checkpointCh }()

	stub := &stubProvider{resp: `{"tasks":[],"decisions":[]}`}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	sum, err := c.Checkpoint(ctx, []Message{{Role: "user", Content: "q"}}, CheckpointOptions{LLM: stub})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if sum != nil {
		t.Errorf("sum = %+v, want nil on timeout", sum)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Checkpoint didn't return promptly on ctx cancel")
	}
	if stub.lastReq != nil {
		t.Error("LLM must not be called when waiting on the checkpoint lock times out")
	}
}

func TestClient_CheckpointLtmExtractDisabled(t *testing.T) {
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	stub := &stubProvider{resp: `{"tasks":[{"title":"new task","summary_delta":"x"}],"decisions":[{"action":"add","content":"fact","topic":"t"}]}`}
	sum, err := c.Checkpoint(context.Background(),
		[]Message{{Role: "user", Content: "q"}},
		CheckpointOptions{LLM: stub, LtmExtract: false, CWD: "/work"})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !strings.EqualFold(strings.Join(sum.UpdatedTasks, ""), "new task") {
		t.Errorf("UpdatedTasks = %v, want [new task] (task update must still land)", sum.UpdatedTasks)
	}
	if len(sum.Facts) != 0 {
		t.Errorf("Facts = %v, want empty (ltm_extract=false)", sum.Facts)
	}
	tasks, _ := c.ListTasks(10)
	if len(tasks) != 1 {
		t.Errorf("tasks = %d, want 1", len(tasks))
	}
	facts, _ := c.ListFacts(10)
	if len(facts) != 0 {
		t.Errorf("facts = %d, want 0 (decisions dropped)", len(facts))
	}
}

func TestClient_CheckpointEmptyConversationSkipsLLM(t *testing.T) {
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	stub := &stubProvider{resp: `{"tasks":[],"decisions":[]}`}
	sum, err := c.Checkpoint(context.Background(), nil, CheckpointOptions{LLM: stub})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if sum != nil {
		t.Errorf("sum = %+v, want nil for empty conversation", sum)
	}
	if stub.lastReq != nil {
		t.Error("LLM must not be called for an empty conversation")
	}
}

func TestClient_CheckpointFullFlow(t *testing.T) {
	c, err := NewClient(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	c.UpsertTask(&Task{Title: "old task", Status: "open", Summary: "prior"})
	openTasks, _ := c.OpenTasks(10)
	oldID := openTasks[0].ID

	stub := &stubProvider{resp: fmt.Sprintf(`{"tasks":[{"title":"new task","summary_delta":"x"},{"id":"%s","summary_delta":"y","done":true}],"decisions":[{"action":"add","content":"fact","topic":"t"}]}`, oldID)}
	sum, err := c.Checkpoint(context.Background(),
		[]Message{{Role: "user", Content: "q"}, {Role: "assistant", Content: "a"}},
		CheckpointOptions{LLM: stub, LtmExtract: true, TokenUsage: 42, CWD: "/work"})
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if sum.Count() != 3 { // 新建 + 关闭 + add fact
		t.Errorf("Count = %d, want 3", sum.Count())
	}
	// TokenUsage 与 run 标记经 ApplyCheckpoint 落到 task
	all, _ := c.ListTasks(10)
	if len(all) != 2 {
		t.Fatalf("tasks = %d, want 2", len(all))
	}
	var created *Task
	for i := range all {
		if all[i].ID != oldID {
			created = &all[i]
		}
	}
	if created == nil || created.TokenUsage != 42 || created.Cwd != "/work" {
		t.Errorf("new task meta wrong: %+v", created)
	}
	facts, _ := c.ListFacts(10)
	// runID 动态生成，只断言落库 + source 前缀
	if len(facts) != 1 || !strings.HasPrefix(facts[0].Source, "checkpoint:cp-") {
		t.Errorf("fact source = %v, want checkpoint:cp- prefix", facts)
	}
}
