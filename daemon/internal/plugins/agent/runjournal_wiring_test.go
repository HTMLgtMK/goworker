package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// 编译期断言：daemon 使用的聚合 journal 满足 agent 的状态 journal 接口。
// 断言放在消费方，避免 session 包反向依赖 agent。
var _ runtimeagent.RunStateJournal = (*session.RunJournalStore)(nil)

// writeRunJournalFile 用生产格式写入 run journal 记录，模拟上一进程留下的数据。
func writeRunJournalFile(t *testing.T, path string, recs ...session.RunStateRecord) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, rec := range recs {
		if err := enc.Encode(rec); err != nil {
			t.Fatalf("marshal record for %s: %v", path, err)
		}
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return buf.Bytes()
}

// readRunJournalFile 解析一个 run journal 文件的全部记录（每行一条）。
func readRunJournalFile(t *testing.T, path string) []session.RunStateRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var recs []session.RunStateRecord
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		var rec session.RunStateRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parse %s line %q: %v", path, line, err)
		}
		recs = append(recs, rec)
	}
	return recs
}

// initHub 构造可跑 Init 的最小 hub；testHub 缺少 Init 注册命令所需的回调。
func initHub(cfg *runtimeconfig.Config) *model.Hub {
	return &model.Hub{
		Config:             cfg,
		RegisterCommand:    func(model.Command) error { return nil },
		SetFallbackHandler: func(func(*model.Context) error) {},
		Tools:              func() []model.Tool { return nil },
	}
}

// TestStartSessionMarksInterruptedRuns 验证 legacy 会话与 runs 迁移后，重开会话
// 只为未终态 run 追加一条 interrupted 记录；原记录保留，已终态文件不变。
func TestStartSessionMarksInterruptedRuns(t *testing.T) {
	root := t.TempDir()

	// legacy 槽（<root>/current.jsonl，既有用户数据的挂载形态）：session.Open
	// 在空根目录种 metadata 行并持 flock，先关闭释放锁，迁移与 daemon 才能触碰。
	legacy, err := session.Open(root)
	if err != nil {
		t.Fatalf("open legacy slot: %v", err)
	}
	sid := legacy.SessionID()
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy slot: %v", err)
	}

	// 上一进程遗留 runs/：run-<nano>-1 末记录 running（未收尾）；run-<nano>-0
	// 已终态 completed（恢复扫描的幂等对照）。
	at := time.Now()
	runsDir := filepath.Join(root, "runs")
	if err := os.MkdirAll(runsDir, 0o700); err != nil {
		t.Fatalf("mkdir runs: %v", err)
	}
	runID := fmt.Sprintf("run-%d-1", at.UnixNano())
	doneID := fmt.Sprintf("run-%d-0", at.UnixNano())
	interruptedBefore := writeRunJournalFile(t, filepath.Join(runsDir, runID+".jsonl"),
		session.RunStateRecord{Kind: "state", SessionID: sid, RunID: runID, State: "running", At: at},
	)
	completedBefore := writeRunJournalFile(t, filepath.Join(runsDir, doneID+".jsonl"),
		session.RunStateRecord{Kind: "state", SessionID: sid, RunID: doneID, State: "running", At: at},
		session.RunStateRecord{Kind: "state", SessionID: sid, RunID: doneID, State: "completed", Persisted: true, At: at.Add(time.Second)},
	)

	// daemon 启动：NewPlugin + Init → startSession 首周期迁移（槽 + runs/ 入
	// per-id）→ Create 新会话为 current。
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = root
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	hub := initHub(cfg)
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	// 迁移落位断言：槽与 runs/ 都在 per-id 目录下，根下旧形态消失。
	if _, err := os.Stat(filepath.Join(root, sid, sid+".jsonl")); err != nil {
		t.Fatalf("legacy slot not migrated to per-id: %v", err)
	}
	migratedRunsDir := filepath.Join(root, sid, "runs")
	if _, err := os.Stat(migratedRunsDir); err != nil {
		t.Fatalf("legacy runs dir not carried over to per-id: %v", err)
	}
	if _, err := os.Stat(runsDir); !os.IsNotExist(err) {
		t.Fatalf("root runs dir still present after migration (err=%v), want moved", err)
	}

	// 重开迁移后的会话（session/load 链）：A2b1 的重开路径执行 P2-7 恢复扫描。
	if err := p.LoadBoundSession(sid, ""); err != nil {
		t.Fatalf("LoadBoundSession(migrated session): %v", err)
	}

	// 未收尾的 run：原内容字节前缀不动 + 追加恰好一条 interrupted 标记。
	interruptedAfter, err := os.ReadFile(filepath.Join(migratedRunsDir, runID+".jsonl"))
	if err != nil {
		t.Fatalf("read %s after reopen: %v", runID, err)
	}
	if !bytes.HasPrefix(interruptedAfter, interruptedBefore) {
		t.Fatalf("%s original content not preserved as prefix by reopen scan", runID)
	}
	recs := readRunJournalFile(t, filepath.Join(migratedRunsDir, runID+".jsonl"))
	if len(recs) != 2 {
		t.Fatalf("%s records = %d, want 2 (原 running + 恰一条标记)", runID, len(recs))
	}
	if recs[0].State != "running" || recs[0].RunID != runID {
		t.Fatalf("%s first record = %+v, want original running record intact", runID, recs[0])
	}
	marker := recs[1]
	if marker.Kind != "state" || marker.State != session.StateInterrupted || marker.RunID != runID || marker.SessionID != sid || marker.At.IsZero() {
		t.Fatalf("%s marker record = %+v, want interrupted marker", runID, marker)
	}

	// 已终态的 run：逐字节不动。
	completedAfter, err := os.ReadFile(filepath.Join(migratedRunsDir, doneID+".jsonl"))
	if err != nil {
		t.Fatalf("read %s after reopen: %v", doneID, err)
	}
	if !bytes.Equal(completedBefore, completedAfter) {
		t.Fatalf("%s modified by reopen scan: %d bytes before, %d after", doneID, len(completedBefore), len(completedAfter))
	}
}

// TestAgentRunJournalsThroughDaemon 验证同步 prompt 将 queued、running、completed
// 状态写入当前会话的 journal，且事件与状态共用同一 journal store。
func TestAgentRunJournalsThroughDaemon(t *testing.T) {
	root := t.TempDir()
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = root
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	hub := initHub(cfg)
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) {
		return fixedProvider{content: "journal-ok"}, nil
	}
	p.refreshSession()

	// P5-4 断言：持久化启用变体下 deps.RunEvents 非 nil，且与 deps.RunJournal 是
	// 同一个 *session.RunJournalStore 实例（事件持久化与 run 状态 journal 同源，
	// startSession 的 st != nil 分支组装，plugin.go）。
	jr, ok := p.deps.RunJournal.(*session.RunJournalStore)
	if !ok {
		t.Fatalf("deps.RunJournal type = %T, want *session.RunJournalStore", p.deps.RunJournal)
	}
	ev, ok := p.deps.RunEvents.(*session.RunJournalStore)
	if !ok {
		t.Fatalf("deps.RunEvents type = %T, want *session.RunJournalStore", p.deps.RunEvents)
	}
	if jr != ev {
		t.Fatalf("deps.RunEvents and deps.RunJournal must be the same *session.RunJournalStore instance")
	}

	ctx, _ := newContext("hello")
	if err := p.handleAgent(ctx); err != nil {
		t.Fatalf("handleAgent: %v", err)
	}

	// 当前会话 runs/ 下恰一个 run 文件（新布局 <root>/<sid>/runs/）。
	sid := p.currentStore().SessionID()
	runsDir := filepath.Join(root, sid, "runs")
	entries, err := os.ReadDir(runsDir)
	if err != nil {
		t.Fatalf("read runs dir %s: %v", runsDir, err)
	}
	if len(entries) != 1 {
		t.Fatalf("runs dir has %d entries, want 1", len(entries))
	}
	recs := readRunJournalFile(t, filepath.Join(runsDir, entries[0].Name()))
	// 事件记录与 state 记录同文件混存(P3-1):只断言 kind=="state" 的行。
	var states []session.RunStateRecord
	for _, rec := range recs {
		if rec.Kind == "state" {
			states = append(states, rec)
		}
	}
	if len(states) != 3 {
		t.Fatalf("run state records = %d (%+v), want 3", len(states), recs)
	}
	if states[0].State != "queued" || states[1].State != "running" || states[2].State != "completed" {
		t.Fatalf("record states = [%s %s %s], want [queued running completed]（StartRun 统一记账面）",
			states[0].State, states[1].State, states[2].State)
	}
	if states[0].RunID == "" || states[0].RunID != states[1].RunID || states[1].RunID != states[2].RunID {
		t.Fatalf("run ids = [%s %s %s], want same non-empty id", states[0].RunID, states[1].RunID, states[2].RunID)
	}
	if states[0].SessionID != sid || states[1].SessionID != sid || states[2].SessionID != sid {
		t.Fatalf("record session ids = [%s %s %s], want %s", states[0].SessionID, states[1].SessionID, states[2].SessionID, sid)
	}
	if !states[2].Persisted {
		t.Fatalf("completed record Persisted = false, want true（Conversation Commit 成功后才记 completed）")
	}
}

// TestAgentRunJournalsThroughDaemon_SessionDisabled 是 TestAgentRunJournalsThroughDaemon
// 的纯内存变体：session.enabled=false → 无 repo/RunJournal（typed-nil 纪律），
// 跑一轮 /agent 后 Session.Dir 树下不出现任何 runs/ 目录。
func TestAgentRunJournalsThroughDaemon_SessionDisabled(t *testing.T) {
	root := t.TempDir()
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = root
	cfg.Session.Enabled = false
	cfg.Memory.Enabled = false
	hub := initHub(cfg)
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() })

	p.deps.NewProvider = func(*runtimeconfig.Config) (core.Provider, error) {
		return fixedProvider{content: "ok"}, nil
	}
	p.refreshSession()

	// P5-4 断言：纯内存变体（st == nil）不设 RunEvents——typed-nil 纪律同
	// spec.go:96，「nil = 不启用事件流」判断必须保持真 nil。
	if p.deps.RunEvents != nil {
		t.Fatalf("deps.RunEvents = %T, want nil when session persistence disabled", p.deps.RunEvents)
	}

	ctx, _ := newContext("hello")
	if err := p.handleAgent(ctx); err != nil {
		t.Fatalf("handleAgent: %v", err)
	}

	// root 整体不存在 = 零落盘（Enabled=false 时 OpenRepository 不会被调用），
	// 存在则其中不得出现 runs/ 目录。
	if p.repo != nil {
		t.Fatalf("repo must stay nil when session persistence disabled")
	}
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatalf("read session dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() && e.Name() == "runs" {
			t.Fatalf("runs dir must not exist when session persistence disabled")
		}
	}
}
