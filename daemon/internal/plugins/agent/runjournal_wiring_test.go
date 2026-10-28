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

// 编译期断言（P2-3 遗留，plan P2-3「新增文件：无」节指定落在本步）：agent 包的
// RunStateJournal 消费面由 session 包的聚合类型 *session.RunJournalStore 满足。
// 断言必须放 daemon（消费方）侧——session 包不得反向 import agent（RunState 是
// agent 类型，勘察修正 D），反向断言无处落地。
var _ runtimeagent.RunStateJournal = (*session.RunJournalStore)(nil)

// writeRunJournalFile 以 P2-2 的字节格式写一个 run journal 文件（json.Encoder
// 单行 + '\n'，对齐 appendRunStateRecord，journal.go:106-119），模拟上一进程的
// 遗留；返回写入的原始字节供前缀/逐字节对照。
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

// initHub 构造可跑完整 Init 的最小 hub（同 TestInit_OpensMemory 先例，
// plugin_test.go:1005-1010：testHub 不带 RegisterCommand/SetFallbackHandler，
// Init 注册命令时会对 nil 函数调用 panic）。
func initHub(cfg *runtimeconfig.Config) *model.Hub {
	return &model.Hub{
		Config:             cfg,
		RegisterCommand:    func(model.Command) error { return nil },
		SetFallbackHandler: func(func(*model.Context) error) {},
		Tools:              func() []model.Tool { return nil },
	}
}

// TestStartSessionMarksInterruptedRuns 验证 P2-7 的启动恢复接线（总方案第九节
// 「Daemon 重启后」的 daemon 落地）：临时 Session.Dir 造 legacy 槽 + 上一进程
// 遗留的 runs/（run-<nano>-1 末记录 running、run-<nano>-0 已终态 completed）→
// NewPlugin+Init（真实 startSession 路径）→ 未收尾的 run 被追加恰好一条
// interrupted 标记且原内容为字节前缀（不动）；已终态的 run 逐字节不动。
func TestStartSessionMarksInterruptedRuns(t *testing.T) {
	root := t.TempDir()

	// legacy 槽（<root>/current.jsonl，既有用户数据的挂载形态）：session.Open
	// 在空根目录种 metadata 行并持 flock，先关闭释放锁，daemon Init 才能挂载。
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

	// daemon 启动：NewPlugin + Init → startSession 挂载 legacy 槽并执行恢复扫描
	//（MarkInterruptedRuns）。Memory 关闭以隔离本测试关注面。
	cfg := runtimeconfig.Default()
	cfg.Session.Dir = root
	cfg.Session.Enabled = true
	cfg.Memory.Enabled = false
	hub := initHub(cfg)
	p := NewPlugin(cfg, runtimeconfig.Paths{})
	if err := p.Init(hub); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop() }) // 释放 repo 持有的 legacy 槽 flock

	// 未收尾的 run：原内容字节前缀不动 + 追加恰好一条 interrupted 标记。
	interruptedAfter, err := os.ReadFile(filepath.Join(runsDir, runID+".jsonl"))
	if err != nil {
		t.Fatalf("read %s after Init: %v", runID, err)
	}
	if !bytes.HasPrefix(interruptedAfter, interruptedBefore) {
		t.Fatalf("%s original content not preserved as prefix by startup scan", runID)
	}
	recs := readRunJournalFile(t, filepath.Join(runsDir, runID+".jsonl"))
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
	completedAfter, err := os.ReadFile(filepath.Join(runsDir, doneID+".jsonl"))
	if err != nil {
		t.Fatalf("read %s after Init: %v", doneID, err)
	}
	if !bytes.Equal(completedBefore, completedAfter) {
		t.Fatalf("%s modified by startup scan: %d bytes before, %d after", doneID, len(completedBefore), len(completedAfter))
	}
}

// TestAgentRunJournalsThroughDaemon 验证 PR4(总方案 §九)的 journal 接线穿透
// daemon 全链:持久化启用路径跑一轮 /agent(handleAgent → StartRunSync →
// manager.StartRun),当前会话 runs/ 下出现恰一个 run 文件,state 记录序
// queued → running → completed(与后台 Run 同型——queued 是 StartRun 的
// durable-first 接受记录,PR4 起同步 prompt 与后台 run 同一记账面;RecordRun
// 形态的 running 起跑记录已退役)。事件记录与 state 记录同文件混存
// (journal_store.go P3-1),断言只认 kind=="state" 的行。
//
// 固定 provider 经 setMemory 同款手法注入（plugin_test.go:240-244）：SessionDeps
// 是值拷贝，改 p.deps 后必须 refreshSession 重建会话生效；refreshSession 与
// startSession 的会话边界构造一致（Register 进 manager + 置 current），新会话
// 继承 startSession 注入的 Store + RunJournal（真实 startSession 接线由此穿透）。
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
