package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/memory"
)

// 本文件是检查点固化入口：把会话 conversation 固化进任务档案（Task + LTM）。
// 执行权在记忆系统（Client.Checkpoint 串行化 read→LLM→write 周期），
// 这里只剩薄适配：快照 conversation → 构建 opts → 交给 Client。
// 触发点 /compact /new /task checkpoint /进程退出 共享同一串行化点，
// 后台固化（/new）与新会话固化互斥 —— 双锁分叉在构造上消失。

// ---- 固化检查点 ----

// checkpoint 把给定 conversation 快照固化进任务档案。
// 快照必须由调用方提供（/new 的后台固化在清 STM 前已取好旧会话快照）。
// 串行化在 Client 内完成：后到的固化必然看到前一个已落库的结果。
func (s *Session) checkpoint(ctx context.Context, conv []core.Message) (*memory.AppliedSummary, error) {
	if s.deps.Memory == nil || len(conv) == 0 {
		return nil, nil
	}
	cfg := s.deps.Hub.Config
	cwd, _ := os.Getwd()
	return s.deps.Memory.Checkpoint(ctx, toMemoryMessages(conv), memory.CheckpointOptions{
		LLM:        &llmAdapter{inner: s.deps.NewProvider(cfg)},
		LtmExtract: cfg.Memory.LtmExtract,
		TokenUsage: s.usage.Snapshot().TotalTokens,
		CWD:        cwd,
	})
}

// checkpointAsync 后台固化旧会话：/new 不阻塞用户输入，固化结果完成后回显。
// writer 是捕获的命令 Writer（stdin 实现线程安全），固化期间用户可立即输入下一行。
// 注意：若 /new 后立刻退出进程，后台固化可能未跑完，这段历史只留 STM 会丢 ——
// 这是异步固化的代价，Stop 的同步固化兜底下一段对话。
func (s *Session) checkpointAsync(conv []core.Message, writer func(string)) {
	// 独立 ctx：命令 ctx 已随请求返回被释放，后台固化不能继承它。
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	sum, err := s.checkpoint(ctx, conv)
	if err != nil {
		writer(fmt.Sprintf("⚠ Consolidation failed (history kept only in STM, lost on exit): %v\n", err))
		return
	}
	if notice := renderCheckpointNotice(sum); notice != "" {
		writer(notice)
	} else {
		// 有内容但 LLM 判定无可存 —— 仍要回一个完成信号，否则用户
		// 无法区分"还在跑"vs"跑完没存"（旧行为无条件回显 N entries）。
		writer("✔ Previous session consolidated (nothing new)\n")
	}
}

// consolidate 增量固化：有 store 时只喂游标之后未固化的消息，无 store 回退全量。
func (s *Session) consolidate(ctx context.Context) (*memory.AppliedSummary, error) {
	if s.deps.Store == nil {
		return s.checkpoint(ctx, s.Conversation()) // 回退全量
	}
	pending := s.deps.Store.PendingAfterCursor()
	if len(pending) == 0 || len(toMemoryMessages(pending)) == 0 {
		return nil, nil
	}
	sum, err := s.checkpoint(ctx, pending)
	if err != nil {
		return nil, err
	}
	if sum != nil {
		if err := s.deps.Store.AdvanceCursor(pending[len(pending)-1].MsgID); err != nil {
			slog.Warn("session: advance cursor failed", "err", err)
		}
	}
	return sum, nil
}

// renderCheckpointNotice 把一次固化的实际结果渲染成用户可见提示：概括 + 每条标题。
// 空/零结果返回空串，调用方自行决定是否静默。
func renderCheckpointNotice(sum *memory.AppliedSummary) string {
	if sum == nil || sum.Count() == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "✔ Memory updated: %d task(s), %d fact(s)\n",
		len(sum.UpdatedTasks)+len(sum.ClosedTasks), len(sum.Facts)+len(sum.DeletedFacts))
	for _, t := range sum.UpdatedTasks {
		b.WriteString("  - task: " + truncate(oneLine(t), 160) + "\n")
	}
	for _, t := range sum.ClosedTasks {
		b.WriteString("  - task (closed): " + truncate(oneLine(t), 160) + "\n")
	}
	for _, f := range sum.Facts {
		b.WriteString("  - fact: " + truncate(oneLine(f), 160) + "\n")
	}
	for _, f := range sum.DeletedFacts {
		b.WriteString("  - fact (deleted): " + truncate(oneLine(f), 160) + "\n")
	}
	return b.String()
}
