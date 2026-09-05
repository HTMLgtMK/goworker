package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// 本文件是检查点固化入口：把会话 conversation 固化进任务档案（Task + LTM）。
// 执行权在记忆系统（Client.Checkpoint 串行化 read→LLM→write 周期），
// 这里只剩薄适配：快照 conversation → 构建 opts → 交给 Client。
// 触发点 /compact /new /task checkpoint /进程退出 共享同一串行化点，
// 后台固化（/new）与新会话固化互斥 —— 双锁分叉在构造上消失。

// ---- 固化检查点 ----

// checkpoint 把给定 conversation 快照固化进任务档案。
// 快照必须由调用方提供（/new 的后台固化在清 STM 前已取好旧会话快照）。
// cfg 由调用方传入：同步调用（Run/compact/Stop）传 s.deps.Config（主 goroutine 串行安全），
// 后台固化（CheckpointAsync）传快照 —— 主线程可并发 /model set，不能在 goroutine 里读共享配置。
// 串行化在 Client 内完成：后到的固化必然看到前一个已落库的结果。
func (s *Session) checkpoint(ctx context.Context, conv []core.Message, cfg *runtimeconfig.Config) (*memory.AppliedSummary, error) {
	if s.deps.Memory == nil || len(conv) == 0 {
		return nil, nil
	}
	cwd, _ := os.Getwd()
	provider, err := s.deps.NewProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("create checkpoint provider: %w", err)
	}
	return s.deps.Memory.Checkpoint(ctx, toMemoryMessages(conv), memory.CheckpointOptions{
		LLM:        &llmAdapter{inner: provider},
		LtmExtract: cfg.Memory.LtmExtract,
		TokenUsage: s.usage.Snapshot().TotalTokens,
		CWD:        cwd,
	})
}

// CheckpointAsync 后台固化旧会话：/new 不阻塞用户输入，固化结果完成后回显。
// writer 是捕获的命令 Writer（stdin 实现线程安全），固化期间用户可立即输入下一行。
// 注意：若 /new 后立刻退出进程，后台固化可能未跑完，这段历史只留 STM 会丢 ——
// 这是异步固化的代价，Stop 的同步固化兜底下一段对话。
func (s *Session) CheckpointAsync(conv []core.Message, writer func(string)) {
	// 独立 ctx：命令 ctx 已随请求返回被释放，后台固化不能继承它。
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// 后台固化读配置做快照：主线程可能并发 /model set 改 cfg.LLM.*，goroutine 里不能碰共享指针。
	// 浅拷贝即可 —— checkpoint 只读 LLM（字符串）+ Memory.LtmExtract（bool），全是值类型。
	cfgSnap := *s.deps.Config
	cfgSnap.LLM = s.deps.Config.LLM.Clone()
	sum, err := s.checkpoint(ctx, conv, &cfgSnap)
	if err != nil {
		writer(fmt.Sprintf("⚠ Consolidation failed (history kept only in STM, lost on exit): %v\n", err))
		return
	}
	if notice := RenderCheckpointNotice(sum); notice != "" {
		writer(notice)
	} else {
		// 有内容但 LLM 判定无可存 —— 仍要回一个完成信号，否则用户
		// 无法区分"还在跑"vs"跑完没存"（旧行为无条件回显 N entries）。
		writer("✔ Previous session consolidated (nothing new)\n")
	}
}

// Consolidate 增量固化：有 store 时只喂游标之后未固化的消息，无 store 回退全量。
func (s *Session) Consolidate(ctx context.Context) (*memory.AppliedSummary, error) {
	if s.deps.Store == nil {
		return s.checkpoint(ctx, s.Conversation(), s.deps.Config) // 回退全量
	}
	pending := s.deps.Store.PendingAfterCursor()
	if len(pending) == 0 || len(toMemoryMessages(pending)) == 0 {
		return nil, nil
	}
	sum, err := s.checkpoint(ctx, pending, s.deps.Config)
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

// RenderCheckpointNotice 把一次固化的实际结果渲染成用户可见提示：概括 + 每条标题。
// 空/零结果返回空串，调用方自行决定是否静默。
func RenderCheckpointNotice(sum *memory.AppliedSummary) string {
	if sum == nil || sum.Count() == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "✔ Memory updated: %d task(s), %d fact(s)\n",
		len(sum.UpdatedTasks)+len(sum.ClosedTasks), len(sum.Facts)+len(sum.DeletedFacts))
	for _, t := range sum.UpdatedTasks {
		b.WriteString("  - task: " + Truncate(OneLine(t), 160) + "\n")
	}
	for _, t := range sum.ClosedTasks {
		b.WriteString("  - task (closed): " + Truncate(OneLine(t), 160) + "\n")
	}
	for _, f := range sum.Facts {
		b.WriteString("  - fact: " + Truncate(OneLine(f), 160) + "\n")
	}
	for _, f := range sum.DeletedFacts {
		b.WriteString("  - fact (deleted): " + Truncate(OneLine(f), 160) + "\n")
	}
	return b.String()
}
