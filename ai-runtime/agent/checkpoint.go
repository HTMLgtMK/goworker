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
// 触发点 /compact /new /task checkpoint /进程退出 共享同一串行化点 —— 双锁分叉在构造上消失。

// ---- 固化检查点 ----

// checkpoint 把给定 conversation 快照固化进任务档案。
// 快照必须由调用方提供（/new 的同步固化在清 STM 前已取好旧会话快照）。
// cfg 由调用方传入：所有触发点都在主 goroutine 串行执行，传 s.deps.Config 安全。
// 串行化在 Client 内完成：后到的固化必然看到前一个已落库的结果。
func (s *Session) checkpoint(ctx context.Context, conv []core.Message, cfg *runtimeconfig.Config) (*memory.AppliedSummary, error) {
	if s.deps.Memory == nil || len(conv) == 0 {
		return nil, nil
	}
	// 记忆档案的 CWD 优先取会话工作目录（任务发生在 client 声明的目录里）；
	// 未声明时回退进程 cwd（与旧行为一致）。
	cwd := s.cwd()
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
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

// publishStage 向状态栏发布阶段文本（EventPhase/PhaseStage），cb.Publish 未注入时静默。
func publishStage(cb RunCallbacks, label string) {
	if cb.Publish != nil {
		cb.Publish(runtimeconfig.EventPhase, runtimeconfig.PhaseEvent{Kind: runtimeconfig.PhaseStage, Label: label})
	}
}

// CheckpointSync 同步固化旧会话（/new）：阻塞至固化完成，固化期间状态栏显示阶段，
// 结果经 cb.Write 回显。仅可在主 goroutine 调用（读共享配置 s.deps.Config）。
// ctx 取消（用户 Esc）时固化中断，历史仅存 STM，返回前回显警告。
func (s *Session) CheckpointSync(ctx context.Context, conv []core.Message, cb RunCallbacks) {
	if s.deps.Memory == nil || len(conv) == 0 {
		return
	}
	publishStage(cb, "固化旧会话记忆")
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	sum, err := s.checkpoint(ctx, conv, s.deps.Config)
	if err != nil {
		if cb.Write != nil {
			cb.Write(fmt.Sprintf("⚠ Consolidation failed (history kept only in STM, lost on exit): %v\n", err))
		}
		return
	}
	if cb.Write == nil {
		return
	}
	if notice := RenderCheckpointNotice(sum); notice != "" {
		cb.Write(notice)
	} else {
		// 有内容但 LLM 判定无可存 —— 仍要回一个完成信号，否则用户
		// 无法区分"跑完没存"和"存了但没东西可存"。
		cb.Write("✔ Previous session consolidated (nothing new)\n")
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
