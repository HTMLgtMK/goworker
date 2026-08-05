package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
	"github.com/tinguo/goworker/memory"
)

// 本文件是检查点固化：把会话 conversation 固化进任务档案（Task + LTM），
// 供 /compact /new /task checkpoint /进程退出 四个触发点共用。

// ---- 固化检查点 ----

// checkpointMemory 把当前 conversation 固化进任务档案（Task + LTM）。
// 同步入口：锁内快照 conversation 后调 checkpointSnapshot。
// 触发点都是低频事件：/compact、进程退出、/task checkpoint —— 这些调用方
// 语义上必须等固化完成（压缩前、退出前），故保持同步。
// 返回本次固化的实际结果明细（nil = 无可固化内容），供调用方回显/留痕。
func (p *AgentPlugin) checkpointMemory(ctx context.Context) (*memory.AppliedSummary, error) {
	p.mu.Lock()
	conv := slices.Clone(p.conversation)
	p.mu.Unlock()
	return p.checkpointSnapshot(ctx, conv)
}

// checkpointSnapshot 把给定 conversation 快照固化进任务档案。
// 一次 LLM 调用（Checkpointer）输出 task 归属/新建/关闭 + 事实决策。
// 读快照→LLM 推理→写回是整个周期，store 的 mutex 只串行化单次写。
// 并发检查点（如 /compact 与退出 Stop、后台固化重叠）会基于过期快照互相覆盖，
// 必须整体互斥。返回本次固化实际应用的明细（AppliedSummary，nil = 无可固化内容）。
// 快照必须由调用方在锁内克隆 —— /new 的后台固化依赖此保证清 STM 后仍能固化旧历史。
func (p *AgentPlugin) checkpointSnapshot(ctx context.Context, conv []core.Message) (*memory.AppliedSummary, error) {
	// checkpointMu 可能被后台固化（/new 异步，最长 120s）持有 —— 拿锁不能无界阻塞，
	// 否则 Stop 的 45s 超时形同虚设：等拿到锁时 ctx 已取消，固化静默失败。
	acquired := make(chan struct{})
	go func() {
		p.checkpointMu.Lock()
		close(acquired)
	}()
	select {
	case <-acquired:
		defer p.checkpointMu.Unlock()
	case <-ctx.Done():
		// 放弃固化，但拿锁的 goroutine 仍会拿到锁 —— 等它完成后自行释放，别让锁永久持有
		go func() {
			<-acquired
			p.checkpointMu.Unlock()
		}()
		return nil, ctx.Err()
	}

	if p.memory == nil || len(conv) == 0 {
		return nil, nil
	}

	cfg := p.hub.Config
	provider := NewOpenAIProvider(cfg.LLM.Endpoint, cfg.LLM.APIKey, cfg.LLM.Model)
	cwd, _ := os.Getwd()
	cp := memory.NewCheckpointer(&llmAdapter{inner: provider}, cwd)

	openTasks, err := p.memory.OpenTasks(50)
	if err != nil {
		return nil, fmt.Errorf("open tasks: %w", err)
	}
	facts, err := p.memory.ListFacts(50)
	if err != nil {
		return nil, fmt.Errorf("list facts: %w", err)
	}
	// 检查点 id：也是 Task.Runs 与 Fact.Source 的标记
	cpID := fmt.Sprintf("cp-%d", time.Now().UnixNano())
	res, err := cp.Run(ctx, toMemoryMessages(conv), openTasks, facts)
	if err != nil {
		return nil, err
	}
	// ltm_extract=false：tasks/decisions 是同一 LLM 调用输出的，无法只跳过调用，
	// 但应用层不落库事实 —— 配置开关生效。
	if !cfg.Memory.LtmExtract {
		res.Decisions = nil
	}
	tokens := 0
	if p.usage != nil {
		tokens = p.usage.Snapshot().TotalTokens
	}
	sum, err := memory.ApplyCheckpoint(p.memory, res, openTasks, facts, cpID, tokens, cwd)
	if err != nil {
		return nil, err
	}
	if sum.Count() > 0 {
		// 明细全文已落库，日志只打条数防巨行；要看内容走 /memory 命令
		slog.Info("memory: checkpoint applied",
			"cp", cpID,
			"tasks", len(sum.UpdatedTasks)+len(sum.ClosedTasks),
			"facts", len(sum.Facts)+len(sum.DeletedFacts))
	}
	return sum, nil
}

// checkpointAsync 后台固化旧会话：/new 不阻塞用户输入，固化结果完成后回显。
// writer 是捕获的命令 Writer（stdin 实现线程安全），固化期间用户可立即输入下一行。
// 注意：若 /new 后立刻退出进程，后台固化可能未跑完，这段历史只留 STM 会丢 ——
// 这是异步固化的代价，Stop 的同步固化兜底下一段对话。
func (p *AgentPlugin) checkpointAsync(conv []core.Message, writer func(string)) {
	// 独立 ctx：命令 ctx 已随请求返回被释放，后台固化不能继承它。
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	sum, err := p.checkpointSnapshot(ctx, conv)
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
