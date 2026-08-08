package memory

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// 固化统一入口：checkpoint + fact 抽取的执行权收进记忆系统，
// 用 checkpointCh 串行化整个 read→LLM→write 周期。
//
// 为什么串行化点放 Client 而不是调用方：Client 是调用方持有的单例，
// 所有会话/触发点（/compact、/new 后台、/task checkpoint、进程退出）共享，
// 固化交错在构造上不可能 —— 后到的固化必然基于前一个已落库的结果做快照。

// CheckpointOptions 是固化入参包：模型/配置旋钮/会话记账，调用方每次提交。
type CheckpointOptions struct {
	LLM        LLM  // 固化推理模型（daemon 侧用适配器注入 core.Provider）
	LtmExtract bool // false = 决策不落库（只更新 task，配置开关）
	TokenUsage int  // 计入 Task.TokenUsage 的会话 token 累计
	CWD        string
}

// Checkpoint 把 conversation 固化进任务档案（Task + LTM）。
// 一次 LLM 调用打包 task 归属/新建/关闭 + 事实决策，应用结果落 store。
//
// 串行化：全程持 checkpointCh 信号量。容量 1 的 buffered channel 是自带
// ctx 语义的互斥量 —— 等待者可被 ctx 打断，无 goroutine 泄漏
// （对比 sync.Mutex + select ctx 的孤儿 goroutine 模式）。
// conversation 为空时不触发 LLM，返回 (nil, nil)。
func (c *Client) Checkpoint(ctx context.Context, conversation []Message, opts CheckpointOptions) (*AppliedSummary, error) {
	select {
	case c.checkpointCh <- struct{}{}:
		defer func() { <-c.checkpointCh }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if len(conversation) == 0 {
		return nil, nil
	}

	// 快照在锁内取 —— 后到的固化必然看到前一个已落库的结果（无 lost update）
	openTasks, err := c.store.OpenTasks(50)
	if err != nil {
		return nil, fmt.Errorf("open tasks: %w", err)
	}
	facts, err := c.store.ListFacts(50)
	if err != nil {
		return nil, fmt.Errorf("list facts: %w", err)
	}

	cp := NewCheckpointer(opts.LLM, opts.CWD)
	// 检查点 id：也是 Task.Runs 与 Fact.Source 的标记
	cpID := fmt.Sprintf("cp-%d", time.Now().UnixNano())
	res, err := cp.Run(ctx, conversation, openTasks, facts)
	if err != nil {
		return nil, err
	}
	// ltm_extract=false：tasks/decisions 是同一 LLM 调用输出的，无法只跳过调用，
	// 但应用层不落库事实 —— 配置开关生效。
	if !opts.LtmExtract {
		res.Decisions = nil
	}
	sum, err := ApplyCheckpoint(c.store, res, openTasks, facts, cpID, opts.TokenUsage, opts.CWD)
	if err != nil {
		return nil, err
	}
	if sum.Count() > 0 {
		// 明细全文已落库，日志只打条数防巨行；要看内容走调用方的 /memory 命令
		slog.Info("memory: checkpoint applied",
			"cp", cpID,
			"tasks", len(sum.UpdatedTasks)+len(sum.ClosedTasks),
			"facts", len(sum.Facts)+len(sum.DeletedFacts))
	}
	return sum, nil
}
