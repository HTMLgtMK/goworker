package memory

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
	// 固化去重的关键是"能看到相关的旧 fact"：按时间截断（ListFacts）会让很久
	// 没更新的旧版本滑出视野，模型无从判重只能 add。改为从会话尾部提检索词，
	// 召回可能与本次会话冲突的旧 fact —— 检索式召回，Mem0 同款思路。
	facts, err := recallFacts(c.store, conversation, checkpointFactRecall)
	if err != nil {
		return nil, fmt.Errorf("recall facts: %w", err)
	}
	// 检索词命不中任何 fact 时（会话尾部用词与旧 fact 无重叠），去重管线会
	// 完全失明 —— 模型看不到旧版本，findOverlapFact 也无从拦截。回退最近几条
	// 兜底：宁可让模型看到可能无关的条目，也别让它对着空表判重。
	if len(facts) == 0 {
		if facts, err = c.store.ListFacts(10); err != nil {
			return nil, fmt.Errorf("list facts: %w", err)
		}
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

// checkpointFactRecall 固化时从会话召回多少条可能冲突的旧 fact 喂给模型。
const checkpointFactRecall = 20

// recallFactQueryLen 从会话尾部取多少字符作为检索词。
const recallFactQueryLen = 4000

// recallFacts 从会话尾部提取检索词，召回可能与本次会话冲突的已有 fact。
// 尾部最贴近本次会话在做的事；检索词越长召回越准但噪声越大，4000 字符平衡。
func recallFacts(store Store, conversation []Message, topK int) ([]Fact, error) {
	query := lastRunes(conversation, recallFactQueryLen)
	if strings.TrimSpace(query) == "" {
		return nil, nil
	}
	return store.SearchFacts(query, topK)
}

// lastRunes 从会话尾部向前收集非空 user/assistant 消息文本，拼到 limit runes
// 为止（正序返回）。跳过 system 消息 —— 压缩摘要（compressor 注入）是旧会话
// 的提炼，混进检索词会把固化引向已过时的主题。
// 单条消息超限时保留其头部（主题词通常在开头），更早的消息让位给最新的。
func lastRunes(msgs []Message, limit int) string {
	var parts []string
	var n int
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "system" {
			continue
		}
		s := strings.TrimSpace(msgs[i].Content)
		if s == "" {
			continue
		}
		r := []rune(s)
		if n+len(r) > limit {
			r = r[:limit-n]
			s = string(r)
		}
		parts = append(parts, s)
		n += len(r)
		if n >= limit {
			break
		}
	}
	slices.Reverse(parts)
	return strings.Join(parts, " ")
}
