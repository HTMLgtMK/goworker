package middlewares

import (
	"context"
	"fmt"
	"strings"

	"github.com/tinguo/goworker/ai-core/core"
)

type CompressReport struct {
	BeforeMsgs int // 压缩前消息条数
	AfterMsgs  int // 压缩后消息条数
	Tokens     int // summarize 调用消耗（模型返回 total，缺省用估算）
}

// Compressor 将较早的对话压成摘要，同时保留最近 keepLast 条原文。
type Compressor struct {
	provider core.Provider
	keepLast int
	// protectSystem 为 true 时保留所有前导 system 消息，避免压掉提示词或记忆块；
	// 显式 compact 时可关闭，以便旧摘要参与下一轮压缩。
	protectSystem bool
	// onCompress 压缩成功后回调（可选），调用方用它把压缩记进 token 账本。
	onCompress func(CompressReport)
}

func NewCompressor(provider core.Provider, keepLast int, protectSystem bool, onCompress ...func(CompressReport)) *Compressor {
	c := &Compressor{provider: provider, keepLast: keepLast, protectSystem: protectSystem}
	if len(onCompress) > 0 {
		c.onCompress = onCompress[0]
	}
	return c
}

// Compress 压缩 history。无可压缩内容（长度 <= keepLast 或找不到安全切点）时原样返回。
func (c *Compressor) Compress(ctx context.Context, history []core.Message) ([]core.Message, error) {
	keepHead := 0
	// 保留前导 system 消息，避免提示词和注入的记忆进入摘要。
	if c.protectSystem {
		for keepHead < len(history) && history[keepHead].Role == "system" {
			keepHead++
		}
	}
	body := history[keepHead:]

	split, ok := c.splitPoint(body)
	if !ok {
		return history, nil
	}
	old, recent := body[:split], body[split:]

	summary, usage, err := c.summarize(ctx, old)
	if err != nil {
		return nil, fmt.Errorf("compress history: %w", err)
	}

	out := make([]core.Message, 0, keepHead+1+len(recent))
	out = append(out, history[:keepHead]...)
	out = append(out, core.Message{Role: "system", Content: summary})
	out = append(out, recent...)
	if c.onCompress != nil {
		tokens := 0
		if usage != nil {
			tokens = usage.TotalTokens
		}
		if tokens == 0 {
			tokens = core.EstimateTokens(old)
		}
		c.onCompress(CompressReport{BeforeMsgs: len(history), AfterMsgs: len(out), Tokens: tokens})
	}
	return out, nil
}

// splitPoint 找到安全切点：split 之后的消息原样保留，之前的进摘要。
// 切点必须避开 assistant tool_call 与 tool 结果的配对 —— OpenAI 要求
// tool 消息的 tool_call_id 指向历史中存在的 assistant 消息，拆开会被 400 拒。
// 规则：切点处不能是 tool 消息；切点前一条不能是带 tool_calls 的 assistant。
func (c *Compressor) splitPoint(history []core.Message) (int, bool) {
	// keepLast <= 0 时切点会越界或 old 为空；old 少于 2 条时压缩没有收益（1 条压成 1 条摘要）
	if c.keepLast <= 0 || len(history) <= c.keepLast+1 {
		return 0, false
	}
	split := len(history) - c.keepLast
	for split >= 2 {
		if history[split].Role != "tool" {
			if prev := history[split-1]; prev.Role != "assistant" || len(prev.ToolCalls) == 0 {
				return split, true
			}
		}
		split--
	}
	// 回退到 split<2 仍未找到安全点：old 太薄或最近 N 条全是工具尾巴 —— 不压，等更多上下文
	return 0, false
}

// summarize 请求模型把旧段压缩成一段摘要。不带工具，只发旧段本体。
// 返回模型报告的用量，供调用方把压缩消耗记进账本。
func (c *Compressor) summarize(ctx context.Context, old []core.Message) (string, *core.UsageInfo, error) {
	req := &core.ChatRequest{
		Model: c.provider.Model(),
		Messages: append(
			[]core.Message{{Role: "system", Content: summaryPrompt(old)}},
			old...,
		),
	}
	resp, err := c.provider.Chat(ctx, req)
	if err != nil {
		return "", nil, fmt.Errorf("summarize history: %w", err)
	}
	if len(resp.Choices) == 0 {
		return "", nil, fmt.Errorf("empty response")
	}
	return strings.TrimSpace(resp.Choices[0].Message.Content), resp.Usage, nil
}

// summaryPrompt 构造面向后续工作的压缩摘要指令。
func summaryPrompt(old []core.Message) string {
	return "Summarize the conversation below for a coding assistant that must continue the work.\n" +
		"Preserve: decided facts and constraints, file paths, shell commands run and their key " +
		"results, tool outputs that matter, the current task state and any pending next steps.\n" +
		"Drop: greetings, repetition, full tool output dumps.\n" +
		"Output a single dense plain-text paragraph, no headers, no bullet lists."
}
