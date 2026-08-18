package core

import (
	"context"
	"fmt"
	"strings"
)

func NewCompressor(provider Provider, keepLast int, protectSystem bool, onCompress ...func(CompressReport)) *Compressor {
	c := &Compressor{provider: provider, keepLast: keepLast, protectSystem: protectSystem}
	if len(onCompress) > 0 {
		c.onCompress = onCompress[0]
	}
	return c
}

// Compress 压缩 history。无可压缩内容（长度 <= keepLast 或找不到安全切点）时原样返回。
func (c *Compressor) Compress(ctx context.Context, history []Message) ([]Message, error) {
	keepHead := 0
	// protectSystem 保护所有连续前导 system 消息（agent 提示词 + 注入的记忆块等），
	// 不让他们被压进滚动摘要 —— 摘要没有 [记忆] 前缀，stripMemoryBlocks 剥不掉，
	// 一旦进 STM 会被下次固化重新归档，形成自指污染。
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

	out := make([]Message, 0, keepHead+1+len(recent))
	out = append(out, history[:keepHead]...)
	out = append(out, Message{Role: "system", Content: summary})
	out = append(out, recent...)
	if c.onCompress != nil {
		tokens := 0
		if usage != nil {
			tokens = usage.TotalTokens
		}
		if tokens == 0 {
			tokens = EstimateTokens(old)
		}
		c.onCompress(CompressReport{BeforeMsgs: len(history), AfterMsgs: len(out), Tokens: tokens})
	}
	return out, nil
}

// splitPoint 找到安全切点：split 之后的消息原样保留，之前的进摘要。
// 切点必须避开 assistant tool_call 与 tool 结果的配对 —— OpenAI 要求
// tool 消息的 tool_call_id 指向历史中存在的 assistant 消息，拆开会被 400 拒。
// 规则：切点处不能是 tool 消息；切点前一条不能是带 tool_calls 的 assistant。
func (c *Compressor) splitPoint(history []Message) (int, bool) {
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
func (c *Compressor) summarize(ctx context.Context, old []Message) (string, *UsageInfo, error) {
	req := &ChatRequest{
		Model: c.provider.Model(),
		Messages: append(
			[]Message{{Role: "system", Content: summaryPrompt(old)}},
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

// summaryPrompt 构造压缩摘要的指令。
//
// TODO: 打磨这段提示词 —— 决定保留清单、输出格式和长度约束。
// 当前版本是能用的基线，重点想清楚：摘要要喂给"继续干活的 agent"，
// 而不是给人看，所以宁可信息密度高，不要追求行文流畅。
func summaryPrompt(old []Message) string {
	return "Summarize the conversation below for a coding assistant that must continue the work.\n" +
		"Preserve: decided facts and constraints, file paths, shell commands run and their key " +
		"results, tool outputs that matter, the current task state and any pending next steps.\n" +
		"Drop: greetings, repetition, full tool output dumps.\n" +
		"Output a single dense plain-text paragraph, no headers, no bullet lists."
}
