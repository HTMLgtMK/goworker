package middlewares

import (
	"log/slog"

	"github.com/tinguo/goworker/daemon/internal/plugins/agent/core"
)

// CompressionMiddleware 在 BeforeModel 预检请求大小：估算用量达到
// compress_at × context_window 时，用 Compressor 压缩历史并替换即将发送的消息。
//
// 阈值必须放在发请求前，不能等 AfterModel 记账 —— 跟踪到 100% 时下一次
// 请求已经溢出（400），连压缩调用本身都没有发送空间。估算不含工具 schema，
// 天然带余量，0.8 是合理默认。
type CompressionMiddleware struct {
	compressor *core.Compressor
	window     int     // 模型上下文窗口，0 = 未配置，不压缩
	compressAt float64 // 0-1 触发阈值，0 = 关闭
	done       bool    // 压缩后仍超阈值 → 滚动压缩无解，本轮不再重复压（防每轮烧 summarize）
}

func NewCompressionMiddleware(compressor *core.Compressor, window int, compressAt float64) *CompressionMiddleware {
	return &CompressionMiddleware{compressor: compressor, window: window, compressAt: compressAt}
}

func (m *CompressionMiddleware) Name() string { return "compress" }

func (m *CompressionMiddleware) OnBeforeModel(ev *core.BeforeModelEvent) *core.MiddlewareResponse {
	if m.done || m.window <= 0 || m.compressAt <= 0 {
		return nil
	}
	threshold := int(float64(m.window) * m.compressAt)
	if core.EstimateTokens(ev.History) < threshold {
		return nil // 还有余量，别打扰模型
	}

	compacted, err := m.compressor.Compress(ev.Ctx, ev.History)
	if err != nil {
		// 压缩失败不打断会话：原历史继续发，最坏回到溢出 400 的老行为；但别静默
		slog.Warn("compress history failed, sending original", "err", err)
		return nil
	}
	// 整体替换：Compressor 返回的是全新切片，不与原历史共享底层数组
	ev.History = compacted
	// 压缩后仍超阈值，说明保留的最近 keepLast 条本身太大，滚动压缩救不了 ——
	// 置 done，避免后续迭代反复把同一条摘要重 roll 成摘要（token 白烧、精度损耗）
	if core.EstimateTokens(ev.History) >= threshold {
		m.done = true
	}
	return nil
}
