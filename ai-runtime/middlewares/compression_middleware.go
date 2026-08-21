package middlewares

import (
	"log/slog"

	"github.com/tinguo/goworker/ai-core/core"
)

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
