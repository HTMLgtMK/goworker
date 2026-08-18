package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// DefaultLLM 返回默认 LLM 配置（不含 APIKey）。
func DefaultLLM() LLMConfig {
	return LLMConfig{
		Endpoint:      "http://localhost:8000/v1",
		Model:         "gpt-4o",
		CompressAt:    0.8, // 用量达窗口 80% 自动压缩，留余量给压缩调用和新输入
		CompactKeep:   10,  // 最近 10 条原文保留，更早的才压缩
		MaxIterations: 15,  // ReAct 最大迭代数，模型连续调工具不至于无限烧 token
	}
}

// DefaultMemory 返回默认记忆配置。Dir 留空，由宿主按配置目录派生。
func DefaultMemory() MemoryConfig {
	return MemoryConfig{
		Enabled:           true,
		TaskKeep:          50,
		TaskInjectN:       3,
		LtmInjectTopK:     8,
		LtmExtract:        true,
		InjectBudgetRatio: 0.15,
		UserMaxChars:      1500,
		AgentsMaxChars:    4096,
	}
}

// ParseContextWindow 解析上下文窗口大小，支持纯数字或 k/m 后缀：
//
//	"32768" → 32768
//	"32k"   → 32768
//	"1.5m"  → 1572864
//
// k/m 按 1024 进制换算，贴合主流模型 2 的幂窗口（32768/65536/131072）。
// 返回 token 数，非法输入返回错误。
func ParseContextWindow(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("空值")
	}
	mult := 1
	switch last := s[len(s)-1]; last {
	case 'k', 'K':
		mult = 1024
		s = s[:len(s)-1]
	case 'm', 'M':
		mult = 1024 * 1024
		s = s[:len(s)-1]
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		return 0, fmt.Errorf("无效上下文窗口 %q（应为正整数或带 k/m 后缀，如 32768 / 32k / 128k）", s)
	}
	n := int(f * float64(mult))
	if n <= 0 {
		return 0, fmt.Errorf("上下文窗口过小: %q", s)
	}
	return n, nil
}

// ParseCompressAt 解析压缩触发阈值（0-1 比例）。
func ParseCompressAt(v string) (float64, error) {
	f, err := strconv.ParseFloat(v, 64)
	// ParseFloat("NaN") 不报错且 NaN 比较恒 false，需显式排除
	if err != nil || math.IsNaN(f) || f < 0 || f > 1 {
		return 0, fmt.Errorf("无效 compress_at: %s（应为 0-1 的比例，如 0.8）", v)
	}
	return f, nil
}

// ParseCompactKeep 解析压缩保留条数（正整数）。
func ParseCompactKeep(v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("无效 compact_keep: %s（应为正整数，如 10）", v)
	}
	return n, nil
}

// ParseMaxIterations 解析 ReAct 最大迭代数（正整数）。
func ParseMaxIterations(v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("无效 max_iterations: %s（应为正整数，如 15）", v)
	}
	return n, nil
}
