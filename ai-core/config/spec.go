// Package config 定义 agent 引擎运行时需要的配置段。
//
// 只含引擎 + memory middleware 真需要的段（LLM + Memory）；
// Session/MCP/Sandbox 属装配层，由 ai-runtime 聚合。YAML 解析由宿主负责，
// 本包类型带 yaml tag 仅供宿主内联时使用。
package config

// LLMConfig 是 LLM 提供商连接配置。
type LLMConfig struct {
	Endpoint      string  `yaml:"endpoint"`
	Model         string  `yaml:"model"`
	APIKey        string  `yaml:"api_key"`
	ContextWindow int     `yaml:"context_window"` // 模型上下文窗口（token），0 = 未知
	CompressAt    float64 `yaml:"compress_at"`    // 历史压缩触发阈值（0-1）：估算用量达窗口该比例时自动压缩，0 = 关闭
	CompactKeep   int     `yaml:"compact_keep"`   // 滚动压缩保留的最近消息条数（原文不压，只压更早的）
	MaxIterations int     `yaml:"max_iterations"` // ReAct 循环最大迭代数（模型往返次数），0 = 默认 15
}

// MemoryConfig 是 agent 记忆模块（MTM 任务档案 + LTM 事实条目）的配置。
// 声明式指令层（USER.md + AGENTS.md）与记忆组件同开关：Enabled=false 时两者都关。
type MemoryConfig struct {
	Dir               string  `yaml:"dir"`                 // 存储目录，默认 <DefaultDir>/memory
	Enabled           bool    `yaml:"enabled"`             // false = 整个记忆模块关闭
	TaskKeep          int     `yaml:"task_keep"`           // 保留任务档案数，0 = 不裁剪
	TaskInjectN       int     `yaml:"task_inject_n"`       // 会话边界时注入最近 N 个未完成任务
	LtmInjectTopK     int     `yaml:"ltm_inject_top_k"`    // 会话边界时注入相关事实条数
	LtmExtract        bool    `yaml:"ltm_extract"`         // 检查点固化时是否 LLM 抽取 LTM
	InjectBudgetRatio float64 `yaml:"inject_budget_ratio"` // 注入块占 context 窗口的比例上限（0-1）
	UserMaxChars      int     `yaml:"user_max_chars"`      // USER.md 画像容量上限（rune），超限 profile 工具报错
	AgentsMaxChars    int     `yaml:"agents_max_chars"`    // AGENTS.md（全局+项目合并）注入上限，超出截断
}
