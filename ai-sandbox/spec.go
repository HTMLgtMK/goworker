// Package sandbox 提供 shell 命令安全检查，不依赖 agent 或 engine，可被任何插件复用。
//
// 类型定义集中在本文件；实现(Assess/Decide/Evaluate/NewFromConfig 等)分布在对应实现文件。
package sandbox

import (
	"os"
	"regexp"
	"sync"
	"time"
)

// ---- 运行模式 ----

// Mode 沙箱运行模式。
type Mode string

const (
	ModeNormal   Mode = "normal"   // 默认：risky 命令需要 agent 向用户请求确认
	ModeStrict   Mode = "strict"   // 拒绝所有 risky 命令
	ModeReadOnly Mode = "readonly" // 只读模式，拒绝所有写操作
	ModeOff      Mode = "off"      // 关闭沙箱（不推荐）
)

// ---- 配置 ----

// Config 沙箱配置。
type Config struct {
	DeniedPatterns []*regexp.Regexp  // 拒绝执行的命令匹配
	RiskyPatterns  []*regexp.Regexp  // 需要用户确认的匹配
	PatternDescs   map[string]string // pattern → 人类可读描述（来自配置或内置默认）
	AllowedWorkDir string            // 限制工作目录，空 = 不限制
	ReadOnly       bool              // 只读模式
	MaxOutputBytes int               // 最大输出字节数，0=不限制
	Mode           Mode              // 沙箱模式
	SafeCommands   map[string]bool   // 追加的只读安全命令名 → 直接放行，不触发 HITL
	AllowRules     []AllowRule       // 用户预批准规则（normal 模式降级 hitl→allow）
}

// ---- YAML DTO ----
//
// 纯结构体、无自定义 UnmarshalYAML——risky_patterns 字符串/结构体双格式的兼容解析
// 由 daemon config 的解析层负责，转换后经 NewFromConfig 进入运行时 Config。

// SandboxConfig 是 sandbox 的 YAML 配置段。
type SandboxConfig struct {
	Mode           string              `yaml:"mode"`
	AllowedWorkDir string              `yaml:"allowed_work_dir"`      // 空 = 使用当前目录
	DeniedPatterns []string            `yaml:"denied_patterns"`       // 空 = 使用 sandbox 默认
	RiskyPatterns  []RiskPatternConfig `yaml:"risky_patterns"`        // 空 = 使用 sandbox 默认
	SafeCommands   []string            `yaml:"safe_commands"`         // 追加的只读安全命令名（直接放行）
	AllowRules     []AllowRuleConfig   `yaml:"allow_rules,omitempty"` // 用户预批准规则（normal 模式免确认）
	AuditLog       bool                `yaml:"audit_log,omitempty"`   // 决策审计 jsonl 落盘（默认关）
}

// RiskPatternConfig 表示一个风险命令模式及其人类可读描述。
type RiskPatternConfig struct {
	Pattern string `yaml:"pattern"`
	Desc    string `yaml:"desc"`
}

// AllowRuleConfig 是一条用户预批准规则：命令 token 前缀 + 风险上限 + 允许副作用子集。
type AllowRuleConfig struct {
	Match   string   `yaml:"match"`             // 归一化主命令 token 前缀："git push"
	MaxRisk string   `yaml:"max_risk"`          // "R0".."R7"，超过该等级的命令不放行
	Effects []string `yaml:"effects,omitempty"` // 允许的副作用子集，空 = 全部允许
	Desc    string   `yaml:"desc,omitempty"`    // 人类可读描述
}

// ---- 风险分级 ----

// RiskLevel 命令风险分级。
//
// R0-R5 是语义阶梯；R6 是"无法分类"——它不是一个安全等级，而是信任缺口，
// 默认进入 HITL（Unknown ≠ Safe）；R7 保留给未来模型分类输出，避免 schema 翻动。
type RiskLevel int

const (
	RiskR0 RiskLevel = iota // benign：只读、无副作用
	RiskR1                  // low：只读，可能跨网络/外部进程
	RiskR2                  // medium：工作区内写
	RiskR3                  // elevated：工作区内破坏/移动/权限
	RiskR4                  // high：提权/密钥/代码执行
	RiskR5                  // critical：denylist 硬拒，不进 HITL
	RiskR6                  // unclassified：无法分类的命令，默认不信任
	RiskR7                  // reserved：未来模型
)

// ---- 副作用 ----

// Effect 命令副作用维度，用位集组合（审计、floor 升级、AllowRule 子集校验）。
type Effect uint32

const (
	EffectFileRead      Effect = 1 << iota // 读文件
	EffectFileWrite                        // 写文件
	EffectNetwork                          // 网络访问
	EffectPrivileged                       // 提权/改变权限
	EffectDestructive                      // 破坏性（删除、格式化）
	EffectSecretAccess                     // 访问密钥/秘密
	EffectProcessSpawn                     // 生成进程
	EffectCodeExecution                    // 代码执行（eval/-c/子shell）
)

// Effects 是 Effect 的位集合。
type Effects uint32

// ---- 来源 ----

// Source 判定来源，供审计区分"规则 vs 分类器 vs 未分类"。
type Source int

const (
	SourceBypass       Source = iota // cfg nil / ModeOff
	SourceRule                       // denylist / risky pattern 命中
	SourceClassifier                 // 三态分类器 + flag 校验
	SourceHeuristic                  // 写操作扫描 / normalize 结构检测
	SourceUnclassified               // 什么都没命中
)

// ---- 评估输入/输出 ----

// Reason 风险评估的一条原因。Code 是稳定机器码（审计/训练数据依赖它），
// 不要用人类可读文本当 key。
type Reason struct {
	Code    string `json:"code"`
	Pattern string `json:"pattern,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// CommandRequest 一条待评估命令。Cwd/Workspace 为未来 containment 预留，MVP 仅用 Command。
type CommandRequest struct {
	Command   string
	Cwd       string
	Workspace string
}

// RiskAssessment 是 Assess 的输出：等级 + 副作用 + 依据。
// Confidence 恒为 1.0（规则命中）或 0（unclassified）——它只为未来模型留位，
// 策略层禁止据此裁决（宪法 III：模型/规则建议永不越过 Policy 授权）。
type RiskAssessment struct {
	Level      RiskLevel
	Effects    Effects
	Reasons    []Reason
	Source     Source
	Confidence float64
}

// ---- 决策 ----

// Decision 是策略层的最终决策。classification ≠ decision（宪法：授权只由 Policy 决定）。
type Decision int

const (
	DecisionAllow   Decision = iota // 放行
	DecisionSandbox                 // 保留枚举：MVP 不产出 containment，视为 allow
	DecisionHitl                    // 中断，请求用户确认
	DecisionDeny                    // 硬拒
)

// AllowRule 用户预批准规则：命令 token 前缀 + 风险上限 + 允许副作用子集。
// MatchTokens 用 token 前缀匹配（非正则），避免配置驱动正则注入。
type AllowRule struct {
	MatchTokens []string
	MaxRisk     RiskLevel
	Effects     Effects // 允许的副作用；0 = 全部
	Desc        string
}

// Outcome 是 Decide 的输出，兼作旧三态 error 契约的载体（Error() 还原）。
type Outcome struct {
	Decision Decision
	Level    RiskLevel
	Effects  Effects
	Reasons  []Reason
	Source   Source
	Command  string
	Override *AllowRule
}

// Policy 持有编译后的策略（当前为 AllowRules），Decide 是纯函数。
type Policy struct {
	allowRules []AllowRule
}

// ---- 审计 ----

// AuditEntry 一条命令决策审计记录。UserDecision 是未来训练数据的标签（宪法 V）。
type AuditEntry struct {
	Timestamp      time.Time `json:"timestamp"`
	Command        string    `json:"command"`
	Cwd            string    `json:"cwd,omitempty"`
	RiskLevel      string    `json:"risk_level"`
	Effects        []string  `json:"effects"`
	Reasons        []Reason  `json:"reasons"`
	Source         string    `json:"source"`
	EngineDecision string    `json:"engine_decision"`         // allow | hitl | deny
	UserDecision   string    `json:"user_decision,omitempty"` // approve | edit | reject | respond（未走 HITL 时空）
	Outcome        string    `json:"outcome"`                 // executed | blocked | aborted
}

// AuditLogger 追加式 jsonl 审计记录器。mutex 串行化写 + fsync，进程崩溃不丢已确认记录。
// 路径语义：OpenAudit(dir) 写 dir/audit.jsonl。
type AuditLogger struct {
	mu sync.Mutex
	f  *os.File
}

// ---- 只读三态分类 ----

// safeVerdict 只读安全命令的三态判定结果。
type safeVerdict int

const (
	verdictUnknown safeVerdict = iota
	verdictSafe
	verdictUnsafe
)

// ---- 错误 ----

// NeedsConfirmationError 由 Check 返回，表示命令匹配风险模式，调用方应请求用户确认。
// 这是 HITL (Human-in-the-Loop) 的触发信号，不是拒绝。
type NeedsConfirmationError struct {
	Command string // 触发检查的命令
	Pattern string // 匹配的正则模式
	Reason  string // 人类可读的描述
}
