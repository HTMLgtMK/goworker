// Package sandbox 提供 shell 命令安全检查，不依赖 agent 或 engine，可被任何插件复用。
package sandbox

import (
	"encoding/json"
	"fmt"
	"strings"
)

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

func (l RiskLevel) String() string {
	if l < RiskR0 || l > RiskR7 {
		return fmt.Sprintf("RiskLevel(%d)", int(l))
	}
	return "R" + string(rune('0'+int(l)))
}

// ParseRiskLevel 解析 "R0".."R7"，非法输入报错。
func ParseRiskLevel(s string) (RiskLevel, error) {
	if len(s) == 2 && s[0] == 'R' && s[1] >= '0' && s[1] <= '7' {
		return RiskLevel(int(s[1] - '0')), nil
	}
	return 0, fmt.Errorf("invalid risk level %q, want R0..R7", s)
}

// MarshalText 让 YAML/JSON 以 "R4" 呈现而非数字。
func (l RiskLevel) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *RiskLevel) UnmarshalText(b []byte) error {
	lv, err := ParseRiskLevel(string(b))
	if err != nil {
		return err
	}
	*l = lv
	return nil
}

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

func (e Effects) Has(f Effect) bool { return uint32(e)&uint32(f) != 0 }

// Add 返回新位集（不可变，符合项目风格）。
func (e Effects) Add(fs ...Effect) Effects {
	out := e
	for _, f := range fs {
		out = Effects(uint32(out) | uint32(f))
	}
	return out
}

// effectNames 顺序稳定：审计/HITL 展示依赖它。
var effectNames = []struct {
	f Effect
	n string
}{
	{EffectFileRead, "file_read"},
	{EffectFileWrite, "file_write"},
	{EffectNetwork, "network"},
	{EffectPrivileged, "privileged"},
	{EffectDestructive, "destructive"},
	{EffectSecretAccess, "secret_access"},
	{EffectProcessSpawn, "process_spawn"},
	{EffectCodeExecution, "code_execution"},
}

// Names 返回副作用的人类可读名列表，如 ["file_write","network"]。
func (e Effects) Names() []string {
	var out []string
	for _, en := range effectNames {
		if e.Has(en.f) {
			out = append(out, en.n)
		}
	}
	return out
}

// MarshalJSON 序列化为 Names() 列表（审计可读）。
func (e Effects) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Names())
}

// Source 判定来源，供审计区分"规则 vs 分类器 vs 未分类"。
type Source int

const (
	SourceBypass       Source = iota // cfg nil / ModeOff
	SourceRule                       // denylist / risky pattern 命中
	SourceClassifier                 // 三态分类器 + flag 校验
	SourceHeuristic                  // 写操作扫描 / normalize 结构检测
	SourceUnclassified               // 什么都没命中
)

func (s Source) String() string {
	switch s {
	case SourceBypass:
		return "bypass"
	case SourceRule:
		return "rule"
	case SourceClassifier:
		return "classifier"
	case SourceHeuristic:
		return "heuristic"
	case SourceUnclassified:
		return "unclassified"
	}
	return "unknown"
}

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

// maxLevel 返回两者中的较高风险。
func maxLevel(a, b RiskLevel) RiskLevel {
	if a > b {
		return a
	}
	return b
}

// joinDetail 提取首个 reason 的人类可读描述（供 HITL 展示/错误信息）。
func firstDetail(rs []Reason) string {
	for _, r := range rs {
		if r.Detail != "" {
			return r.Detail
		}
	}
	return ""
}

func firstPattern(rs []Reason) string {
	for _, r := range rs {
		if r.Pattern != "" {
			return r.Pattern
		}
	}
	return ""
}

// describeCommand 给审计用的命令摘要（截断长命令避免审计文件被撑爆）。
func describeCommand(cmd string) string {
	const max = 200
	cmd = strings.ReplaceAll(cmd, "\n", "\\n")
	if len(cmd) > max {
		return cmd[:max] + "…"
	}
	return cmd
}
