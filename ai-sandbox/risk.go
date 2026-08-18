package sandbox

import (
	"encoding/json"
	"fmt"
	"strings"
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

// maxLevel 返回两者中的较高风险。
func maxLevel(a, b RiskLevel) RiskLevel {
	if a > b {
		return a
	}
	return b
}

// firstDetail 提取首个 reason 的人类可读描述（供 HITL 展示/错误信息）。
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
