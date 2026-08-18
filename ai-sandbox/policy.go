package sandbox

import (
	"fmt"
	"path/filepath"
)

func (d Decision) String() string {
	switch d {
	case DecisionAllow:
		return "allow"
	case DecisionSandbox:
		return "sandbox"
	case DecisionHitl:
		return "hitl"
	case DecisionDeny:
		return "deny"
	}
	return "unknown"
}

// Error 把决策映射回旧三态契约：
//
//	allow/sandbox → nil；hitl → *NeedsConfirmationError；deny → 其他 error。
//
// 这是 Check shim 的兼容底座，errors.As(&NeedsConfirmationError) 零改动通过。
func (o Outcome) Error() error {
	switch o.Decision {
	case DecisionAllow, DecisionSandbox:
		return nil
	case DecisionHitl:
		return &NeedsConfirmationError{
			Command: describeCommand(o.Command),
			Pattern: firstPattern(o.Reasons),
			Reason:  firstDetail(o.Reasons),
		}
	default:
		return fmt.Errorf("sandbox: command denied: %s", firstDetail(o.Reasons))
	}
}

// NewPolicy 从 sandbox.Config 构建策略。
func NewPolicy(cfg *Config) *Policy {
	if cfg == nil {
		return &Policy{}
	}
	return &Policy{allowRules: cfg.AllowRules}
}

// Decide 依据风险评估 + 运行模式输出决策。纯函数，无 I/O。
// 流程：effects floor 抬升等级 → 决策矩阵 →（normal 模式 AllowRule 降级）。
func (p *Policy) Decide(req CommandRequest, cfg *Config, ra RiskAssessment) Outcome {
	out := Outcome{
		Command:  req.Command,
		Level:    ra.Level,
		Effects:  ra.Effects,
		Reasons:  ra.Reasons,
		Source:   ra.Source,
		Override: nil,
	}
	if cfg == nil || cfg.Mode == ModeOff {
		out.Decision = DecisionAllow
		out.Level = RiskR0
		return out
	}

	// effects floor：副作用把等级抬到语义下限之上。
	effLv := ra.Level
	if f := floorByEffects(ra.Effects); f > effLv {
		effLv = f
	}
	out.Level = effLv
	out.Decision = decisionFor(effLv, cfg.Mode)

	// AllowRule 降级：仅 normal 模式、风险/副作用在规则范围内时 hitl→allow。
	if out.Decision == DecisionHitl && cfg.Mode == ModeNormal {
		if rule := p.matchAllowRule(req, ra, effLv); rule != nil {
			out.Decision = DecisionAllow
			out.Override = rule
		}
	}
	return out
}

// decisionFor 是风险等级 × 运行模式的决策矩阵。
func decisionFor(level RiskLevel, mode Mode) Decision {
	switch mode {
	case ModeStrict, ModeReadOnly:
		// strict 拒全部风险，readonly 拒全部写——R0/R1 之外一律硬拒。
		if level <= RiskR1 {
			return DecisionAllow
		}
		return DecisionDeny
	default: // ModeNormal
		switch {
		case level <= RiskR1:
			return DecisionAllow
		case level == RiskR5:
			return DecisionDeny // denylist 恒硬拒，不进 HITL
		default:
			return DecisionHitl // R2-R4、R6、R7
		}
	}
}

// floorByEffects 把副作用映射到风险等级下限。
func floorByEffects(e Effects) RiskLevel {
	switch {
	case e.Has(EffectCodeExecution) || e.Has(EffectPrivileged) || e.Has(EffectSecretAccess):
		return RiskR4
	case e.Has(EffectDestructive):
		return RiskR3
	case e.Has(EffectFileWrite) || e.Has(EffectProcessSpawn):
		return RiskR2
	}
	return RiskR0
}

// matchAllowRule 按命令 token 前缀 + 风险上限 + effects 子集匹配用户预批准规则。
func (p *Policy) matchAllowRule(req CommandRequest, ra RiskAssessment, effLv RiskLevel) *AllowRule {
	if len(p.allowRules) == 0 {
		return nil
	}
	toks := mainTokens(req.Command)
	if len(toks) == 0 {
		return nil
	}
	for i := range p.allowRules {
		rule := &p.allowRules[i]
		if !tokensMatch(toks, rule.MatchTokens) {
			continue
		}
		if effLv > rule.MaxRisk {
			continue
		}
		if rule.Effects != 0 && (ra.Effects & ^rule.Effects) != 0 {
			continue // 副作用越出规则允许子集
		}
		return rule
	}
	return nil
}

// mainTokens 提取命令主段的 token 前缀（跳过 VAR=x 赋值；env 包装器解析真实命令）。
func mainTokens(cmd string) []string {
	segs := splitShellSegments(cmd)
	if len(segs) == 0 {
		return nil
	}
	name, args := splitMain(segs[0])
	if name == "" {
		return nil
	}
	base := filepath.Base(name)
	if base == "env" {
		realName, realArgs, ok := envResolve(args)
		if !ok {
			return append([]string{base}, args...)
		}
		return append([]string{filepath.Base(realName)}, realArgs...)
	}
	return append([]string{base}, args...)
}

// tokensMatch 判断 cmdTokens 是否以 matchTokens 为前缀（token 精确匹配，非子串）。
func tokensMatch(cmdTokens, matchTokens []string) bool {
	if len(matchTokens) > len(cmdTokens) {
		return false
	}
	for i, m := range matchTokens {
		if cmdTokens[i] != m {
			return false
		}
	}
	return true
}

// Evaluate 是组合入口：Assess → Policy.Decide。
func Evaluate(req CommandRequest, cfg *Config) Outcome {
	if cfg == nil || cfg.Mode == ModeOff {
		return Outcome{Decision: DecisionAllow, Level: RiskR0, Source: SourceBypass, Command: req.Command}
	}
	ra := Assess(req.Command, cfg)
	return NewPolicy(cfg).Decide(req, cfg, ra)
}
