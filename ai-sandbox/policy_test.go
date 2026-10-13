package sandbox

import (
	"testing"
)

func TestNewFromConfig_InvalidAutoMaxRiskDefaultsToConservativeThreshold(t *testing.T) {
	cfg := NewFromConfig(&SandboxConfig{Mode: "auto", AutoMaxRisk: "unknown-level"})
	if cfg.AutoMaxRisk != RiskLow {
		t.Fatalf("AutoMaxRisk = %s, want %s", cfg.AutoMaxRisk, RiskLow)
	}
}

// 决策矩阵：level × mode → Decision。
func TestPolicy_DecisionMatrix(t *testing.T) {
	mkCfg := func(mode string) *Config { return NewFromConfig(&SandboxConfig{Mode: mode}) }

	cases := []struct {
		level RiskLevel
		mode  string
		want  Decision
	}{
		// normal：benign/low allow，medium-reserved hitl，critical 也允许 HITL
		{RiskBenign, "normal", DecisionAllow},
		{RiskLow, "normal", DecisionAllow},
		{RiskMedium, "normal", DecisionHitl},
		{RiskElevated, "normal", DecisionHitl},
		{RiskHigh, "normal", DecisionHitl},
		{RiskCritical, "normal", DecisionHitl},     // critical 进入 HITL，hard deny 另测
		{RiskUnclassified, "normal", DecisionHitl}, // unknown → HITL（核心行为）
		{RiskReserved, "normal", DecisionHitl},
		// auto：只有不超过阈值的风险自动放行，其余进入 HITL
		{RiskBenign, "auto", DecisionAllow},
		{RiskLow, "auto", DecisionAllow},
		{RiskMedium, "auto", DecisionHitl},
		{RiskHigh, "auto", DecisionHitl},
		{RiskCritical, "auto", DecisionHitl},
		{RiskUnclassified, "auto", DecisionHitl},
		// strict：只放 benign/low，其余硬拒
		{RiskBenign, "strict", DecisionAllow},
		{RiskLow, "strict", DecisionAllow},
		{RiskMedium, "strict", DecisionDeny},
		{RiskCritical, "strict", DecisionDeny},
		{RiskUnclassified, "strict", DecisionDeny},
		// readonly：只放 benign/low，其余硬拒
		{RiskLow, "readonly", DecisionAllow},
		{RiskMedium, "readonly", DecisionDeny},
		{RiskHigh, "readonly", DecisionDeny},
		// off：全部放行
		{RiskCritical, "off", DecisionAllow},
		{RiskUnclassified, "off", DecisionAllow},
	}
	for _, tc := range cases {
		t.Run(tc.mode+"/"+tc.level.String(), func(t *testing.T) {
			p := NewPolicy(mkCfg(tc.mode))
			ra := RiskAssessment{Level: tc.level}
			got := p.Decide(CommandRequest{Command: "x"}, mkCfg(tc.mode), ra).Decision
			if got != tc.want {
				t.Errorf("Decide(level=%s, mode=%s) = %v, want %v", tc.level, tc.mode, got, tc.want)
			}
		})
	}
}

func TestPolicy_HardDenyIsIndependentOfRiskLevel(t *testing.T) {
	cfg := newTestConfig("normal")
	p := NewPolicy(cfg)

	critical := p.Decide(CommandRequest{Command: "critical"}, cfg, RiskAssessment{Level: RiskCritical})
	if critical.Decision != DecisionHitl {
		t.Fatalf("ordinary critical decision = %v, want hitl", critical.Decision)
	}

	blocked := p.Decide(CommandRequest{Command: "blocked"}, cfg, RiskAssessment{Level: RiskCritical, HardDeny: true})
	if blocked.Decision != DecisionDeny {
		t.Fatalf("hard-deny critical decision = %v, want deny", blocked.Decision)
	}
}

func TestPolicy_AutoMaxRisk(t *testing.T) {
	cfg := NewFromConfig(&SandboxConfig{Mode: "auto", AutoMaxRisk: "high"})
	policy := NewPolicy(cfg)

	if got := policy.Decide(CommandRequest{}, cfg, RiskAssessment{Level: RiskHigh}).Decision; got != DecisionAllow {
		t.Errorf("high with auto_max_risk=high = %v, want allow", got)
	}
	if got := policy.Decide(CommandRequest{}, cfg, RiskAssessment{Level: RiskCritical}).Decision; got != DecisionHitl {
		t.Errorf("critical with auto_max_risk=high = %v, want hitl", got)
	}
}

// Effects floor：副作用把等级抬到下限之上。
func TestPolicy_EffectFloors(t *testing.T) {
	cfg := newTestConfig("normal")
	p := NewPolicy(cfg)

	cases := []struct {
		name   string
		level  RiskLevel
		effs   []Effect
		wantLv RiskLevel
	}{
		{"code exec floor", RiskLow, []Effect{EffectCodeExecution}, RiskHigh},
		{"privileged floor", RiskLow, []Effect{EffectPrivileged}, RiskHigh},
		{"secret floor", RiskLow, []Effect{EffectSecretAccess}, RiskHigh},
		{"destructive floor", RiskLow, []Effect{EffectDestructive}, RiskElevated},
		{"file write floor", RiskLow, []Effect{EffectFileWrite}, RiskMedium},
		{"network no floor", RiskLow, []Effect{EffectNetwork}, RiskLow}, // 网络访问不升级
		{"read no floor", RiskLow, []Effect{EffectFileRead}, RiskLow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := Effects(0)
			for _, f := range tc.effs {
				e = e.Add(f)
			}
			ra := RiskAssessment{Level: tc.level, Effects: e}
			out := p.Decide(CommandRequest{}, cfg, ra)
			if out.Level != tc.wantLv {
				t.Errorf("effective level = %s, want %s", out.Level, tc.wantLv)
			}
		})
	}
}

// 端到端：Evaluate 组合 Assess + Decide。
func TestPolicy_EvaluateEndToEnd(t *testing.T) {
	normal := newTestConfig("normal")
	strict := newTestConfig("strict")
	readonly := newTestConfig("readonly")

	// 只读命令在三个模式都放行
	for _, cmd := range []string{"git status", "ls", "curl -s URL"} {
		if out := Evaluate(CommandRequest{Command: cmd}, normal); out.Decision != DecisionAllow {
			t.Errorf("normal %q = %v, want allow", cmd, out.Decision)
		}
	}
	// unknown 在 normal 变 hitl（核心行为变化）
	if out := Evaluate(CommandRequest{Command: "pip install foo"}, normal); out.Decision != DecisionHitl {
		t.Errorf("unknown normal = %v, want hitl", out.Decision)
	}
	// denylist 恒 deny
	if out := Evaluate(CommandRequest{Command: "rm -rf /"}, normal); out.Decision != DecisionDeny {
		t.Errorf("rm -rf / = %v, want deny", out.Decision)
	}
	// readonly 写操作硬拒
	if out := Evaluate(CommandRequest{Command: "cat a > /tmp/b"}, readonly); out.Decision != DecisionDeny {
		t.Errorf("readonly write = %v, want deny", out.Decision)
	}
	// strict 拒绝中风险
	if out := Evaluate(CommandRequest{Command: "rm -rf /tmp/x"}, strict); out.Decision != DecisionDeny {
		t.Errorf("strict rm = %v, want deny", out.Decision)
	}
}

// AllowRule override：normal 模式命中且风险/副作用在范围内 → hitl 降级 allow。
func TestPolicy_AllowRuleOverride(t *testing.T) {
	mkCfg := func(mode string) *Config {
		return NewFromConfig(&SandboxConfig{
			Mode: mode,
			AllowRules: []AllowRuleConfig{
				{Match: "git push", MaxRisk: "high", Effects: []string{"network", "file_write"}, Desc: "trusted push"},
			},
		})
	}

	t.Run("git push within rule allows", func(t *testing.T) {
		cfg := mkCfg("normal")
		out := Evaluate(CommandRequest{Command: "git push origin main"}, cfg)
		if out.Decision != DecisionAllow {
			t.Errorf("git push = %v, want allow (rule hit)", out.Decision)
		}
		if out.Override == nil {
			t.Error("Override should be set on allow-by-rule")
		}
	})

	t.Run("git fetch not matched by git push rule", func(t *testing.T) {
		// 规则 "git push" 不命中 git fetch；git fetch 是网络写 → 常规 hitl
		cfg := mkCfg("normal")
		if out := Evaluate(CommandRequest{Command: "git fetch"}, cfg); out.Decision != DecisionHitl {
			t.Errorf("git fetch = %v, want hitl (rule not matched, network write)", out.Decision)
		}
	})

	t.Run("git push --force exceeds effects subset", func(t *testing.T) {
		// destructive 不在 rule.Effects 允许子集 → 规则不放过 → 仍 hitl
		cfg := mkCfg("normal")
		out := Evaluate(CommandRequest{Command: "git push --force origin main"}, cfg)
		if out.Decision != DecisionHitl {
			t.Errorf("git push --force = %v, want hitl (destructive outside effects)", out.Decision)
		}
	})

	t.Run("strict not overridden by rule", func(t *testing.T) {
		cfg := mkCfg("strict")
		if out := Evaluate(CommandRequest{Command: "git push origin main"}, cfg); out.Decision != DecisionDeny {
			t.Errorf("strict git push = %v, want deny (rule must not override mode)", out.Decision)
		}
	})

	t.Run("readonly not overridden by rule", func(t *testing.T) {
		cfg := mkCfg("readonly")
		if out := Evaluate(CommandRequest{Command: "git push origin main"}, cfg); out.Decision != DecisionDeny {
			t.Errorf("readonly git push = %v, want deny", out.Decision)
		}
	})

	t.Run("rule below risk floor not applied", func(t *testing.T) {
		// pip install → unclassified；规则 max_risk medium 太低 → 不降级
		cfg := NewFromConfig(&SandboxConfig{
			Mode:       "normal",
			AllowRules: []AllowRuleConfig{{Match: "pip install", MaxRisk: "medium"}},
		})
		if out := Evaluate(CommandRequest{Command: "pip install foo"}, cfg); out.Decision != DecisionHitl {
			t.Errorf("pip install with low max_risk = %v, want hitl", out.Decision)
		}
	})
}

func TestAllowRuleForCommand_NormalizesMainTokens(t *testing.T) {
	rule, ok := AllowRuleForCommand("env FOO=bar git push origin main", RiskHigh, []string{"network"})
	if !ok {
		t.Fatal("AllowRuleForCommand returned ok=false")
	}
	want := []string{"git", "push", "origin", "main"}
	if len(rule.MatchTokens) != len(want) {
		t.Fatalf("MatchTokens = %v, want %v", rule.MatchTokens, want)
	}
	for i := range want {
		if rule.MatchTokens[i] != want[i] {
			t.Errorf("MatchTokens[%d] = %q, want %q", i, rule.MatchTokens[i], want[i])
		}
	}
	if rule.MaxRisk != RiskHigh || !rule.Effects.Has(EffectNetwork) {
		t.Errorf("rule = %+v, want high with network effect", rule)
	}
}

func TestPolicy_CheckShimContract(t *testing.T) {
	cfg := newTestConfig("normal")

	// allow → nil
	if err := Check("git status", cfg); err != nil {
		t.Errorf("Check(git status) = %v, want nil", err)
	}
	// hitl → NeedsConfirmationError
	if err := Check("pip install foo", cfg); !isNeedsConf(err) {
		t.Errorf("Check(pip install) = %v, want NeedsConfirmationError", err)
	}
	// deny → 硬 error（非 NeedsConfirmationError）
	if err := Check("rm -rf /", cfg); err == nil || isNeedsConf(err) {
		t.Errorf("Check(rm -rf /) = %v, want hard deny", err)
	}
	// off / nil cfg → nil
	if err := Check("rm -rf /", NewFromConfig(&SandboxConfig{Mode: "off"})); err != nil {
		t.Errorf("off mode = %v, want nil", err)
	}
	if err := Check("rm -rf /", nil); err != nil {
		t.Errorf("nil cfg = %v, want nil", err)
	}
}
