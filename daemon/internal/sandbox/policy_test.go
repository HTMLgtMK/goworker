package sandbox

import (
	"testing"

	"github.com/tinguo/goworker/daemon/internal/config"
)

// 决策矩阵：level × mode → Decision。
func TestPolicy_DecisionMatrix(t *testing.T) {
	mkCfg := func(mode string) *Config { return NewFromConfig(&config.SandboxConfig{Mode: mode}) }

	cases := []struct {
		level RiskLevel
		mode  string
		want  Decision
	}{
		// normal：R0/R1 allow，R2-R4/R6/R7 hitl，R5 deny
		{RiskR0, "normal", DecisionAllow},
		{RiskR1, "normal", DecisionAllow},
		{RiskR2, "normal", DecisionHitl},
		{RiskR3, "normal", DecisionHitl},
		{RiskR4, "normal", DecisionHitl},
		{RiskR5, "normal", DecisionDeny}, // denylist 恒硬拒
		{RiskR6, "normal", DecisionHitl}, // unknown → HITL（核心行为）
		{RiskR7, "normal", DecisionHitl},
		// strict：只放 R0/R1，其余硬拒
		{RiskR0, "strict", DecisionAllow},
		{RiskR1, "strict", DecisionAllow},
		{RiskR2, "strict", DecisionDeny},
		{RiskR5, "strict", DecisionDeny},
		{RiskR6, "strict", DecisionDeny},
		// readonly：只放 R0/R1，其余硬拒
		{RiskR1, "readonly", DecisionAllow},
		{RiskR2, "readonly", DecisionDeny},
		{RiskR4, "readonly", DecisionDeny},
		// off：全部放行
		{RiskR5, "off", DecisionAllow},
		{RiskR6, "off", DecisionAllow},
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
		{"code exec floor", RiskR1, []Effect{EffectCodeExecution}, RiskR4},
		{"privileged floor", RiskR1, []Effect{EffectPrivileged}, RiskR4},
		{"secret floor", RiskR1, []Effect{EffectSecretAccess}, RiskR4},
		{"destructive floor", RiskR1, []Effect{EffectDestructive}, RiskR3},
		{"file write floor", RiskR1, []Effect{EffectFileWrite}, RiskR2},
		{"network no floor", RiskR1, []Effect{EffectNetwork}, RiskR1}, // 网络访问不升级
		{"read no floor", RiskR1, []Effect{EffectFileRead}, RiskR1},
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
		return NewFromConfig(&config.SandboxConfig{
			Mode: mode,
			AllowRules: []config.AllowRuleConfig{
				{Match: "git push", MaxRisk: "R4", Effects: []string{"network", "file_write"}, Desc: "trusted push"},
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
		// pip install → R6；规则 max_risk R2 太低 → 不降级
		cfg := NewFromConfig(&config.SandboxConfig{
			Mode:       "normal",
			AllowRules: []config.AllowRuleConfig{{Match: "pip install", MaxRisk: "R2"}},
		})
		if out := Evaluate(CommandRequest{Command: "pip install foo"}, cfg); out.Decision != DecisionHitl {
			t.Errorf("pip install with low max_risk = %v, want hitl", out.Decision)
		}
	})
}

// Check shim 保持三态契约：迁移门禁。
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
	if err := Check("rm -rf /", NewFromConfig(&config.SandboxConfig{Mode: "off"})); err != nil {
		t.Errorf("off mode = %v, want nil", err)
	}
	if err := Check("rm -rf /", nil); err != nil {
		t.Errorf("nil cfg = %v, want nil", err)
	}
}
