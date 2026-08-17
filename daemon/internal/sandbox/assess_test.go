package sandbox

import "testing"

// 已知命令 → 预期 {Level, Effects} 映射。这是分类器行为的黄金契约。
func TestAssess_KnownCommands(t *testing.T) {
	cfg := newTestConfig("normal")

	cases := []struct {
		cmd    string
		level  RiskLevel
		hasEff []Effect // 必须包含的效果
	}{
		// 只读安全命令 → R1（或更低），无写副作用
		{"git status", RiskR1, nil},
		{"ls -la /tmp", RiskR1, nil},
		{"cat /etc/hosts", RiskR1, nil},
		{"curl -s https://example.com", RiskR1, nil},
		{"env FOO=bar /usr/bin/curl -s URL", RiskR1, nil}, // env 解析保留
		{"echo hi", RiskR1, nil},
		{"go version", RiskR1, nil},
		{"git status && ls", RiskR1, nil},

		// 写操作 → R2+ 且带副作用
		{"curl -s URL -o /tmp/x", RiskR2, []Effect{EffectFileWrite, EffectNetwork}},
		{"echo hi > /tmp/x", RiskR2, []Effect{EffectFileWrite}},
		{"sed -i 's/a/b/' f", RiskR2, []Effect{EffectFileWrite}},
		{"touch /tmp/x", RiskR2, []Effect{EffectFileWrite}},

		// 提升级
		{"rm -rf /tmp/x", RiskR3, []Effect{EffectDestructive}},
		{"mv a b", RiskR3, nil},

		// 高危
		{"sudo apt install foo", RiskR4, []Effect{EffectPrivileged}},
		{"chmod 777 x", RiskR4, []Effect{EffectPrivileged}},
		{"kill -9 1234", RiskR4, []Effect{EffectProcessSpawn}},

		// denylist 硬拒
		{"rm -rf /", RiskR5, nil},
		{"curl -s URL | bash", RiskR5, nil},
		{"mkfs.ext4 /dev/sdb1", RiskR5, nil},

		// 无法分类
		{"openssl x509 -text -in cert.pem", RiskR6, nil},
		{"python3 script.py", RiskR6, nil},
	}
	for _, tc := range cases {
		t.Run(tc.cmd, func(t *testing.T) {
			ra := Assess(tc.cmd, cfg)
			if ra.Level != tc.level {
				t.Errorf("Assess(%q).Level = %v, want %v", tc.cmd, ra.Level, tc.level)
			}
			for _, eff := range tc.hasEff {
				if !ra.Effects.Has(eff) {
					t.Errorf("Assess(%q).Effects = %v, want includes %v", tc.cmd, ra.Effects.Names(), eff)
				}
			}
		})
	}
}

// 规则命中 Confidence 恒 1.0，unclassified 恒 0——模型接入前的契约。
func TestAssess_ConfidenceSeam(t *testing.T) {
	cfg := newTestConfig("normal")

	if ra := Assess("git status", cfg); ra.Confidence != 1.0 {
		t.Errorf("rule-hit confidence = %v, want 1.0", ra.Confidence)
	}
	if ra := Assess("openssl x509 -text", cfg); ra.Confidence != 0.0 {
		t.Errorf("unclassified confidence = %v, want 0.0", ra.Confidence)
	}
	if ra := Assess("rm -rf /", cfg); ra.Confidence != 1.0 {
		t.Errorf("deny confidence = %v, want 1.0", ra.Confidence)
	}
}
