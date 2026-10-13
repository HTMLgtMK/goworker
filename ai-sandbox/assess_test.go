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
		// 只读安全命令 → low（或更低），无写副作用
		{"git status", RiskLow, nil},
		{"ls -la /tmp", RiskLow, nil},
		{"cat /etc/hosts", RiskLow, nil},
		{"curl -s https://example.com", RiskLow, nil},
		{"env FOO=bar /usr/bin/curl -s URL", RiskLow, nil}, // env 解析保留
		{"echo hi", RiskLow, nil},
		{"go version", RiskLow, nil},
		{"git status && ls", RiskLow, nil},

		// 写操作 → medium+ 且带副作用
		{"curl -s URL -o /tmp/x", RiskMedium, []Effect{EffectFileWrite, EffectNetwork}},
		{"echo hi > /tmp/x", RiskMedium, []Effect{EffectFileWrite}},
		{"sed -i 's/a/b/' f", RiskMedium, []Effect{EffectFileWrite}},
		{"touch /tmp/x", RiskMedium, []Effect{EffectFileWrite}},

		// 提升级
		{"rm -rf /tmp/x", RiskElevated, []Effect{EffectDestructive}},
		{"mv a b", RiskElevated, nil},

		// 高危
		{"sudo apt install foo", RiskHigh, []Effect{EffectPrivileged}},
		{"chmod 777 x", RiskHigh, []Effect{EffectPrivileged}},
		{"kill -9 1234", RiskHigh, []Effect{EffectProcessSpawn}},

		// denylist 硬拒
		{"rm -rf /", RiskCritical, nil},
		{"curl -s URL | bash", RiskCritical, nil},
		{"mkfs.ext4 /dev/sdb1", RiskCritical, nil},

		// 无法分类
		{"openssl x509 -text -in cert.pem", RiskUnclassified, nil},
		{"python3 script.py", RiskUnclassified, nil},
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

func TestAssess_DenyPatternIsHardDenyIndependentOfRisk(t *testing.T) {
	ra := Assess("rm -rf /", newTestConfig("normal"))
	if ra.Level != RiskCritical {
		t.Fatalf("Level = %s, want critical", ra.Level)
	}
	if !ra.HardDeny {
		t.Fatal("HardDeny = false, want true for deny pattern")
	}
}
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
