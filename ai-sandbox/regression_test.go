package sandbox

import (
	"testing"
)

// 回归测试：review 挖出的 5 个安全洞，修好后必须保持关闭。

// R1: env 不应成为万能包装器 —— env 包住写命令/写参数必须仍被拦截。
func TestRegression_EnvNoBypass(t *testing.T) {
	cfg := newTestConfig("normal")
	confirm := []string{
		`env rm -rf /tmp/data`,
		`env FOO=bar curl -s URL -o /tmp/out.html`,
		`env curl -s URL -d 'a=b'`,
		`env -i curl -s URL -X POST`,
		`env -u HOME curl -s URL -o /tmp/out.html`,
		`env -- curl -s URL -d 'a=b'`,
	}
	for _, cmd := range confirm {
		if err := Check(cmd, cfg); !isNeedsConf(err) {
			t.Errorf("Check(%q) = %v, want NeedsConfirmationError", cmd, err)
		}
	}
	// env 包只读命令应放行
	allow := []string{
		`env FOO=bar curl -s URL`,
		`env curl -s URL`,
	}
	for _, cmd := range allow {
		if err := Check(cmd, cfg); err != nil {
			t.Errorf("Check(%q) = %v, want nil", cmd, err)
		}
	}
}

// R2: /dev/ 豁免只能作用于内置 `>\s*/dev/` 规则，自定义含 /dev/ 的 deny 必须仍生效。
func TestRegression_DevExemptionScoped(t *testing.T) {
	cfg := NewFromConfig(&SandboxConfig{
		Mode:           "normal",
		DeniedPatterns: []string{`mount\s+/dev/`, `>\s*/dev/`},
	})
	// 自定义 deny `mount /dev/` 带 /dev/null 重定向，仍应拒绝
	if err := Check(`mount /dev/sda1 /mnt >/dev/null 2>&1`, cfg); err == nil {
		t.Errorf("custom deny mount /dev/ = nil, want deny")
	}
	// 内置 `>\s*/dev/` 对伪设备仍豁免
	if err := Check(`echo hi > /dev/null`, cfg); err != nil {
		t.Errorf("builtin > /dev/null = %v, want nil", err)
	}
}

// R3: curl 长格式 --request 写方法必须被识别。
func TestRegression_CurlRequestLongForm(t *testing.T) {
	cfg := newTestConfig("normal")
	confirm := []string{
		`curl -s --request POST https://example.com`,
		`curl -s --request=DELETE https://example.com`,
		`curl -s --request PUT -H 'Content-Type: application/json' https://example.com`,
	}
	for _, cmd := range confirm {
		if err := Check(cmd, cfg); !isNeedsConf(err) {
			t.Errorf("Check(%q) = %v, want NeedsConfirmationError", cmd, err)
		}
	}
	allow := []string{
		`curl -s --request GET https://example.com`,
	}
	for _, cmd := range allow {
		if err := Check(cmd, cfg); err != nil {
			t.Errorf("Check(%q) = %v, want nil", cmd, err)
		}
	}
}

// R4: readonly 模式下写操作必须硬拒，不允许 HITL 批准。
func TestRegression_ReadonlyHardDeny(t *testing.T) {
	cfg := newTestConfig("readonly")
	deny := []string{
		`cat a > /tmp/b`,
		`echo hi > /tmp/x`,
		`curl -s URL -o /tmp/out.html`,
	}
	for _, cmd := range deny {
		err := Check(cmd, cfg)
		if err == nil {
			t.Errorf("readonly Check(%q) = nil, want deny", cmd)
		}
		if isNeedsConf(err) {
			t.Errorf("readonly Check(%q) = NeedsConfirmationError, want hard deny", cmd)
		}
	}
}

// R5: safe_commands 支持 "kubectl get"（命令+子命令）形式。
func TestRegression_SafeCommandsSubcommand(t *testing.T) {
	cfg := NewFromConfig(&SandboxConfig{
		Mode:         "normal",
		SafeCommands: []string{"kubectl get"},
	})
	// 文档示例形式应生效：kubectl get 放行
	if err := Check(`kubectl get pods | grep x`, cfg); err != nil {
		t.Errorf("safe 'kubectl get pods | grep x' = %v, want nil", err)
	}
	// 非声明子命令仍应确认（kubectl delete 不在白名单）
	if err := Check(`kubectl delete pod x`, cfg); !isNeedsConf(err) {
		t.Errorf("kubectl delete = %v, want NeedsConfirmationError", err)
	}
}
