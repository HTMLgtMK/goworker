package config

import (
	"strings"
	"testing"
)

func TestSetFieldWeb(t *testing.T) {
	tests := []struct {
		key    string
		val    string
		check  func(*Config) bool
		errMsg string // 空 = 期望成功
	}{
		{
			key: "frontend.web.entry", val: "/opt/gw/gateway.js",
			check: func(c *Config) bool { return c.Frontend.Web.Entry == "/opt/gw/gateway.js" },
		},
		{
			key: "frontend.web.port", val: "5177",
			check: func(c *Config) bool { return c.Frontend.Web.Port == 5177 },
		},
		{
			key: "frontend.web.port", val: "70000",
			errMsg: "1-65535",
		},
		{
			key: "frontend.web.port", val: "abc",
			errMsg: "无效 frontend.web.port",
		},
		{
			key: "frontend.web.host", val: "0.0.0.0",
			check: func(c *Config) bool { return c.Frontend.Web.Host == "0.0.0.0" },
		},
		{
			key: "frontend.web.workspace", val: "/Users/x/Work/repo",
			check: func(c *Config) bool { return c.Frontend.Web.Workspace == "/Users/x/Work/repo" },
		},
		{
			key:    "frontend.web.unknown",
			val:    "x",
			errMsg: "未知配置项",
		},
	}

	for _, tt := range tests {
		cfg := Default()
		err := cfg.SetField(tt.key, tt.val)
		if tt.errMsg != "" {
			if err == nil {
				t.Errorf("SetField(%s, %s) 应报错（含 %q），得到 nil", tt.key, tt.val, tt.errMsg)
			} else if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("SetField(%s, %s) 错误信息 %q 应含 %q", tt.key, tt.val, err.Error(), tt.errMsg)
			}
			continue
		}
		if err != nil {
			t.Fatalf("SetField(%s, %s): %v", tt.key, tt.val, err)
		}
		if !tt.check(cfg) {
			t.Errorf("SetField(%s, %s) 后字段值不符合预期: %+v", tt.key, tt.val, cfg.Frontend.Web)
		}
	}
}
