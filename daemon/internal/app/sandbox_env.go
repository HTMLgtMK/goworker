package app

import (
	"fmt"
	"log/slog"
	"os"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

// applySandboxModeOverride 用 GOWORKER_SANDBOX_MODE 收紧无人值守入口的沙箱门禁：
// dispatcher 侧可经 worker 的 env 配置下发（如 GOWORKER_SANDBOX_MODE=strict），
// Android 侧可在启动前设置该 env。空值不动配置；非法值返回错误 —— 拒绝启动
// 优于静默放行。New（mobile/CLI 壳）与 RunACPWorker 共用，取值集合统一。
func applySandboxModeOverride(cfg *runtimeconfig.Config) error {
	mode := os.Getenv("GOWORKER_SANDBOX_MODE")
	if mode == "" {
		return nil
	}
	switch mode {
	case "normal", "auto", "strict", "readonly", "off":
		cfg.Sandbox.Mode = mode
	default:
		return fmt.Errorf("invalid GOWORKER_SANDBOX_MODE %q (normal|auto|strict|readonly|off)", mode)
	}
	slog.Info("sandbox mode overridden via environment", "mode", mode)
	return nil
}
