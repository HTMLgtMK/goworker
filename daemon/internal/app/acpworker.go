package app

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/service"
)

// RunACPWorker 以 stdio ACP Agent 模式服务 ZCode 会话：配置解析、HTTP 客户端、
// provider 装配与 agent 插件共用同一套（ProviderFactory），stdin EOF 即退出。
//
// 刻意不走 New()：worker 是 dispatcher spawn 的无人值守单任务进程，不装
// Engine/插件/内置命令，也没有前端 —— 只共享配置/日志装配与 ProviderFactory。
func RunACPWorker() error {
	cfg, log, err := loadConfigAndLogger()
	if err != nil {
		return err
	}
	defer log.Close()

	// 沙箱下发：dispatcher 侧可经 worker 的 env 配置收紧无人值守 worker 的命令门禁，
	// 如 env: ["GOWORKER_SANDBOX_MODE=strict"]。取值与 sandbox 模式一致，非法值拒绝启动。
	runtimeCfg := cfg.ToRuntime()
	if mode := os.Getenv("GOWORKER_SANDBOX_MODE"); mode != "" {
		switch mode {
		case "normal", "strict", "readonly", "off":
			runtimeCfg.Sandbox.Mode = mode
		default:
			return fmt.Errorf("invalid GOWORKER_SANDBOX_MODE %q (normal|strict|readonly|off)", mode)
		}
		slog.Info("acp worker: sandbox mode overridden", "mode", mode)
	}

	server := service.ServeACPWorker(service.Stdio(), service.ACPWorkerDeps{
		Config:   runtimeCfg,
		AuditDir: filepath.Join(config.DefaultDir(), "audit"),
	})
	// 阻塞到对端关闭输入（dispatcher spawn 的子进程随任务结束被回收）
	<-server.Done()
	// 对端已关闭，Close 的错误无从上报也不再影响结果 —— 与原实现一致地忽略
	_ = server.Close()
	return nil
}
