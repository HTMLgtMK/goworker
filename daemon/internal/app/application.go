// Package app 是 daemon 的装配层，三种运行形态共用一套配置/日志装配：
//
//   - CLI 壳（cmd/goworker）与 Android 壳（mobile）：New() 装配完整后端
//     （配置→日志→Engine→插件→StartAll），壳只负责前端与退出策略；
//   - ACP worker（RunACPWorker）：dispatcher spawn 的无人值守单任务进程。
//     它不走 New() —— worker 不装 Engine/插件/内置命令，没有前端，只共享
//     ProviderFactory 与配置/日志装配，以 stdio 直接服务 ACP 协议；
//     每任务一进程，轻启动是硬要求。
package app

import (
	"fmt"
	"log/slog"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/ai-runtime/logger"
	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/engine"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/core/service"
)

// Application 是装配完成的 agent 后端：Engine + 插件 + 配置 + 日志。
// 字段供壳层取用（CLI 读 Config.Frontend.Stdin，mobile 读 Engine.Eval 等）。
type Application struct {
	Config  *config.Config
	Runtime *runtimeconfig.Config
	Engine  *engine.Engine
	Log     *logger.Logger
	// Agent 是 agent 插件实例（GOWORKER_NO_PLUGINS 剔除 agent 时为 nil）。
	// vscode 前端经它做会话清单/load；最小壳下 CLI 壳跳过 vscode 前端启动。
	Agent *service.AgentPlugin
}

// loadConfigAndLogger 装配前置两步：配置装载 + 日志初始化。New 与 RunACPWorker 共用。
// slog 全局默认 logger 在这里接上（插件/provider 都走 slog）。
func loadConfigAndLogger() (*config.Config, *logger.Logger, error) {
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)
	if cfg == nil {
		// Load 遇到无效配置会返回 nil（具体原因它已经记日志了）。
		// 不挡一下的话下一行 cfg.Log 就是空指针解引用——配置写错一个字段
		// 换来一个段错误，排查成本高得离谱。
		return nil, nil, fmt.Errorf("配置无效: %s\n详见上方日志；修好后重试，或删掉该文件用默认配置。", cfgPath)
	}

	// 初始化日志器（等级过滤 + 可选文件轮转/清理）
	log, err := logger.Setup(cfg.Log)
	if err != nil {
		return nil, nil, fmt.Errorf("init logger: %w", err)
	}
	// 插件/provider 用全局 slog 打日志（agent 插件没有自己的 logger 句柄），
	// 默认它只进 stderr、忽略 config 的 log.level/log.file —— 接上配置的 handler 后，
	// log.level: debug 才能看到 llm.chat request/response 这类调试日志。
	slog.SetDefault(log.Logger)
	log.Info("config loaded", "path", cfgPath)
	if cfg.Log.File != "" {
		log.Info("log file", "path", cfg.Log.File)
	} else {
		log.Info("log to stderr only")
	}
	return cfg, log, nil
}

// New 完成全部装配：配置 → 日志 → Engine → AgentPlugin → 内置命令 → StartAll。
// 出错返回 error，不 panic、不 os.Exit —— 退出策略归壳层（CLI 打印后 exit，mobile 转 Java 异常）。
func New() (*Application, error) {
	cfg, log, err := loadConfigAndLogger()
	if err != nil {
		return nil, err
	}

	// ai-runtime 聚合配置（各插件的目录路径注入见 RegisterPlugins 装配链）
	runtimeCfg := cfg.ToRuntime()

	e := engine.NewEngine(cfg, runtimeCfg, log)

	// 注册拦截器
	e.Use(engine.LoggingInterceptor(log))

	a := &Application{Config: cfg, Runtime: runtimeCfg, Engine: e, Log: log}

	// 注册插件：按构建标签装配（CMake GOWORKER_NO_PLUGINS → goworker_no_<name> off-tag）
	if err := RegisterPlugins(a); err != nil {
		// 失败路径与原 main 的 defer 语义对齐：先 StopAll（已启动的插件回收）再关日志，
		// 最后经 error 交壳层决定退出方式（CLI exit 1 / mobile 转 Java 异常）。
		log.Error("register plugins failed", "error", err)
		e.StopAll()
		log.Close()
		return nil, fmt.Errorf("register plugins failed: %w", err)
	}

	// 内置命令
	e.RegisterBuiltinCommands()

	// 启动插件
	if err := e.StartAll(); err != nil {
		log.Error("start plugins failed", "error", err)
		e.StopAll()
		log.Close()
		return nil, fmt.Errorf("start plugins failed: %w", err)
	}

	// 发送启动事件
	e.Notify(model.Event{Type: model.EventPluginStarted, Payload: "system"})

	return a, nil
}

// Stop 释放资源：插件停止 + 日志关闭。对应原 main 的 defer 链，壳层在所有退出路径调用。
func (a *Application) Stop() {
	a.Engine.StopAll()
	a.Log.Close()
}
