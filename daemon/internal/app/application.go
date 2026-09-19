// Package app 是 daemon 的装配层：CLI 壳（cmd/goworker）与 Android 壳（mobile）
// 共享同一套组装逻辑 —— 配置装载、日志初始化、Engine 装配、插件注册与生命周期。
// 壳只负责前端与退出策略，不再各自复制装配代码。
package app

import (
	"fmt"
	"log/slog"
	"path/filepath"

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
}

// New 完成全部装配：配置 → 日志 → Engine → AgentPlugin → 内置命令 → StartAll。
// 出错返回 error，不 panic、不 os.Exit —— 退出策略归壳层（CLI 打印后 exit，mobile 转 Java 异常）。
func New() (*Application, error) {
	// 加载全局配置
	cfgPath := config.DefaultPath()
	cfg := config.Load(cfgPath)
	if cfg == nil {
		// Load 遇到无效配置会返回 nil（具体原因它已经记日志了）。
		// 不挡一下的话下一行 cfg.Log 就是空指针解引用——配置写错一个字段
		// 换来一个段错误，排查成本高得离谱。
		return nil, fmt.Errorf("配置无效: %s\n详见上方日志；修好后重试，或删掉该文件用默认配置", cfgPath)
	}

	// 初始化日志器（等级过滤 + 可选文件轮转/清理）
	log, err := logger.Setup(cfg.Log)
	if err != nil {
		return nil, fmt.Errorf("init logger: %w", err)
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

	// ai-runtime 聚合配置 + 目录路径注入（daemon 是路径中枢，ai-runtime 不反向依赖 DefaultDir）
	runtimeCfg := cfg.ToRuntime()
	paths := runtimeconfig.Paths{
		ConfigDir:     config.DefaultDir(),
		SkillsUser:    filepath.Join(config.DefaultDir(), "skills"),
		SkillsProject: filepath.Join(".goworker", "skills"),
		AuditDir:      filepath.Join(config.DefaultDir(), "audit"),
	}

	e := engine.NewEngine(cfg, runtimeCfg, log)

	// 注册拦截器
	e.Use(engine.LoggingInterceptor(log))

	// 注册插件（ai-runtime 的 agent 插件：配置与路径构造函数注入）
	if err := e.Register(service.NewPlugin(runtimeCfg, paths)); err != nil {
		log.Close()
		return nil, fmt.Errorf("register AgentPlugin failed: %w", err)
	}

	// 内置命令
	e.RegisterBuiltinCommands()

	// 启动插件
	if err := e.StartAll(); err != nil {
		log.Close()
		return nil, fmt.Errorf("start plugins failed: %w", err)
	}

	// 发送启动事件
	e.Notify(model.Event{Type: model.EventPluginStarted, Payload: "system"})

	return &Application{Config: cfg, Runtime: runtimeCfg, Engine: e, Log: log}, nil
}

// Stop 释放资源：插件停止 + 日志关闭。对应原 main 的 defer 链，壳层在所有退出路径调用。
func (a *Application) Stop() {
	a.Engine.StopAll()
	a.Log.Close()
}
