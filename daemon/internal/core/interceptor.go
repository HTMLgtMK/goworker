package core

import (
	spec "github.com/tinguo/goworker/ai-runtime/plugin"
	"github.com/tinguo/goworker/ai-runtime/logger"
)

// LoggingInterceptor 是命令级中间件的扩展点，后续的命令审计/统计逻辑挂在这里。
// 原有的 "command invoked" console 日志已按需求移除——没有审计需求前，每条命令往 stderr
// 刷一行纯属噪音。user 解析先保留：将来做审计时它是第一个要用到的字段。
func LoggingInterceptor(l *logger.Logger) spec.PluginInterceptor {
	return func(ctx *spec.Context, next func() error) error {
		user := ctx.Session.UserID
		if user == "" {
			user = "anonymous"
		}
		return next()
	}
}

// SessionMiddleware 从 ctx.Values["token"] 还原 Session 信息。
func SessionInterceptor(provider spec.SessionProvider) spec.PluginInterceptor {
	return func(ctx *spec.Context, next func() error) error {
		if provider != nil {
			if token, ok := ctx.Values["token"].(string); ok && token != "" {
				if session, valid := provider.Resume(token); valid {
					ctx.Session = session
				}
			}
		}
		return next()
	}
}
