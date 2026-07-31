package core

import (
	"github.com/tinguo/goworker/daemon/internal/logger"
	"github.com/tinguo/goworker/daemon/internal/spec"
)

// LoggingMiddleware 记录每个命令的执行日志。
func LoggingInterceptor(l *logger.Logger) spec.PluginInterceptor {
	return func(ctx *spec.Context, next func() error) error {
		user := ctx.Session.UserID
		if user == "" {
			user = "anonymous"
		}
		l.Info("command invoked", "user", user, "args", ctx.Args)
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
