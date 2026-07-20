package core

import (
	"log"

	"github.com/tinguo/goworker/daemon/internal/spec"
)

// LoggingMiddleware 记录每个命令的执行日志。
func LoggingMiddleware() spec.Middleware {
	return func(ctx *spec.Context, next func() error) error {
		user := ctx.Session.UserID
		if user == "" {
			user = "anonymous"
		}
		log.Printf("[cmd] user=%s args=%v", user, ctx.Args)
		return next()
	}
}

// SessionMiddleware 从 ctx.Values["token"] 还原 Session 信息。
func SessionMiddleware(provider spec.SessionProvider) spec.Middleware {
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
