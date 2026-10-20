// Service composition belongs to app: it selects and wires concrete services,
// while lifecycle orchestration lives in daemon/internal/core/service.
package app

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	"github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	acpservice "github.com/tinguo/goworker/daemon/internal/core/service/acp"
)

type serviceRegistration struct {
	name     string
	register func(*Application, applicationOptions) error
}

var serviceRegistrations = []serviceRegistration{
	{name: "acp", register: registerACPService},
}

// registerServices wires every application-owned service into the generic core
// registry. Adding a service belongs here rather than in Application.New so the
// application lifecycle stays independent of concrete service types.
func registerServices(a *Application, options applicationOptions) error {
	for _, registration := range serviceRegistrations {
		if err := registration.register(a, options); err != nil {
			return fmt.Errorf("register service %q: %w", registration.name, err)
		}
	}
	return nil
}

func registerACPService(a *Application, options applicationOptions) error {
	// 显式注入连接 = 装配层明确要求 ACP（mobile Host 的进程内管道形态），
	// 配置开关只管默认的 Unix socket 形态 —— 连接都给了没 service 是装配 bug。
	if !a.Config.Frontend.ACP.Enabled && options.acpConnection == nil {
		return nil
	}

	sessions, err := resolveACPSessions(a)
	if err != nil {
		return err
	}

	var logger *slog.Logger
	if a.Log != nil {
		logger = a.Log.Logger
	}
	acp, err := newACPService(acpServiceOptions{
		socketPath: a.Config.Frontend.ACP.Socket,
		connection: options.acpConnection,
		evaluate:   a.Engine.Eval,
		commands:   a.Engine.Commands,
		sessions:   sessions,
		logger:     logger,
	})
	if err != nil {
		return fmt.Errorf("construct ACP service: %w", err)
	}
	if err := a.Services.Register(acp); err != nil {
		return fmt.Errorf("register ACP service: %w", err)
	}
	return nil
}

func resolveACPSessions(a *Application) (model.SessionSource, error) {
	sessions, err := a.SessionSource()
	if err == nil {
		return sessions, nil
	}
	if errors.Is(err, ErrCapabilityUnavailable) {
		return nil, nil
	}
	return nil, fmt.Errorf("resolve session source: %w", err)
}

type acpServiceOptions struct {
	socketPath string
	connection io.ReadWriteCloser
	evaluate   acpservice.Evaluator
	commands   func() []model.Command
	sessions   model.SessionSource
	logger     *slog.Logger
}

// newACPService constructs the daemon's main ACP endpoint without starting it.
// Application registers the returned concrete service with the core registry so
// all application services share one lifecycle.
func newACPService(options acpServiceOptions) (*acpservice.Service, error) {
	if options.evaluate == nil {
		return nil, errors.New("app: ACP evaluator is required")
	}
	if options.socketPath == "" {
		options.socketPath = filepath.Join(config.DefaultDir(), "frontend", "acp.sock")
	}
	if options.logger == nil {
		options.logger = slog.Default()
	}
	if options.sessions == nil {
		options.logger.Warn("acp service: session capability unavailable, sessions will not be declared to the daemon")
	}

	var listenerFactory acpservice.ListenerFactory
	if options.connection != nil {
		listenerFactory = newACPConnectionListenerFactory(options.connection, options.logger)
	} else {
		listenerFactory = newACPUnixListenerFactory(options.socketPath, options.logger)
	}

	return acpservice.NewService(
		acpservice.NewFrontendHandlerFactory(options.evaluate, options.commands, options.sessions),
		listenerFactory,
	), nil
}

func newACPUnixListenerFactory(socketPath string, logger *slog.Logger) acpservice.ListenerFactory {
	return func() (acpservice.Listener, error) {
		listener, err := acpservice.NewUnixListener(socketPath)
		if err != nil {
			return nil, fmt.Errorf("create Unix listener: %w", err)
		}
		if logger != nil {
			logger.Info("acp service listening", "socket", socketPath)
		}
		return listener, nil
	}
}

func newACPConnectionListenerFactory(conn io.ReadWriteCloser, logger *slog.Logger) acpservice.ListenerFactory {
	return func() (acpservice.Listener, error) {
		listener, err := acpservice.NewConnectionListener(conn)
		if err != nil {
			return nil, fmt.Errorf("create connection listener: %w", err)
		}
		if logger != nil {
			logger.Info("acp service attached to connection")
		}
		return listener, nil
	}
}
