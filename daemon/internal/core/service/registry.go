package service

import (
	"errors"
	"fmt"
	"sync"
)

// Service is the lifecycle contract shared by application-owned background
// services. Stop does not return until the service has fully shut down.
type Service interface {
	Name() string
	Start() error
	Stop() error
}

// Registry owns application services in registration order. Registration order
// is also the startup order; shutdown always runs in the reverse order.
type Registry struct {
	mu       sync.Mutex
	services []Service
	byName   map[string]Service
	state    registryState
	startErr error
	stopErr  error
}

type registryState uint8

const (
	registryOpen registryState = iota
	registryStarted
	registryStopped
)

// NewRegistry returns an empty registry that accepts service registrations.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]Service)}
}

// Register adds service to the registry. Services cannot be added after the
// registry has started (or stopped), because changing lifecycle order halfway
// through startup is a reliable way to manufacture shutdown bugs.
func (r *Registry) Register(service Service) error {
	if r == nil {
		return errors.New("service: nil registry")
	}
	if service == nil {
		return errors.New("service: nil service")
	}
	name := service.Name()
	if name == "" {
		return errors.New("service: service name is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != registryOpen {
		return errors.New("service: cannot register after start")
	}
	if r.byName == nil {
		r.byName = make(map[string]Service)
	}
	if _, exists := r.byName[name]; exists {
		return fmt.Errorf("service: service %q is already registered", name)
	}
	r.services = append(r.services, service)
	r.byName[name] = service
	return nil
}

// Get returns the service registered under name.
func (r *Registry) Get(name string) (Service, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	service, ok := r.byName[name]
	return service, ok
}

// Start starts every registered service in registration order. If a service
// fails, already-started services are stopped in reverse order before the
// failure is returned. A second call returns the original result without
// invoking any service again.
func (r *Registry) Start() error {
	if r == nil {
		return errors.New("service: nil registry")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case registryStarted:
		return nil
	case registryStopped:
		return r.startErrOrStopped()
	}

	for i, service := range r.services {
		if err := service.Start(); err != nil {
			startErr := fmt.Errorf("start service %q: %w", service.Name(), err)
			rollbackErr := r.stopStartedLocked(i)
			if rollbackErr != nil {
				startErr = errors.Join(startErr, rollbackErr)
			}
			r.startErr = startErr
			r.stopErr = rollbackErr
			r.state = registryStopped
			return startErr
		}
	}
	r.state = registryStarted
	return nil
}

// Stop stops all started services in reverse registration order. Every service
// is given a chance to stop; errors are aggregated with errors.Join. Repeated
// calls return the same result without invoking services again.
func (r *Registry) Stop() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == registryStopped {
		return r.stopErr
	}
	if r.state == registryOpen {
		r.state = registryStopped
		return nil
	}

	r.stopErr = r.stopStartedLocked(len(r.services))
	r.state = registryStopped
	return r.stopErr
}

func (r *Registry) startErrOrStopped() error {
	if r.startErr != nil {
		return r.startErr
	}
	return errors.New("service: registry has been stopped")
}

// stopStartedLocked stops the services in [0, end) in reverse order. The
// caller must hold r.mu. It is used both for normal shutdown and startup
// rollback.
func (r *Registry) stopStartedLocked(end int) error {
	var joined error
	for i := end - 1; i >= 0; i-- {
		serviceErr := r.services[i].Stop()
		if serviceErr != nil {
			joined = errors.Join(joined, fmt.Errorf("stop service %q: %w", r.services[i].Name(), serviceErr))
		}
	}
	return joined
}
