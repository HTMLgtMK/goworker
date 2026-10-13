package pluginregistry

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Dependencies contains the host capabilities available to a built-in plugin factory.
type Dependencies struct {
	Runtime *runtimeconfig.Config
	Paths   runtimeconfig.Paths
}

// Factory constructs a plugin without performing work during package initialization.
type Factory func(context.Context, Dependencies) (model.Plugin, error)

// Descriptor describes one compile-time available built-in plugin.
type Descriptor struct {
	Name         string
	Dependencies []string
	New          Factory
}

// Registry stores plugin factories by stable plugin name.
type Registry struct {
	mu          sync.RWMutex
	descriptors map[string]Descriptor
}

func New() *Registry {
	return &Registry{descriptors: make(map[string]Descriptor)}
}

var defaultRegistry = New()

func Register(descriptor Descriptor) {
	if err := defaultRegistry.Register(descriptor); err != nil {
		panic(err)
	}
}

func Snapshot() ([]Descriptor, error) {
	return defaultRegistry.Snapshot()
}

func (r *Registry) Register(descriptor Descriptor) error {
	name := strings.TrimSpace(descriptor.Name)
	if name == "" {
		return fmt.Errorf("plugin descriptor name is required")
	}
	if descriptor.New == nil {
		return fmt.Errorf("plugin %q factory is required", name)
	}
	if name != descriptor.Name {
		return fmt.Errorf("plugin descriptor name %q contains surrounding whitespace", descriptor.Name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.descriptors[name]; exists {
		return fmt.Errorf("plugin %q already registered", name)
	}
	descriptor.Dependencies = append([]string(nil), descriptor.Dependencies...)
	r.descriptors[name] = descriptor
	return nil
}

func (r *Registry) Snapshot() ([]Descriptor, error) {
	r.mu.RLock()
	out := make([]Descriptor, 0, len(r.descriptors))
	for _, descriptor := range r.descriptors {
		descriptor.Dependencies = append([]string(nil), descriptor.Dependencies...)
		out = append(out, descriptor)
	}
	r.mu.RUnlock()
	return orderDescriptors(out)
}
func orderDescriptors(descriptors []Descriptor) ([]Descriptor, error) {
	byName := make(map[string]Descriptor, len(descriptors))
	indegree := make(map[string]int, len(descriptors))
	dependents := make(map[string][]string, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.Name == "" {
			return nil, fmt.Errorf("plugin descriptor name is required")
		}
		if _, exists := byName[descriptor.Name]; exists {
			return nil, fmt.Errorf("plugin %q already described", descriptor.Name)
		}
		descriptor.Dependencies = append([]string(nil), descriptor.Dependencies...)
		byName[descriptor.Name] = descriptor
		indegree[descriptor.Name] = 0
	}

	for _, descriptor := range descriptors {
		seen := make(map[string]struct{}, len(descriptor.Dependencies))
		for _, dependency := range descriptor.Dependencies {
			if _, duplicate := seen[dependency]; duplicate {
				continue
			}
			seen[dependency] = struct{}{}
			if _, exists := byName[dependency]; !exists {
				return nil, fmt.Errorf("plugin %q depends on missing plugin %q", descriptor.Name, dependency)
			}
			indegree[descriptor.Name]++
			dependents[dependency] = append(dependents[dependency], descriptor.Name)
		}
	}

	ready := make([]string, 0, len(descriptors))
	for name, degree := range indegree {
		if degree == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)

	ordered := make([]Descriptor, 0, len(descriptors))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		ordered = append(ordered, byName[name])
		for _, dependent := range dependents[name] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sort.Strings(ready)
	}
	if len(ordered) != len(descriptors) {
		return nil, fmt.Errorf("plugin dependency cycle detected")
	}
	return ordered, nil
}
