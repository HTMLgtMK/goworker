// Plugin composition root: compiled-in plugins are registered here in dependency
// order. Optional plugins use paired build-tag files; CMake maps exclusions to off-tags.
// Add a plugin by adding its tagged files and registry factory.
package app

import (
	"context"
	"fmt"

	_ "github.com/tinguo/goworker/daemon/internal/builtin"
	"github.com/tinguo/goworker/daemon/internal/pluginregistry"
)

// RegisterPlugins constructs and registers every compiled-in plugin in dependency order.
func RegisterPlugins(a *Application) error {
	descriptors, err := pluginregistry.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot plugin registry: %w", err)
	}

	deps := pluginregistry.Dependencies{
		Runtime: a.Runtime,
		Paths:   defaultPaths(),
	}
	for _, descriptor := range descriptors {
		plugin, err := descriptor.New(context.Background(), deps)
		if err != nil {
			return fmt.Errorf("construct plugin %q: %w", descriptor.Name, err)
		}
		if plugin == nil {
			continue
		}
		if plugin.Name() != descriptor.Name {
			return fmt.Errorf("plugin factory %q returned %q", descriptor.Name, plugin.Name())
		}
		if err := a.Engine.Register(plugin); err != nil {
			return fmt.Errorf("register plugin %q: %w", descriptor.Name, err)
		}
	}
	return nil
}
