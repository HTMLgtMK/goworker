//go:build !goworker_no_agent

package service

import (
	"context"
	"fmt"

	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/pluginregistry"
)

func init() {
	pluginregistry.Register(pluginregistry.Descriptor{
		Name: "agent",
		New:  newAgentFactory,
	})
}

func newAgentFactory(_ context.Context, deps pluginregistry.Dependencies) (model.Plugin, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("agent: runtime config is required")
	}
	return NewPlugin(deps.Runtime, deps.Paths), nil
}
