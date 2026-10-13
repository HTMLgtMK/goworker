//go:build !goworker_no_dispatcher

package dispatcher

import (
	"context"
	"fmt"

	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/pluginregistry"
)

func init() {
	pluginregistry.Register(pluginregistry.Descriptor{
		Name: "dispatcher",
		New:  newDispatcherPlugin,
	})
}

func newDispatcherPlugin(_ context.Context, deps pluginregistry.Dependencies) (model.Plugin, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("dispatcher: runtime config is required")
	}
	if !deps.Runtime.Dispatch.Enabled {
		return nil, nil
	}
	return NewPlugin(deps.Runtime, deps.Paths), nil
}
