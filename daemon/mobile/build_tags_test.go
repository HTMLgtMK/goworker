//go:build goworker_no_dispatcher && !goworker_no_agent

package mobile

import (
	"testing"

	"github.com/tinguo/goworker/daemon/internal/pluginregistry"
)

func TestAgentOnlyMobileRegistry(t *testing.T) {
	descriptors, err := pluginregistry.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(descriptors) != 1 || descriptors[0].Name != "agent" {
		t.Fatalf("mobile plugins = %#v, want [agent]", descriptors)
	}
}
