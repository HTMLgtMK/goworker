package pluginregistry

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/core/model"
)

func TestOrderDescriptorsReturnsStableTopologicalOrder(t *testing.T) {
	input := []Descriptor{
		{Name: "beta", Dependencies: []string{"zeta"}},
		{Name: "alpha", Dependencies: []string{"zeta"}},
		{Name: "zeta"},
		{Name: "root"},
	}
	before := append([]Descriptor(nil), input...)
	before[0].Dependencies = append([]string(nil), input[0].Dependencies...)

	ordered, err := orderDescriptors(input)
	if err != nil {
		t.Fatalf("orderDescriptors() error = %v", err)
	}
	got := make([]string, 0, len(ordered))
	for _, item := range ordered {
		got = append(got, item.Name)
	}
	want := []string{"root", "zeta", "alpha", "beta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orderDescriptors() = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(input, before) {
		t.Fatalf("orderDescriptors() mutated its input: got %#v, want %#v", input, before)
	}
}

func TestOrderDescriptorsRejectsMissingDependency(t *testing.T) {
	_, err := orderDescriptors([]Descriptor{{Name: "agent", Dependencies: []string{"missing"}}})
	if err == nil || !strings.Contains(err.Error(), `plugin "missing"`) {
		t.Fatalf("orderDescriptors() error = %v, want missing dependency", err)
	}
}

func TestOrderDescriptorsRejectsDependencyCycle(t *testing.T) {
	_, err := orderDescriptors([]Descriptor{
		{Name: "agent", Dependencies: []string{"dispatcher"}},
		{Name: "dispatcher", Dependencies: []string{"agent"}},
	})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("orderDescriptors() error = %v, want dependency cycle", err)
	}
}

func TestRegistryRejectsInvalidAndDuplicateDescriptors(t *testing.T) {
	registry := New()
	if err := registry.Register(Descriptor{}); err == nil {
		t.Fatal("Register() accepted an empty descriptor")
	}
	factory := func(_ context.Context, _ Dependencies) (model.Plugin, error) { return nil, nil }
	if err := registry.Register(Descriptor{Name: "agent", New: factory}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.Register(Descriptor{Name: "agent", New: factory}); err == nil {
		t.Fatal("Register() accepted a duplicate descriptor")
	}
}
