package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

// With returns a new capability set containing capability.
func (c Capabilities) With(capability Capability) (Capabilities, error) {
	if capability == nil {
		return Capabilities{}, fmt.Errorf("acp: capability is nil")
	}
	key := capability.Key()
	if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) {
		return Capabilities{}, fmt.Errorf("acp: capability key %q is invalid", key)
	}
	if c.Has(key) {
		return Capabilities{}, fmt.Errorf("acp: duplicate capability key %q", key)
	}
	if _, err := json.Marshal(capability); err != nil {
		return Capabilities{}, fmt.Errorf("acp: marshal capability %q: %w", key, err)
	}

	items := append([]Capability(nil), c.items...)
	items = append(items, capability)
	return Capabilities{items: items}, nil
}

// Has reports whether the capability set contains key.
func (c Capabilities) Has(key string) bool {
	for _, capability := range c.items {
		if capability.Key() == key {
			return true
		}
	}
	return false
}

// Get returns the capability registered under key.
func (c Capabilities) Get(key string) (Capability, bool) {
	for _, capability := range c.items {
		if capability.Key() == key {
			return capability, true
		}
	}
	return nil, false
}

// MarshalJSON encodes capabilities as the ACP capability object.
func (c Capabilities) MarshalJSON() ([]byte, error) {
	values := make(map[string]json.RawMessage, len(c.items))
	for _, capability := range c.items {
		key := capability.Key()
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("acp: duplicate capability key %q", key)
		}
		value, err := json.Marshal(capability)
		if err != nil {
			return nil, fmt.Errorf("acp: marshal capability %q: %w", key, err)
		}
		values[key] = value
	}
	return json.Marshal(values)
}

// UnmarshalJSON preserves incoming capabilities as opaque typed values. A
// server that needs capability-specific behavior should decode the returned
// payload into its concrete capability type before acting on it.
func (c *Capabilities) UnmarshalJSON(data []byte) error {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("acp: decode capabilities: %w", err)
	}
	if values == nil {
		return fmt.Errorf("acp: capabilities must be a JSON object")
	}

	items := make([]Capability, 0, len(values))
	for key, value := range values {
		if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) {
			return fmt.Errorf("acp: capability key %q is invalid", key)
		}
		items = append(items, rawCapability{
			key:   key,
			value: append(json.RawMessage(nil), value...),
		})
	}
	*c = Capabilities{items: items}
	return nil
}

func (FileSystemCapability) Key() string { return "fs" }

func (c FileSystemCapability) MarshalJSON() ([]byte, error) {
	type payload FileSystemCapability
	return json.Marshal(payload(c))
}

func (LoadSessionCapability) Key() string { return "loadSession" }

func (LoadSessionCapability) MarshalJSON() ([]byte, error) {
	return []byte("true"), nil
}

type rawCapability struct {
	key   string
	value json.RawMessage
}

func (c rawCapability) Key() string { return c.key }

func (c rawCapability) MarshalJSON() ([]byte, error) {
	return append(json.RawMessage(nil), c.value...), nil
}
