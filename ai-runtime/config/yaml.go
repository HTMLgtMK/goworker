package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

type llmConfigAlias LLMConfig

// UnmarshalYAML rejects obsolete model fields and replaces an explicitly supplied registry.
func (c *LLMConfig) UnmarshalYAML(node *yaml.Node) error {
	var fields map[string]any
	if err := node.Decode(&fields); err != nil {
		return err
	}
	for _, key := range []string{"default_provider", "endpoint", "model", "api_key", "context_window", "max_tokens"} {
		if _, ok := fields[key]; ok {
			return fmt.Errorf("llm.%s is no longer supported; use current and providers.<name>.models", key)
		}
	}
	if thinking, ok := fields["thinking"].(map[string]any); ok {
		for _, key := range []string{"request_mode", "effort"} {
			if _, ok := thinking[key]; ok {
				return fmt.Errorf("llm.thinking.%s is no longer supported; configure it on a model", key)
			}
		}
	}
	if providers, ok := fields["providers"].(map[string]any); ok {
		for name, raw := range providers {
			provider, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			for _, key := range []string{"model", "context_window", "thinking", "max_tokens"} {
				if _, ok := provider[key]; ok {
					return fmt.Errorf("llm.providers.%s.%s is no longer supported; configure it under models", name, key)
				}
			}
		}
	}
	decoded := llmConfigAlias(c.Clone())
	if _, ok := fields["providers"]; ok {
		decoded.Providers = nil
	}
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*c = LLMConfig(decoded)
	return nil
}
