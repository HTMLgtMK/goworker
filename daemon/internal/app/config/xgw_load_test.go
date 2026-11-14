package config

import (
	"testing"
)

func TestExampleFileLoadsAndValidates(t *testing.T) {
	cfg := Load("../../config.yaml.example")
	if cfg == nil {
		t.Fatal("example config failed to load")
	}
	if err := cfg.LLM.Validate(); err != nil {
		t.Fatalf("example config validate: %v", err)
	}
	if cfg.LLM.Current.Provider != "openai" || cfg.LLM.Current.Model != "gpt-4o" {
		t.Fatalf("current = %+v", cfg.LLM.Current)
	}
	if _, ok := cfg.LLM.Providers["openai"].Models["gpt-4o"]; !ok {
		t.Fatal("example config missing openai/gpt-4o model entry")
	}
}
