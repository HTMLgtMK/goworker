package service

import (
	"strings"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// providers_test.go 是 model.ProviderManageSource 实现面的测试：upsert 新建/
// merge 更新/APIKey 保留、remove 的默认与最后一道守卫、detail 的 APIKey 脱敏。
// SaveConfig 桩把 candidate 原子写回 *cfg（testHub），p.cfg 即时可见。

func TestProviderUpsertCreate(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, saved := testHub(cfg)
	p := newAgentPlugin(hub)

	id, err := p.ProviderUpsert(model.ProviderUpsertRequest{
		Name:     "zhipu",
		Type:     "openai",
		Endpoint: "https://api.example.com/v1",
		APIKey:   "k-1",
		Models:   []string{"glm-4.7", " glm-4.6 ", ""},
	})
	if err != nil {
		t.Fatalf("ProviderUpsert(create): %v", err)
	}
	if id != "zhipu" {
		t.Fatalf("id = %q, want zhipu", id)
	}
	if len(*saved) != 1 {
		t.Fatalf("SaveConfig calls = %d, want 1", len(*saved))
	}
	provider := cfg.LLM.Providers["zhipu"]
	if provider.Endpoint != "https://api.example.com/v1" || provider.APIKey != "k-1" {
		t.Fatalf("provider = %+v, want endpoint/key persisted", provider)
	}
	if len(provider.Models) != 2 || provider.Models[1] != "glm-4.6" {
		t.Fatalf("models = %v, want trimmed dedup [glm-4.7 glm-4.6]", provider.Models)
	}
	if provider.Model != "glm-4.7" {
		t.Fatalf("Model = %q, want catalog head glm-4.7（落盘即补齐，不等 WithDefaults）", provider.Model)
	}
}

func TestProviderUpsertCreateRejectsBadNameAndDuplicate(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, saved := testHub(cfg)
	p := newAgentPlugin(hub)

	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{Name: "a.b", Type: "openai", Endpoint: "https://x/v1", Models: []string{"m"}}); err == nil {
		t.Fatal("invalid provider name should fail")
	}
	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{Name: "openai", Type: "openai", Endpoint: "https://x/v1", Models: []string{"m"}}); err == nil {
		t.Fatal("duplicate provider name should fail")
	}
	if len(*saved) != 0 {
		t.Fatalf("rejected upserts must not SaveConfig, got %d", len(*saved))
	}
}

func TestProviderUpsertUpdateMergesAndKeepsAPIKey(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{ID: "openai", Endpoint: "https://api.example.com/v1", Type: "openai", APIKey: "k-1", Models: []string{"gpt-4o", "gpt-4o-mini"}}); err != nil {
		t.Fatalf("seed update: %v", err)
	}
	// 二次更新：apiKey 留空 = 保留原值；contextWindow 零值 = 保留原值。
	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{ID: "openai", Endpoint: "https://api2.example.com/v1", Type: "openai", Models: []string{"gpt-5"}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	provider := cfg.LLM.Providers["openai"]
	if provider.APIKey != "k-1" {
		t.Fatalf("APIKey = %q, want preserved k-1", provider.APIKey)
	}
	if provider.Endpoint != "https://api2.example.com/v1" {
		t.Fatalf("Endpoint = %q, want overwritten", provider.Endpoint)
	}
	if len(provider.Models) != 1 || provider.Models[0] != "gpt-5" {
		t.Fatalf("Models = %v, want [gpt-5]", provider.Models)
	}
	if provider.ContextWindow != 256000 {
		t.Fatalf("ContextWindow = %d, want preserved 256000", provider.ContextWindow)
	}
}

func TestProviderUpsertAnthropicRequiresMaxTokens(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{Name: "claude", Type: "anthropic", Endpoint: "https://api.anthropic.com", APIKey: "sk-1", Models: []string{"claude-4"}}); err == nil {
		t.Fatal("anthropic without max_tokens should fail")
	}
	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{Name: "claude", Type: "anthropic", Endpoint: "https://api.anthropic.com", APIKey: "sk-1", Models: []string{"claude-4"}, MaxTokens: 8192, AuthType: "x-api-key"}); err != nil {
		t.Fatalf("anthropic with max_tokens: %v", err)
	}
	provider := cfg.LLM.Providers["claude"]
	if provider.AuthType != runtimeconfig.AnthropicAuthAPIKey {
		t.Fatalf("AuthType = %q, want x-api-key", provider.AuthType)
	}
	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{Name: "claude2", Type: "anthropic", Endpoint: "https://api.anthropic.com", APIKey: "sk-1", Models: []string{"claude-4"}, MaxTokens: 8192}); err != nil {
		t.Fatalf("anthropic default auth_type: %v", err)
	}
	if cfg.LLM.Providers["claude2"].AuthType != runtimeconfig.AnthropicAuthBearer {
		t.Fatalf("AuthType = %q, want bearer default", cfg.LLM.Providers["claude2"].AuthType)
	}
}

func TestProviderDetailMasksAPIKey(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{ID: "openai", Endpoint: "https://api.example.com/v1", Type: "openai", APIKey: "secret-key-1", Models: []string{"gpt-4o"}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	detail, err := p.ProviderDetail("openai")
	if err != nil {
		t.Fatalf("ProviderDetail: %v", err)
	}
	if !detail.APIKeySet {
		t.Fatal("APIKeySet = false, want true")
	}
	if detail.IsDefault != true {
		t.Fatalf("IsDefault = %v, want true (openai 是缺省)", detail.IsDefault)
	}
	// 密钥永不出域：结构体没有 APIKey 字段，这里再防一次字符串泄漏。
	blob := strings.TrimSpace(strings.Join([]string{detail.Endpoint, detail.Name, detail.Type}, " "))
	if strings.Contains(blob, "secret-key-1") {
		t.Fatal("detail leaked api key material")
	}
}

func TestProviderRemoveGuards(t *testing.T) {
	cfg := runtimeconfig.Default()
	hub, _ := testHub(cfg)
	p := newAgentPlugin(hub)

	// 默认 provider 不可删。
	if err := p.ProviderRemove("openai"); err == nil || !strings.Contains(err.Error(), "默认 provider") {
		t.Fatalf("remove default = %v, want 默认 provider 拒绝", err)
	}
	// 最后一个 provider 不可删：先删不掉默认，构造两个再删默认外的。
	if _, err := p.ProviderUpsert(model.ProviderUpsertRequest{Name: "second", Type: "openai", Endpoint: "https://x/v1", Models: []string{"m"}}); err != nil {
		t.Fatalf("upsert second: %v", err)
	}
	if err := p.ProviderRemove("second"); err != nil {
		t.Fatalf("remove second: %v", err)
	}
	if _, ok := cfg.LLM.Providers["second"]; ok {
		t.Fatal("second should be removed from config")
	}
	if err := p.ProviderRemove("missing"); err == nil {
		t.Fatal("remove unknown provider should fail")
	}
}
