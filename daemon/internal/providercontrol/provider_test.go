package providercontrol

import (
	"errors"
	"strings"
	"sync"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

type memoryBackend struct {
	mu       sync.Mutex
	cfg      *runtimeconfig.Config
	revision string
}

func newMemoryBackend() *memoryBackend {
	return &memoryBackend{
		cfg:      &runtimeconfig.Config{LLM: runtimeconfig.DefaultLLM()},
		revision: "epoch-1",
	}
}

func (b *memoryBackend) ConfigSnapshotWithRevision() (*runtimeconfig.Config, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	copy := *b.cfg
	copy.LLM = b.cfg.LLM.Clone()
	return &copy, b.revision
}

func (b *memoryBackend) UpdateConfigAtRevision(expected string, mutate func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if expected != b.revision {
		return nil, b.revision, ErrRevisionConflict
	}
	candidate := *b.cfg
	candidate.LLM = b.cfg.LLM.Clone()
	if err := mutate(&candidate); err != nil {
		return nil, b.revision, err
	}
	b.cfg = &candidate
	b.revision = b.revision + "-next"
	result := *b.cfg
	result.LLM = b.cfg.LLM.Clone()
	return &result, b.revision, nil
}

func TestListAndDetailNeverExposeSecretsOrEndpointCredentials(t *testing.T) {
	backend := newMemoryBackend()
	cfg, _ := backend.ConfigSnapshotWithRevision()
	provider := cfg.LLM.Providers["openai"]
	provider.APIKey = "do-not-return-this"
	provider.Endpoint = "https://user:pass@example.test/v1?token=secret#fragment"
	cfg.LLM.Providers["openai"] = provider
	backend.cfg = cfg

	service := New(backend)
	list, err := service.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Providers) != 1 || !list.Providers[0].HasAPIKey {
		t.Fatalf("provider list did not return safe key presence: %#v", list.Providers)
	}
	if got := list.Providers[0].Endpoint; got != "https://example.test/v1" {
		t.Fatalf("endpoint was not sanitized: %q", got)
	}
	if strings.Contains(list.Providers[0].Endpoint, "user") || strings.Contains(list.Providers[0].Endpoint, "token") {
		t.Fatalf("endpoint leaked credentials: %q", list.Providers[0].Endpoint)
	}
	detail, providerView, err := service.Detail("openai")
	if err != nil {
		t.Fatal(err)
	}
	if detail.Revision != list.Revision || providerView.HasAPIKey != true {
		t.Fatalf("detail snapshot mismatch: %#v", detail)
	}
	if _, ok := any(providerView).(interface{ APIKey() string }); ok {
		t.Fatal("provider view unexpectedly exposes a key accessor")
	}
}

func TestUpsertKeepsOrReplacesSecretAndCanChangeCurrentAtomically(t *testing.T) {
	backend := newMemoryBackend()
	cfg, _ := backend.ConfigSnapshotWithRevision()
	provider := cfg.LLM.Providers["openai"]
	provider.APIKey = "old-secret"
	cfg.LLM.Providers["openai"] = provider
	backend.cfg = cfg
	service := New(backend)

	current := &Current{Provider: "openai", Model: "gpt-4o-mini"}
	snapshot, result, err := service.Upsert(UpsertRequest{
		Revision:     "epoch-1",
		Name:         "openai",
		Type:         string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint:     "https://api.example.test/v1",
		SecretAction: "keep",
		Models: map[string]Model{
			"gpt-4o-mini": {ContextWindow: 128000, RequestMode: string(runtimeconfig.ThinkingRequestAuto), Effort: string(runtimeconfig.ThinkingEffortMedium)},
		},
		Current: current,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Current != *current || snapshot.Revision != "epoch-1-next" {
		t.Fatalf("upsert did not atomically select current/revision: %#v", snapshot)
	}
	if !result.HasAPIKey {
		t.Fatal("keep action discarded existing key")
	}
	stored, _ := backend.ConfigSnapshotWithRevision()
	if got := stored.LLM.Providers["openai"].APIKey; got != "old-secret" {
		t.Fatalf("keep action changed stored key: %q", got)
	}

	_, result, err = service.Upsert(UpsertRequest{
		Revision:     "epoch-1-next",
		Name:         "openai",
		Type:         string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint:     "https://api.example.test/v1",
		SecretAction: "replace",
		APIKey:       "new-secret",
		Models: map[string]Model{
			"gpt-4o-mini": {ContextWindow: 128000, RequestMode: string(runtimeconfig.ThinkingRequestAuto), Effort: string(runtimeconfig.ThinkingEffortMedium)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.HasAPIKey {
		t.Fatal("replacement key presence is not reported")
	}
	stored, _ = backend.ConfigSnapshotWithRevision()
	if got := stored.LLM.Providers["openai"].APIKey; got != "new-secret" {
		t.Fatalf("replace action not persisted: %q", got)
	}
	if strings.Contains(result.Endpoint, "new-secret") {
		t.Fatal("response leaked secret")
	}
}

func TestUpsertRejectsStaleRevisionAndInvalidCurrentModel(t *testing.T) {
	service := New(newMemoryBackend())
	request := UpsertRequest{
		Revision: "stale",
		Name:     "new-provider",
		Type:     string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint: "https://api.example.test/v1",
		Models: map[string]Model{
			"model-a": {ContextWindow: 1000, RequestMode: string(runtimeconfig.ThinkingRequestAuto), Effort: string(runtimeconfig.ThinkingEffortMedium)},
		},
	}
	if _, _, err := service.Upsert(request); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}

	request.Revision = "epoch-1"
	request.Current = &Current{Provider: "new-provider", Model: "missing"}
	if _, _, err := service.Upsert(request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid current selection, got %v", err)
	}
}

func TestUpsertRequiresModelsAndExplicitSecretReplacement(t *testing.T) {
	service := New(newMemoryBackend())
	base := UpsertRequest{
		Revision: "epoch-1",
		Name:     "openai",
		Type:     string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint: "https://api.example.test/v1",
		Models: map[string]Model{
			"gpt-4o": {ContextWindow: 1000, RequestMode: string(runtimeconfig.ThinkingRequestAuto), Effort: string(runtimeconfig.ThinkingEffortMedium)},
		},
	}
	base.Models = nil
	if _, _, err := service.Upsert(base); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty models should be rejected: %v", err)
	}
	base.Models = map[string]Model{
		"gpt-4o": {ContextWindow: 1000, RequestMode: string(runtimeconfig.ThinkingRequestAuto), Effort: string(runtimeconfig.ThinkingEffortMedium)},
	}
	base.APIKey = "unexpected-secret"
	if _, _, err := service.Upsert(base); !errors.Is(err, ErrInvalid) {
		t.Fatalf("secret without replace action should be rejected: %v", err)
	}

	base.APIKey = ""
	base.Endpoint = "https://user:pass@example.test/proxy/openai/v1?token=secret"
	if _, _, err := service.Upsert(base); !errors.Is(err, ErrInvalid) {
		t.Fatalf("credential-bearing endpoint URL should be rejected: %v", err)
	}
	base.Endpoint = "https://example.test/proxy/openai/v1"
	if _, _, err := service.Upsert(base); err != nil {
		t.Fatalf("valid endpoint base path should be accepted: %v", err)
	}
}

func TestRemoveProtectsCurrentAndDeletesNonCurrent(t *testing.T) {
	backend := newMemoryBackend()
	cfg, _ := backend.ConfigSnapshotWithRevision()
	cfg.LLM.Providers["other"] = cfg.LLM.Providers["openai"]
	backend.cfg = cfg
	service := New(backend)
	if _, err := service.Remove("openai", "epoch-1"); !errors.Is(err, ErrCurrentProvider) {
		t.Fatalf("removing current provider should be rejected, got %v", err)
	}
	if revision, err := service.Remove("other", "epoch-1"); err != nil || revision != "epoch-1-next" {
		t.Fatalf("removing non-current provider failed: revision=%q err=%v", revision, err)
	}
	list, err := service.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Providers) != 1 || list.Providers[0].Name != "openai" {
		t.Fatalf("provider was not removed: %#v", list.Providers)
	}
}

func TestClearSecretMustLeaveValidProvider(t *testing.T) {
	service := New(newMemoryBackend())
	_, _, err := service.Upsert(UpsertRequest{
		Revision:     "epoch-1",
		Name:         "anthropic",
		Type:         string(runtimeconfig.ProviderTypeAnthropic),
		Endpoint:     "https://api.anthropic.com/v1",
		AuthType:     string(runtimeconfig.AnthropicAuthAPIKey),
		SecretAction: "clear",
		Models: map[string]Model{
			"claude": {ContextWindow: 1000, MaxTokens: 100},
		},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("clearing required Anthropic key should fail validation: %v", err)
	}
}

func TestCurrentReadReturnsSelectionSnapshot(t *testing.T) {
	backend := newMemoryBackend()
	cfg, _ := backend.ConfigSnapshotWithRevision()
	provider := cfg.LLM.Providers["openai"]
	provider.Models["gpt-4o-mini"] = runtimeconfig.ModelConfig{ContextWindow: 128000}
	cfg.LLM.Providers["openai"] = provider
	backend.cfg = cfg

	service := New(backend)
	got, err := service.Current()
	if err != nil {
		t.Fatal(err)
	}
	if got.Current != (Current{Provider: "openai", Model: "gpt-4o"}) {
		t.Fatalf("current = %+v, want openai/gpt-4o", got.Current)
	}
	if got.Revision != "epoch-1" {
		t.Fatalf("revision = %q, want epoch-1", got.Revision)
	}
}

func TestSetCurrentSwitchesSelectionAtomically(t *testing.T) {
	backend := newMemoryBackend()
	cfg, _ := backend.ConfigSnapshotWithRevision()
	provider := cfg.LLM.Providers["openai"]
	provider.Models["gpt-4o-mini"] = runtimeconfig.ModelConfig{ContextWindow: 128000}
	cfg.LLM.Providers["openai"] = provider
	backend.cfg = cfg
	service := New(backend)

	snapshot, err := service.SetCurrent("epoch-1", Current{Provider: "openai", Model: "gpt-4o-mini"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Current != (Current{Provider: "openai", Model: "gpt-4o-mini"}) {
		t.Fatalf("current = %+v, want gpt-4o-mini", snapshot.Current)
	}
	stored, _ := backend.ConfigSnapshotWithRevision()
	if stored.LLM.Current.Model != "gpt-4o-mini" {
		t.Fatalf("stored selection = %+v, want model switch with alias sync", stored.LLM.Current)
	}
}

func TestSetCurrentRejectsUnknownProviderModelAndStaleRevision(t *testing.T) {
	service := New(newMemoryBackend())
	if _, err := service.SetCurrent("epoch-1", Current{Provider: "ghost", Model: "gpt-4o"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown provider = %v, want ErrNotFound", err)
	}
	if _, err := service.SetCurrent("epoch-1", Current{Provider: "openai", Model: "ghost"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown model = %v, want ErrInvalid", err)
	}
	if _, err := service.SetCurrent("stale", Current{Provider: "openai", Model: "gpt-4o"}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale revision = %v, want ErrRevisionConflict", err)
	}
}

type controlledBackend struct {
	config   *runtimeconfig.Config
	revision string
	update   func(string, func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error)
}

func (b *controlledBackend) ConfigSnapshotWithRevision() (*runtimeconfig.Config, string) {
	return b.config, b.revision
}

func (b *controlledBackend) UpdateConfigAtRevision(expected string, mutate func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error) {
	if b.update != nil {
		return b.update(expected, mutate)
	}
	return nil, "", ErrUnavailable
}

func TestReadOperationsRejectUnavailableSnapshots(t *testing.T) {
	config, _ := newMemoryBackend().ConfigSnapshotWithRevision()
	tests := []struct {
		name    string
		service *Service
	}{
		{name: "nil service"},
		{name: "nil backend", service: New(nil)},
		{name: "nil config", service: New(&controlledBackend{revision: "epoch-1"})},
		{name: "empty revision", service: New(&controlledBackend{config: config})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.service.List(); !errors.Is(err, ErrUnavailable) {
				t.Errorf("List() error = %v, want ErrUnavailable", err)
			}
			if _, _, err := tt.service.Detail("openai"); !errors.Is(err, ErrUnavailable) {
				t.Errorf("Detail() error = %v, want ErrUnavailable", err)
			}
			if _, err := tt.service.Current(); !errors.Is(err, ErrUnavailable) {
				t.Errorf("Current() error = %v, want ErrUnavailable", err)
			}
		})
	}
}

func TestMutationBoundariesAndBackendFailures(t *testing.T) {
	service := New(newMemoryBackend())
	if _, _, err := service.Upsert(UpsertRequest{Name: "openai"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("Upsert without revision = %v, want ErrInvalid", err)
	}
	if _, _, err := service.Upsert(UpsertRequest{
		Revision: "epoch-1",
		Name:     "openai",
		Endpoint: "https://api.example.test/v1",
		Models:   map[string]Model{"": {}},
	}); !errors.Is(err, ErrInvalid) {
		t.Errorf("Upsert with empty model ID = %v, want ErrInvalid", err)
	}
	if _, err := service.Remove("", "epoch-1"); !errors.Is(err, ErrInvalid) {
		t.Errorf("Remove without name = %v, want ErrInvalid", err)
	}
	if _, err := service.Remove("missing", "epoch-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Remove missing provider = %v, want ErrNotFound", err)
	}
	if _, err := service.SetCurrent("", Current{Provider: "openai", Model: "gpt-4o"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetCurrent without revision = %v, want ErrInvalid", err)
	}
	if _, err := service.SetCurrent("epoch-1", Current{Provider: "openai"}); !errors.Is(err, ErrInvalid) {
		t.Errorf("SetCurrent without model = %v, want ErrInvalid", err)
	}

	backendError := errors.New("backend persistence failed")
	failingBackend := &controlledBackend{update: func(string, func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error) {
		return nil, "", backendError
	}}
	request := UpsertRequest{
		Revision: "epoch-1",
		Name:     "openai",
		Type:     string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint: "https://api.example.test/v1",
		Models:   map[string]Model{"gpt-4o": {ContextWindow: 1000}},
	}
	if _, _, err := New(failingBackend).Upsert(request); !errors.Is(err, backendError) {
		t.Errorf("Upsert backend error = %v, want wrapped backend error", err)
	}
	if _, err := New(failingBackend).SetCurrent("epoch-1", Current{Provider: "openai", Model: "gpt-4o"}); !errors.Is(err, backendError) {
		t.Errorf("SetCurrent backend error = %v, want wrapped backend error", err)
	}

	emptyResultBackend := &controlledBackend{update: func(_ string, mutate func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error) {
		candidate, _ := newMemoryBackend().ConfigSnapshotWithRevision()
		if err := mutate(candidate); err != nil {
			return nil, "", err
		}
		return nil, "epoch-2", nil
	}}
	if _, _, err := New(emptyResultBackend).Upsert(request); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Upsert with nil backend result = %v, want ErrUnavailable", err)
	}
	if _, err := New(emptyResultBackend).SetCurrent("epoch-1", Current{Provider: "openai", Model: "gpt-4o"}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("SetCurrent with nil backend result = %v, want ErrUnavailable", err)
	}
}

func TestResolveSecretRejectsInvalidActions(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		supplied string
		wantErr  bool
	}{
		{name: "replacement without value", action: "replace", wantErr: true},
		{name: "clear with supplied value", action: "clear", supplied: "secret", wantErr: true},
		{name: "unknown action", action: "rotate", wantErr: true},
		{name: "keep existing key", action: "keep"},
		{name: "keep absent key", action: "keep"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old, exists := "old-secret", true
			if tt.name == "keep absent key" {
				old, exists = "", false
			}
			got, err := resolveSecret(old, exists, tt.action, tt.supplied)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("resolveSecret() error = %v, want ErrInvalid", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSecret() error = %v", err)
			}
			if got != old {
				t.Errorf("resolveSecret() = %q, want existing value %q", got, old)
			}
		})
	}
}

func TestModelNameRoundTripsThroughUpsertAndViews(t *testing.T) {
	backend := newMemoryBackend()
	service := New(backend)

	_, providerView, err := service.Upsert(UpsertRequest{
		Revision:     "epoch-1",
		Name:         "openai",
		Type:         string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint:     "https://api.example.test/v1",
		SecretAction: "keep",
		Models: map[string]Model{
			"gpt-4o": {Name: "GPT-4o", ContextWindow: 256000,
				RequestMode: string(runtimeconfig.ThinkingRequestAuto), Effort: string(runtimeconfig.ThinkingEffortMedium)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := providerView.Models["gpt-4o"].Name; got != "GPT-4o" {
		t.Fatalf("upsert view model name = %q, want GPT-4o", got)
	}
	stored, _ := backend.ConfigSnapshotWithRevision()
	if got := stored.LLM.Providers["openai"].Models["gpt-4o"].DisplayName; got != "GPT-4o" {
		t.Fatalf("stored display name = %q, want GPT-4o", got)
	}

	list, err := service.List()
	if err != nil {
		t.Fatal(err)
	}
	if got := list.Providers[0].Models["gpt-4o"].Name; got != "GPT-4o" {
		t.Fatalf("list view model name = %q, want GPT-4o", got)
	}
}
