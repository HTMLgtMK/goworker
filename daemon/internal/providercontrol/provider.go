package providercontrol

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
)

var (
	ErrUnavailable      = errors.New("provider control unavailable")
	ErrNotFound         = errors.New("provider not found")
	ErrRevisionConflict = errors.New("provider configuration revision conflict")
	ErrInvalid          = errors.New("invalid provider configuration")
	ErrCurrentProvider  = errors.New("cannot remove the current provider")
)

// Backend provides detached snapshots and revision-aware, transactional configuration operations.
// UpdateConfigAtRevision checks expected inside the same critical section as mutation and persistence,
// and returns a detached post-commit snapshot plus its new opaque revision.
type Backend interface {
	ConfigSnapshotWithRevision() (*runtimeconfig.Config, string)
	UpdateConfigAtRevision(expected string, mutate func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error)
}

// Current identifies the selected provider and model.
type Current struct {
	Provider string
	Model    string
}

// Model is the safe, model-specific portion of a provider view.
// Name is the display name; the Models map key is the model identity (ID).
type Model struct {
	Name          string
	ContextWindow int
	RequestMode   string
	Effort        string
	MaxTokens     int
}

// ProviderView deliberately excludes APIKey. Endpoint is sanitized before exposure.
type ProviderView struct {
	Name      string
	Type      string
	Endpoint  string
	AuthType  string
	HasAPIKey bool
	Models    map[string]Model
}

// Snapshot is one consistent provider registry view.
type Snapshot struct {
	Revision  string
	Current   Current
	Providers []ProviderView
}

// UpsertRequest is the validated user intent for creating or replacing one provider.
// EndpointAction is keep or replace; omitted values use legacy endpoint round-trip behavior.
// SecretAction is keep, replace, or clear. APIKey is never included in any response.
type UpsertRequest struct {
	Revision       string
	Name           string
	Type           string
	Endpoint       string
	EndpointAction string
	AuthType       string
	SecretAction   string
	APIKey         string
	Models         map[string]Model
	Current        *Current
}

// Service exposes revisioned CRUD operations over the runtime configuration.
type Service struct {
	backend Backend
}

func New(backend Backend) *Service { return &Service{backend: backend} }

func (s *Service) List() (Snapshot, error) {
	cfg, revision, err := s.snapshot()
	if err != nil {
		return Snapshot{}, err
	}
	providers := make([]ProviderView, 0, len(cfg.LLM.Providers))
	for name, provider := range cfg.LLM.Providers {
		providers = append(providers, view(name, provider))
	}
	sort.Slice(providers, func(i, j int) bool { return providers[i].Name < providers[j].Name })
	return Snapshot{
		Revision:  revision,
		Current:   Current{Provider: cfg.LLM.Current.Provider, Model: cfg.LLM.Current.Model},
		Providers: providers,
	}, nil
}

func (s *Service) Detail(name string) (Snapshot, ProviderView, error) {
	cfg, revision, err := s.snapshot()
	if err != nil {
		return Snapshot{}, ProviderView{}, err
	}
	provider, ok := cfg.LLM.Providers[name]
	if !ok {
		return Snapshot{}, ProviderView{}, ErrNotFound
	}
	return Snapshot{
		Revision: revision,
		Current:  Current{Provider: cfg.LLM.Current.Provider, Model: cfg.LLM.Current.Model},
	}, view(name, provider), nil
}

func (s *Service) Upsert(req UpsertRequest) (Snapshot, ProviderView, error) {
	if s == nil || s.backend == nil {
		return Snapshot{}, ProviderView{}, ErrUnavailable
	}
	if req.Revision == "" {
		return Snapshot{}, ProviderView{}, fmt.Errorf("%w: revision is required", ErrInvalid)
	}
	if strings.TrimSpace(req.Name) != req.Name || req.Name == "" {
		return Snapshot{}, ProviderView{}, fmt.Errorf("%w: provider name is invalid", ErrInvalid)
	}
	switch req.EndpointAction {
	case "", "replace":
		if validateEndpoint(req.Endpoint) != nil {
			return Snapshot{}, ProviderView{}, fmt.Errorf("%w: provider endpoint is invalid", ErrInvalid)
		}
	case "keep":
		// Endpoint is intentionally ignored; an existing provider is required below.
	default:
		return Snapshot{}, ProviderView{}, fmt.Errorf("%w: unknown endpoint action", ErrInvalid)
	}
	if len(req.Models) == 0 {
		return Snapshot{}, ProviderView{}, fmt.Errorf("%w: provider must define at least one model", ErrInvalid)
	}
	models := make(map[string]runtimeconfig.ModelConfig, len(req.Models))
	for id, model := range req.Models {
		if id == "" {
			return Snapshot{}, ProviderView{}, fmt.Errorf("%w: model id is required", ErrInvalid)
		}
		models[id] = runtimeconfig.ModelConfig{
			DisplayName:   model.Name,
			ContextWindow: model.ContextWindow,
			Thinking: runtimeconfig.ProviderThinkingConfig{
				RequestMode: runtimeconfig.ThinkingRequestMode(model.RequestMode),
				Effort:      runtimeconfig.ThinkingEffort(model.Effort),
			},
			MaxTokens: model.MaxTokens,
		}
	}

	updated, revision, err := s.backend.UpdateConfigAtRevision(req.Revision, func(cfg *runtimeconfig.Config) error {
		if cfg == nil {
			return ErrUnavailable
		}
		candidate := cfg.LLM.Clone()
		previous, exists := candidate.Providers[req.Name]
		apiKey, err := resolveSecret(previous.APIKey, exists, req.SecretAction, req.APIKey)
		if err != nil {
			return err
		}
		endpoint := req.Endpoint
		if req.EndpointAction == "keep" {
			if !exists {
				return fmt.Errorf("%w: cannot keep endpoint for a new provider", ErrInvalid)
			}
			endpoint = previous.Endpoint
		}
		if req.EndpointAction == "" && exists && endpoint == safeEndpoint(previous.Endpoint) {
			// An unchanged redacted value preserves the configured path.
			endpoint = previous.Endpoint
		}
		candidate.Providers[req.Name] = runtimeconfig.ProviderConfig{
			Type:     runtimeconfig.ProviderType(req.Type),
			Endpoint: endpoint,
			APIKey:   apiKey,
			AuthType: runtimeconfig.AnthropicAuthType(req.AuthType),
			Models:   models,
		}
		if req.Current != nil {
			candidate.Current = runtimeconfig.CurrentConfig{Provider: req.Current.Provider, Model: req.Current.Model}
		}
		if err := candidate.Validate(); err != nil {
			// Do not relay validation details: config errors can include untrusted identifiers
			// or endpoint strings and are not suitable for the protocol response.
			return ErrInvalid
		}
		cfg.LLM = candidate
		return nil
	})
	if err != nil {
		return Snapshot{}, ProviderView{}, err
	}
	if updated == nil {
		return Snapshot{}, ProviderView{}, ErrUnavailable
	}
	provider, ok := updated.LLM.Providers[req.Name]
	if !ok {
		return Snapshot{}, ProviderView{}, ErrUnavailable
	}
	return Snapshot{
		Revision: revision,
		Current:  Current{Provider: updated.LLM.Current.Provider, Model: updated.LLM.Current.Model},
	}, view(req.Name, provider), nil
}

func (s *Service) Remove(name, expectedRevision string) (string, error) {
	if s == nil || s.backend == nil {
		return "", ErrUnavailable
	}
	if name == "" || expectedRevision == "" {
		return "", fmt.Errorf("%w: provider name and revision are required", ErrInvalid)
	}
	_, revision, err := s.backend.UpdateConfigAtRevision(expectedRevision, func(cfg *runtimeconfig.Config) error {
		if cfg == nil {
			return ErrUnavailable
		}
		if cfg.LLM.Current.Provider == name {
			return ErrCurrentProvider
		}
		if _, exists := cfg.LLM.Providers[name]; !exists {
			return ErrNotFound
		}
		candidate := cfg.LLM.Clone()
		delete(candidate.Providers, name)
		if err := candidate.Validate(); err != nil {
			return ErrInvalid
		}
		cfg.LLM = candidate
		return nil
	})
	return revision, err
}

func (s *Service) snapshot() (*runtimeconfig.Config, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", ErrUnavailable
	}
	cfg, revision := s.backend.ConfigSnapshotWithRevision()
	if cfg == nil || revision == "" {
		return nil, "", ErrUnavailable
	}
	return cfg, revision, nil
}

// CurrentSnapshot is the read view of the current provider/model selection.
type CurrentSnapshot struct {
	Revision string
	Current  Current
}

// Current returns the current selection and its registry revision.
func (s *Service) Current() (CurrentSnapshot, error) {
	cfg, revision, err := s.snapshot()
	if err != nil {
		return CurrentSnapshot{}, err
	}
	return CurrentSnapshot{
		Revision: revision,
		Current:  Current{Provider: cfg.LLM.Current.Provider, Model: cfg.LLM.Current.Model},
	}, nil
}

// SetCurrent atomically switches the current provider/model under optimistic
// concurrency. Both fields are required; the model must be registered on the
// target provider.
func (s *Service) SetCurrent(expected string, current Current) (CurrentSnapshot, error) {
	if s == nil || s.backend == nil {
		return CurrentSnapshot{}, ErrUnavailable
	}
	if expected == "" {
		return CurrentSnapshot{}, fmt.Errorf("%w: revision is required", ErrInvalid)
	}
	if current.Provider == "" || current.Model == "" {
		return CurrentSnapshot{}, fmt.Errorf("%w: provider and model are required", ErrInvalid)
	}
	updated, revision, err := s.backend.UpdateConfigAtRevision(expected, func(cfg *runtimeconfig.Config) error {
		if cfg == nil {
			return ErrUnavailable
		}
		provider, ok := cfg.LLM.Providers[current.Provider]
		if !ok {
			return ErrNotFound
		}
		if _, ok := provider.Models[current.Model]; !ok {
			return fmt.Errorf("%w: model %q is not registered on provider %q", ErrInvalid, current.Model, current.Provider)
		}
		candidate := cfg.LLM.Clone()
		candidate.Current = runtimeconfig.CurrentConfig{Provider: current.Provider, Model: current.Model}
		if err := candidate.Validate(); err != nil {
			return ErrInvalid
		}
		cfg.LLM = candidate
		return nil
	})
	if err != nil {
		return CurrentSnapshot{}, err
	}
	if updated == nil {
		return CurrentSnapshot{}, ErrUnavailable
	}
	return CurrentSnapshot{
		Revision: revision,
		Current:  Current{Provider: updated.LLM.Current.Provider, Model: updated.LLM.Current.Model},
	}, nil
}

func resolveSecret(old string, exists bool, action, supplied string) (string, error) {
	switch action {
	case "", "keep":
		if supplied != "" {
			return "", fmt.Errorf("%w: secret must use replace action", ErrInvalid)
		}
		if exists {
			return old, nil
		}
		return "", nil
	case "replace":
		if supplied == "" {
			return "", fmt.Errorf("%w: replacement secret is empty", ErrInvalid)
		}
		return supplied, nil
	case "clear":
		if supplied != "" {
			return "", fmt.Errorf("%w: clear action cannot include a secret", ErrInvalid)
		}
		return "", nil
	default:
		return "", fmt.Errorf("%w: unknown secret action", ErrInvalid)
	}
}

func view(name string, provider runtimeconfig.ProviderConfig) ProviderView {
	models := make(map[string]Model, len(provider.Models))
	for id, model := range provider.Models {
		models[id] = Model{
			Name:          model.DisplayName,
			ContextWindow: model.ContextWindow,
			RequestMode:   string(model.Thinking.RequestMode),
			Effort:        string(model.Thinking.Effort),
			MaxTokens:     model.MaxTokens,
		}
	}
	return ProviderView{
		Name:      name,
		Type:      string(provider.Type),
		Endpoint:  safeEndpoint(provider.Endpoint),
		AuthType:  string(provider.AuthType),
		HasAPIKey: provider.APIKey != "",
		Models:    models,
	}
}

// validateEndpoint keeps authentication material out of URL authority/query fields. Base paths
// are preserved verbatim and must identify an API route, not carry credentials; opaque path tokens
// cannot be distinguished reliably from legitimate reverse-proxy prefixes.
func validateEndpoint(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return ErrInvalid
	}
	return nil
}

// safeEndpoint uses the same redaction contract as config display. In particular, URL paths
// are opaque and may contain reverse-proxy credentials, so none are returned to clients.
func safeEndpoint(raw string) string {
	return appconfig.RedactEndpoint(raw)
}
