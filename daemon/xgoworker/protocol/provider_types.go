package protocol

// ProviderCurrent identifies the provider and model selected by the daemon.
type ProviderCurrent struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// ProviderModel contains model-specific request limits and reasoning settings.
// Name is the display name; the models map key is the model identity (ID).
type ProviderModel struct {
	Name          string                 `json:"name,omitempty"`
	ContextWindow int                    `json:"contextWindow,omitempty"`
	Thinking      ProviderThinkingConfig `json:"thinking,omitempty"`
	MaxTokens     int                    `json:"maxTokens,omitempty"`
}

// ProviderThinkingConfig describes OpenAI-compatible reasoning request settings.
type ProviderThinkingConfig struct {
	RequestMode string `json:"requestMode,omitempty"`
	Effort      string `json:"effort,omitempty"`
}

// Provider is a credential-safe view of a configured model provider.
// API keys are never represented in a response; HasAPIKey is informational only.
type Provider struct {
	Name      string                   `json:"name"`
	Type      string                   `json:"type"`
	Endpoint  string                   `json:"endpoint"`
	AuthType  string                   `json:"authType,omitempty"`
	HasAPIKey bool                     `json:"hasApiKey"`
	Models    map[string]ProviderModel `json:"models"`
}

// ProviderListResult is the current provider registry snapshot.
type ProviderListResult struct {
	Revision  string          `json:"revision"`
	Current   ProviderCurrent `json:"current"`
	Providers []Provider      `json:"providers"`
}

// ProviderGetParams selects one provider by registry name.
type ProviderGetParams struct {
	Name string `json:"name"`
}

// ProviderCurrentGetResult is the current provider/model selection snapshot.
type ProviderCurrentGetResult struct {
	Revision string          `json:"revision"`
	Current  ProviderCurrent `json:"current"`
}

// ProviderCurrentSetParams switches the daemon's current provider/model.
// Revision is the optimistic-concurrency key from a previous read.
type ProviderCurrentSetParams struct {
	Revision string `json:"revision"`
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// ProviderCurrentSetResult reports the committed selection and its new revision.
type ProviderCurrentSetResult struct {
	Revision string          `json:"revision"`
	Current  ProviderCurrent `json:"current"`
}

// ProviderGetResult is a credential-safe detail snapshot.
type ProviderGetResult struct {
	Revision string          `json:"revision"`
	Current  ProviderCurrent `json:"current"`
	Provider Provider        `json:"provider"`
}

// ProviderUpsertParams creates or replaces provider metadata and model settings.
// SecretAction is "keep", "replace", or "clear"; secret values are accepted only
// on input and are never included in a response.
type ProviderUpsertParams struct {
	Revision     string                   `json:"revision"`
	Name         string                   `json:"name"`
	Type         string                   `json:"type"`
	Endpoint     string                   `json:"endpoint"`
	AuthType     string                   `json:"authType,omitempty"`
	SecretAction string                   `json:"secretAction"`
	APIKey       string                   `json:"apiKey,omitempty"`
	Models       map[string]ProviderModel `json:"models"`
	Current      *ProviderCurrent         `json:"current,omitempty"`
}

// ProviderUpsertResult returns the committed revision and sanitized provider view.
type ProviderUpsertResult struct {
	Revision string          `json:"revision"`
	Current  ProviderCurrent `json:"current"`
	Provider Provider        `json:"provider"`
}

// ProviderRemoveParams identifies a provider and its expected registry revision.
type ProviderRemoveParams struct {
	Revision string `json:"revision"`
	Name     string `json:"name"`
}

// ProviderRemoveResult reports the committed registry revision.
type ProviderRemoveResult struct {
	Revision string `json:"revision"`
}
