package provider

import (
	"net/http"
)

type BaseProvider struct {
	name     string
	endpoint string
	apiKey   string
	model    string
	client   *http.Client
}

func NewBaseProvider(name, endpoint, apiKey, model string, client *http.Client) *BaseProvider {
	if client == nil {
		panic("BaseProvider requires a non-nil http.Client")
	}

	return &BaseProvider{
		name:     name,
		endpoint: endpoint,
		apiKey:   apiKey,
		model:    model,
		client:   client,
	}
}

func (p *BaseProvider) Name() string     { return p.name }
func (p *BaseProvider) Endpoint() string { return p.endpoint }
func (p *BaseProvider) ApiKey() string   { return p.apiKey }
func (p *BaseProvider) Model() string    { return p.model }
func (p *BaseProvider) Client() *http.Client {
	if p.client == nil {
		panic("BaseProvider requires a non-nil http.Client")
	}
	return p.client
}
