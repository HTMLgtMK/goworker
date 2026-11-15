package acp

import (
	"context"
	"encoding/json"
	"errors"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/providercontrol"
	xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"
)

// xgwProviderControlProvider is an optional composition hook. The session source may expose the
// daemon-wide provider control service without expanding the base ACP session contract.
type xgwProviderControlProvider interface {
	ProviderControl() *providercontrol.Service
}

func registerXGWProviderMethods(server *aiacp.Server, handler *FrontendHandler) {
	if server == nil || handler == nil {
		return
	}
	if _, err := handler.providerControl(); err != nil {
		return
	}
	handler.registerXGWMethod(server, xgwprotocol.ProviderMethodList, handler.xgwProviderList)
	handler.registerXGWMethod(server, xgwprotocol.ProviderMethodDetail, handler.xgwProviderDetail)
	handler.registerXGWMethod(server, xgwprotocol.ProviderMethodCurrentGet, handler.xgwProviderCurrentGet)
	if handler.providerWriteAllowed {
		handler.registerXGWMethod(server, xgwprotocol.ProviderMethodCurrentSet, handler.xgwProviderCurrentSet)
		handler.registerXGWMethod(server, xgwprotocol.ProviderMethodUpsert, handler.xgwProviderUpsert)
		handler.registerXGWMethod(server, xgwprotocol.ProviderMethodRemove, handler.xgwProviderRemove)
	}
}

func (h *FrontendHandler) xgwProviderList(_ context.Context, _ json.RawMessage) (any, error) {
	service, err := h.providerControl()
	if err != nil {
		return nil, err
	}
	snapshot, err := service.List()
	if err != nil {
		return nil, xgwProviderError(xgwprotocol.ProviderMethodList, err)
	}
	return providerListResult(snapshot), nil
}

func (h *FrontendHandler) xgwProviderDetail(_ context.Context, params json.RawMessage) (any, error) {
	var req xgwprotocol.ProviderGetParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params", xgwprotocol.ProviderMethodDetail)
	}
	service, err := h.providerControl()
	if err != nil {
		return nil, err
	}
	snapshot, provider, err := service.Detail(req.Name)
	if err != nil {
		return nil, xgwProviderError(xgwprotocol.ProviderMethodDetail, err)
	}
	return xgwprotocol.ProviderGetResult{
		Revision: snapshot.Revision,
		Current:  providerCurrent(snapshot.Current),
		Provider: providerDTO(provider),
	}, nil
}

func (h *FrontendHandler) xgwProviderUpsert(_ context.Context, params json.RawMessage) (any, error) {
	if err := h.requireProviderWrite(xgwprotocol.ProviderMethodUpsert); err != nil {
		return nil, err
	}
	var req xgwprotocol.ProviderUpsertParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params", xgwprotocol.ProviderMethodUpsert)
	}
	service, err := h.providerControl()
	if err != nil {
		return nil, err
	}
	models := make(map[string]providercontrol.Model, len(req.Models))
	for id, model := range req.Models {
		models[id] = providercontrol.Model{
			Name:          model.Name,
			ContextWindow: model.ContextWindow,
			RequestMode:   model.Thinking.RequestMode,
			Effort:        model.Thinking.Effort,
			MaxTokens:     model.MaxTokens,
		}
	}
	var current *providercontrol.Current
	if req.Current != nil {
		current = &providercontrol.Current{Provider: req.Current.Provider, Model: req.Current.Model}
	}
	snapshot, provider, err := service.Upsert(providercontrol.UpsertRequest{
		Revision:       req.Revision,
		Name:           req.Name,
		Type:           req.Type,
		Endpoint:       req.Endpoint,
		EndpointAction: req.EndpointAction,
		AuthType:       req.AuthType,
		SecretAction:   req.SecretAction,
		APIKey:         req.APIKey,
		Models:         models,
		Current:        current,
	})
	if err != nil {
		return nil, xgwProviderError(xgwprotocol.ProviderMethodUpsert, err)
	}
	return xgwprotocol.ProviderUpsertResult{
		Revision: snapshot.Revision,
		Current:  providerCurrent(snapshot.Current),
		Provider: providerDTO(provider),
	}, nil
}

func (h *FrontendHandler) xgwProviderCurrentGet(_ context.Context, _ json.RawMessage) (any, error) {
	service, err := h.providerControl()
	if err != nil {
		return nil, err
	}
	snapshot, err := service.Current()
	if err != nil {
		return nil, xgwProviderError(xgwprotocol.ProviderMethodCurrentGet, err)
	}
	return xgwprotocol.ProviderCurrentGetResult{
		Revision: snapshot.Revision,
		Current:  providerCurrent(snapshot.Current),
	}, nil
}

func (h *FrontendHandler) xgwProviderCurrentSet(_ context.Context, params json.RawMessage) (any, error) {
	if err := h.requireProviderWrite(xgwprotocol.ProviderMethodCurrentSet); err != nil {
		return nil, err
	}
	var req xgwprotocol.ProviderCurrentSetParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params", xgwprotocol.ProviderMethodCurrentSet)
	}
	service, err := h.providerControl()
	if err != nil {
		return nil, err
	}
	snapshot, err := service.SetCurrent(req.Revision, providercontrol.Current{Provider: req.Provider, Model: req.Model})
	if err != nil {
		return nil, xgwProviderError(xgwprotocol.ProviderMethodCurrentSet, err)
	}
	return xgwprotocol.ProviderCurrentSetResult{
		Revision: snapshot.Revision,
		Current:  providerCurrent(snapshot.Current),
	}, nil
}

func (h *FrontendHandler) xgwProviderRemove(_ context.Context, params json.RawMessage) (any, error) {
	if err := h.requireProviderWrite(xgwprotocol.ProviderMethodRemove); err != nil {
		return nil, err
	}
	var req xgwprotocol.ProviderRemoveParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, xgwError("%s: invalid params", xgwprotocol.ProviderMethodRemove)
	}
	service, err := h.providerControl()
	if err != nil {
		return nil, err
	}
	revision, err := service.Remove(req.Name, req.Revision)
	if err != nil {
		return nil, xgwProviderError(xgwprotocol.ProviderMethodRemove, err)
	}
	return xgwprotocol.ProviderRemoveResult{Revision: revision}, nil
}

func (h *FrontendHandler) requireProviderWrite(method string) error {
	if h == nil || !h.providerWriteAllowed {
		return xgwError("%s: provider write is not authorized for this connection", method)
	}
	return nil
}

func (h *FrontendHandler) providerControl() (*providercontrol.Service, error) {
	if h != nil && h.sessions != nil {
		if source, ok := h.sessions.(xgwProviderControlProvider); ok {
			if service := source.ProviderControl(); service != nil {
				return service, nil
			}
		}
	}
	return nil, xgwError("x-goworker: provider management unavailable")
}

func xgwProviderError(method string, err error) error {
	switch {
	case errors.Is(err, providercontrol.ErrRevisionConflict), errors.Is(err, model.ErrConfigRevisionConflict):
		return xgwError("%s: configuration revision conflict; reload providers and retry", method)
	case errors.Is(err, providercontrol.ErrNotFound):
		return xgwError("%s: provider not found", method)
	case errors.Is(err, providercontrol.ErrCurrentProvider):
		return xgwError("%s: cannot remove the current provider", method)
	case errors.Is(err, providercontrol.ErrInvalid):
		return xgwError("%s: invalid provider configuration", method)
	case errors.Is(err, providercontrol.ErrUnavailable):
		return xgwError("%s: provider management unavailable", method)
	default:
		// Backend errors may contain filesystem paths or URL values; do not send them to clients.
		return xgwError("%s: provider operation failed", method)
	}
}

func providerListResult(snapshot providercontrol.Snapshot) xgwprotocol.ProviderListResult {
	providers := make([]xgwprotocol.Provider, 0, len(snapshot.Providers))
	for _, provider := range snapshot.Providers {
		providers = append(providers, providerDTO(provider))
	}
	return xgwprotocol.ProviderListResult{
		Revision:  snapshot.Revision,
		Current:   providerCurrent(snapshot.Current),
		Providers: providers,
	}
}

func providerCurrent(current providercontrol.Current) xgwprotocol.ProviderCurrent {
	return xgwprotocol.ProviderCurrent{Provider: current.Provider, Model: current.Model}
}

func providerDTO(provider providercontrol.ProviderView) xgwprotocol.Provider {
	models := make(map[string]xgwprotocol.ProviderModel, len(provider.Models))
	for id, model := range provider.Models {
		models[id] = xgwprotocol.ProviderModel{
			Name:          model.Name,
			ContextWindow: model.ContextWindow,
			Thinking: xgwprotocol.ProviderThinkingConfig{
				RequestMode: model.RequestMode,
				Effort:      model.Effort,
			},
			MaxTokens: model.MaxTokens,
		}
	}
	return xgwprotocol.Provider{
		Name:      provider.Name,
		Type:      provider.Type,
		Endpoint:  provider.Endpoint,
		AuthType:  provider.AuthType,
		HasAPIKey: provider.HasAPIKey,
		Models:    models,
	}
}
