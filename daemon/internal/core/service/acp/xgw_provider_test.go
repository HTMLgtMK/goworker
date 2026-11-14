package acp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/engine"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	"github.com/tinguo/goworker/daemon/internal/providercontrol"
	xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"
)

type providerControlSessionSource struct {
	*fakeSessionSource
	service *providercontrol.Service
}

func (s *providerControlSessionSource) ProviderControl() *providercontrol.Service { return s.service }

var _ model.SessionSource = (*providerControlSessionSource)(nil)

type acpProviderBackend struct {
	cfg      *runtimeconfig.Config
	revision string
}

func (b *acpProviderBackend) ConfigSnapshotWithRevision() (*runtimeconfig.Config, string) {
	copy := *b.cfg
	copy.LLM = b.cfg.LLM.Clone()
	return &copy, b.revision
}

func (b *acpProviderBackend) UpdateConfigAtRevision(expected string, mutate func(*runtimeconfig.Config) error) (*runtimeconfig.Config, string, error) {
	if expected != b.revision {
		return nil, b.revision, providercontrol.ErrRevisionConflict
	}
	candidate := *b.cfg
	candidate.LLM = b.cfg.LLM.Clone()
	if err := mutate(&candidate); err != nil {
		return nil, b.revision, err
	}
	b.cfg = &candidate
	b.revision += "-next"
	updated, revision := b.ConfigSnapshotWithRevision()
	return updated, revision, nil
}

func newProviderControlHandler() (*FrontendHandler, *acpProviderBackend) {
	backend := &acpProviderBackend{
		cfg:      &runtimeconfig.Config{LLM: runtimeconfig.DefaultLLM()},
		revision: "epoch-1",
	}
	service := providercontrol.New(backend)
	source := &providerControlSessionSource{fakeSessionSource: &fakeSessionSource{}, service: service}
	return &FrontendHandler{sessions: source}, backend
}

func TestXGWProviderHandlersCRUDAndCurrentSelection(t *testing.T) {
	h, backend := newProviderControlHandler()
	ctx := context.Background()
	listValue, err := h.xgwProviderList(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	list := listValue.(xgwprotocol.ProviderListResult)
	if list.Revision != "epoch-1" || len(list.Providers) != 1 || list.Current.Model != "gpt-4o" {
		t.Fatalf("unexpected provider list: %#v", list)
	}

	provider := backend.cfg.LLM.Providers["openai"]
	provider.APIKey = "top-secret"
	provider.Endpoint = "https://user:pass@example.test/v1?access_token=top-secret"
	backend.cfg.LLM.Providers["openai"] = provider

	listValue, err = h.xgwProviderList(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(listValue)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "top-secret") || strings.Contains(string(encoded), "user:pass") {
		t.Fatalf("provider list leaked secret material: %s", encoded)
	}

	upsert := xgwprotocol.ProviderUpsertParams{
		Revision:     "epoch-1",
		Name:         "openai",
		Type:         string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint:     "https://api.example.test/v1",
		SecretAction: "keep",
		Models: map[string]xgwprotocol.ProviderModel{
			"gpt-4o-mini": {
				ContextWindow: 128000,
				Thinking: xgwprotocol.ProviderThinkingConfig{
					RequestMode: string(runtimeconfig.ThinkingRequestAuto),
					Effort:      string(runtimeconfig.ThinkingEffortMedium),
				},
			},
		},
		Current: &xgwprotocol.ProviderCurrent{Provider: "openai", Model: "gpt-4o-mini"},
	}
	params, _ := json.Marshal(upsert)
	upsertValue, err := h.xgwProviderUpsert(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	upsertResult := upsertValue.(xgwprotocol.ProviderUpsertResult)
	if upsertResult.Current.Model != "gpt-4o-mini" || upsertResult.Revision != "epoch-1-next" || !upsertResult.Provider.HasAPIKey {
		t.Fatalf("upsert response missing committed view: %#v", upsertResult)
	}
	upsertJSON, _ := json.Marshal(upsertResult)
	if strings.Contains(string(upsertJSON), "top-secret") {
		t.Fatalf("upsert response leaked key: %s", upsertJSON)
	}

	getParams, _ := json.Marshal(xgwprotocol.ProviderGetParams{Name: "openai"})
	gotValue, err := h.xgwProviderDetail(ctx, getParams)
	if err != nil {
		t.Fatal(err)
	}
	got := gotValue.(xgwprotocol.ProviderGetResult)
	if got.Provider.Endpoint != "https://api.example.test/v1" || got.Provider.Models["gpt-4o-mini"].ContextWindow != 128000 {
		t.Fatalf("detail did not reflect sanitized committed config: %#v", got)
	}

	removeParams, _ := json.Marshal(xgwprotocol.ProviderRemoveParams{Name: "openai", Revision: upsertResult.Revision})
	if _, err := h.xgwProviderRemove(ctx, removeParams); err == nil || !strings.Contains(err.Error(), "cannot remove the current provider") {
		t.Fatalf("removing current provider should fail explicitly: %v", err)
	}
}

func TestXGWProviderRealEngineRevisionConflict(t *testing.T) {
	cfg := appconfig.Default()
	backend := engine.NewEngine(cfg, cfg.ToRuntime(), nil)
	h := &FrontendHandler{sessions: &providerControlSessionSource{
		fakeSessionSource: &fakeSessionSource{},
		service:           providercontrol.New(backend),
	}}
	params := mustProviderJSON(t, xgwprotocol.ProviderCurrentSetParams{
		Revision: "stale", Provider: "openai", Model: "gpt-4o",
	})
	_, err := h.xgwProviderCurrentSet(context.Background(), params)
	if err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("real engine stale revision = %v, want revision conflict", err)
	}
}

func TestXGWProviderHandlersRejectStaleRevisionWithoutLeakingBackendError(t *testing.T) {
	h, _ := newProviderControlHandler()
	params, _ := json.Marshal(xgwprotocol.ProviderUpsertParams{
		Revision: "old-epoch",
		Name:     "other",
		Type:     string(runtimeconfig.ProviderTypeOpenAI),
		Endpoint: "https://api.example.test/v1",
		Models: map[string]xgwprotocol.ProviderModel{
			"model": {ContextWindow: 1024, Thinking: xgwprotocol.ProviderThinkingConfig{
				RequestMode: string(runtimeconfig.ThinkingRequestAuto),
				Effort:      string(runtimeconfig.ThinkingEffortMedium),
			}},
		},
	})
	_, err := h.xgwProviderUpsert(context.Background(), params)
	if err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("stale revision error = %v, want conflict", err)
	}
	if errors.Is(err, providercontrol.ErrRevisionConflict) {
		t.Fatal("wire adapter must return a sanitized RPC error, not raw backend sentinel")
	}
}

func TestProviderMethodsAreNotAdvertisedWithoutInjectedService(t *testing.T) {
	h := &FrontendHandler{sessions: &fakeSessionSource{}}
	if _, err := h.providerControl(); err == nil {
		t.Fatal("provider methods must not be registered without an injected service")
	}
}

func TestXGWProviderCurrentGetAndSet(t *testing.T) {
	h, backend := newProviderControlHandler()
	ctx := context.Background()

	// GET：无参数读当前选择，带 revision。
	gotValue, err := h.xgwProviderCurrentGet(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := gotValue.(xgwprotocol.ProviderCurrentGetResult)
	if got.Revision != "epoch-1" || got.Current.Provider != "openai" || got.Current.Model != "gpt-4o" {
		t.Fatalf("current get = %#v", got)
	}

	// SET：切换到已注册模型，revision 乐观并发。
	provider := backend.cfg.LLM.Providers["openai"]
	provider.Models["gpt-4o-mini"] = runtimeconfig.ModelConfig{ContextWindow: 128000}
	backend.cfg.LLM.Providers["openai"] = provider

	setValue, err := h.xgwProviderCurrentSet(ctx, mustProviderJSON(t, xgwprotocol.ProviderCurrentSetParams{
		Revision: "epoch-1",
		Provider: "openai",
		Model:    "gpt-4o-mini",
	}))
	if err != nil {
		t.Fatal(err)
	}
	set := setValue.(xgwprotocol.ProviderCurrentSetResult)
	if set.Revision != "epoch-1-next" || set.Current.Model != "gpt-4o-mini" {
		t.Fatalf("current set = %#v", set)
	}
	if backend.cfg.LLM.Current.Model != "gpt-4o-mini" {
		t.Fatalf("backend selection not switched: %+v", backend.cfg.LLM.Current)
	}

	// SET 校验：未知模型 -32000；过期 revision 冲突；字段缺失 -32000。
	if _, err := h.xgwProviderCurrentSet(ctx, mustProviderJSON(t, xgwprotocol.ProviderCurrentSetParams{
		Revision: set.Revision,
		Provider: "openai",
		Model:    "ghost-model",
	})); err == nil || !strings.Contains(err.Error(), "invalid provider configuration") {
		t.Fatalf("unknown model err = %v", err)
	}
	if _, err := h.xgwProviderCurrentSet(ctx, mustProviderJSON(t, xgwprotocol.ProviderCurrentSetParams{
		Revision: "stale",
		Provider: "openai",
		Model:    "gpt-4o",
	})); err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("stale revision err = %v", err)
	}
	if _, err := h.xgwProviderCurrentSet(ctx, mustProviderJSON(t, xgwprotocol.ProviderCurrentSetParams{
		Revision: set.Revision,
	})); err == nil {
		t.Fatal("missing provider/model should be rejected")
	}
}

func mustProviderJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestXGWProviderModelNameFlowsThroughWire(t *testing.T) {
	h, _ := newProviderControlHandler()
	ctx := context.Background()

	params := mustProviderJSON(t, xgwprotocol.ProviderUpsertParams{
		Revision:     "epoch-1",
		Name:         "openai",
		Type:         "openai",
		Endpoint:     "https://api.example.test/v1",
		SecretAction: "keep",
		Models: map[string]xgwprotocol.ProviderModel{
			"gpt-4o": {Name: "GPT-4o", ContextWindow: 256000,
				Thinking: xgwprotocol.ProviderThinkingConfig{RequestMode: "auto", Effort: "medium"}},
		},
	})
	value, err := h.xgwProviderUpsert(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Provider struct {
			Models map[string]struct {
				Name string `json:"name"`
			} `json:"models"`
		} `json:"provider"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.Provider.Models["gpt-4o"].Name; got != "GPT-4o" {
		t.Fatalf("wire model name = %q, want GPT-4o", got)
	}
}
