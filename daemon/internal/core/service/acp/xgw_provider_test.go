package acp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	appconfig "github.com/tinguo/goworker/daemon/internal/app/config"
	"github.com/tinguo/goworker/daemon/internal/core/engine"
	"github.com/tinguo/goworker/daemon/internal/core/model"
	agentservice "github.com/tinguo/goworker/daemon/internal/plugins/agent"
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
	return &FrontendHandler{sessions: source, providerWriteAllowed: true}, backend
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
	if got.Provider.Endpoint != "https://api.example.test/%5Bredacted%5D" || got.Provider.Models["gpt-4o-mini"].ContextWindow != 128000 {
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
	h := &FrontendHandler{
		sessions: &providerControlSessionSource{
			fakeSessionSource: &fakeSessionSource{},
			service:           providercontrol.New(backend),
		},
		providerWriteAllowed: true,
	}
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

func TestUnauthorizedACPPromptCannotChangeDaemonProviderConfig(t *testing.T) {
	t.Setenv("GOWORKER_CONFIG_DIR", t.TempDir())
	cfg := appconfig.Default()
	cfg.Memory.Enabled = false
	cfg.Session.Enabled = true
	cfg.Session.Dir = filepath.Join(t.TempDir(), "sessions")
	e := engine.NewEngine(cfg, cfg.ToRuntime(), nil)
	if err := e.Register(agentservice.NewPlugin(cfg.ToRuntime(), runtimeconfig.Paths{ConfigDir: t.TempDir()})); err != nil {
		t.Fatalf("register agent plugin: %v", err)
	}
	e.RegisterBuiltinCommands()
	t.Cleanup(e.StopAll)
	source, ok := e.Capability(model.SessionSourceCapability)
	if !ok {
		t.Fatal("agent plugin did not register session source")
	}
	sessions, ok := source.(model.SessionSource)
	if !ok {
		t.Fatalf("session capability has type %T", source)
	}

	newClient := func(allowed bool) (*aiacp.Client, string) {
		t.Helper()
		serverConn, clientConn := net.Pipe()
		handler := NewFrontendHandlerFactory(e.Eval, e.Commands, sessions, NewSessionGate(), func(string) bool { return allowed })().(*FrontendHandler)
		server, err := aiacp.NewServer(serverConn, handler)
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		server.Start()
		t.Cleanup(func() { _ = server.Close() })
		client, err := aiacp.NewClient("command-auth-test", clientConn, nil)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if _, err := client.Initialize(context.Background()); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		sessionID, err := client.NewSession(context.Background(), t.TempDir())
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		return client, sessionID
	}
	prompt := func(client *aiacp.Client, sessionID, input string) error {
		t.Helper()
		_, err := client.Prompt(context.Background(), sessionID, []protocol.ContentBlock{{Type: "text", Text: input}})
		return err
	}

	before := e.ConfigSnapshot().LLM.Providers["openai"].Endpoint
	unauthorized, unauthorizedSession := newClient(false)
	for _, input := range []string{
		"/model set endpoint=https://attacker.example/changed",
		"/model use openai",
		"/config set llm.providers.openai.endpoint=https://attacker.example/changed",
		"/cfg set llm.providers.openai.endpoint=https://attacker.example/changed",
	} {
		if err := prompt(unauthorized, unauthorizedSession, input); err == nil {
			t.Errorf("unauthorized ACP prompt %q unexpectedly succeeded", input)
		}
		if got := e.ConfigSnapshot().LLM.Providers["openai"].Endpoint; got != before {
			t.Fatalf("unauthorized ACP prompt %q changed endpoint from %q to %q", input, before, got)
		}
	}
	// The session was created/bound on this connection, but a session binding is not a grant.
	if err := prompt(unauthorized, unauthorizedSession, "/model"); err != nil {
		t.Fatalf("unauthorized ACP model read: %v", err)
	}
	if err := prompt(unauthorized, unauthorizedSession, "/config"); err != nil {
		t.Fatalf("unauthorized ACP config read: %v", err)
	}

	authorized, authorizedSession := newClient(true)
	if err := prompt(authorized, authorizedSession, "/model set endpoint=https://trusted.example/new-route"); err != nil {
		t.Fatalf("authorized /model set: %v", err)
	}
	if got := e.ConfigSnapshot().LLM.Providers["openai"].Endpoint; got != "https://trusted.example/new-route" {
		t.Fatalf("authorized /model set endpoint = %q", got)
	}
	if err := prompt(authorized, authorizedSession, "/config set llm.providers.openai.endpoint=https://trusted.example/config-route"); err != nil {
		t.Fatalf("authorized /config set: %v", err)
	}
	if got := e.ConfigSnapshot().LLM.Providers["openai"].Endpoint; got != "https://trusted.example/config-route" {
		t.Fatalf("authorized /config set endpoint = %q", got)
	}
}

func TestProviderWriteAuthorizationIsConnectionScopedAndDiscoverable(t *testing.T) {
	t.Run("unknown connection is denied", func(t *testing.T) {
		stack, backend := newProviderWireStack(t, nil)
		if _, err := stack.client.Initialize(context.Background()); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		methods := providerWireMethods(t, stack.client)
		for _, method := range []string{
			xgwprotocol.ProviderMethodList,
			xgwprotocol.ProviderMethodDetail,
			xgwprotocol.ProviderMethodCurrentGet,
		} {
			if !strings.Contains(strings.Join(methods, "|"), method) {
				t.Errorf("authorized read method %q missing from discovery: %v", method, methods)
			}
		}
		for _, method := range []string{
			xgwprotocol.ProviderMethodCurrentSet,
			xgwprotocol.ProviderMethodUpsert,
			xgwprotocol.ProviderMethodRemove,
		} {
			if strings.Contains(strings.Join(methods, "|"), method) {
				t.Errorf("unauthorized write method %q advertised: %v", method, methods)
			}
			var result json.RawMessage
			if err := stack.client.Call(context.Background(), method, map[string]any{}, &result); err == nil {
				t.Errorf("unauthorized method %q unexpectedly succeeded", method)
			}
		}
		if backend.revision != "epoch-1" {
			t.Fatalf("unauthorized calls mutated provider config; revision = %q", backend.revision)
		}

		// Registration is not the only guard: direct in-process dispatch of each
		// mutator must still fail closed on an unprivileged handler.
		for _, invoke := range []func() error{
			func() error {
				_, err := stack.handler.xgwProviderCurrentSet(context.Background(), mustProviderJSON(t, xgwprotocol.ProviderCurrentSetParams{Revision: "epoch-1", Provider: "openai", Model: "gpt-4o"}))
				return err
			},
			func() error {
				_, err := stack.handler.xgwProviderUpsert(context.Background(), mustProviderJSON(t, xgwprotocol.ProviderUpsertParams{Revision: "epoch-1"}))
				return err
			},
			func() error {
				_, err := stack.handler.xgwProviderRemove(context.Background(), mustProviderJSON(t, xgwprotocol.ProviderRemoveParams{Revision: "epoch-1", Name: "openai"}))
				return err
			},
		} {
			if err := invoke(); err == nil || !strings.Contains(err.Error(), "not authorized") {
				t.Errorf("direct unauthorized handler call error = %v, want authorization denial", err)
			}
		}
		if backend.revision != "epoch-1" {
			t.Fatalf("direct unauthorized calls mutated provider config; revision = %q", backend.revision)
		}
	})

	t.Run("explicitly authorized connection can mutate", func(t *testing.T) {
		var authorizedConnection string
		stack, backend := newProviderWireStack(t, func(connectionID string) bool {
			authorizedConnection = connectionID
			return connectionID != ""
		})
		if _, err := stack.client.Initialize(context.Background()); err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		if authorizedConnection == "" || authorizedConnection != stack.handler.connectionID {
			t.Fatalf("authorizer connection ID = %q, handler ID = %q", authorizedConnection, stack.handler.connectionID)
		}
		methods := providerWireMethods(t, stack.client)
		for _, method := range []string{
			xgwprotocol.ProviderMethodCurrentSet,
			xgwprotocol.ProviderMethodUpsert,
			xgwprotocol.ProviderMethodRemove,
		} {
			if !strings.Contains(strings.Join(methods, "|"), method) {
				t.Fatalf("authorized write method %q missing from discovery: %v", method, methods)
			}
		}

		var setResult xgwprotocol.ProviderCurrentSetResult
		if err := stack.client.Call(context.Background(), xgwprotocol.ProviderMethodCurrentSet,
			xgwprotocol.ProviderCurrentSetParams{Revision: backend.revision, Provider: "openai", Model: "gpt-4o"}, &setResult); err != nil {
			t.Fatalf("authorized current/set: %v", err)
		}
		var upsertResult xgwprotocol.ProviderUpsertResult
		if err := stack.client.Call(context.Background(), xgwprotocol.ProviderMethodUpsert,
			xgwprotocol.ProviderUpsertParams{
				Revision: backend.revision, Name: "secondary", Type: "openai",
				Endpoint: "https://api.example.test/v1", SecretAction: "keep",
				Models: map[string]xgwprotocol.ProviderModel{
					"model": {ContextWindow: 4096, Thinking: xgwprotocol.ProviderThinkingConfig{RequestMode: "auto", Effort: "medium"}},
				},
			}, &upsertResult); err != nil {
			t.Fatalf("authorized provider/upsert: %v", err)
		}
		var removeResult xgwprotocol.ProviderRemoveResult
		if err := stack.client.Call(context.Background(), xgwprotocol.ProviderMethodRemove,
			xgwprotocol.ProviderRemoveParams{Revision: backend.revision, Name: "secondary"}, &removeResult); err != nil {
			t.Fatalf("authorized provider/remove: %v", err)
		}
		if backend.revision == "epoch-1" {
			t.Fatalf("expected authorized mutations to commit, revision=%q", backend.revision)
		}
		if _, exists := backend.cfg.LLM.Providers["secondary"]; exists {
			t.Fatal("provider/remove left the secondary provider in the registry")
		}
	})
}

type providerWireStack struct {
	client  *aiacp.Client
	handler *FrontendHandler
}

func newProviderWireStack(t *testing.T, authorizer ProviderWriteAuthorizer) (*providerWireStack, *acpProviderBackend) {
	t.Helper()
	base, backend := newProviderControlHandler()
	serverConn, clientConn := net.Pipe()
	handler := NewFrontendHandlerFactory(nil, nil, base.sessions, NewSessionGate(), authorizer)().(*FrontendHandler)
	server, err := aiacp.NewServer(serverConn, handler, aiacp.WithAgentCapability(xgwprotocol.XGWorkerCapability{}))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	registerXGWMethods(server, handler)
	server.Start()
	t.Cleanup(func() { _ = server.Close() })
	client, err := aiacp.NewClient("provider-auth-test", clientConn, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return &providerWireStack{client: client, handler: handler}, backend
}

func providerWireMethods(t *testing.T, client *aiacp.Client) []string {
	t.Helper()
	var result xgwprotocol.CapabilitiesResult
	if err := client.Call(context.Background(), xgwprotocol.MethodCapabilities, nil, &result); err != nil {
		t.Fatalf("x-goworker/capabilities: %v", err)
	}
	return result.Methods
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
