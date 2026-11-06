package service

import (
	"fmt"
	"strings"

	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// providers.go 实现 model.ProviderManageSource（x-goworker/provider/detail|
// upsert|remove 的数据面，UI 管理页后端）。配置变更全部走 handleModel 同款的
// candidate-clone + LLM.Validate + hub.SaveConfig（写盘 + 引擎内存同步，运行中
// 即时生效）；thinking 字段与表单无关，更新路径原样保留。
//
// APIKey 纪律：ProviderDetail 只出 APIKeySet 布尔，密钥永不经 wire 回传；
// ProviderUpsert 的空 APIKey = 保留原值（表单"留空不变"语义）。

// ProviderDetail 实现 model.ProviderManageSource：单 provider 全量快照，
// APIKey 只给 APIKeySet 布尔。
func (p *AgentPlugin) ProviderDetail(id string) (*model.ProviderDetail, error) {
	if p.cfg == nil {
		return nil, fmt.Errorf("provider 配置不可用")
	}
	provider, ok := p.cfg.LLM.Providers[id]
	if !ok {
		return nil, fmt.Errorf("provider %q 不存在", id)
	}
	return providerDetailOf(id, provider, id == p.cfg.LLM.DefaultProvider), nil
}

func providerDetailOf(id string, provider runtimeconfig.ProviderConfig, isDefault bool) *model.ProviderDetail {
	return &model.ProviderDetail{
		ID:            id,
		Name:          id,
		Type:          string(provider.Type),
		Endpoint:      provider.Endpoint,
		APIKeySet:     provider.APIKey != "",
		Models:        provider.ModelCatalog(),
		ContextWindow: provider.ContextWindow,
		MaxTokens:     provider.MaxTokens,
		AuthType:      string(provider.AuthType),
		RequestMode:   string(provider.Thinking.RequestMode),
		Effort:        string(provider.Thinking.Effort),
		IsDefault:     isDefault,
	}
}

// ProviderUpsert 实现 model.ProviderManageSource：candidate-clone + Validate +
// SaveConfig。更新 = merge（APIKey/ContextWindow/MaxTokens/AuthType 零值保留，
// thinking 不触碰）；新建 = 全量构造（Name 即 map key；openai thinking 缺省由
// WithDefaults 补齐，anthropic auth_type 缺省 bearer、max_tokens 必填）。返回
// 落盘后的 provider id。
func (p *AgentPlugin) ProviderUpsert(req model.ProviderUpsertRequest) (string, error) {
	if p.cfg == nil {
		return "", fmt.Errorf("provider 配置不可用")
	}
	candidate := *p.cfg
	candidate.LLM = p.cfg.LLM.Clone()
	if candidate.LLM.Providers == nil {
		candidate.LLM.Providers = map[string]runtimeconfig.ProviderConfig{}
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		// 新建：Name 即 map key。
		id = strings.TrimSpace(req.Name)
		if !runtimeconfig.ValidProviderName(id) {
			return "", fmt.Errorf("无效 provider 名 %q（可用字符：字母/数字/-/_，不含点）", id)
		}
		if _, exists := candidate.LLM.Providers[id]; exists {
			return "", fmt.Errorf("provider %q 已存在", id)
		}
		provider := runtimeconfig.ProviderConfig{
			Type:     runtimeconfig.ProviderType(strings.TrimSpace(req.Type)),
			Endpoint: strings.TrimSpace(req.Endpoint),
			APIKey:   req.APIKey,
			Models:   normalizeModelList(req.Models),
		}
		if provider.ContextWindow = req.ContextWindow; provider.ContextWindow <= 0 {
			provider.ContextWindow = 256000
		}
		provider.MaxTokens = req.MaxTokens
		// Validate 内部的 WithDefaults 不回写 candidate——落盘前显式补 Model
		//（models-only 新建 → 目录头），否则 SessionModes 显示 "名 ()"。
		if provider.Model == "" {
			if catalog := provider.ModelCatalog(); len(catalog) > 0 {
				provider.Model = catalog[0]
			}
		}
		if provider.Type == runtimeconfig.ProviderTypeAnthropic {
			provider.AuthType = runtimeconfig.AnthropicAuthType(strings.TrimSpace(req.AuthType))
			if provider.AuthType == "" {
				provider.AuthType = runtimeconfig.AnthropicAuthBearer
			}
			if provider.MaxTokens <= 0 {
				return "", fmt.Errorf("anthropic provider 需要 max_tokens（正整数）")
			}
		}
		candidate.LLM.Providers[id] = provider
		if err := candidate.LLM.Validate(); err != nil {
			return "", err
		}
		if err := p.hub.SaveConfig(&candidate); err != nil {
			return "", err
		}
		return id, nil
	}
	// 更新：merge 进现有配置（表单必填面覆盖，可选零值保留）。
	provider, ok := candidate.LLM.Providers[id]
	if !ok {
		return "", fmt.Errorf("provider %q 不存在", id)
	}
	if endpoint := strings.TrimSpace(req.Endpoint); endpoint != "" {
		provider.Endpoint = endpoint
	}
	if providerType := strings.TrimSpace(req.Type); providerType != "" {
		provider.Type = runtimeconfig.ProviderType(providerType)
	}
	provider.Models = normalizeModelList(req.Models)
	if req.APIKey != "" {
		provider.APIKey = req.APIKey
	}
	if req.ContextWindow > 0 {
		provider.ContextWindow = req.ContextWindow
	}
	if req.MaxTokens > 0 {
		provider.MaxTokens = req.MaxTokens
	}
	if authType := strings.TrimSpace(req.AuthType); authType != "" {
		provider.AuthType = runtimeconfig.AnthropicAuthType(authType)
	}
	candidate.LLM.Providers[id] = provider
	if err := candidate.LLM.Validate(); err != nil {
		return "", err
	}
	if err := p.hub.SaveConfig(&candidate); err != nil {
		return "", err
	}
	return id, nil
}

// ProviderRemove 实现 model.ProviderManageSource：默认 provider 与最后一个
// provider 不可删（Validate 要求 default_provider 存在，这里前置给明确文案）。
func (p *AgentPlugin) ProviderRemove(id string) error {
	if p.cfg == nil {
		return fmt.Errorf("provider 配置不可用")
	}
	if _, ok := p.cfg.LLM.Providers[id]; !ok {
		return fmt.Errorf("provider %q 不存在", id)
	}
	if id == p.cfg.LLM.DefaultProvider {
		return fmt.Errorf("provider %q 是默认 provider，先切换默认后才能删除", id)
	}
	if len(p.cfg.LLM.Providers) <= 1 {
		return fmt.Errorf("至少保留一个 provider")
	}
	candidate := *p.cfg
	candidate.LLM = p.cfg.LLM.Clone()
	delete(candidate.LLM.Providers, id)
	if err := candidate.LLM.Validate(); err != nil {
		return err
	}
	return p.hub.SaveConfig(&candidate)
}

// normalizeModelList 去空白、丢空行、保序去重——表单的 models 文本行直接进来。
func normalizeModelList(models []string) []string {
	seen := make(map[string]bool, len(models))
	out := make([]string, 0, len(models))
	for _, m := range models {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	return out
}
