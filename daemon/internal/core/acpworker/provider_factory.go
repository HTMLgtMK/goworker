package acpworker

import (
	"fmt"
	"net/http"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
	runtimeprovider "github.com/tinguo/goworker/ai-runtime/provider"
	runtimeanthropic "github.com/tinguo/goworker/ai-runtime/provider/anthropic"
	runtimeopenai "github.com/tinguo/goworker/ai-runtime/provider/openai"
)

// ProviderFactory 构造「按配置解析默认 provider」的工厂。
// daemon 的 agent 插件与 goworker acp worker 模式共用同一装配，
// 保证两种入口的协议适配行为一致。
func ProviderFactory(client *http.Client) func(*runtimeconfig.Config) (core.Provider, error) {
	return func(cfg *runtimeconfig.Config) (core.Provider, error) {
		provider, model, err := cfg.LLM.ResolveCurrent()
		if err != nil {
			return nil, err
		}
		switch provider.Type {
		case runtimeconfig.ProviderTypeOpenAI:
			return runtimeopenai.NewProvider(
				cfg.LLM.Current.Provider,
				provider.Endpoint,
				provider.APIKey,
				cfg.LLM.Current.Model,
				client,
				runtimeopenai.WithThinkingOptions(runtimeprovider.ThinkingOptions{
					RequestMode: model.Thinking.RequestMode,
					Effort:      model.Thinking.Effort,
				}),
			), nil

		case runtimeconfig.ProviderTypeAnthropic:
			return runtimeanthropic.NewProvider(
				cfg.LLM.Current.Provider,
				provider.Endpoint,
				provider.APIKey,
				cfg.LLM.Current.Model,
				client,
				runtimeanthropic.AnthropicAuthType(provider.AuthType),
				runtimeanthropic.WithAnthropicMaxTokens(model.MaxTokens),
			), nil
		default:
			return nil, fmt.Errorf("unsupported provider type %q", provider.Type)
		}
	}
}
