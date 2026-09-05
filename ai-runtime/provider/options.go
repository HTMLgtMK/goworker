package provider

import (
	runtimeconfig "github.com/tinguo/goworker/ai-runtime/config"
)

type ThinkingOptions struct {
	RequestMode runtimeconfig.ThinkingRequestMode
	Effort      runtimeconfig.ThinkingEffort
}
